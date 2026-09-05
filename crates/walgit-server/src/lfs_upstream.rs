//! Read-through LFS upstream (`upstream.lfs`, per repo via D24 settings).
//!
//! A repository imported from GitHub keeps its LFS history in GitHub's LFS
//! server; the import never copies it. Instead of a sync job, the batch API
//! asks the upstream for the objects this store lacks (one batch call per
//! request, bounded), and `GET objects/<oid>` streams the bytes through while
//! tee-ing them into the store (persisted only after a complete,
//! sha256-verified read). Pushes through walgit upload straight into our store,
//! so the upstream only ever serves history.
//!
//! Token: `upstream.token_env` names an environment variable on the maintaining
//! host holding the token (settings are published to the bucket, so never the
//! token itself); sent as HTTP Basic `x-access-token:<token>` (GitHub's LFS
//! endpoint). Unset = unauthenticated upstream (tests).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use base64::Engine;
use bytes::Bytes;
use futures::StreamExt;
use serde::{Deserialize, Serialize};

/// Upstream batch calls are bounded: a slow upstream must not hold a push.
pub const BATCH_TIMEOUT: Duration = Duration::from_secs(10);

/// What the upstream told us about one object it has.
#[derive(Debug, Clone)]
pub struct UpstreamObject {
    pub oid: String,
    pub size: u64,
    pub href: String,
    pub header: HashMap<String, String>,
}

#[derive(Serialize)]
struct BatchReq<'a> {
    operation: &'a str,
    transfers: [&'a str; 1],
    objects: Vec<BatchReqObj<'a>>,
}
#[derive(Serialize)]
struct BatchReqObj<'a> {
    oid: &'a str,
    size: u64,
}
#[derive(Deserialize)]
struct BatchResp {
    objects: Vec<BatchRespObj>,
}
#[derive(Deserialize)]
struct BatchRespObj {
    oid: String,
    #[serde(default)]
    size: u64,
    #[serde(default)]
    actions: Option<BatchRespActions>,
}
#[derive(Deserialize)]
struct BatchRespActions {
    download: Option<BatchRespAction>,
}
#[derive(Deserialize)]
struct BatchRespAction {
    href: String,
    #[serde(default)]
    header: HashMap<String, String>,
}

#[derive(Clone)]
pub struct Upstream {
    client: reqwest::Client,
}

impl Default for Upstream {
    fn default() -> Self {
        Self::new()
    }
}

impl Upstream {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .build()
                .expect("reqwest client"),
        }
    }

    /// Ask the upstream which of `objects` it can serve (`operation=download`).
    /// Returns only the objects it has; any failure is logged and yields an
    /// empty map (the caller then behaves as if there were no upstream).
    #[tracing::instrument(name = "lfs.upstream.batch", skip_all, fields(upstream = %upstream, asked = objects.len(), found = tracing::field::Empty))]
    pub async fn batch(
        &self,
        upstream: &str,
        token_env: Option<&str>,
        objects: &[(String, u64)],
    ) -> HashMap<String, UpstreamObject> {
        let started = Instant::now();
        let result = self.batch_inner(upstream, token_env, objects).await;
        let (found, outcome) = match &result {
            Ok(m) => (m.len(), "ok"),
            Err(_) => (0, "error"),
        };
        tracing::Span::current().record("found", found);
        metrics::counter!("walgit_lfs_upstream_total", "op" => "batch", "result" => outcome)
            .increment(1);
        match result {
            Ok(m) => m,
            Err(error) => {
                tracing::warn!(%error, elapsed_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX), "lfs upstream batch failed; treating as absent");
                HashMap::new()
            }
        }
    }

    async fn batch_inner(
        &self,
        upstream: &str,
        token_env: Option<&str>,
        objects: &[(String, u64)],
    ) -> anyhow::Result<HashMap<String, UpstreamObject>> {
        if objects.is_empty() {
            return Ok(HashMap::new());
        }
        let body = BatchReq {
            operation: "download",
            transfers: ["basic"],
            objects: objects
                .iter()
                .map(|(oid, size)| BatchReqObj { oid, size: *size })
                .collect(),
        };
        let mut req = self
            .client
            .post(format!("{upstream}/objects/batch"))
            .timeout(BATCH_TIMEOUT)
            .header("Accept", "application/vnd.git-lfs+json")
            .header("Content-Type", "application/vnd.git-lfs+json")
            .json(&body);
        if let Some(secret) = token_env {
            let token = self.secret(secret).await?;
            let basic =
                base64::engine::general_purpose::STANDARD.encode(format!("x-access-token:{token}"));
            req = req.header("Authorization", format!("Basic {basic}"));
        }
        let resp = req.send().await?;
        let status = resp.status();
        anyhow::ensure!(status.is_success(), "upstream batch: HTTP {status}");
        let parsed: BatchResp = resp.json().await?;
        let asked: HashMap<&str, u64> = objects.iter().map(|(o, s)| (o.as_str(), *s)).collect();
        let mut out = HashMap::new();
        for o in parsed.objects {
            let Some(dl) = o.actions.and_then(|a| a.download) else {
                continue;
            };
            if !asked.contains_key(o.oid.as_str()) {
                continue;
            }
            out.insert(
                o.oid.clone(),
                UpstreamObject {
                    size: if o.size > 0 {
                        o.size
                    } else {
                        asked[o.oid.as_str()]
                    },
                    oid: o.oid,
                    href: dl.href,
                    header: dl.header,
                },
            );
        }
        Ok(out)
    }

    /// Open the object's bytes at the upstream: `(content length, stream)`.
    #[tracing::instrument(name = "lfs.upstream.open", skip_all, fields(oid = %obj.oid, size = obj.size))]
    pub async fn open(
        &self,
        obj: &UpstreamObject,
    ) -> anyhow::Result<(
        u64,
        futures::stream::BoxStream<'static, std::io::Result<Bytes>>,
    )> {
        let mut req = self.client.get(&obj.href);
        for (k, v) in &obj.header {
            req = req.header(k.as_str(), v.as_str());
        }
        let resp = match req.send().await {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                metrics::counter!("walgit_lfs_upstream_total", "op" => "download", "result" => "error").increment(1);
                anyhow::bail!("upstream object: HTTP {}", r.status());
            }
            Err(e) => {
                metrics::counter!("walgit_lfs_upstream_total", "op" => "download", "result" => "error").increment(1);
                return Err(e.into());
            }
        };
        let len = resp.content_length().unwrap_or(obj.size);
        metrics::counter!("walgit_lfs_upstream_total", "op" => "download", "result" => "ok")
            .increment(1);
        let stream = resp
            .bytes_stream()
            .map(|c| c.map_err(|e| std::io::Error::other(e.to_string())))
            .boxed();
        Ok((len, stream))
    }

    /// The upstream token: the value of the environment variable `upstream.token_env` names.
    pub async fn secret(&self, env_name: &str) -> anyhow::Result<String> {
        let v = std::env::var(env_name).map_err(|_| {
            anyhow::anyhow!("upstream.token_env {env_name:?} is not set in this host's environment")
        })?;
        let v = v.trim().to_string();
        anyhow::ensure!(!v.is_empty(), "upstream.token_env {env_name:?} is empty");
        Ok(v)
    }
}
