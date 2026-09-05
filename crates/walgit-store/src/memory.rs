//! In-memory backend: reference semantics for tests. Versions are a global
//! monotonic counter so they are unique across keys and time, like generations.

use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use bytes::Bytes;
use futures::StreamExt;
use parking_lot::Mutex;

use crate::{
    BoxStream, GetOptions, GetResult, ObjectMeta, ObjectStore, PutBody, PutMode, PutOptions,
    Result, StoreError, Version, util,
};

#[derive(Default)]
pub struct MemoryStore {
    objects: Mutex<BTreeMap<String, (Version, Bytes)>>,
    counter: AtomicU64,
    /// Optional artificial latency per op (tests of races/batching).
    pub latency: Option<std::time::Duration>,
    /// Tests of edge offload (X-Accel-Redirect): when set, `accel_target` returns a
    /// URL + bearer pair like GCS would.
    pub fake_object_urls: std::sync::atomic::AtomicBool,
    /// Test switch: `signed_get_url` / `signed_put_url` fail like a store whose
    /// signing permission is unavailable or denied.
    pub signing_fails: bool,
    /// Tests of `serve_via = "signed_url"` uploads. With a base URL set,
    /// `signed_put_url` answers like a backend that can sign a checksummed PUT:
    /// `<base>/<key>` plus the checksum header such a backend would sign. Unset =
    /// this backend's real answer (`Ok(None)`). Point it at a mock bucket and the
    /// whole client-side flow is testable without one.
    pub fake_signed_put_base: Mutex<Option<String>>,
}

/// Header the fake presigned PUT makes the client send. A real backend names its
/// own (`x-amz-checksum-sha256` on S3); callers copy whatever they are handed.
pub const FAKE_SIGNED_PUT_CHECKSUM_HEADER: &str = "x-walgit-test-checksum-sha256";

impl MemoryStore {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn shared() -> Arc<Self> {
        Arc::new(Self::new())
    }
    fn next_version(&self) -> Version {
        Version::new(
            self.counter
                .fetch_add(1, Ordering::Relaxed)
                .wrapping_add(1)
                .to_string(),
        )
    }
    async fn delay(&self) {
        if let Some(d) = self.latency {
            tokio::time::sleep(d).await;
        }
    }
    pub fn len(&self) -> usize {
        self.objects.lock().len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

async fn body_bytes(body: PutBody) -> Result<Bytes> {
    Ok(match body {
        PutBody::Bytes(b) => b,
        PutBody::Stream { len, stream } => {
            util::collect(stream, usize::try_from(len).map_err(StoreError::other)?).await?
        }
        PutBody::File(p) => Bytes::from(tokio::fs::read(&p).await.map_err(StoreError::other)?),
    })
}

#[async_trait::async_trait]
impl ObjectStore for MemoryStore {
    async fn signed_get_url(&self, key: &str, _ttl: std::time::Duration) -> Result<Option<String>> {
        if self.signing_fails {
            return Err(StoreError::other(anyhow::anyhow!(
                "signBlob for {key}: PERMISSION_DENIED (VPC_SERVICE_CONTROLS) [test]"
            )));
        }
        Ok(None)
    }

    async fn signed_put_url(
        &self,
        key: &str,
        ttl: std::time::Duration,
        checksum_sha256: &[u8; 32],
    ) -> Result<Option<crate::SignedPut>> {
        use base64::Engine;
        if self.signing_fails {
            return Err(StoreError::other(anyhow::anyhow!(
                "signBlob for {key}: PERMISSION_DENIED (VPC_SERVICE_CONTROLS) [test]"
            )));
        }
        let Some(base) = self.fake_signed_put_base.lock().clone() else {
            return Ok(None);
        };
        Ok(Some(crate::SignedPut {
            url: format!("{base}/{key}?X-Test-Signature=1"),
            headers: vec![(
                FAKE_SIGNED_PUT_CHECKSUM_HEADER.to_string(),
                base64::engine::general_purpose::STANDARD.encode(checksum_sha256),
            )],
            expires_in: ttl,
        }))
    }

    fn backend(&self) -> &'static str {
        "memory"
    }

    async fn accel_target(&self, key: &str) -> Option<crate::AccelTarget> {
        self.fake_object_urls
            .load(Ordering::Relaxed)
            .then(|| crate::AccelTarget {
                url: format!("https://storage.example.test/test-bucket/{key}"),
                authorization: Some("Bearer test-store-access-token".to_string()),
            })
    }

    async fn get(&self, key: &str, opts: GetOptions) -> Result<GetResult> {
        self.delay().await;
        let (version, data) = {
            let g = self.objects.lock();
            match g.get(key) {
                Some((v, d)) => (v.clone(), d.clone()),
                None => return Err(StoreError::NotFound { key: key.into() }),
            }
        };
        if let Some(m) = &opts.if_match
            && *m != version
        {
            return Err(StoreError::PreconditionFailed {
                key: key.into(),
                current: Some(version),
            });
        }
        if opts.if_none_match.as_ref() == Some(&version) {
            return Ok(GetResult::NotModified { version });
        }
        let size = data.len() as u64;
        let slice = match &opts.range {
            Some(r) => {
                let start = usize::try_from(r.start.min(size)).map_err(StoreError::other)?;
                let end = usize::try_from(r.end.min(size)).map_err(StoreError::other)?;
                if start > end {
                    return Err(StoreError::InvalidArgument(format!(
                        "bad range {r:?} for size {size}"
                    )));
                }
                data.slice(start..end)
            }
            None => data,
        };
        Ok(GetResult::Object {
            meta: ObjectMeta {
                key: key.into(),
                size,
                version,
            },
            body: util::once(slice),
        })
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        self.delay().await;
        Ok(self.objects.lock().get(key).map(|(v, d)| ObjectMeta {
            key: key.into(),
            size: d.len() as u64,
            version: v.clone(),
        }))
    }

    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let data = body_bytes(body).await?;
        self.delay().await;
        let mut g = self.objects.lock();
        let current = g.get(key).map(|(v, _)| v.clone());
        match (&opts.mode, &current) {
            (PutMode::Overwrite, _) | (PutMode::Create, None) => {}
            (PutMode::Create, Some(v)) => {
                return Err(StoreError::PreconditionFailed {
                    key: key.into(),
                    current: Some(v.clone()),
                });
            }
            (PutMode::Update(want), Some(v)) if want == v => {}
            (PutMode::Update(_), cur) => {
                return Err(StoreError::PreconditionFailed {
                    key: key.into(),
                    current: cur.clone(),
                });
            }
        }
        let version = self.next_version();
        let size = data.len() as u64;
        g.insert(key.to_owned(), (version.clone(), data));
        Ok(ObjectMeta {
            key: key.into(),
            size,
            version,
        })
    }

    fn supports_compose(&self) -> bool {
        true
    }
    fn compose_is_native(&self) -> bool {
        true
    }

    async fn compose(
        &self,
        dest: &str,
        sources: &[String],
        opts: PutOptions,
    ) -> Result<ObjectMeta> {
        let mut buf = bytes::BytesMut::new();
        {
            let g = self.objects.lock();
            for src in sources {
                let (_, data) = g
                    .get(src)
                    .ok_or_else(|| StoreError::NotFound { key: src.clone() })?;
                buf.extend_from_slice(data);
            }
        }
        self.put(dest, PutBody::Bytes(buf.freeze()), opts).await
    }

    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()> {
        self.delay().await;
        let mut g = self.objects.lock();
        match (g.get(key), if_version) {
            (None, None) => Ok(()),
            (None, Some(_)) => Err(StoreError::NotFound { key: key.into() }),
            (Some((v, _)), Some(want)) if *v != want => Err(StoreError::PreconditionFailed {
                key: key.into(),
                current: Some(v.clone()),
            }),
            _ => {
                g.remove(key);
                Ok(())
            }
        }
    }

    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let g = self.objects.lock();
        let items: Vec<ObjectMeta> = g
            .range(prefix.to_owned()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .filter(|(k, _)| start_after.is_none_or(|s| k.as_str() > s))
            .map(|(k, (v, d))| ObjectMeta {
                key: k.clone(),
                size: d.len() as u64,
                version: v.clone(),
            })
            .collect();
        futures::stream::iter(items.into_iter().map(Ok)).boxed()
    }

    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        let g = self.objects.lock();
        let mut out: Vec<String> = g
            .range(prefix.to_owned()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .filter_map(|(k, _)| {
                let rest = k.strip_prefix(prefix)?;
                rest.split_once('/')
                    .map(|(head, _)| format!("{prefix}{head}/"))
            })
            .collect();
        out.dedup();
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectStoreExt;

    #[tokio::test]
    async fn cas_semantics() {
        let s = MemoryStore::new();
        let m1 = s.put_bytes("k", "a", PutMode::Create).await.unwrap();
        assert!(
            s.put_bytes("k", "b", PutMode::Create)
                .await
                .unwrap_err()
                .is_precondition_failed()
        );
        let m2 = s
            .put_bytes("k", "b", PutMode::Update(m1.version.clone()))
            .await
            .unwrap();
        assert!(
            s.put_bytes("k", "c", PutMode::Update(m1.version.clone()))
                .await
                .unwrap_err()
                .is_precondition_failed()
        );
        assert!(s.get_if_changed("k", &m2.version).await.unwrap().is_none());
        let (m3, b) = s.get_if_changed("k", &m1.version).await.unwrap().unwrap();
        assert_eq!(m3.version, m2.version);
        assert_eq!(&b[..], b"b");
        assert!(
            s.delete("k", Some(m1.version))
                .await
                .unwrap_err()
                .is_precondition_failed()
        );
        s.delete("k", Some(m2.version)).await.unwrap();
        assert!(s.get_bytes("k").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn range_and_list() {
        let s = MemoryStore::new();
        s.put_bytes("p/a", "hello world", PutMode::Overwrite)
            .await
            .unwrap();
        s.put_bytes("p/b", "x", PutMode::Overwrite).await.unwrap();
        s.put_bytes("q/c", "y", PutMode::Overwrite).await.unwrap();
        let r = s
            .get(
                "p/a",
                GetOptions {
                    range: Some(6..11),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let (_, b) = r.bytes().await.unwrap().unwrap();
        assert_eq!(&b[..], b"world");
        let keys: Vec<_> = s.list("p/", None).map(|m| m.unwrap().key).collect().await;
        assert_eq!(keys, ["p/a", "p/b"]);
        let keys: Vec<_> = s
            .list("p/", Some("p/a"))
            .map(|m| m.unwrap().key)
            .collect()
            .await;
        assert_eq!(keys, ["p/b"]);
    }
}
