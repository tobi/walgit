//! Git smart HTTP protocol (v0/v2): info/refs, upload-pack, receive-pack.
//!
//! References:
//! * <https://git-scm.com/docs/http-protocol>
//! * <https://git-scm.com/docs/protocol-v2>

use std::collections::HashMap;
use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::AppState;
use crate::error::ApiError;
use crate::pktline;
use crate::repo::RepoRoute;
use crate::stream::{VecWriter, body_to_async_read, maybe_gunzip, write_body_pipe};
use tracing::Instrument;

/// Cache-control headers for smart endpoints (info/refs and pkt responses).
fn no_cache_headers() -> [(axum::http::HeaderName, &'static str); 3] {
    [
        (
            axum::http::header::CACHE_CONTROL,
            "no-cache, max-age=0, must-revalidate",
        ),
        (axum::http::header::EXPIRES, "Fri, 01 Jan 1980 00:00:00 GMT"),
        (axum::http::header::PRAGMA, "no-cache"),
    ]
}
/// `GET /{owner}/{repo}[.git]/info/refs?service=git-upload-pack|git-receive-pack`
pub async fn info_refs(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    query: &str,
) -> Result<Response, ApiError> {
    let service_param = parse_query(query, "service").unwrap_or_default();
    // Auth: read for upload-pack, write for receive-pack advertisement.
    let is_receive = service_param == "git-receive-pack";
    let auth_result = if is_receive {
        st.auth.require_write(headers).await
    } else {
        st.auth.require_read(headers).await
    };
    if let Err(e) = auth_result {
        // Git clients do not display 401/403 bodies, but they do print a pkt-line `ERR` message
        // ("fatal: remote error: ..."). Tell humans how to authenticate instead of leaving them
        // with "error 401" — but ONLY where a retry cannot help (the account is not allowed, the
        // verifier is down). A credential that is merely invalid/expired MUST be a real 401: that is
        // what makes git `erase` it from its credential helpers (the in-memory cache the installer
        // puts in front of ours), ask them again (gcloud refreshes an expired token) and retry. The
        // 200 + ERR answer made git keep — and re-`store` — a dead cached token for the cache's
        // whole lifetime (rig, 2026-08-22: every clone failed for 50 minutes).
        let has_creds = headers.contains_key(axum::http::header::AUTHORIZATION);
        let retry_cannot_help = matches!(
            e,
            crate::auth::AuthError::Forbidden | crate::auth::AuthError::Unavailable
        );
        if is_git_client(headers) && !service_param.is_empty() && has_creds && retry_cannot_help {
            return Ok(git_err_response(
                &service_param,
                &auth_help_message(st, headers, &e),
            ));
        }
        return Err(auth_err(e));
    }
    if is_receive && let Some(msg) = push_url_must_be_git(st, route, headers) {
        return Ok(git_err_response("git-receive-pack", &msg));
    }

    let service = match service_param.as_str() {
        "git-upload-pack" => walgit_git::Service::UploadPack,
        "git-receive-pack" => walgit_git::Service::ReceivePack,
        other => return Err(ApiError::BadRequest(format!("unknown service: {other}"))),
    };

    let handle = open_repo(st, &route.id, is_receive).await?;
    // Advertisements need refs only: never wait for (or require) the pack set.
    let _guard = handle.sync_refs().await.map_err(wal_err)?;

    let protocol = walgit_git::pkt::Protocol::from_git_protocol_header(
        headers.get("git-protocol").and_then(|v| v.to_str().ok()),
    );

    // Build the response: service header + flush, then advertisement.
    let mut buf = Vec::with_capacity(2048);
    let svc_line = format!("# service={service_param}\n");
    pktline::encode_text(&mut buf, &svc_line);
    pktline::encode_flush(&mut buf);

    if let (walgit_git::pkt::Protocol::V2, walgit_git::Service::UploadPack) = (protocol, service) {
        v2_capability_advert(st, &route.id, &handle, &mut buf).await?;
    } else {
        // v0 (and receive-pack always).
        let repo_key = route.id.to_string();
        let ver = handle.manifest_version();
        if let Some(cached) = st
            .caches
            .ref_advert
            .get_v0(&repo_key, ver.as_ref(), service)
        {
            buf.extend_from_slice(&cached);
        } else {
            let start = buf.len();
            let cfg = handle.validated_effective_config().map_err(wal_err)?;
            handle
                .local()
                .advertise_refs_v0(service, &mut buf, Some(&cfg.refs.advertise))
                .map_err(git_err)?;
            let advert_bytes = buf[start..].to_vec();
            st.caches
                .ref_advert
                .insert_v0(&repo_key, ver.as_ref(), service, advert_bytes);
        }
    }

    let ct = format!("application/x-{service_param}-advertisement");
    Ok(build_response(StatusCode::OK, &ct, no_cache_headers(), buf))
}

fn parse_query(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        if let Some((k, v)) = pair.split_once('=')
            && k == key
        {
            return Some(v.to_string());
        }
    }
    None
}

/// Build the protocol v2 capability advertisement for upload-pack.
async fn v2_capability_advert(
    st: &AppState,
    _id: &walgit_git::RepoId,
    handle: &Arc<walgit_wal::RepoHandle>,
    buf: &mut Vec<u8>,
) -> Result<(), ApiError> {
    let ver = env!("CARGO_PKG_VERSION");
    pktline::encode_text(buf, "version 2\n");
    pktline::encode_text(buf, &format!("agent=walgit/{ver}\n"));
    pktline::encode_text(buf, "ls-refs=unborn\n");
    let mut fetch = String::from("fetch=shallow wait-for-done");
    if st.cfg.git.allow_filter {
        fetch.push_str(" filter");
    }
    // With sideband-all every response line is sideband-framed, which lets us
    // narrate what the server is doing (band 2 → "remote: * …") *before* the
    // packfile section: auth, WAL sync, materialization progress. Both engines frame their sections that way.
    fetch.push_str(" sideband-all packfile-uris packfile-indexes");
    pktline::encode_text(buf, &format!("{fetch}\n"));
    pktline::encode_text(buf, "server-option\n");
    let fmt = match handle.local().object_format() {
        walgit_git::ObjectFormat::Sha1 => "sha1",
        walgit_git::ObjectFormat::Sha256 => "sha256",
    };
    pktline::encode_text(buf, &format!("object-format={fmt}\n"));
    pktline::encode_flush(buf);
    Ok(())
}

/// `POST /{owner}/{repo}[.git]/git-upload-pack`
pub async fn upload_pack(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    body: Body,
) -> Result<Response, ApiError> {
    st.auth.require_read(headers).await.map_err(auth_err)?;

    let handle = open_repo(st, &route.id, false).await?;

    let protocol = walgit_git::pkt::Protocol::from_git_protocol_header(
        headers.get("git-protocol").and_then(|v| v.to_str().ok()),
    );

    let enc = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok());
    let reader = maybe_gunzip(enc, body_to_async_read(body));

    // sync() is deferred to each handler path so the ReadGuard lives for the
    // entire streaming response (packs must not be removed mid-clone).
    match protocol {
        walgit_git::pkt::Protocol::V2 => upload_pack_v2(st, route, headers, &handle, reader).await,
        walgit_git::pkt::Protocol::V0 => upload_pack_v0(st, route, headers, &handle, reader).await,
    }
}

async fn upload_pack_v2(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    handle: &Arc<walgit_wal::RepoHandle>,
    reader: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
) -> Result<Response, ApiError> {
    let (cmd, reader) = walgit_git::pkt::read_command(reader)
        .await
        .map_err(git_err)?;
    match cmd.name.as_str() {
        "ls-refs" => {
            let _guard = handle.sync_refs().await.map_err(wal_err)?;
            let req = walgit_git::pkt::parse_ls_refs(&cmd);
            let req = walgit_git::pkt::read_ls_refs_args(reader, req)
                .await
                .map_err(git_err)?;
            let args = walgit_git::LsRefsArgs {
                ref_prefixes: req.prefixes,
                symrefs: req.symrefs,
                peel: req.peel,
                unborn: req.unborn,
            };
            let repo_key = route.id.to_string();
            let version = handle.manifest_version();
            let mut lines = if let Some(lines) =
                st.caches
                    .ref_advert
                    .get_v2_ls_refs(&repo_key, version.as_ref(), &args)
            {
                lines
            } else {
                let lines = handle.local().ls_refs(&args).map_err(git_err)?;
                st.caches.ref_advert.insert_v2_ls_refs(
                    &repo_key,
                    version.as_ref(),
                    &args,
                    lines.clone(),
                );
                lines
            };
            let all_refs = cmd.cap("server-option") == Some("ref-view=all")
                || cmd
                    .args
                    .iter()
                    .any(|arg| arg == "server-option=ref-view=all");
            if !all_refs {
                let cfg = handle.validated_effective_config().map_err(wal_err)?;
                let refs = handle.local().ref_view().map_err(git_err)?;
                lines.retain(|line| {
                    let name = if line.name == "HEAD" {
                        refs.head_target()
                    } else {
                        &line.name
                    };
                    walgit_config::refs::selectors_match(
                        &cfg.refs.advertise,
                        name,
                        refs.head_target(),
                    )
                });
            }
            let mut buf = Vec::with_capacity(1024);
            for line in &lines {
                pktline::encode_text(&mut buf, &line.render(&args));
            }
            pktline::encode_flush(&mut buf);
            Ok(text_response(
                "application/x-git-upload-pack-result",
                no_cache_headers(),
                buf,
            ))
        }
        "fetch" => {
            if let Some(r) = not_served_here(st, &route.id, "git-upload-pack").await {
                return Ok(r);
            }
            let req = parse_fetch_request(reader).await?;
            // A want list far beyond any honest request is a blobless clone checking out HEAD without
            // `--sparse`/`--no-checkout` (git lazily asks for every blob of the tree in one fetch, with
            // `no-progress`, so nothing we narrate there is ever seen): refuse it with the fix before
            // any sync or pack work. `git.max_wants = 0` disables the bound.
            if st.cfg.git.max_wants > 0 && req.wants.len() > st.cfg.git.max_wants {
                let msg = too_many_wants_message(st, headers, route, req.wants.len());
                tracing::warn!(repo = %route.id, wants = req.wants.len(), max = st.cfg.git.max_wants, "fetch refused: too many wants");
                metrics::counter!("walgit_fetch_too_many_wants_total", "repo" => route.id.to_string()).increment(1);
                return Ok(if req.sideband_all {
                    let mut buf = sideband_pkt(3, &msg);
                    pktline::encode_flush(&mut buf);
                    text_response(
                        "application/x-git-upload-pack-result",
                        no_cache_headers(),
                        buf,
                    )
                } else {
                    git_err_response("git-upload-pack", &msg)
                });
            }
            // Narrated fetch: the client accepted sideband-all and wants
            // progress → stream immediately and say what we are doing while
            // the local copy syncs, then hand over to upload-pack.
            if req.sideband_all && !req.no_progress {
                return Ok(narrated_fetch(st, route, headers, handle, req).await);
            }
            // Objects are needed from here on. Surface "too big for this
            // instance" as a pkt-line ERR git prints verbatim (with the fix),
            // before any pack bytes start streaming.
            if let Err(e) = handle.sync().await {
                return Ok(match e {
                    walgit_wal::WalError::TooLarge { .. } => git_err_response(
                        "git-upload-pack",
                        &too_large_message(st, headers, route, &e),
                    ),
                    e => return Err(wal_err(e)),
                });
            }
            let (writer, body) = write_body_pipe(256 * 1024);
            // Move the Arc<RepoHandle> into the spawned task so the ReadGuard
            // from sync() lives for the entire streaming response — packs must
            // not be removed mid-clone.
            let handle = handle.clone();
            let engine = st.cfg.git.upload_pack_engine;
            let uri_base = anonymous_uri_base(st, route, headers);
            tokio::spawn(async move {
                let guard = match handle.sync().await {
                    Ok(g) => g,
                    Err(e) => {
                        tracing::warn!(error = ?e, "upload_pack v2 sync failed");
                        return;
                    }
                };
                // guard is held until the task ends (after streaming completes).
                if let Err(e) = run_fetch(&handle, engine, req, writer, uri_base).await {
                    tracing::warn!(error = ?e, "upload_pack v2 fetch failed");
                }
                drop(guard);
            });
            Ok(stream_response(
                "application/x-git-upload-pack-result",
                no_cache_headers(),
                body,
            ))
        }
        "object-info" => {
            let _guard = handle.sync().await.map_err(wal_err)?;
            let req = walgit_git::pkt::parse_object_info(&cmd);
            let mut sizes_buf = Vec::with_capacity(256);
            let repo = handle.local().gix();
            for hex in &req.oids {
                let size = gix_hash::ObjectId::from_hex(hex.as_bytes())
                    .ok()
                    .and_then(|oid| repo.find_object(oid).ok())
                    .map_or(-1, |o| o.data.len() as i64);
                pktline::encode_text(&mut sizes_buf, &format!("size {size}\n"));
            }
            pktline::encode_flush(&mut sizes_buf);
            Ok(text_response(
                "application/x-git-upload-pack-result",
                no_cache_headers(),
                sizes_buf,
            ))
        }
        other => Err(ApiError::BadRequest(format!("unknown v2 command: {other}"))),
    }
}

/// One sideband packet (`band` 1 = data, 2 = progress, 3 = error).
fn sideband_pkt(band: u8, text: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(text.len() + 8);
    let mut data = Vec::with_capacity(text.len() + 2);
    data.push(band);
    data.extend_from_slice(text.as_bytes());
    if !text.ends_with('\n') {
        data.push(b'\n');
    }
    for chunk in data.chunks(pktline::MAX_DATA_LEN) {
        pktline::encode_line(&mut buf, chunk);
    }
    buf
}

/// `git clone` shows band-2 lines as `remote: …`; prefix with `* ` so they read
/// as a narration of what the server is doing.
async fn say<W: tokio::io::AsyncWrite + Unpin>(w: &mut W, text: &str) -> bool {
    use tokio::io::AsyncWriteExt;
    let pkt = sideband_pkt(2, &format!("* {text}"));
    w.write_all(&pkt).await.is_ok() && w.flush().await.is_ok()
}

fn human(n: u64) -> String {
    walgit_wal::remote::human_bytes(n)
}

/// Run a v2 fetch on the synced local copy with the configured engine. When
/// this instance serves the repo's base pack(s) remotely (no store mount, set
/// does not fit — `RepoHandle::remote_served`), stock git cannot read them, so
/// the gix engine runs regardless of config with the remote-reader faulter:
/// history from the commit-graph chain, base objects by range read.
async fn run_fetch<W: tokio::io::AsyncWrite + Unpin + Send>(
    handle: &Arc<walgit_wal::RepoHandle>,
    engine: walgit_config::UploadPackEngine,
    mut req: walgit_git::UploadPackRequest,
    mut writer: W,
    uri_base: Option<String>,
) -> Result<(), walgit_git::GitError> {
    let local = handle.local().clone();
    if !handle.remote_served().is_empty() {
        warn_large_dynamic_clone(handle, &req, &mut writer).await;
        let reader = handle
            .remote_reader()
            .await
            .map_err(|e| walgit_git::GitError::Protocol(format!("remote reader: {e}")))?;
        let faulter = walgit_wal::remote::Faulter::new(reader, local.clone());
        let t0 = std::time::Instant::now();
        let stats = local
            .upload_pack_gix_with(req, writer, Some(&faulter))
            .await?;
        let (faulted, rounds) = faulter.stats();
        tracing::info!(
            repo = %handle.id(),
            objects = stats.objects,
            bytes = stats.bytes,
            faulted,
            rounds,
            ms = u64::try_from(t0.elapsed().as_millis()).unwrap_or(u64::MAX),
            "gix fetch over remote-served base"
        );
        return Ok(());
    }
    // Per-request choice (`git.upload_pack_engine = "auto"`): the gix engine
    // for commit fetches/clones (tree-diff enumeration, streaming — a large repository
    // benchmark 7 s vs 12 s warm), stock git when the wants are blobs (a
    // partial clone's lazy checkout: 15 k blob wants; git reuses the base's
    // deltas — 43 s vs 78 s).
    // `auto` (D2 amendment 2026-08-21): stock git wherever git can read the
    // packs — local copies and mount-linked bases alike. The gix engine is kept
    // for what git cannot do: a remote-served base (handled above, with the
    // faulter), and an explicit `engine = "gix"`. Why: on a large repository's remainder
    // fetches gix (a) copied one entry under another object's id at 05:4xZ
    // (client: "The same object … appears twice in the pack") and (b) on a
    // controlled replay — 113,683 objects / 1,990 commits, no churn — was
    // OOM-killed at 178 GB anon RSS on a 173 GB box. Stock git did the same
    // fetches in 5–12 s.
    let engine = match engine {
        walgit_config::UploadPackEngine::Auto => walgit_config::UploadPackEngine::Git,
        e => e,
    };
    match engine {
        walgit_config::UploadPackEngine::Gix | walgit_config::UploadPackEngine::Auto => {
            warn_large_dynamic_clone(handle, &req, &mut writer).await;
            local
                .upload_pack_gix_with(req, writer, None)
                .await
                .map(|_| ())
        }
        walgit_config::UploadPackEngine::Git => {
            if let Some(base) = uri_base
                && let Some(selection) = crate::packfile_uri::select(handle, &req, &base).await
            {
                return local.upload_pack_with_uris(req, &selection, writer).await;
            }
            warn_large_dynamic_clone(handle, &req, &mut writer).await;
            req.packfile_uris_protocols.clear();
            let body = walgit_git::build_v2_fetch_request(&req);
            local
                .upload_pack_raw(walgit_git::pkt::Protocol::V2, &body[..], writer)
                .await
        }
    }
}

async fn warn_large_dynamic_clone<W: tokio::io::AsyncWrite + Unpin>(
    handle: &walgit_wal::RepoHandle,
    request: &walgit_git::UploadPackRequest,
    writer: &mut W,
) {
    if request.sideband_all
        && !request.no_progress
        && request.done
        && request.haves.is_empty()
        && request.filter.is_none()
        && request.deepen.is_none()
        && request.deepen_since.is_none()
        && request.deepen_not.is_empty()
        && request.shallow.is_empty()
        && handle
            .manifest()
            .packs
            .iter()
            .map(|p| p.pack_size)
            .sum::<u64>()
            >= handle
                .effective_config()
                .packfile_uri
                .uri_min_bytes
                .as_u64()
    {
        let _ = say(writer, "WARNING: reusable pack delivery is unavailable for this request; this full clone uses dynamic transfer and may take longer. Use a shallow or filtered clone when appropriate.").await;
    }
}

fn anonymous_uri_base(st: &AppState, route: &RepoRoute, headers: &HeaderMap) -> Option<String> {
    (st.cfg.server.auth.mode == walgit_config::AuthMode::None || st.cfg.server.auth.anonymous_read)
        .then(|| format!("{}/{}", request_base_url(st, headers), route.id))
}

/// `handle.sync()` while narrating on band 2: the repo's progress packets
/// (notices, bars, task changes) as they happen, and a heartbeat every 5 s so
/// the connection never goes silent (a serverless host's frontend cut a push that
/// sent nothing for ~100 s while the broker materialized a large repository's side-files).
async fn sync_narrated<'h, W: tokio::io::AsyncWrite + Unpin>(
    handle: &'h Arc<walgit_wal::RepoHandle>,
    writer: &mut W,
    t0: std::time::Instant,
) -> Result<walgit_wal::ReadGuard<'h>, walgit_wal::WalError> {
    let mut rx = handle.subscribe_progress();
    if !handle.packs_ready() {
        let _ = say(
            writer,
            "local copy is missing packs on this instance; materializing from the WAL…",
        )
        .await;
    }
    let sync = handle.sync();
    tokio::pin!(sync);
    let mut last_bar = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_secs(1))
        .unwrap();
    loop {
        tokio::select! {
            biased;
            r = &mut sync => break r,
            p = rx.recv() => match p {
                Ok(walgit_wal::Progress::Notice { text }) => { let _ = say(writer, &text).await; }
                Ok(walgit_wal::Progress::Progress { label, done, total, unit, percent }) => {
                    if last_bar.elapsed() >= std::time::Duration::from_secs(1) || total.is_some_and(|t| done >= t) {
                        last_bar = std::time::Instant::now();
                        let line = match (total, percent) {
                            (Some(t), Some(pc)) if unit == "bytes" => format!("{label}: {pc:.0}% ({} / {})", human(done), human(t)),
                            (Some(t), Some(pc)) => format!("{label}: {pc:.0}% ({done} / {t} {unit})"),
                            _ if unit == "bytes" => format!("{label}: {}", human(done)),
                            _ => format!("{label}: {done} {unit}"),
                        };
                        let _ = say(writer, &line).await;
                    }
                }
                Ok(walgit_wal::Progress::Task { task }) => {
                    if task.ok.is_some() {
                        let _ = say(writer, &format!("task {} finished: {}", task.kind, task.summary)).await;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(_) => {
                    // Channel closed: just await the sync.
                    break (&mut sync).await;
                }
            },
            () = tokio::time::sleep(std::time::Duration::from_secs(5)) => {
                let _ = say(writer, &format!("still syncing ({}s)…", t0.elapsed().as_secs())).await;
            }
        }
    }
}

/// v2 `fetch` with narration: stream progress lines while the repo syncs
/// (materialize on a cold instance, WAL catch-up), then run `git upload-pack`
/// which continues in the same sideband framing. Errors after the stream has
/// started go out on band 3 (`remote error: …`).
async fn narrated_fetch(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    handle: &Arc<walgit_wal::RepoHandle>,
    req: walgit_git::UploadPackRequest,
) -> Response {
    let (mut writer, body) = write_body_pipe(256 * 1024);
    let handle = handle.clone();
    let repo = route.id.to_string();
    let who = st
        .auth
        .require_read(headers)
        .await
        .ok()
        .map_or_else(|| "anonymous".into(), |p| p.name);
    let cache_max = st.cfg.cache_budget_bytes();
    let engine = st.cfg.git.upload_pack_engine;
    let uri_base = anonymous_uri_base(st, route, headers);
    let max_wants = st.cfg.git.max_wants;
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let t0 = std::time::Instant::now();
        if !say(
            &mut writer,
            &format!("walgit: {repo} — authenticated as {who}"),
        )
        .await
        {
            return;
        }
        let applied = handle.applied_seq();
        let _ = say(
            &mut writer,
            &format!(
                "refs from the WAL at seq {applied}; you sent {} want(s), {} have(s){}",
                req.wants.len(),
                req.haves.len(),
                if req.haves.is_empty() {
                    " (full clone)"
                } else {
                    ""
                }
            ),
        )
        .await;
        // The initial fetch of a blobless clone is the one moment the user can still be told: the
        // lazy blob fetch that follows a checkout carries `no-progress`, so nothing said there is seen.
        if req.haves.is_empty()
            && req
                .filter
                .as_deref()
                .is_some_and(|f| f.starts_with("blob:none"))
            && req.deepen.is_none()
        {
            let _ = say(
                &mut writer,
                &format!(
                    "blobless clone: without --sparse or --no-checkout, checking out HEAD fetches every blob of its tree in \
                     one request next{}; `git sparse-checkout add <dir>` pulls only what you need",
                    if max_wants > 0 { format!(" (this host refuses requests above {max_wants} objects)") } else { String::new() }
                ),
            )
            .await;
        }
        // Sync with narration: forward the repo's progress packets while it runs.
        let guard = sync_narrated(&handle, &mut writer, t0).await;
        let guard = match guard {
            Ok(g) => g,
            Err(e) => {
                let msg = match &e {
                    walgit_wal::WalError::TooLarge { bytes, max } => format!(
                        "walgit: this repository's pack set is {} — larger than this instance's cache ({}); retry on a host with enough capacity or use a bounded clone",
                        human(*bytes),
                        human(if *max == 0 { cache_max } else { *max })
                    ),
                    e => format!("walgit: sync failed: {e}"),
                };
                // git dies on the first band-3 packet: send the whole
                // message (with its fix) in one.
                let _ = writer.write_all(&sideband_pkt(3, &msg)).await;
                let mut f = Vec::new();
                pktline::encode_flush(&mut f);
                let _ = writer.write_all(&f).await;
                let _ = writer.flush().await;
                return;
            }
        };
        let local = guard.local().clone();
        let packs = local.packs().map_or(0, |p| p.len());
        let remote = handle.remote_served();
        let _ = say(
            &mut writer,
            &format!(
                "local copy ready ({packs} pack(s){}, {:.1}s); computing what you are missing and packing it…",
                if remote.is_empty() { String::new() } else { format!(" + {} base pack(s) read from the bucket by range", remote.len()) },
                t0.elapsed().as_secs_f64()
            ),
        )
        .await;
        if let Err(e) = run_fetch(&handle, engine, req, writer, uri_base).await {
            tracing::warn!(error = ?e, "narrated upload_pack v2 fetch failed");
        }
        drop(guard);
    });
    stream_response(
        "application/x-git-upload-pack-result",
        no_cache_headers(),
        body,
    )
}

async fn upload_pack_v0(
    st: &AppState,
    route: &RepoRoute,
    headers: &HeaderMap,
    handle: &Arc<walgit_wal::RepoHandle>,
    reader: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
) -> Result<Response, ApiError> {
    if let Some(r) = not_served_here(st, &route.id, "git-upload-pack").await {
        return Ok(r);
    }
    if let Err(e) = handle.sync().await {
        return Ok(match e {
            walgit_wal::WalError::TooLarge { .. } => git_err_response(
                "git-upload-pack",
                &too_large_message(st, headers, route, &e),
            ),
            e => return Err(wal_err(e)),
        });
    }
    if !handle.remote_served().is_empty() {
        // The v0 fetch runs stock git, which cannot read a remotely served
        // base; protocol v2 (git ≥ 2.26 default) uses the gix engine instead.
        return Ok(git_err_response(
            "git-upload-pack",
            "walgit: this repository's base pack is read from the bucket on this instance; fetch with protocol v2 (git -c protocol.version=2 …, the default since git 2.26)",
        ));
    }
    let (writer, body) = write_body_pipe(256 * 1024);
    // Move the Arc<RepoHandle> into the spawned task so the ReadGuard from
    // sync() lives for the entire streaming response.
    let handle = handle.clone();
    tokio::spawn(async move {
        let guard = match handle.sync().await {
            Ok(g) => g,
            Err(e) => {
                tracing::warn!(error = ?e, "upload_pack v0 sync failed");
                return;
            }
        };
        let local = guard.local().clone();
        // guard held until task ends (after streaming completes).
        if let Err(e) = local
            .upload_pack_raw(walgit_git::pkt::Protocol::V0, reader, writer)
            .await
        {
            tracing::warn!(error = ?e, "upload_pack v0 failed");
        }
        drop(guard);
    });
    Ok(stream_response(
        "application/x-git-upload-pack-result",
        no_cache_headers(),
        body,
    ))
}

/// Push URLs are `/<area>/<repository>.git` only (the `.git` suffix is required).
fn push_url_must_be_git(st: &AppState, route: &RepoRoute, headers: &HeaderMap) -> Option<String> {
    if route.had_git_suffix {
        return None;
    }
    Some(format!(
        "walgit: push URL must be {}/<area>/<repository>.git",
        request_base_url(st, headers)
    ))
}

/// `POST /{owner}/{repo}.git/git-receive-pack`
pub async fn receive_pack(
    st: &Arc<AppState>,
    route: &RepoRoute,
    headers: &HeaderMap,
    mut body: Body,
) -> Result<Response, ApiError> {
    let principal = st.auth.require_write(headers).await.map_err(auth_err)?;
    if let Some(msg) = push_url_must_be_git(st, route, headers) {
        return refuse_push(body, headers, msg).await;
    }

    // Draining after SIGTERM: a push that starts now could not finish its
    // publish inside the grace; refuse it with Retry-After (git: rerun).
    if walgit_wal::tasks::shutting_down() {
        metrics::counter!("walgit_push_refused_total", "reason" => "draining").increment(1);
        return refuse_push(
            body,
            headers,
            "walgit: this host is restarting; retry in a few seconds".into(),
        )
        .await
        .map(|r| with_retry_after(r, StatusCode::SERVICE_UNAVAILABLE));
    }
    // Placement (D29/D30): object work for a repository this host does not
    // serve is refused before any forward, sync or pack read — 503 +
    // Retry-After so the edge's fallback never hangs a client, and the report
    // names the host that does serve it.
    if !st.cfg.placement.serves(route.id.owner(), route.id.name()) {
        let host = maintainer_of(st, &route.id)
            .await
            .unwrap_or_else(|| "another host".into());
        metrics::counter!("walgit_push_refused_total", "reason" => "not_served_here").increment(1);
        tracing::info!(repo = %route.id, %host, "push refused: repository is not served by this host");
        return refuse_push(
            body,
            headers,
            format!("walgit: {} is served by {host}; retry shortly", route.id),
        )
        .await
        .map(|r| with_retry_after(r, StatusCode::SERVICE_UNAVAILABLE));
    }
    // D28: the host that maintains a repository is its writer. A maintainer
    // that does not maintain this repository (the serverless host broker for
    // acme/monorepo) refuses at once, naming the writer.
    if st.cfg.has_role(walgit_config::Role::Maintain)
        && !st
            .cfg
            .placement
            .maintains(route.id.owner(), route.id.name())
    {
        let writer = maintainer_of(st, &route.id)
            .await
            .unwrap_or_else(|| "its maintainer".into());
        let url = format!(
            "{}/{}.git",
            crate::smart::request_base_url(st, headers),
            route.id
        );
        metrics::counter!("walgit_push_refused_total", "reason" => "not_assigned").increment(1);
        tracing::info!(repo = %route.id, %writer, "push refused: repository is written by another host");
        return refuse_push(
            body,
            headers,
            format!("walgit: {} is written by {writer}; push to {url}", route.id),
        )
        .await;
    }

    // Buffering preserves a body, not permission to replay it. Only proven
    // pre-delivery failure permits local fallback; ambiguous delivery never does.
    // Larger (or unknown-length) requests stay fully streaming.
    let already_forwarded = headers
        .get("x-walgit-forwarded")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.trim() == "1");
    if let Some(broker_url) = st
        .cfg
        .wal
        .push_broker_url
        .as_deref()
        .filter(|url| !url.trim().is_empty())
        && !already_forwarded
    {
        let content_len = headers
            .get(axum::http::header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok());
        let replayable =
            content_len.is_some_and(|len| len <= st.cfg.wal.push_broker_buffer_bytes.as_u64());
        let fallback_bytes = if replayable {
            let max =
                usize::try_from(st.cfg.wal.push_broker_buffer_bytes.as_u64()).unwrap_or(usize::MAX);
            let bytes = to_bytes(body, max)
                .await
                .map_err(|e| ApiError::BadRequest(format!("body read: {e}")))?;
            body = Body::from(bytes.clone());
            Some(bytes)
        } else {
            None
        };
        let forward_body = if let Some(bytes) = &fallback_bytes {
            Body::from(bytes.clone())
        } else {
            body
        };
        match crate::forward::receive_pack(
            broker_url,
            route,
            headers,
            forward_body,
            &principal,
            st.cfg.wal.push_broker_token.as_deref(),
        )
        .await
        {
            crate::forward::ForwardOutcome::Response(response) => return Ok(response),
            crate::forward::ForwardOutcome::Ambiguous => {
                return Ok((
                    StatusCode::BAD_GATEWAY,
                    "push broker response lost; outcome unknown; inspect remote refs before retrying",
                )
                    .into_response());
            }
            crate::forward::ForwardOutcome::Fallback => {
                if let Some(bytes) = fallback_bytes {
                    metrics::counter!("walgit_push_forwarded_total", "outcome" => "fallback")
                        .increment(1);
                    body = Body::from(bytes);
                } else {
                    metrics::counter!("walgit_push_forwarded_total", "outcome" => "error")
                        .increment(1);
                    return Ok((
                        StatusCode::SERVICE_UNAVAILABLE,
                        "push broker unavailable; retry the push",
                    )
                        .into_response());
                }
            }
        }
    }

    let handle = open_repo(st, &route.id, true).await?;

    let enc = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok());
    let reader = maybe_gunzip(enc, body_to_async_read(body));

    // Parse commands + capabilities first (they need no objects); pack bytes
    // follow in `pack_reader`. Knowing the capabilities before the sync lets
    // us narrate the sync on band 2 when the client speaks side-band-64k.
    let (txn, caps, pack_reader) = walgit_git::receive::parse(reader).await.map_err(git_err)?;
    let pack_reader: Box<dyn tokio::io::AsyncRead + Unpin + Send> = Box::new(pack_reader);
    // Wal's verify_txn treats empty string as the zero oid (create/delete).
    // receive::parse emits the 40-zero hex; normalize to empty for both ends.
    let mut txn = txn;
    for u in &mut txn.updates {
        if is_zero_oid(&u.old_oid) {
            u.old_oid.clear();
        }
        if is_zero_oid(&u.new_oid) {
            u.new_oid.clear();
        }
    }

    // The same predicate the maintainer's plan uses for `wrong-host`: a push
    // needs the serving copy; if it cannot fit here, say so instead of starting
    // a download that never completes (the broker spent minutes on a large repository).
    if !handle.serve_fits() {
        let url = format!(
            "{}/{}.git",
            crate::smart::request_base_url(st, headers),
            route.id
        );
        metrics::counter!("walgit_push_refused_total", "reason" => "wrong_host").increment(1);
        tracing::warn!(repo = %route.id, "push refused: serving copy does not fit this host");
        let msg = format!(
            "walgit: this host cannot hold {}'s serving copy; push to {url}",
            route.id
        );
        let report = refusal_report(&caps, &txn, &msg).await;
        return Ok(receive_response(report));
    }

    // Correlates the event with the user-visible request (docs/EVENTS.md);
    // the front forwards it to the broker.
    let request_id = headers
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .map(std::string::ToString::to_string);

    if !caps.side_band_64k {
        // No sideband: the response is the report alone, after the work.
        let guard = handle.sync().await.map_err(wal_err)?;
        let report = receive_pack_process(
            st,
            &handle,
            guard,
            txn,
            caps,
            pack_reader,
            &principal,
            request_id,
        )
        .await?;
        return Ok(receive_response(report));
    }

    // Streaming: the report comes at the end; everything before it is band-2
    // narration (sync/materialize progress, heartbeat), so the connection is
    // never silent while the broker brings a big repository's side-files in.
    let (mut writer, body) = write_body_pipe(64 * 1024);
    let handle = handle.clone();
    let st_arc = st.clone();
    let repo = route.id.to_string();
    let who = principal.clone();
    tokio::spawn(async move {
        use tokio::io::AsyncWriteExt;
        let t0 = std::time::Instant::now();
        let _ = say(
            &mut writer,
            &format!("walgit: {repo} — push by {}", who.name),
        )
        .await;
        let guard = match sync_narrated(&handle, &mut writer, t0).await {
            Ok(g) => g,
            Err(e) => {
                tracing::warn!(repo = %repo, error = %e, "receive-pack: sync failed");
                let report =
                    refusal_report(&caps, &txn, &format!("walgit: sync failed: {e}")).await;
                let _ = writer.write_all(&report).await;
                let _ = writer.flush().await;
                return;
            }
        };
        if t0.elapsed().as_secs() >= 2 {
            let _ = say(
                &mut writer,
                &format!(
                    "local copy ready ({:.1}s); unpacking and checking your objects…",
                    t0.elapsed().as_secs_f64()
                ),
            )
            .await;
        }
        let txn_for_report = walgit_proto::v1::RefTransaction {
            updates: txn.updates.clone(),
            ..Default::default()
        };
        let report = match receive_pack_process(
            &st_arc,
            &handle,
            guard,
            txn,
            caps.clone(),
            pack_reader,
            &who,
            request_id,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(repo = %repo, error = %e.message(), "receive-pack failed");
                refusal_report(&caps, &txn_for_report, &format!("walgit: {}", e.message())).await
            }
        };
        let _ = writer.write_all(&report).await;
        let _ = writer.flush().await;
    });
    Ok(stream_response(
        "application/x-git-receive-pack-result",
        no_cache_headers(),
        body,
    ))
}

/// Everything after the sync: unpack, connectivity, policy, publish → the
/// report-status bytes (already sideband-framed when the client asked).
async fn receive_pack_process(
    st: &AppState,
    handle: &Arc<walgit_wal::RepoHandle>,
    _guard: walgit_wal::ReadGuard<'_>,
    txn: walgit_proto::v1::RefTransaction,
    caps: walgit_git::receive::ReceiveCaps,
    pack_reader: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
    principal: &crate::auth::Principal,
    request_id: Option<String>,
) -> Result<Vec<u8>, ApiError> {
    let route_id = handle.id().clone();
    let max_bytes = Some(st.cfg.server.max_push_bytes.as_u64());
    let opts = walgit_git::IngestOptions {
        fsck: st.cfg.wal.fsck_objects,
        max_bytes,
        thin: true,
    };
    let local = handle.local().clone();
    let ingest = local
        .ingest_pack(pack_reader, opts)
        .instrument(tracing::info_span!("receive.ingest"))
        .await;

    let unpack_err: Option<String> = match &ingest {
        Ok(_) => None,
        Err(e) => Some(format!("unpack failed: {e}")),
    };

    // Every pushed tip must exist before anything is published, pack or no
    // pack: a ref-only push carries a zero-object pack (`ingest` is `Ok(None)`)
    // and this block used to be skipped for it, so a ref could be published
    // pointing at an object nobody has (#37). With `wal.check_connectivity`
    // the walk covers the tips and everything new under them; without it the
    // tips themselves are still looked up.
    if unpack_err.is_none() {
        let tips: Vec<gix_hash::ObjectId> = txn
            .updates
            .iter()
            .filter(|u| !u.new_oid.is_empty() && !is_zero_oid(&u.new_oid))
            .filter_map(|u| gix_hash::ObjectId::from_hex(u.new_oid.as_bytes()).ok())
            .collect();
        if !tips.is_empty() {
            let verdict: Result<(), String> = if st.cfg.wal.check_connectivity {
                local
                    .check_connectivity_async(&tips, true)
                    .instrument(tracing::info_span!(
                        "receive.connectivity",
                        tips = tips.len()
                    ))
                    .await
                    .map_err(|e| format!("connectivity: {e}"))
            } else {
                let repo = local.clone();
                let tips = tips.clone();
                tokio::task::spawn_blocking(move || {
                    tips.iter()
                        .find(|t| !repo.has_object(t))
                        .map_or(Ok(()), |t| Err(format!("missing object {t}")))
                })
                .await
                .map_err(|e| ApiError::Internal(format!("tip check: {e}")))?
            };
            if let Err(msg) = verdict {
                // Every refusal names the reason on each ref: `unpack ng`
                // alone makes git print "remote failed to report status".
                tracing::warn!(repo = %route_id, error = %msg, "receive-pack: tip check failed");
                metrics::counter!("walgit_push_refused_total", "reason" => "connectivity")
                    .increment(1);
                return Ok(refusal_report(&caps, &txn, &msg).await);
            }
        }
    }

    // On unpack failure, report and abort (nothing was published).
    if let Some(msg) = unpack_err {
        tracing::warn!(repo = %route_id, error = %msg, "receive-pack: unpack failed");
        metrics::counter!("walgit_push_refused_total", "reason" => "unpack").increment(1);
        return Ok(refusal_report(&caps, &txn, &msg).await);
    }

    let unpack_result: Result<(), String> = Ok(());

    let policy = crate::policy::load(&st.store, &route_id)
        .await
        .map_err(|e| ApiError::Internal(format!("load policy: {e}")))?;
    let baseline = st.policy_baseline.as_ref();
    let mut forces = std::collections::HashSet::<String>::new();
    if crate::policy::needs_force_check(baseline, &policy) {
        for u in &txn.updates {
            if crate::policy::classify(&u.old_oid, &u.new_oid) == crate::policy::RefOp::Update {
                match local.is_ancestor(&u.old_oid, &u.new_oid).await {
                    Ok(true) => {}
                    Ok(false) | Err(_) => {
                        forces.insert(u.name.clone());
                    }
                }
            }
        }
    }
    let ev = crate::policy::evaluate(baseline, &policy, &principal.name, &txn, |u| {
        forces.contains(&u.name)
    });
    if !ev.any_allowed() {
        let report = build_report(&caps, unpack_result, &ev.per_ref).await;
        return Ok(report);
    }
    let mut txn = ev.publish;
    let mut per_ref_policy = ev.per_ref;

    // Release the sync read guard before publishing. `publish_push_synced`
    // reuses this request's freshness check while still syncing after CAS
    // conflicts.
    drop(_guard);

    // Writer-side peel: replicas advertise annotated tags without objects.
    local.fill_peeled(&mut txn);
    let meta = push_meta(&caps, principal, &txn, &request_id);
    let pack_ref = match ingest {
        Ok(Some(p)) => Some(p),
        _ => None,
    };
    let publish = handle
        .publish_push_synced(pack_ref, txn, meta)
        .instrument(tracing::info_span!("receive.publish"))
        .await;
    let (seq, per_ref_pub): (u64, Vec<(String, Result<(), String>)>) = match publish {
        Ok(r) => (
            r.seq,
            r.per_ref
                .into_iter()
                .map(|(n, r)| (n, r.map_err(|e| e.to_string())))
                .collect(),
        ),
        Err(e) => {
            tracing::error!(error = ?e, "receive-pack publish failed");
            return Err(ApiError::Internal(format!("publish failed: {e}")));
        }
    };
    for (name, res) in per_ref_pub {
        if let Some(slot) = per_ref_policy.iter_mut().find(|(n, _)| *n == name) {
            slot.1 = res;
        } else {
            per_ref_policy.push((name, res));
        }
    }
    let per_ref = per_ref_policy;
    tracing::info!(seq, refs = per_ref.len(), "receive-pack published");

    let report = build_report(&caps, unpack_result, &per_ref).await;
    Ok(report)
}

async fn build_report(
    caps: &walgit_git::receive::ReceiveCaps,
    unpack: Result<(), String>,
    per_ref: &[(String, Result<(), String>)],
) -> Vec<u8> {
    let mut w = VecWriter::new();
    let _ = walgit_git::receive::report_status(caps, unpack, per_ref, &mut w).await;
    w.into_inner()
}

/// The report for a push refused before any work: `unpack ng <msg>`, `ng` on
/// every ref; with side-band the message goes out on band 2 first (`remote:
/// …`). Not band 3: git treats it as a fatal transport error ("the remote end
/// hung up unexpectedly") and never shows the per-ref `[remote rejected]`.
async fn refusal_report(
    caps: &walgit_git::receive::ReceiveCaps,
    txn: &walgit_proto::v1::RefTransaction,
    msg: &str,
) -> Vec<u8> {
    let per_ref: Vec<(String, Result<(), String>)> = txn
        .updates
        .iter()
        .map(|u| (u.name.clone(), Err(msg.to_string())))
        .collect();
    let mut out = Vec::new();
    if caps.side_band_64k {
        out.extend_from_slice(&sideband_pkt(2, &format!("{msg}\n")));
    }
    out.extend(build_report(caps, Err(msg.to_string()), &per_ref).await);
    out
}

/// Refuse a push whose commands have not been parsed yet: read the command
/// section only (no pack bytes), answer the refusal, drop the rest of the body.
async fn refuse_push(body: Body, headers: &HeaderMap, msg: String) -> Result<Response, ApiError> {
    let enc = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok());
    let reader = maybe_gunzip(enc, body_to_async_read(body));
    let (txn, caps, _pack) = walgit_git::receive::parse(reader).await.map_err(git_err)?;
    Ok(receive_response(refusal_report(&caps, &txn, &msg).await))
}

/// Which alive maintainer owns `id` (heartbeats; the refusal path only).
async fn maintainer_of(st: &AppState, id: &walgit_git::RepoId) -> Option<String> {
    let hbs = crate::maintain::heartbeats(st).await.ok()?;
    hbs.into_iter()
        .find(|h| {
            walgit_config::repo_listed(&h.repos, id.owner(), id.name())
                && !walgit_config::repo_listed(&h.exclude, id.owner(), id.name())
        })
        .map(|h| h.host)
}

/// Turn a refusal into a 503 the edge and scripts can act on (`Retry-After`),
/// keeping the git report body so git itself still prints the reason.
fn with_retry_after(mut resp: Response, status: StatusCode) -> Response {
    *resp.status_mut() = status;
    resp.headers_mut().insert(
        axum::http::header::RETRY_AFTER,
        HeaderValue::from_static("15"),
    );
    resp
}

fn receive_response(report: Vec<u8>) -> Response {
    let mut resp = (StatusCode::OK, report).into_response();
    resp.headers_mut().insert(
        axum::http::header::CONTENT_TYPE,
        "application/x-git-receive-pack-result".parse().unwrap(),
    );
    resp
}

fn push_meta(
    caps: &walgit_git::receive::ReceiveCaps,
    principal: &crate::auth::Principal,
    txn: &walgit_proto::v1::RefTransaction,
    request_id: &Option<String>,
) -> HashMap<String, String> {
    let mut m = HashMap::new();
    m.insert("agent".to_string(), caps.agent.clone().unwrap_or_default());
    m.insert("principal".to_string(), principal.name.clone());
    m.insert("push_options".to_string(), txn.push_options.join("\n"));
    if let Some(rid) = request_id {
        m.insert("request_id".to_string(), rid.clone());
    }
    m
}

fn is_zero_oid(hex: &str) -> bool {
    hex.chars().all(|c| c == '0') && !hex.is_empty()
}

/// Parse the v2 `fetch` command body (want/have/done/...) into [`UploadPackRequest`].
/// Reads pkt-lines until a flush (stateless) or `done` + flush.
async fn parse_fetch_request(
    mut reader: Box<dyn tokio::io::AsyncRead + Unpin + Send>,
) -> Result<walgit_git::UploadPackRequest, ApiError> {
    let mut req = walgit_git::UploadPackRequest {
        wants: Vec::new(),
        haves: Vec::new(),
        done: false,
        thin_pack: false,
        no_progress: false,
        include_tag: false,
        ofs_delta: false,
        sideband_all: false,
        wait_for_done: false,
        filter: None,
        deepen: None,
        deepen_since: None,
        deepen_not: Vec::new(),
        shallow: Vec::new(),
        want_refs: Vec::new(),
        packfile_uris_protocols: Vec::new(),
        packfile_indexes: false,
    };
    loop {
        let line = walgit_git::pkt::read_pkt_line(&mut reader)
            .await
            .map_err(git_err)?;
        match line {
            None
            | Some(walgit_git::pkt::PktLine::Flush | walgit_git::pkt::PktLine::Delim)
            | Some(walgit_git::pkt::PktLine::ResponseEnd) => break,
            Some(walgit_git::pkt::PktLine::Data(b)) => {
                let s = String::from_utf8_lossy(&b);
                let s = s.trim_end_matches('\n');
                if let Some(hex) = s.strip_prefix("want ") {
                    if let Ok(oid) = gix_hash::ObjectId::from_hex(hex.as_bytes()) {
                        req.wants.push(oid);
                    }
                } else if let Some(hex) = s.strip_prefix("have ") {
                    if let Ok(oid) = gix_hash::ObjectId::from_hex(hex.as_bytes()) {
                        req.haves.push(oid);
                    }
                } else if s == "done" {
                    req.done = true;
                } else if s == "thin-pack" {
                    req.thin_pack = true;
                } else if s == "no-progress" {
                    req.no_progress = true;
                } else if s == "include-tag" {
                    req.include_tag = true;
                } else if s == "ofs-delta" {
                    req.ofs_delta = true;
                } else if s == "sideband-all" {
                    req.sideband_all = true;
                } else if s == "wait-for-done" {
                    req.wait_for_done = true;
                } else if let Some(spec) = s.strip_prefix("filter ") {
                    req.filter = Some(spec.to_string());
                } else if let Some(n) = s.strip_prefix("deepen ") {
                    req.deepen = n.parse().ok();
                } else if let Some(t) = s.strip_prefix("deepen-since ") {
                    req.deepen_since = t.parse().ok();
                } else if let Some(r) = s.strip_prefix("deepen-not ") {
                    req.deepen_not.push(r.to_string());
                } else if let Some(hex) = s.strip_prefix("shallow ") {
                    if let Ok(oid) = gix_hash::ObjectId::from_hex(hex.as_bytes()) {
                        req.shallow.push(oid);
                    }
                } else if let Some(r) = s.strip_prefix("want-ref ") {
                    req.want_refs.push(r.to_string());
                } else if let Some(p) = s.strip_prefix("packfile-uris ") {
                    req.packfile_uris_protocols = p.split(',').map(String::from).collect();
                } else if s == "packfile-indexes" {
                    req.packfile_indexes = true;
                }
            }
        }
    }
    Ok(req)
}

// ---- helpers ----

pub(crate) async fn open_repo(
    st: &AppState,
    id: &walgit_git::RepoId,
    write: bool,
) -> Result<Arc<walgit_wal::RepoHandle>, ApiError> {
    if write && st.cfg.server.auto_create_on_push {
        let format = walgit_git::ObjectFormat::from(st.cfg.git.object_format);
        Ok(st
            .registry
            .open_or_create(id, format)
            .await
            .map_err(wal_err)?)
    } else {
        match st.registry.open(id).await {
            Ok(h) => Ok(h),
            Err(walgit_wal::WalError::NotFound) => Err(ApiError::NotFound(id.to_string())),
            Err(e) => Err(wal_err(e)),
        }
    }
}

fn text_response(
    ct: &str,
    headers: [(axum::http::HeaderName, &'static str); 3],
    body: Vec<u8>,
) -> Response {
    build_response(StatusCode::OK, ct, headers, body)
}

fn stream_response(
    ct: &str,
    headers: [(axum::http::HeaderName, &'static str); 3],
    body: Body,
) -> Response {
    build_response(StatusCode::OK, ct, headers, body)
}

pub(crate) fn build_response<B: axum::response::IntoResponse>(
    status: StatusCode,
    ct: &str,
    extra: [(axum::http::HeaderName, &'static str); 3],
    body: B,
) -> Response {
    let mut resp = (status, body).into_response();
    let h = resp.headers_mut();
    h.insert(axum::http::header::CONTENT_TYPE, ct.parse().unwrap());
    for (k, v) in extra {
        h.insert(k, v.parse().unwrap());
    }
    resp
}

fn is_git_client(headers: &HeaderMap) -> bool {
    headers
        .get(axum::http::header::USER_AGENT)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|ua| {
            ua.starts_with("git/") || ua.starts_with("JGit/") || ua.contains("git-lfs")
        })
}

/// Public base URL (`scheme://host`, no trailing slash) for links we hand to
/// clients: `server.public_url` if configured, else reconstructed from the
/// request (`X-Forwarded-Proto`/`X-Forwarded-Host`/`Host`), else the listen port.
pub(crate) fn request_base_url(st: &AppState, headers: &HeaderMap) -> String {
    if let Some(u) = &st.cfg.server.public_url {
        return u.trim_end_matches('/').to_string();
    }
    let host = headers
        .get("x-forwarded-host")
        .or_else(|| headers.get(axum::http::header::HOST))
        .and_then(|v| v.to_str().ok())
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty());
    match host {
        Some(h) => {
            let scheme = headers
                .get("x-forwarded-proto")
                .and_then(|v| v.to_str().ok())
                .map(|s| s.split(',').next().unwrap_or("").trim().to_string())
                .filter(|s| s == "http" || s == "https")
                .unwrap_or_else(|| {
                    if st.cfg.tls_enabled() {
                        "https".into()
                    } else if h.starts_with("localhost")
                        || h.starts_with("127.")
                        || h.starts_with("[::1]")
                    {
                        "http".into()
                    } else {
                        "https".into()
                    }
                });
            format!("{scheme}://{h}")
        }
        None => crate::listen_url(&st.cfg),
    }
}

/// One-time client setup text for `base_url` (web UI overview + auth errors).
pub(crate) fn client_setup(st: &AppState, base_url: &str) -> String {
    crate::setup::recipes(&st.cfg, base_url, None).setup_text
}

/// Human-readable instructions for authenticating a git client against this host.
pub(crate) fn auth_help_message(
    st: &AppState,
    headers: &HeaderMap,
    e: &crate::auth::AuthError,
) -> String {
    let base = request_base_url(st, headers);
    let host = base
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .to_string();
    let why = match e {
        crate::auth::AuthError::Forbidden => "your identity is not allowed to access this host",
        crate::auth::AuthError::Unavailable => {
            "the token verifier is temporarily unavailable; retry"
        }
        _ => "a valid bearer token is required; refresh or replace an expired token",
    };
    format!(
        "walgit: authentication failed: {why}.\n\
         To authenticate git for {host} (Google Identity-Aware Proxy):\n\
         \n{}",
        client_setup(st, &base)
    )
}

/// Message for a repository whose pack set cannot live on this instance.
fn too_large_message(
    st: &AppState,
    headers: &HeaderMap,
    route: &RepoRoute,
    e: &walgit_wal::WalError,
) -> String {
    let base = request_base_url(st, headers);
    format!(
        "walgit: {e}. Retry on a host with enough capacity, or use a bounded clone:\n  git clone --depth=1 {base}/{}.git\n",
        route.id
    )
}

/// Placement (D29/D30): object work for a repository this host does not serve
/// is refused before any sync — 503 + `Retry-After` (the edge/scripts act on
/// it) with a pkt-line `ERR` naming the host that does serve it.
pub(crate) async fn not_served_here(
    st: &AppState,
    id: &walgit_git::RepoId,
    service: &str,
) -> Option<Response> {
    if walgit_wal::tasks::shutting_down() {
        metrics::counter!("walgit_not_served_here_total", "service" => "draining").increment(1);
        return Some(with_retry_after(
            git_err_response(
                service,
                "walgit: this host is restarting; retry in a few seconds",
            ),
            StatusCode::SERVICE_UNAVAILABLE,
        ));
    }
    if st.cfg.placement.serves(id.owner(), id.name()) {
        return None;
    }
    let host = maintainer_of(st, id)
        .await
        .unwrap_or_else(|| "another host".into());
    metrics::counter!("walgit_not_served_here_total", "service" => service.to_string())
        .increment(1);
    tracing::info!(repo = %id, %host, service, "refused: repository is not served by this host");
    Some(with_retry_after(
        git_err_response(
            service,
            &format!("walgit: {id} is served by {host}; retry shortly"),
        ),
        StatusCode::SERVICE_UNAVAILABLE,
    ))
}

/// A 200 response carrying a pkt-line `ERR <msg>` so git prints it verbatim.
/// The refusal for a fetch whose want list exceeds `git.max_wants`, with the fix.
fn too_many_wants_message(
    st: &AppState,
    headers: &HeaderMap,
    route: &RepoRoute,
    wants: usize,
) -> String {
    let base = request_base_url(st, headers);
    let repo = &route.id;
    format!(
        "walgit: this fetch asks for {wants} objects at once (this host's bound is {max}). That is what a \
         `git clone --filter=blob:none` does right after cloning when it checks out HEAD: every blob of the tree \
         in one request. Clone blobless with a sparse or no checkout instead, then fetch blobs as you need them:\n  \
         git clone --filter=blob:none --sparse {base}/{repo}.git\n  \
         git sparse-checkout add <dir>…\nor, for the whole tree, a full clone: \
         git clone {base}/{repo}.git",
        max = st.cfg.git.max_wants,
    )
}

fn git_err_response(service: &str, msg: &str) -> Response {
    let mut buf = Vec::with_capacity(msg.len() + 64);
    pktline::encode_text(&mut buf, &format!("ERR {msg}"));
    pktline::encode_flush(&mut buf);
    text_response(
        match service {
            "git-receive-pack" => "application/x-git-receive-pack-advertisement",
            _ => "application/x-git-upload-pack-advertisement",
        },
        no_cache_headers(),
        buf,
    )
}

pub(crate) fn auth_err(e: crate::auth::AuthError) -> ApiError {
    match e {
        crate::auth::AuthError::Invalid | crate::auth::AuthError::Unauthorized => {
            ApiError::Unauthorized
        }
        crate::auth::AuthError::Forbidden => ApiError::Forbidden,
        crate::auth::AuthError::Unavailable => {
            ApiError::ServiceUnavailable("auth provider unavailable".into())
        }
    }
}
fn git_err(e: walgit_git::GitError) -> ApiError {
    ApiError::Internal(format!("git: {e}"))
}
pub(crate) fn wal_err(e: walgit_wal::WalError) -> ApiError {
    match &e {
        walgit_wal::WalError::NotFound => ApiError::NotFound(e.to_string()),
        walgit_wal::WalError::TooLarge { .. } => ApiError::ServiceUnavailable(e.to_string()),
        // A store call that timed out / was throttled: fail fast, let the
        // client retry (never hang the request on the bucket).
        walgit_wal::WalError::Store(se) if se.is_retryable() => {
            ApiError::ServiceUnavailable(format!("object store: {se}"))
        }
        _ => ApiError::Internal(format!("wal: {e}")),
    }
}
