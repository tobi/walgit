//! The SSE envelope (web/API.md §2b): any JSON endpoint whose answer needs
//! long work (materializing packs, downloading pack indexes, reading objects
//! from the store, running a maintenance task) can stream, when the client
//! sends `Accept: text/event-stream`, a sequence of packets
//!
//! ```text
//! event: notice    data: {"text": "..."}
//! event: progress  data: {"label": "...", "done": n, "total": n?, "unit": "bytes", "percent": 42.0?}
//! event: task      data: {TaskRecord}                 (a background task this request depends on)
//! event: error     data: {"status": 503, "message": "..."}      terminal
//! event: result    data: <exactly the JSON the plain endpoint returns>  terminal
//! ```
//!
//! plus `: keepalive` comments. Packets come from [`walgit_wal::Progress`]
//! (the repo's channel + a request-local reporter). Fast answers never use the
//! envelope: the server replies with plain cacheable JSON whenever it can
//! answer immediately, so the browser HTTP cache keeps working.

use std::convert::Infallible;
use std::future::Future;

use axum::body::Body;
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures::StreamExt;
use serde::Serialize;
use walgit_wal::Progress;
use walgit_wal::progress::ProgressRx;

use crate::error::ApiError;

pub const KEEPALIVE: std::time::Duration = std::time::Duration::from_secs(10);

pub fn wants_sse(headers: &HeaderMap) -> bool {
    headers
        .get_all(header::ACCEPT)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .any(|a| a.contains("text/event-stream"))
}

pub fn packet(event: &str, data: &impl Serialize) -> Bytes {
    Bytes::from(format!(
        "event: {event}\ndata: {}\n\n",
        serde_json::to_string(data).unwrap_or_else(|_| "null".into())
    ))
}
pub fn packet_raw(event: &str, json: &[u8]) -> Bytes {
    let mut b = Vec::with_capacity(json.len() + event.len() + 16);
    b.extend_from_slice(b"event: ");
    b.extend_from_slice(event.as_bytes());
    b.extend_from_slice(b"\ndata: ");
    b.extend_from_slice(json);
    b.extend_from_slice(b"\n\n");
    Bytes::from(b)
}
pub fn progress_packet(p: &Progress) -> Bytes {
    packet(p.event_name(), p)
}
#[derive(Serialize)]
pub struct ErrorPacket<'a> {
    pub status: u16,
    pub message: &'a str,
}
pub fn error_packet(e: &ApiError) -> Bytes {
    packet(
        "error",
        &ErrorPacket {
            status: e.status().as_u16(),
            message: &e.message(),
        },
    )
}

pub fn sse_response(
    stream: impl futures::Stream<Item = Result<Bytes, Infallible>> + Send + 'static,
) -> Response {
    let mut resp = Response::new(Body::from_stream(stream));
    *resp.status_mut() = StatusCode::OK;
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        "text/event-stream; charset=utf-8".parse().unwrap(),
    );
    resp.headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    resp.headers_mut()
        .insert("X-Accel-Buffering", "no".parse().unwrap());
    resp
}

/// A finished JSON answer: body + the headers the plain response carries.
pub struct Rendered {
    pub body: Bytes,
    pub content_type: &'static str,
    pub cache_control: &'static str,
    pub etag: Option<String>,
}

impl Rendered {
    pub fn json(body: Bytes, cache_control: &'static str, etag: Option<String>) -> Self {
        Rendered {
            body,
            content_type: "application/json",
            cache_control,
            etag,
        }
    }
    /// Plain HTTP response (honours `If-None-Match` when an `ETag` is set).
    pub fn into_response(self, req: &HeaderMap) -> Response {
        if let Some(etag) = &self.etag {
            let hit = req
                .get(header::IF_NONE_MATCH)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| v.split(',').any(|t| t.trim() == etag || t.trim() == "*"));
            if hit {
                let mut r = StatusCode::NOT_MODIFIED.into_response();
                r.headers_mut().insert(header::ETAG, etag.parse().unwrap());
                r.headers_mut()
                    .insert(header::CACHE_CONTROL, self.cache_control.parse().unwrap());
                return r;
            }
        }
        let mut r = (StatusCode::OK, Body::from(self.body)).into_response();
        r.headers_mut()
            .insert(header::CONTENT_TYPE, self.content_type.parse().unwrap());
        r.headers_mut()
            .insert(header::CACHE_CONTROL, self.cache_control.parse().unwrap());
        if let Some(e) = &self.etag {
            r.headers_mut().insert(header::ETAG, e.parse().unwrap());
        }
        r
    }
}

/// Stream `work` as an envelope: progress from `sources` (repo channel,
/// request-local reporter, ...) until the work resolves, then `result` or
/// `error`. The work keeps running if the client disconnects (it is usually a
/// shared sync that other requests wait on too).
pub fn envelope<F>(sources: Vec<ProgressRx>, work: F) -> Response
where
    F: Future<Output = Result<Rendered, ApiError>> + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(256);
    // Forward progress packets.
    let mut forwarders = Vec::new();
    for mut src in sources {
        let tx = tx.clone();
        forwarders.push(tokio::spawn(async move {
            loop {
                match src.recv().await {
                    Ok(p) => {
                        if tx.send(progress_packet(&p)).await.is_err() {
                            break;
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                }
            }
        }));
    }
    // Keepalive.
    {
        let tx = tx.clone();
        forwarders.push(tokio::spawn(async move {
            loop {
                tokio::time::sleep(KEEPALIVE).await;
                if tx
                    .send(Bytes::from_static(b": keepalive\n\n"))
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }));
    }
    tokio::spawn(async move {
        let res = work.await;
        // Let already-queued progress packets go first.
        let pkt = match res {
            Ok(r) => packet_raw("result", &r.body),
            Err(e) => error_packet(&e),
        };
        let _ = tx.send(pkt).await;
        for f in forwarders {
            f.abort();
        }
        drop(tx);
    });
    // Open the stream with a comment so proxies flush headers immediately.
    let head =
        futures::stream::once(async { Ok::<_, Infallible>(Bytes::from_static(b": walgit\n\n")) });
    let body = tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok::<_, Infallible>);
    sse_response(head.chain(body))
}

/// Stream a (running or finished) task: replay, then live packets, then the
/// terminal `result` (`{"task": TaskRecord, "value": ...}`) or `error`.
pub fn task_stream(state: std::sync::Arc<walgit_wal::tasks::TaskState>) -> Response {
    let (replay, mut live, outcome) = state.attach();
    let (tx, rx) = tokio::sync::mpsc::channel::<Bytes>(256);
    tokio::spawn(async move {
        let _ = tx.send(packet("task", &state.record())).await;
        for p in &replay {
            if tx.send(progress_packet(p)).await.is_err() {
                return;
            }
        }
        let send_outcome = |o: Result<walgit_wal::tasks::TaskOutcome, (u16, String)>| match o {
            Ok(out) => packet("result", &out),
            Err((status, message)) => packet(
                "error",
                &serde_json::json!({ "status": status, "message": message, "task": state.record() }),
            ),
        };
        if let Some(o) = outcome {
            let _ = tx.send(send_outcome(o)).await;
            return;
        }
        let mut done = state.done_rx();
        loop {
            // Finished between attach() and here (send_replace stores the value).
            if *done.borrow() {
                break;
            }
            tokio::select! {
                r = live.recv() => match r {
                    Ok(p) => { if tx.send(progress_packet(&p)).await.is_err() { return; } }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(_) => break,
                },
                _ = done.changed() => {
                    if *done.borrow() { break; }
                }
                () = tokio::time::sleep(KEEPALIVE) => {
                    if tx.send(Bytes::from_static(b": keepalive\n\n")).await.is_err() { return; }
                }
            }
        }
        // Drain anything still buffered after completion.
        while let Ok(p) = live.try_recv() {
            let _ = tx.send(progress_packet(&p)).await;
        }
        if let Some(o) = state.outcome() {
            let _ = tx.send(send_outcome(o)).await;
        }
    });
    let body = tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok::<_, Infallible>);
    sse_response(body)
}
