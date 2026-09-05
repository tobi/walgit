//! Streaming bridges between axum/hyper bodies and tokio `AsyncRead`/`AsyncWrite`.
//!
//! * Incoming request body -> `AsyncRead` (with optional gzip inflate).
//! * Outgoing response: the write half of a tokio duplex pipe rendered as a
//!   hyper `Body` via `Body::from_stream`, so git pkt-line / pack output streams
//!   straight to the client with no full buffering.

use std::io;

use axum::body::Body;
use futures::stream::StreamExt;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::io::{ReaderStream, StreamReader};

/// Convert an axum request body into an `AsyncRead`. Map errors to `io::Error`.
pub fn body_to_async_read(body: Body) -> impl AsyncRead + Unpin + Send {
    let stream = body
        .into_data_stream()
        .map(|res| res.map_err(|e| io::Error::other(e.to_string())));
    StreamReader::new(stream)
}

/// Wrap an `AsyncRead` in gzip decompression when `content_encoding` is `gzip`.
/// Returns the original reader otherwise. The gzip decoder requires `AsyncBufRead`,
/// so the reader is wrapped in a `BufReader`.
pub fn maybe_gunzip<R: AsyncRead + Unpin + Send + 'static>(
    content_encoding: Option<&str>,
    reader: R,
) -> Box<dyn AsyncRead + Unpin + Send> {
    match content_encoding
        .map(|s| s.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("gzip") => Box::new(async_compression::tokio::bufread::GzipDecoder::new(
            tokio::io::BufReader::new(reader),
        )),
        _ => Box::new(reader),
    }
}

/// Convert an `AsyncRead` into an axum `Body` (streamed, chunked).
pub fn body_from_async_read<R: AsyncRead + Unpin + Send + 'static>(reader: R) -> Body {
    Body::from_stream(ReaderStream::new(reader))
}

/// A duplex pipe: write on the returned `DuplexStream` (impl `AsyncWrite`),
/// read the resulting `Body` on the other side. Use when an API hands us an
/// `AsyncWrite` to fill (e.g. `LocalRepo::upload_pack`). Drop the writer to
/// signal EOF to the reader.
pub fn write_body_pipe(buf: usize) -> (tokio::io::DuplexStream, Body) {
    let (a, b) = tokio::io::duplex(buf);
    (a, Body::from_stream(ReaderStream::new(b)))
}

/// A simple `AsyncWrite` that collects bytes into a `Vec<u8>`. Used to render
/// small pkt-line responses (report-status, ls-refs) into a buffer.
pub struct VecWriter(pub Vec<u8>);

impl Default for VecWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl VecWriter {
    pub fn new() -> Self {
        Self(Vec::new())
    }
    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }
}

impl AsyncWrite for VecWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<io::Result<usize>> {
        self.get_mut().0.extend_from_slice(buf);
        std::task::Poll::Ready(Ok(buf.len()))
    }
    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<io::Result<()>> {
        std::task::Poll::Ready(Ok(()))
    }
}
