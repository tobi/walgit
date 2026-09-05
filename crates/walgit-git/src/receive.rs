//! receive-pack request parsing and report-status framing.
//!
//! The receive-pack request body (stateless-rpc) is:
//!   command pkt-lines: "<old-oid> <new-oid> <refname>\0<caps>" on the first
//!   line, then "<old-oid> <new-oid> <refname>" for each subsequent command,
//!   terminated by a flush.
//!   If `push-options` was negotiated, a pkt-line section of option values
//!   follows, terminated by another flush.
//!   The packfile bytes (starting with `PACK`) follow immediately in the same
//!   stream.

use std::collections::VecDeque;

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};

use crate::pkt::{self, PktLine};
use crate::{GitError, RefSnapshotData};

/// Capabilities negotiated by the client in the first receive-pack command.
#[derive(Debug, Default, Clone)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "Git capabilities are independent protocol flags"
)]
pub struct ReceiveCaps {
    pub report_status: bool,
    pub report_status_v2: bool,
    pub side_band_64k: bool,
    pub atomic: bool,
    pub quiet: bool,
    pub push_options: bool,
    pub ofs_delta: bool,
    pub agent: Option<String>,
    pub object_format: Option<String>,
    /// Shallow commits announced by a shallow client (`shallow <oid>` lines
    /// before the commands; pushes from `--depth` clones).
    pub shallow: Vec<String>,
}

/// A reader that first drains an in-memory `prefix` (bytes the parser
/// over-read from the body) and then continues reading from the underlying
/// `AsyncRead`. The pack bytes start here.
pub struct PrefixedReader<R: AsyncRead + Unpin> {
    prefix: VecDeque<u8>,
    inner: R,
}

impl<R: AsyncRead + Unpin> PrefixedReader<R> {
    pub fn new(prefix: Vec<u8>, inner: R) -> Self {
        PrefixedReader {
            prefix: prefix.into(),
            inner,
        }
    }

    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for PrefixedReader<R> {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if !this.prefix.is_empty() {
            let n = this.prefix.len().min(buf.remaining());
            for byte in this.prefix.drain(..n) {
                buf.put_slice(&[byte]);
            }
            return std::task::Poll::Ready(Ok(()));
        }
        std::pin::Pin::new(&mut this.inner).poll_read(cx, buf)
    }
}

/// Parse the receive-pack request head (commands + capabilities + optional
/// push-options) from `r`. Returns the ref transaction, the negotiated
/// capabilities, and a [`PrefixedReader`] positioned at the start of the
/// packfile bytes (which may be absent for a pure ref-deletion request).
pub async fn parse<R: AsyncRead + Unpin>(
    mut r: R,
) -> Result<
    (
        walgit_proto::v1::RefTransaction,
        ReceiveCaps,
        PrefixedReader<R>,
    ),
    GitError,
> {
    let mut updates: Vec<walgit_proto::v1::RefUpdate> = Vec::new();
    let mut caps = ReceiveCaps::default();
    let mut push_options: Vec<String> = Vec::new();
    let mut atomic = false;

    // A shallow client announces its shallow commits first ("shallow <oid>"
    // lines, git send-pack's advertise_shallow_grafts): record them (they only
    // matter for connectivity of what the client *has*, which we never assume)
    // and move on to the first command line, which carries the NUL +
    // capability string.
    let first = loop {
        match pkt::read_pkt_line(&mut r).await? {
            Some(PktLine::Data(b)) if b.starts_with(b"shallow ") => {
                caps.shallow.push(
                    String::from_utf8_lossy(
                        b.strip_prefix(b"shallow ")
                            .ok_or_else(|| GitError::Protocol("missing shallow prefix".into()))?,
                    )
                    .trim()
                    .to_string(),
                );
            }
            other => break other,
        }
    };
    match first {
        None | Some(PktLine::Flush) => {
            // No commands at all: empty transaction. Pack may or may not follow.
            let txn = walgit_proto::v1::RefTransaction {
                updates,
                push_options,
                atomic,
            };
            return Ok((txn, caps, PrefixedReader::new(Vec::new(), r)));
        }
        Some(PktLine::Delim | PktLine::ResponseEnd) => {
            return Err(GitError::Protocol(
                "unexpected delim before commands".into(),
            ));
        }
        Some(PktLine::Data(b)) => {
            let (update, cap_str) = parse_command_line(&b)?;
            updates.push(update);
            apply_caps(&mut caps, &cap_str);
            if caps.atomic {
                atomic = true;
            }
        }
    }

    // Remaining command lines until flush.
    loop {
        let line = pkt::read_pkt_line(&mut r).await?;
        match line {
            None | Some(PktLine::Flush | PktLine::Delim | PktLine::ResponseEnd) => break,
            Some(PktLine::Data(b)) if b.starts_with(b"shallow ") => {
                caps.shallow.push(
                    String::from_utf8_lossy(
                        b.strip_prefix(b"shallow ")
                            .ok_or_else(|| GitError::Protocol("missing shallow prefix".into()))?,
                    )
                    .trim()
                    .to_string(),
                );
            }
            Some(PktLine::Data(b)) => {
                let (update, _) = parse_command_line(&b)?;
                updates.push(update);
            }
        }
    }

    // Push-options section, if negotiated.
    if caps.push_options {
        loop {
            let line = pkt::read_pkt_line(&mut r).await?;
            match line {
                None | Some(PktLine::Flush | PktLine::Delim | PktLine::ResponseEnd) => break,
                Some(PktLine::Data(b)) => {
                    push_options.push(
                        String::from_utf8_lossy(&b)
                            .trim_end_matches('\n')
                            .to_string(),
                    );
                }
            }
        }
    }

    let txn = walgit_proto::v1::RefTransaction {
        updates,
        push_options,
        atomic,
    };
    Ok((txn, caps, PrefixedReader::new(Vec::new(), r)))
}

fn parse_command_line(b: &[u8]) -> Result<(walgit_proto::v1::RefUpdate, String), GitError> {
    // First line: "<old> <new> <ref>\0<caps>". Subsequent lines have no caps.
    let mut sections = b.splitn(2, |&c| c == 0);
    let cmd_bytes = sections.next().unwrap_or_default();
    let caps_bytes = sections.next().unwrap_or_default();
    let s = String::from_utf8_lossy(cmd_bytes);
    let s = s.trim_end_matches('\n');
    let mut parts = s.splitn(3, ' ');
    let old = parts
        .next()
        .ok_or_else(|| GitError::Protocol("missing old oid".into()))?;
    let new = parts
        .next()
        .ok_or_else(|| GitError::Protocol("missing new oid".into()))?;
    let name = parts
        .next()
        .ok_or_else(|| GitError::Protocol("missing ref name".into()))?;
    let update = walgit_proto::v1::RefUpdate {
        name: name.to_string(),
        old_oid: old.to_string(),
        new_oid: new.to_string(),
        new_symbolic_target: String::new(),
        new_peeled: String::new(),
    };
    crate::validate_ref_update(&update)?;
    Ok((update, String::from_utf8_lossy(caps_bytes).to_string()))
}

fn apply_caps(caps: &mut ReceiveCaps, s: &str) {
    for tok in s.split([' ', '\n']) {
        let tok = tok.trim();
        if tok.is_empty() {
            continue;
        }
        match tok {
            "report-status" => caps.report_status = true,
            "report-status-v2" => caps.report_status_v2 = true,
            "side-band-64k" => caps.side_band_64k = true,
            "atomic" => caps.atomic = true,
            "quiet" => caps.quiet = true,
            "push-options" => caps.push_options = true,
            "ofs-delta" => caps.ofs_delta = true,
            _ => match tok.split_once('=') {
                Some(("agent", value)) => caps.agent = Some(value.to_string()),
                Some(("object-format", value)) => caps.object_format = Some(value.to_string()),
                _ => {}
            },
        }
    }
}

/// Write a report-status (or report-status-v2) response, optionally sideband-
/// framed when `caps.side_band_64k`. Always ends with a flush.
///
/// `unpack` is the unpack-ack result; `per_ref` is `(ref_name, result)` for
/// each command. The output is written to `out`.
pub async fn report_status<W: AsyncWrite + Unpin>(
    caps: &ReceiveCaps,
    unpack: Result<(), String>,
    per_ref: &[(String, Result<(), String>)],
    out: W,
) -> Result<(), GitError> {
    let mut body = Vec::new();
    // unpack line
    match &unpack {
        Ok(()) => body.extend_from_slice(b"unpack ok\n"),
        Err(msg) => {
            body.extend_from_slice(b"unpack ng ");
            body.extend_from_slice(msg.as_bytes());
            body.push(b'\n');
        }
    }
    // per-ref lines
    let use_v2 = caps.report_status_v2;
    for (name, res) in per_ref {
        match res {
            Ok(()) => {
                body.extend_from_slice(b"ok ");
                body.extend_from_slice(name.as_bytes());
                body.push(b'\n');
            }
            Err(msg) => {
                // Both v0 and v2: one pkt-line `ng <ref> <reason>`.
                // A reason on its own line is parsed as another status token
                // (`invalid ref status from remote: …`).
                body.extend_from_slice(b"ng ");
                body.extend_from_slice(name.as_bytes());
                body.push(b' ');
                body.extend_from_slice(msg.as_bytes());
                body.push(b'\n');
            }
        }
    }
    // report-status-v2 allows `option …` lines only after an `ok <ref>`; a rejected atomic
    // transaction is reported as `ng <ref> <reason>` per command and nothing else. (An `option
    // atomic` line after the `ng`s made every rejected pusher see "'option' without a matching
    // 'ok/ng' directive" instead of the reason.)
    let _ = use_v2;

    // Encode the complete status once. Besides avoiding repeated formatting,
    // this keeps the non-sideband response to one large write; a 20k-ref
    // mirror otherwise performs one async write per ref.
    let mut framed = Vec::with_capacity(body.len() + per_ref.len() * 8 + 4);
    for line in body.split_inclusive(|&b| b == b'\n') {
        pkt::encode_data(&mut framed, line);
    }
    pkt::encode_flush(&mut framed);

    if caps.side_band_64k {
        // The report-status must be pkt-line encoded *before* being wrapped
        // in sideband frames; git demuxes channel 1 then parses pkt-lines.
        let mut sb = pkt::Sideband::new(out);
        sb.write_data(&framed).await?;
        sb.flush().await?;
    } else {
        let mut out = out;
        out.write_all(&framed).await.map_err(GitError::Io)?;
        out.flush().await.map_err(GitError::Io)?;
    }
    Ok(())
}

/// Convenience: build a [`RefTransaction`] from a ref snapshot diff is not
/// provided; callers construct transactions directly. This helper converts a
/// [`RefSnapshotData`] into a transaction that creates all refs (`old_oid` =
/// zero), useful for materializing a checkpoint.
pub fn txn_from_snapshot(snap: &RefSnapshotData) -> walgit_proto::v1::RefTransaction {
    let mut updates: Vec<walgit_proto::v1::RefUpdate> = snap
        .refs
        .iter()
        .map(|r| walgit_proto::v1::RefUpdate {
            name: r.name.clone(),
            old_oid: String::new(),
            new_oid: r.oid.clone(),
            new_symbolic_target: String::new(),
            new_peeled: String::new(),
        })
        .collect();
    if !snap.head_target.is_empty() {
        updates.push(walgit_proto::v1::RefUpdate {
            name: "HEAD".to_string(),
            old_oid: String::new(),
            new_oid: String::new(),
            new_symbolic_target: snap.head_target.clone(),
            new_peeled: String::new(),
        });
    }
    walgit_proto::v1::RefTransaction {
        updates,
        push_options: Vec::new(),
        atomic: true,
    }
}
