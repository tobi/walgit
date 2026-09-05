//! Git pkt-line protocol: async reader/writer, sideband encoder, protocol
//! detection, and v2 command parsing.
//!
//! See `Documentation/git/protocol-pack.txt` and `protocol-v2.txt`.

use std::collections::HashMap;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::GitError;

/// Max payload bytes in a single pkt-line data frame (the 4-byte length header
/// plus this must fit in a u16; git caps total at 65520).
pub const MAX_PKT_DATA: usize = 65516;

#[derive(Debug, Clone)]
pub enum PktLine {
    Data(Bytes),
    Flush,
    Delim,
    ResponseEnd,
}

impl PktLine {
    pub fn as_data(&self) -> Option<&[u8]> {
        match self {
            PktLine::Data(b) => Some(b),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    V0,
    V2,
}

impl Protocol {
    /// Parse the `Git-Protocol` HTTP header value. `version=2` -> V2, else V0.
    pub fn from_git_protocol_header(header: Option<&str>) -> Self {
        match header {
            Some(h) => {
                for part in h.split(|c: char| c == ';' || c.is_ascii_whitespace()) {
                    let part = part.trim();
                    if part.eq_ignore_ascii_case("version=2") {
                        return Protocol::V2;
                    }
                }
                Protocol::V0
            }
            None => Protocol::V0,
        }
    }

    pub fn git_protocol_env(&self) -> &'static str {
        match self {
            Protocol::V2 => "version=2",
            Protocol::V0 => "version=0",
        }
    }
}

/// Read a single pkt-line from `r`.
///
/// Returns `Ok(None)` on an immediate flush at EOF position is represented as
/// `PktLine::Flush`; this helper returns `Ok(PktLine::Flush)` for `0000`,
/// `Ok(PktLine::Delim)` for `0001`, `Ok(PktLine::ResponseEnd)` for `0002`.
/// Returns `Ok(None)` only when the stream is at EOF with no bytes available
/// (clean terminator).
pub async fn read_pkt_line<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<PktLine>, GitError> {
    let mut hdr = [0u8; 4];
    let n = read_exact_or_eof(r, &mut hdr).await?;
    if n == 0 {
        return Ok(None);
    }
    if n < 4 {
        return Err(GitError::Protocol("short pkt-line header".into()));
    }
    let len = parse_pkt_len(hdr)?;
    match len {
        0 => return Ok(Some(PktLine::Flush)),
        1 => return Ok(Some(PktLine::Delim)),
        2 => return Ok(Some(PktLine::ResponseEnd)),
        _ => {}
    }
    if len < 4 {
        return Err(GitError::Protocol(format!("invalid pkt-line length {len}")));
    }
    let body = len - 4;
    let mut buf = BytesMut::zeroed(body);
    if body > 0 {
        r.read_exact(&mut buf).await.map_err(io_to_git)?;
    }
    Ok(Some(PktLine::Data(buf.freeze())))
}

async fn read_exact_or_eof<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut [u8],
) -> Result<usize, GitError> {
    let mut filled = 0;
    let mut remaining = buf;
    while !remaining.is_empty() {
        let n = r.read(remaining).await.map_err(io_to_git)?;
        if n == 0 {
            break;
        }
        remaining = remaining
            .get_mut(n..)
            .ok_or_else(|| GitError::Protocol("reader exceeded buffer".into()))?;
        filled += n;
    }
    Ok(filled)
}

fn parse_pkt_len(hdr: [u8; 4]) -> Result<usize, GitError> {
    let mut val = 0usize;
    for b in hdr {
        let d = match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'f' => b - b'a' + 10,
            b'A'..=b'F' => b - b'A' + 10,
            _ => return Err(GitError::Protocol(format!("non-hex pkt-line char {b:#x}"))),
        };
        val = val * 16 + d as usize;
    }
    Ok(val)
}

fn pkt_len_hex(len: usize) -> String {
    format!("{len:04x}")
}

/// Write a data pkt-line. Splits `data` into `MAX_PKT_DATA`-sized frames.
pub async fn write_pkt_line<W: AsyncWrite + Unpin>(w: &mut W, data: &[u8]) -> Result<(), GitError> {
    if data.is_empty() {
        // An empty data pkt-line is "0004".
        w.write_all(b"0004").await.map_err(io_to_git)?;
        return Ok(());
    }
    for chunk in data.chunks(MAX_PKT_DATA) {
        let total = chunk.len() + 4;
        w.write_all(pkt_len_hex(total).as_bytes())
            .await
            .map_err(io_to_git)?;
        w.write_all(chunk).await.map_err(io_to_git)?;
    }
    Ok(())
}

/// Write a flush pkt-line (`0000`).
pub async fn write_flush<W: AsyncWrite + Unpin>(w: &mut W) -> Result<(), GitError> {
    w.write_all(b"0000").await.map_err(io_to_git)
}

/// Write a delim pkt-line (`0001`).
pub async fn write_delim<W: AsyncWrite + Unpin>(w: &mut W) -> Result<(), GitError> {
    w.write_all(b"0001").await.map_err(io_to_git)
}

/// Write a response-end pkt-line (`0002`).
pub async fn write_response_end<W: AsyncWrite + Unpin>(w: &mut W) -> Result<(), GitError> {
    w.write_all(b"0002").await.map_err(io_to_git)
}

/// Sideband-64k multiplexer. Frames data on channel 1, progress on channel 2,
/// errors on channel 3, each as pkt-lines with a one-byte channel prefix.
pub struct Sideband<W: AsyncWrite + Unpin> {
    w: W,
}

impl<W: AsyncWrite + Unpin> Sideband<W> {
    pub fn new(w: W) -> Self {
        Sideband { w }
    }

    pub fn inner_mut(&mut self) -> &mut W {
        &mut self.w
    }

    pub fn into_inner(self) -> W {
        self.w
    }

    /// Write pack data (channel 1), chunked to fit the 65520 pkt-line limit.
    pub async fn write_data(&mut self, buf: &[u8]) -> Result<(), GitError> {
        self.write_channel(1, buf).await
    }

    /// Write progress messages (channel 2).
    pub async fn write_progress(&mut self, msg: &[u8]) -> Result<(), GitError> {
        self.write_channel(2, msg).await
    }

    /// Write an error message (channel 3).
    pub async fn write_error(&mut self, msg: &[u8]) -> Result<(), GitError> {
        self.write_channel(3, msg).await
    }

    async fn write_channel(&mut self, channel: u8, buf: &[u8]) -> Result<(), GitError> {
        // Max data per frame: MAX_PKT_DATA minus the 1-byte channel prefix.
        const MAX: usize = MAX_PKT_DATA - 1;
        if buf.is_empty() {
            let total = 4 + 1;
            self.w
                .write_all(pkt_len_hex(total).as_bytes())
                .await
                .map_err(io_to_git)?;
            self.w.write_all(&[channel]).await.map_err(io_to_git)?;
            return Ok(());
        }
        for chunk in buf.chunks(MAX) {
            let total = chunk.len() + 4 + 1;
            self.w
                .write_all(pkt_len_hex(total).as_bytes())
                .await
                .map_err(io_to_git)?;
            self.w.write_all(&[channel]).await.map_err(io_to_git)?;
            self.w.write_all(chunk).await.map_err(io_to_git)?;
        }
        Ok(())
    }

    /// Flush the sideband stream (writes a `0000` flush pkt-line).
    pub async fn flush(&mut self) -> Result<(), GitError> {
        self.w.write_all(b"0000").await.map_err(io_to_git)
    }
}

/// A parsed v2 command: its name, capability lines (key -> optional value), and
/// positional argument lines.
#[derive(Debug, Default, Clone)]
pub struct V2Command {
    pub name: String,
    /// Capabilities/arguments of the form `key` or `key=value`, collected into
    /// a map. For capabilities without a value the value is an empty string.
    pub caps: HashMap<String, String>,
    /// Raw argument lines in order (everything that isn't `key=value` shaped,
    /// plus duplicates that the map would collapse).
    pub args: Vec<String>,
}

impl V2Command {
    pub fn cap(&self, key: &str) -> Option<&str> {
        self.caps.get(key).map(std::string::String::as_str)
    }
    pub fn has_cap(&self, key: &str) -> bool {
        self.caps.contains_key(key)
    }
}

/// Read the v2 command preamble: the first `command=<name>` pkt-line plus all
/// capability/argument lines up to (and consuming) the flush/delim that
/// terminates the command header. Returns the parsed command and the reader,
/// positioned for the command body (e.g. fetch `want`/`have` lines).
pub async fn read_command<R: AsyncRead + Unpin>(mut r: R) -> Result<(V2Command, R), GitError> {
    let mut cmd = V2Command::default();
    let mut saw_name = false;
    loop {
        let line = read_pkt_line(&mut r).await?;
        let Some(line) = line else { break };
        match line {
            PktLine::Flush | PktLine::Delim | PktLine::ResponseEnd => break,
            PktLine::Data(b) => {
                let s = String::from_utf8_lossy(&b);
                let s = s.trim_end().to_string();
                if let Some(rest) = s.strip_prefix("command=") {
                    cmd.name = rest.to_string();
                    saw_name = true;
                } else if let Some((k, v)) = s.split_once('=') {
                    cmd.caps.insert(k.to_string(), v.to_string());
                } else {
                    // Could be a capability ("thin-pack") or a positional arg.
                    if !s.is_empty() && !s.contains(' ') {
                        // Bare capability token; record with empty value so
                        // has_cap works, but also keep in args for fidelity.
                        cmd.caps.entry(s.clone()).or_insert_with(String::new);
                    }
                    cmd.args.push(s);
                }
            }
        }
    }
    if !saw_name && cmd.name.is_empty() {
        return Err(GitError::Protocol("v2 request missing command=".into()));
    }
    Ok((cmd, r))
}

/// Arguments for the v2 `ls-refs` command.
#[derive(Debug, Default, Clone)]
pub struct LsRefsRequest {
    pub prefixes: Vec<String>,
    pub symrefs: bool,
    pub peel: bool,
    pub unborn: bool,
}

/// Parse `ls-refs` capability/arg lines (from a [`V2Command`]) into the typed
/// request. Git sends command-specific lines after the command delimiter, so
/// callers that have not consumed that section should use
/// [`read_ls_refs_args`] after this function.
pub fn parse_ls_refs(cmd: &V2Command) -> LsRefsRequest {
    let mut req = LsRefsRequest::default();
    for a in &cmd.args {
        parse_ls_refs_line(&mut req, a);
    }
    for (key, value) in &cmd.caps {
        if key == "ref-prefix" && !value.is_empty() {
            req.prefixes.push(value.clone());
        } else if value.is_empty() {
            parse_ls_refs_line(&mut req, key);
        }
    }
    req
}

fn parse_ls_refs_line(req: &mut LsRefsRequest, line: &str) {
    if let Some(p) = line.strip_prefix("ref-prefix ") {
        req.prefixes.push(p.to_string());
    } else if let Some(p) = line.strip_prefix("ref-prefix=") {
        req.prefixes.push(p.to_string());
    } else if line == "symrefs" {
        req.symrefs = true;
    } else if line == "peel" {
        req.peel = true;
    } else if line == "unborn" {
        req.unborn = true;
    }
}

/// Read the command-specific section of a protocol-v2 `ls-refs` request.
/// The command preamble parser stops after the delimiter; Git then sends
/// `symrefs`, `peel`, `unborn`, and one `ref-prefix <prefix>` per line.
pub async fn read_ls_refs_args<R: AsyncRead + Unpin>(
    mut r: R,
    mut req: LsRefsRequest,
) -> Result<LsRefsRequest, GitError> {
    loop {
        let line = read_pkt_line(&mut r).await?;
        match line {
            Some(PktLine::Data(data)) => {
                let line = String::from_utf8_lossy(&data);
                parse_ls_refs_line(&mut req, line.trim_end());
            }
            Some(PktLine::Delim) => {}
            Some(PktLine::Flush | PktLine::ResponseEnd) | None => break,
        }
    }
    Ok(req)
}

/// Arguments for the v2 `object-info` command.
#[derive(Debug, Default, Clone)]
pub struct ObjectInfoRequest {
    pub oids: Vec<String>,
}

pub fn parse_object_info(cmd: &V2Command) -> ObjectInfoRequest {
    let mut req = ObjectInfoRequest::default();
    for a in &cmd.args {
        if let Some(o) = a.strip_prefix("oid ") {
            req.oids.push(o.to_string());
        }
    }
    req
}

/// `bundle-uri` has no arguments in v2.
pub fn parse_bundle_uri(_cmd: &V2Command) {}

fn io_to_git(e: std::io::Error) -> GitError {
    GitError::Io(e)
}

/// Encode a literal data pkt-line into a buffer (sync helper for building
/// advertisement/section bytes).
#[expect(
    clippy::indexing_slicing,
    reason = "Each index is masked to 0..16 for the 16-byte hex table"
)]
pub fn encode_data(buf: &mut Vec<u8>, data: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let total = data.len() + 4;
    buf.extend_from_slice(&[
        HEX[(total >> 12) & 0xf],
        HEX[(total >> 8) & 0xf],
        HEX[(total >> 4) & 0xf],
        HEX[total & 0xf],
    ]);
    buf.extend_from_slice(data);
}
pub fn encode_flush(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"0000");
}

pub fn encode_delim(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"0001");
}

pub fn encode_response_end(buf: &mut Vec<u8>) {
    buf.extend_from_slice(b"0002");
}

/// Encode a v2 section: a `<section>\n` header pkt-line, then `lines` (each
/// already including any trailing newline the caller wants), then a flush.
pub fn encode_section(buf: &mut Vec<u8>, section: &str, lines: &[&str]) {
    let mut header = Vec::with_capacity(section.len() + 1);
    header.extend_from_slice(section.as_bytes());
    header.push(b'\n');
    encode_data(buf, &header);
    for l in lines {
        encode_data(buf, l.as_bytes());
    }
    encode_flush(buf);
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_ls_refs_accepts_one_ref_prefix_per_argument() {
        let cmd = V2Command {
            name: "ls-refs".into(),
            caps: HashMap::new(),
            args: vec![
                "ref-prefix refs/heads/ref-199".into(),
                "ref-prefix refs/tags/".into(),
            ],
        };
        let req = parse_ls_refs(&cmd);
        assert_eq!(req.prefixes, vec!["refs/heads/ref-199", "refs/tags/"]);
    }

    #[tokio::test]
    async fn read_ls_refs_args_reads_after_delimiter() {
        let mut body = Vec::new();
        encode_delim(&mut body);
        encode_data(&mut body, b"ref-prefix refs/heads/ref-199\n");
        encode_data(&mut body, b"peel\n");
        encode_flush(&mut body);
        let req = read_ls_refs_args(std::io::Cursor::new(body), LsRefsRequest::default())
            .await
            .unwrap();
        assert_eq!(req.prefixes, vec!["refs/heads/ref-199"]);
        assert!(req.peel);
    }
}
