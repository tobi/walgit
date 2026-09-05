//! Sync pkt-line wire-format *encode* helpers for building response buffers
//! (v2 capability advertisement, ls-refs lines). Async reading, typed command
//! parsing and sideband framing are provided by `walgit_git::pkt`; this module
//! only writes the framing into a `Vec<u8>` so axum can stream it.

/// Maximum data payload per pkt-line (excluding the 4-byte length header).
pub const MAX_DATA_LEN: usize = 65516;

/// Encode a data line into `buf`. Panics if `data` exceeds [`MAX_DATA_LEN`].
pub fn encode_line(buf: &mut Vec<u8>, data: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    assert!(data.len() <= MAX_DATA_LEN, "pkt-line too long");
    let len = data.len() + 4;

    buf.extend_from_slice(&[
        HEX[(len >> 12) & 0xf],
        HEX[(len >> 8) & 0xf],
        HEX[(len >> 4) & 0xf],
        HEX[len & 0xf],
    ]);
    buf.extend_from_slice(data);
}

pub fn encode_text(buf: &mut Vec<u8>, text: &str) {
    if text.as_bytes().last() == Some(&b'\n') {
        encode_line(buf, text.as_bytes());
    } else {
        let mut line = String::with_capacity(text.len() + 1);
        line.push_str(text);
        line.push('\n');
        encode_line(buf, line.as_bytes());
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_service_line() {
        let mut buf = Vec::new();
        encode_text(&mut buf, "# service=git-upload-pack");
        encode_flush(&mut buf);
        assert_eq!(&buf[..30], b"001e# service=git-upload-pack\n");
        assert_eq!(&buf[30..], b"0000");
    }
}
