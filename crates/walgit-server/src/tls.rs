//! In-process TLS for the standalone shape (D39): `server.tls.mode = "self_signed" | "files"`.
//!
//! A reverse proxy may terminate TLS and run h2c to walgit; a
//! standalone `walgit-server` on a laptop or a single VM has no edge, so it
//! terminates TLS itself. The listener performs the handshake lazily, on the
//! connection's first read/write, so one slow client never serializes the
//! accept loop. ALPN offers `h2` and `http/1.1`; hyper's auto builder sniffs
//! the HTTP/2 preface, so both work over the same port.
//!
//! Self-signed certificates are generated with rcgen, written once to
//! `<cache.dir>/tls/{cert,key}.pem` next to `cert.sans` (the SAN list they
//! were issued for) and regenerated only when that list changes, so a browser
//! or git that trusted the certificate keeps trusting it across restarts. The
//! certificate is public material and is served at `/services/public/ca.pem`.

use std::{
    io,
    path::Path,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::Context as _;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::{Accept, TlsAcceptor, server::TlsStream};
use walgit_config::{Config, TlsMode};

use crate::TcpAccept;

/// The loaded server identity: rustls config plus the PEM chain for `/services/public/ca.pem`.
pub struct Tls {
    pub acceptor: TlsAcceptor,
    pub cert_pem: String,
    /// `sha256:<hex>` of the leaf certificate (logged at startup, shown on /readyz).
    pub fingerprint: String,
}

pub fn load(cfg: &Config) -> anyhow::Result<Option<Arc<Tls>>> {
    let (cert_pem, key_pem) = match cfg.server.tls.mode {
        TlsMode::Off => return Ok(None),
        TlsMode::Files => {
            let cert = cfg.server.tls.cert.as_ref().expect("validated");
            let key = cfg.server.tls.key.as_ref().expect("validated");
            (
                std::fs::read_to_string(cert)
                    .with_context(|| format!("reading server.tls.cert {}", cert.display()))?,
                std::fs::read_to_string(key)
                    .with_context(|| format!("reading server.tls.key {}", key.display()))?,
            )
        }
        TlsMode::SelfSigned => self_signed(&cfg.tls_dir(), &cfg.tls_hostnames())?,
    };
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .collect::<Result<_, _>>()
        .context("parsing TLS certificate PEM")?;
    anyhow::ensure!(
        !certs.is_empty(),
        "TLS certificate PEM holds no certificate"
    );
    let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_pem.as_bytes())
        .context("parsing TLS private key PEM")?
        .ok_or_else(|| anyhow::anyhow!("TLS key PEM holds no private key"))?;
    let fingerprint = {
        use sha2::Digest;
        format!(
            "sha256:{}",
            hex::encode(sha2::Sha256::digest(certs[0].as_ref()))
        )
    };
    let mut sc = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .context("rustls protocol versions")?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .context("building rustls server config")?;
    sc.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    Ok(Some(Arc::new(Tls {
        acceptor: TlsAcceptor::from(Arc::new(sc)),
        cert_pem,
        fingerprint,
    })))
}

/// Load or create the self-signed pair under `dir` for exactly `hostnames`.
fn self_signed(dir: &Path, hostnames: &[String]) -> anyhow::Result<(String, String)> {
    let cert_p = dir.join("cert.pem");
    let key_p = dir.join("key.pem");
    let sans_p = dir.join("cert.sans");
    let wanted = hostnames.join("\n");
    if let (Ok(c), Ok(k), Ok(s)) = (
        std::fs::read_to_string(&cert_p),
        std::fs::read_to_string(&key_p),
        std::fs::read_to_string(&sans_p),
    ) && s.trim() == wanted
    {
        return Ok((c, k));
    }
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let key = rcgen::KeyPair::generate().context("generating TLS key")?;
    let mut params =
        rcgen::CertificateParams::new(hostnames.to_vec()).context("certificate params")?;
    params.distinguished_name.push(
        rcgen::DnType::CommonName,
        hostnames.first().map_or("walgit", String::as_str),
    );
    params.not_before = rcgen::date_time_ymd(2024, 1, 1);
    params.not_after = rcgen::date_time_ymd(2124, 1, 1);
    let cert = params
        .self_signed(&key)
        .context("self-signing TLS certificate")?;
    let cert_pem = cert.pem();
    let key_pem = key.serialize_pem();
    write_private(&key_p, &key_pem)?;
    std::fs::write(&cert_p, &cert_pem).with_context(|| format!("writing {}", cert_p.display()))?;
    std::fs::write(&sans_p, &wanted).with_context(|| format!("writing {}", sans_p.display()))?;
    tracing::info!(dir = %dir.display(), sans = ?hostnames, "generated self-signed TLS certificate");
    Ok((cert_pem, key_pem))
}

fn write_private(path: &Path, body: &str) -> anyhow::Result<()> {
    use std::io::Write;
    let mut f = std::fs::OpenOptions::new();
    f.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        f.mode(0o600);
    }
    f.open(path)
        .and_then(|mut f| f.write_all(body.as_bytes()))
        .with_context(|| format!("writing {}", path.display()))
}

/// `axum::serve::Listener` that wraps every accepted TCP connection in a
/// lazily-handshaking TLS stream (`TCP_NODELAY` set, like the plain listener).
pub struct TlsListener {
    pub(crate) tcp: TcpAccept,
    pub acceptor: TlsAcceptor,
}

impl axum::serve::Listener for TlsListener {
    type Io = LazyTls;
    type Addr = std::net::SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        loop {
            match self.tcp.accept().await {
                Ok((stream, addr)) => {
                    if let Err(e) = stream.set_nodelay(true) {
                        tracing::debug!(error = ?e, %addr, "failed to set TCP_NODELAY");
                    }
                    return (LazyTls::Handshaking(self.acceptor.accept(stream)), addr);
                }
                Err(e) => tracing::warn!(error = ?e, "TCP accept failed"),
            }
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        self.tcp.local_addr()
    }
}

/// A TLS connection whose handshake completes on first use.
pub enum LazyTls {
    Handshaking(Accept<TcpStream>),
    Ready(TlsStream<TcpStream>),
    Failed,
}

impl LazyTls {
    /// Drive the handshake; `Ready(Ok(stream))` once established.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<&mut TlsStream<TcpStream>>> {
        loop {
            match self {
                LazyTls::Ready(s) => return Poll::Ready(Ok(s)),
                LazyTls::Failed => {
                    return Poll::Ready(Err(io::Error::other("TLS handshake failed")));
                }
                LazyTls::Handshaking(accept) => match Pin::new(accept).poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(s)) => *self = LazyTls::Ready(s),
                    Poll::Ready(Err(e)) => {
                        tracing::debug!(error = %e, "TLS handshake failed");
                        *self = LazyTls::Failed;
                    }
                },
            }
        }
    }
}

impl AsyncRead for LazyTls {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(s)) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for LazyTls {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Ready(Ok(s)) => Pin::new(s).poll_write(cx, buf),
        }
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            LazyTls::Ready(s) => Pin::new(s).poll_flush(cx),
            _ => Poll::Ready(Ok(())),
        }
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut *self {
            LazyTls::Ready(s) => Pin::new(s).poll_shutdown(cx),
            _ => Poll::Ready(Ok(())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn self_signed_is_persisted_and_regenerated_only_when_sans_change() {
        let dir = tempfile::tempdir().unwrap();
        let a = self_signed(dir.path(), &["localhost".into()]).unwrap();
        let b = self_signed(dir.path(), &["localhost".into()]).unwrap();
        assert_eq!(a.0, b.0, "same SANs ⇒ same certificate");
        let c = self_signed(dir.path(), &["localhost".into(), "walgit.localhost".into()]).unwrap();
        assert_ne!(a.0, c.0, "new SAN ⇒ regenerated");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(dir.path().join("key.pem"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
    }

    #[test]
    fn load_builds_an_acceptor_with_h2_and_h1_alpn() {
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::default();
        cfg.cache.dir = dir.path().to_path_buf();
        cfg.server.tls.mode = TlsMode::SelfSigned;
        cfg.server.public_url = Some("https://walgit.localhost:8888".into());
        let tls = load(&cfg).unwrap().expect("tls on");
        assert!(tls.fingerprint.starts_with("sha256:"));
        assert!(tls.cert_pem.contains("BEGIN CERTIFICATE"));
        assert_eq!(cfg.tls_hostnames().last().unwrap(), "walgit.localhost");
        assert!(load(&Config::default()).unwrap().is_none());
    }
}
