//! Human-facing web frontend for `atmoq serve`: a port-80 HTTP→HTTPS
//! redirect and a TLS landing page (in the spirit of rainbow's
//! https://bsky.network page). Hand-rolled HTTP/1.1 — it answers GETs with
//! one text page; a framework would outweigh the feature.

use anyhow::{Context, Result};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const BANNER: &str = r#"        _
   __ _| |_ _ __ ___   ___   __ _
  / _` | __| '_ ` _ \ / _ \ / _` |
 | (_| | |_| | | | | | (_) | (_| |
  \__,_|\__|_| |_| |_|\___/ \__, |
                               |_|
"#;

pub fn landing_page(host: &str, broadcast: &str, track: &str) -> String {
    // Version in the banner so "what build is production running" is one curl
    // away (deploys are otherwise indistinguishable from the outside).
    let version = env!("CARGO_PKG_VERSION");
    format!(
        "{BANNER}\n\
        This is an atproto [https://atproto.com] relay,\n\
        running the 'atmoq' codebase [https://github.com/streamplace/atmoq] v{version},\n\
        serving the firehose over MoQ [https://moq.dev].\n\
        \n\
        The firehose MoQ broadcast is at:\n\
        \n\
        url:       https://{host}\n\
        broadcast: {broadcast}\n\
        track:     {track}\n\
        \n\
        Consume it with:\n\
        \n\
        cargo install atmoq\n\
        atmoq firehose --moq-host https://{host}\n"
    )
}

/// Plain-HTTP listener: redirect everything to https://<same host><path>.
pub async fn serve_redirect(bind: std::net::SocketAddr, fallback_host: String) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding http redirect on {bind}"))?;
    tracing::info!(%bind, "http redirect listening");
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let fallback = fallback_host.clone();
        tokio::spawn(async move {
            let Some((path, host)) = read_request(&mut stream).await else {
                return;
            };
            let host = host.unwrap_or(fallback);
            let host = host.split(':').next().unwrap_or(&host).to_owned();
            let response = format!(
                "HTTP/1.1 301 Moved Permanently\r\nLocation: https://{host}{path}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}

/// TLS listener answering every request with the landing page.
pub async fn serve_landing(
    bind: std::net::SocketAddr,
    cert_path: &std::path::Path,
    key_path: &std::path::Path,
    page: String,
) -> Result<()> {
    let certs: Vec<_> = rustls_pemfile::certs(&mut std::io::BufReader::new(
        std::fs::File::open(cert_path).context("opening TLS cert")?,
    ))
    .collect::<std::result::Result<_, _>>()
    .context("parsing TLS cert")?;
    let key = rustls_pemfile::private_key(&mut std::io::BufReader::new(
        std::fs::File::open(key_path).context("opening TLS key")?,
    ))
    .context("parsing TLS key")?
    .context("no private key found")?;
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("building TLS config")?;
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));

    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding https landing on {bind}"))?;
    tracing::info!(%bind, "https landing page listening");
    let page = Arc::new(page);
    loop {
        let Ok((stream, _)) = listener.accept().await else {
            continue;
        };
        let acceptor = acceptor.clone();
        let page = page.clone();
        tokio::spawn(async move {
            let Ok(mut stream) = acceptor.accept(stream).await else {
                return;
            };
            if read_request(&mut stream).await.is_none() {
                return;
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                page.len(),
                page
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}

/// Read one request head; return (path, host-header) on a plausible GET.
async fn read_request<S: tokio::io::AsyncRead + Unpin>(
    stream: &mut S,
) -> Option<(String, Option<String>)> {
    let mut buf = vec![0u8; 4096];
    let mut len = 0;
    loop {
        let n = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            stream.read(&mut buf[len..]),
        )
        .await
        .ok()?
        .ok()?;
        if n == 0 {
            return None;
        }
        len += n;
        if buf[..len].windows(4).any(|w| w == b"\r\n\r\n") || len == buf.len() {
            break;
        }
    }
    let head = String::from_utf8_lossy(&buf[..len]);
    let mut lines = head.lines();
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let _method = parts.next()?;
    let path = parts.next().unwrap_or("/").to_owned();
    let host = lines
        .filter_map(|l| l.split_once(':'))
        .find(|(k, _)| k.eq_ignore_ascii_case("host"))
        .map(|(_, v)| v.trim().to_owned());
    Some((path, host))
}
