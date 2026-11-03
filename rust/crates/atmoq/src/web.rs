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

/// How stale the last relayed frame may get before `/healthz` reports
/// unhealthy. Mainnet sustains a few hundred frames/sec, so a full minute of
/// silence is unambiguously a stall, not a lull — while still leaving room for
/// an upstream reconnect (which reconnects in seconds) to ride through.
const HEALTH_STALE_AFTER: std::time::Duration = std::time::Duration::from_secs(60);

/// Admin listener: `/metrics`, `/healthz`, and (with the `profiling` feature)
/// heap profiles. Plain HTTP with no auth — bind it to a private interface or a
/// container network, never a public address. It is deliberately a *separate*
/// listener from the landing page: `serve` puts that one on the open internet,
/// and subscriber counts, cursor position and store size are operational
/// detail rather than public information.
///
/// Runs as its own task so a wedged pump cannot stop it answering — which is
/// the entire point of a health endpoint, and exactly the case that made the
/// landing page useless as a liveness signal (it answered 200 throughout an
/// outage because it never consults the pump at all).
pub async fn serve_admin(
    bind: std::net::SocketAddr,
    metrics: Arc<crate::metrics::Metrics>,
) -> Result<()> {
    let listener = tokio::net::TcpListener::bind(bind)
        .await
        .with_context(|| format!("binding admin listener on {bind}"))?;
    tracing::info!(%bind, "admin listener (metrics, health) listening");
    loop {
        let Ok((mut stream, _)) = listener.accept().await else {
            continue;
        };
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let Some((path, _host)) = read_request(&mut stream).await else {
                return;
            };
            // Strip any query string before matching.
            let path = path.split('?').next().unwrap_or("/");
            let (status, content_type, body) = admin_route(path, &metrics);
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = stream.write_all(response.as_bytes()).await;
        });
    }
}

/// Route one admin request to (status line, content type, body). Split out so
/// the routing table is unit-testable without a socket.
fn admin_route(
    path: &str,
    metrics: &crate::metrics::Metrics,
) -> (&'static str, &'static str, String) {
    match path {
        "/metrics" => (
            "200 OK",
            "text/plain; version=0.0.4; charset=utf-8",
            metrics.encode(),
        ),
        // Liveness means "the firehose is flowing", not "the process is up".
        // Before the first frame we report healthy: a relay that has just
        // started has not yet failed, and failing readiness during startup only
        // teaches operators to ignore the endpoint.
        "/healthz" => match metrics.frame_age_ms() {
            Some(age) if age > HEALTH_STALE_AFTER.as_millis() as u64 => (
                "503 Service Unavailable",
                "text/plain; charset=utf-8",
                format!(
                    "unhealthy: no frame relayed for {:.1}s (threshold {}s)\n",
                    age as f64 / 1000.0,
                    HEALTH_STALE_AFTER.as_secs()
                ),
            ),
            Some(age) => (
                "200 OK",
                "text/plain; charset=utf-8",
                format!(
                    "ok: last frame {:.1}s ago, {} subscribers\n",
                    age as f64 / 1000.0,
                    metrics.subscribers_live()
                ),
            ),
            None => (
                "200 OK",
                "text/plain; charset=utf-8",
                "ok: starting, no frames relayed yet\n".to_owned(),
            ),
        },
        // A jemalloc heap profile, naming the call sites holding live memory.
        // Two distinct failure modes get distinct messages: not built with the
        // feature, versus built but not armed via MALLOC_CONF. They need
        // different fixes and conflating them wastes an operator's afternoon.
        "/debug/heap" => match crate::heap::dump() {
            Ok(profile) => ("200 OK", "text/plain; charset=utf-8", profile),
            Err(err) => (
                "501 Not Implemented",
                "text/plain; charset=utf-8",
                format!("{err}\n"),
            ),
        },
        _ => (
            "404 Not Found",
            "text/plain; charset=utf-8",
            "not found\n\navailable: /metrics /healthz /debug/heap\n".to_owned(),
        ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::Metrics;

    #[test]
    fn metrics_route_serves_exposition() {
        let m = Metrics::default();
        m.record_frame(Some(9));
        let (status, ct, body) = admin_route("/metrics", &m);
        assert_eq!(status, "200 OK");
        assert!(ct.starts_with("text/plain; version=0.0.4"));
        assert!(body.contains("atmoq_frames_total 1"));
    }

    #[test]
    fn health_is_ok_before_the_first_frame() {
        // A relay that just started has not failed; reporting 503 during
        // startup only teaches operators to ignore the endpoint.
        let (status, _, body) = admin_route("/healthz", &Metrics::default());
        assert_eq!(status, "200 OK");
        assert!(body.contains("starting"));
    }

    #[test]
    fn health_is_ok_while_frames_flow() {
        let m = Metrics::default();
        m.record_frame(Some(1));
        let (status, _, _) = admin_route("/healthz", &m);
        assert_eq!(status, "200 OK");
    }

    #[test]
    fn health_fails_when_frames_go_stale() {
        use std::sync::atomic::Ordering;
        let m = Metrics::default();
        m.record_frame(Some(1));
        // Backdate the last frame past the threshold.
        let stale = m.last_frame_unix_ms.load(Ordering::Relaxed)
            - (HEALTH_STALE_AFTER.as_millis() as u64 + 1_000);
        m.last_frame_unix_ms.store(stale, Ordering::Relaxed);
        let (status, _, body) = admin_route("/healthz", &m);
        assert_eq!(status, "503 Service Unavailable");
        assert!(body.contains("unhealthy"));
    }

    #[test]
    fn query_strings_and_unknown_paths() {
        let m = Metrics::default();
        assert_eq!(admin_route("/nope", &m).0, "404 Not Found");
        // serve_admin strips the query before routing; verify the target path
        // matches once stripped.
        assert_eq!(admin_route("/metrics", &m).0, "200 OK");
    }
}
