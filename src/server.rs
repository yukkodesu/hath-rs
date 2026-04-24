use crate::config::Config;
use crate::error::Result;
use crate::request::{self, RequestType};
use crate::response;
use crate::stats::Stats;
use crate::cache::CacheHandler;
use crate::rpc_client::RpcClient;
use crate::rpc::{self, Action};

use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::Service;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use http_body_util::Full;
use bytes::Bytes;
use hyper::header;
use std::collections::HashMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tokio_rustls::TlsAcceptor;
use rustls::ServerConfig;
use regex::Regex;
use std::sync::LazyLock;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

/// Shared state accessible from all request handlers.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub stats: Arc<Stats>,
    pub cache: Arc<Mutex<CacheHandler>>,
    pub rpc_client: Arc<RpcClient>,
    pub allow_normal_connections: Arc<std::sync::atomic::AtomicBool>,
    /// Flood control table (IP -> entry). Uses Arc<Mutex> for shared access.
    pub flood_control: Arc<Mutex<HashMap<String, FloodControlEntry>>>,
}

#[derive(Debug, Clone)]
pub struct FloodControlEntry {
    pub connect_count: u32,
    pub last_connect: Instant,
    pub block_until: Option<Instant>,
}

impl FloodControlEntry {
    pub fn is_blocked(&self) -> bool {
        self.block_until.is_some_and(|b| b > Instant::now())
    }

    pub fn is_stale(&self, now: Instant) -> bool {
        self.last_connect < now - Duration::from_secs(60)
    }

    /// Returns true if the connection should be allowed.
    pub fn hit(&mut self) -> bool {
        let now = Instant::now();
        let elapsed_ms = (now - self.last_connect).as_millis() as u32;
        self.connect_count = self.connect_count.saturating_sub(elapsed_ms / 1000).saturating_add(1);
        self.last_connect = now;

        if self.connect_count > 10 {
            self.block_until = Some(now + Duration::from_secs(60));
            false
        } else {
            true
        }
    }
}

static LOCAL_NETWORK_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(localhost|127\.|10\.|192\.168\.|172\.(1[6-9]|2[0-9]|3[0-1])\.|169\.254\.|::1|0:0:0:0:0:0:0:1|fc|fd)")
        .expect("invalid regex")
});

pub struct HathService {
    pub state: AppState,
}

impl Service<Request<Incoming>> for HathService {
    type Response = Response<Full<Bytes>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let state = self.state.clone();
        // Extract remote_addr from extensions (injected by accept loop)
        let remote_addr = req.extensions().get::<SocketAddr>().copied();

        Box::pin(async move {
            let client_ip = remote_addr.map(|a| a.ip()).unwrap_or_else(|| "0.0.0.0".parse().unwrap());

            // Build request line for parsing
            let request_line = format!(
                "{} {} {:?}",
                req.method(),
                req.uri().path_and_query().map(|p| p.as_str()).unwrap_or("/"),
                req.version()
            );

            let request_type = request::parse_request(&request_line, client_ip, &state.config);

            let mut resp = match request_type {
                RequestType::FileServe { keystamp_valid, hv_file, .. } => {
                    if !keystamp_valid {
                        response::forbidden_response()
                    } else if let Some(hv) = hv_file {
                        let cache_path = hv.cache_path(&state.config.cache_dir);
                        if cache_path.exists() {
                            state.stats.record_file_sent();
                            response::file_response(&hv, &state.config.cache_dir).await
                        } else {
                            response::not_found_response()
                        }
                    } else {
                        response::not_found_response()
                    }
                }
                RequestType::ServerCommand { command, valid, .. } => {
                    if valid {
                        handle_server_command(&command, &state).await
                    } else {
                        response::forbidden_response()
                    }
                }
                RequestType::SpeedTest { testsize, valid, .. } => {
                    if valid {
                        response::speedtest_response(testsize as usize)
                    } else {
                        response::forbidden_response()
                    }
                }
                RequestType::Favicon => response::redirect_response("https://e-hentai.org/favicon.ico"),
                RequestType::Robots => response::robots_response(),
                RequestType::NotFound => response::not_found_response(),
            };

            // Add Server header to every response
            if let Ok(ref mut r) = resp {
                r.headers_mut().insert(
                    header::SERVER,
                    header::HeaderValue::from_static("Genetic Lifeform and Distributed Open Server 1.6.5")
                );
                // Add Date header
                let date = chrono::Utc::now().format("%a, %d %b %Y %H:%M:%S GMT").to_string();
                if let Ok(v) = header::HeaderValue::from_str(&date) {
                    r.headers_mut().insert(header::DATE, v);
                }
            }

            match resp {
                Ok(r) => Ok(r),
                Err(e) => {
                    tracing::error!("Error building response: {}", e);
                    Ok(Response::builder()
                        .status(500)
                        .body(Full::new(Bytes::from("Internal Server Error")))
                        .unwrap())
                }
            }
        })
    }
}

/// Handle servercmd API commands. Must support all Java commands.
async fn handle_server_command(command: &str, state: &AppState) -> crate::error::Result<Response<Full<Bytes>>> {
    match command.to_lowercase().as_str() {
        "still_alive" => response::text_response(hyper::StatusCode::OK, "I feel FANTASTIC and I'm still alive"),
        "threaded_proxy_test" => {
            response::text_response(hyper::StatusCode::OK, "OK:0-0")
        }
        "speed_test" => {
            response::text_response(hyper::StatusCode::OK, "")
        }
        "refresh_settings" => {
            match state.rpc_client.refresh_settings().await {
                Ok(sr) if sr.status == crate::rpc::ResponseStatus::Ok => {
                    response::text_response(hyper::StatusCode::OK, "")
                }
                _ => response::text_response(hyper::StatusCode::OK, ""),
            }
        }
        "start_downloader" => {
            response::text_response(hyper::StatusCode::OK, "")
        }
        "refresh_certs" => {
            response::text_response(hyper::StatusCode::OK, "")
        }
        _ => response::text_response(hyper::StatusCode::OK, "INVALID_COMMAND"),
    }
}

/// Prune stale flood control entries. Called periodically from main loop.
pub async fn prune_flood_control(state: &AppState) {
    let mut fc = state.flood_control.lock().await;
    let now = Instant::now();
    fc.retain(|_, entry| !entry.is_stale(now));
}

/// Nuke old connections (simplified — Hyper handles most connection lifecycle).
pub async fn nuke_old_connections(_state: &AppState) {
    // Hyper's http1::Builder doesn't expose connection tracking.
    // We rely on Hyper's built-in timeouts instead of Java's manual nuke.
}

/// Start the TLS HTTP server. Sends readiness via `ready_tx` after successful bind.
pub async fn start_server(
    state: AppState,
    shutdown: tokio_util::sync::CancellationToken,
    ready_tx: Option<tokio::sync::oneshot::Sender<std::result::Result<u16, String>>>,
) -> Result<()> {
    let setup_result = async {
        let cert_path = state.config.data_dir.join("hathcert.p12");

        // Download cert if not present
        if !cert_path.exists() {
            let cert_url = rpc::make_rpc_url(Action::GetCertificate, "", &state.config)?;
            let downloader = crate::downloader::FileDownloader::new(
                cert_url, 10000, 300000,
                crate::downloader::DownloadMode::File(cert_path.clone()),
                false,
            );
            downloader.download().await?;
        }

        let cert_data = std::fs::read(&cert_path)?;
        let keystore = p12_keystore::KeyStore::from_pkcs12(&cert_data, state.config.client_key.as_str(),p12_keystore::Pkcs12ImportPolicy::Strict)
            .map_err(|e| crate::error::HathError::Tls(rustls::Error::General(e.to_string())))?;

        let (_, keychain) = keystore.private_key_chain()
            .ok_or_else(|| crate::error::HathError::Tls(rustls::Error::General(
                "no private key in PKCS12".into()
            )))?;

        let certs: Vec<CertificateDer> = keychain.certs()
            .iter()
            .map(|c| CertificateDer::from(c.as_der().to_vec()))
            .collect();
        let key: PrivateKeyDer = PrivatePkcs8KeyDer::from(keychain.key().as_der().to_vec()).into();

        let tls_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(crate::error::HathError::Tls)?;

        let tls_acceptor = TlsAcceptor::from(Arc::new(tls_config));

        let addr = SocketAddr::from(([0, 0, 0, 0], state.config.client_port));
        let listener = TcpListener::bind(addr).await.map_err(crate::error::HathError::Io)?;

        Ok::<_, crate::error::HathError>((listener, tls_acceptor, state.config.client_port))
    }.await;

    let (listener, tls_acceptor, port) = match setup_result {
        Ok(v) => {
            if let Some(tx) = ready_tx {
                let _ = tx.send(Ok(v.2));
            }
            v
        }
        Err(e) => {
            if let Some(tx) = ready_tx {
                let _ = tx.send(Err(e.to_string()));
            }
            return Err(e);
        }
    };

    tracing::info!("HTTPServer listening on port {}", port);

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            result = listener.accept() => {
                let (stream, remote_addr) = match result {
                    Ok(c) => c,
                    Err(_) => continue,
                };

                let acceptor = tls_acceptor.clone();
                let state = state.clone();
                let allow = state.allow_normal_connections.load(std::sync::atomic::Ordering::Relaxed);

                // Flood control check for non-local, non-RPC traffic
                let host_addr = remote_addr.ip().to_string().to_lowercase();
                let is_local = LOCAL_NETWORK_RE.is_match(&host_addr)
                    || state.config.client_host.replace("::ffff:", "") == host_addr;
                let is_rpc = state.config.rpc_servers.iter().any(|s| s.to_string().to_lowercase() == host_addr);

                if !allow && !is_rpc {
                    drop(stream);
                    continue;
                }

                if !is_local && !is_rpc && !state.config.disable_flood_control {
                    let mut fc = state.flood_control.lock().await;
                    let entry = fc.entry(host_addr.clone()).or_insert_with(|| FloodControlEntry {
                        connect_count: 0,
                        last_connect: Instant::now(),
                        block_until: None,
                    });
                    if entry.is_blocked() || !entry.hit() {
                        tracing::warn!("Flood control activated for {}", host_addr);
                        continue;
                    }
                }

                tokio::spawn(async move {
                    let tls_stream = match acceptor.accept(stream).await {
                        Ok(s) => s,
                        Err(_) => return,
                    };

                    let io = TokioIo::new(tls_stream);

                    let service = HathService { state };

                    if let Err(e) = http1::Builder::new()
                        .serve_connection(io, service)
                        .await
                        && !e.to_string().contains("connection closed") {
                            tracing::debug!("HTTP connection error: {}", e);
                        }
                });
            }
        }
    }

    Ok(())
}
