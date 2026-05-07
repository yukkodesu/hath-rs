use super::body::StreamingBody;
use super::handler::{RequestClientContext, RequestContext, handle_request};
use super::middleware::session::SessionHandle;
use super::request::{self, RequestType};
use super::{AppState, LOCAL_NETWORK_RE};
use crate::utils;
use hyper::body::Incoming;
use hyper::header;
use hyper::service::Service;
use hyper::{Request, Response};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

pub(crate) struct HathService {
    pub(crate) state: AppState,
    pub(crate) remote_addr: SocketAddr,
    pub(crate) session: SessionHandle,
}

impl Service<Request<Incoming>> for HathService {
    type Response = Response<StreamingBody>;
    type Error = hyper::Error;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let state = self.state.clone();
        let remote_addr = self.remote_addr;
        let session = self.session.clone();

        Box::pin(async move {
            // Load config once for this request (owned Arc, safe across .await)
            let config = state.config.load_full();

            // Normalize IPv4-mapped IPv6 to plain IPv4 (dual-stack listener).
            let client_ip = utils::normalize_ip(remote_addr.ip());

            // Determine if this is a local/RPC connection (skip bandwidth throttling)
            let host_addr = client_ip.to_string().to_lowercase();
            let is_local = LOCAL_NETWORK_RE.is_match(&host_addr) || config.client_host == host_addr;
            let is_rpc = config
                .rpc_servers
                .iter()
                .any(|s| s.to_string().to_lowercase() == host_addr);

            // Determine bandwidth monitor for this request.
            // Java: Only local connections skip throttling.
            // RPC servers on non-local IPs are still throttled.
            let bwm_for_request = if is_local {
                None
            } else {
                state.bandwidth_monitor.load_full()
            };

            let request_type = request::parse_request(
                req.method().as_str(),
                req.uri()
                    .path_and_query()
                    .map(|p| p.as_str())
                    .unwrap_or("/"),
                client_ip,
                &config,
            );
            match &request_type {
                RequestType::ServerCommand { valid: true, .. } => session.mark_servercmd(),
                _ => session.mark_normal(),
            }

            // Clone BWM for later header throttling (bwm_for_request is consumed by response builders)
            let bwm_for_header = bwm_for_request.clone();

            let mut resp = handle_request(
                request_type,
                RequestContext {
                    state: state.clone(),
                    config: config.clone(),
                    client: RequestClientContext::new(is_local, is_rpc, bwm_for_request),
                },
            )
            .await;

            // Body chunk throttling is handled inside StreamingBody::poll_frame.

            // Add Server header to every response
            if let Ok(ref mut r) = resp {
                r.headers_mut().insert(
                    header::SERVER,
                    header::HeaderValue::from_static(
                        "Genetic Lifeform and Distributed Open Server 1.6.5",
                    ),
                );
                // Add Date header
                let date = chrono::Utc::now()
                    .format("%a, %d %b %Y %H:%M:%S GMT")
                    .to_string();
                if let Ok(v) = header::HeaderValue::from_str(&date) {
                    r.headers_mut().insert(header::DATE, v);
                }
            }

            // Header accounting/throttling: use actual serialized header bytes.
            // Java: bwm.waitForQuota(myThread, headerBytes.length) where headerBytes
            // is the full serialized HTTP response header; Stats.bytesSent records
            // the same header byte count for non-local clients.
            if let Ok(ref r) = resp {
                let reason_len = r.status().canonical_reason().map_or(0, |s| s.len());
                // Status line: "HTTP/1.1 XXX reason\r\n"
                let status_line_len = 13 + reason_len; // "HTTP/1.1 " + "XXX " + reason + "\r\n"
                let headers_len: usize = r
                    .headers()
                    .iter()
                    .map(|(k, v)| k.as_str().len() + 2 + v.as_bytes().len() + 2) // "Key: Value\r\n"
                    .sum();
                let total_header_bytes = status_line_len + headers_len + 2; // + trailing \r\n
                if let Some(ref bwm) = bwm_for_header {
                    bwm.wait_for_quota(total_header_bytes).await;
                }
                if !is_local {
                    state.stats.record_bytes_sent(total_header_bytes as u64);
                }
            }

            match resp {
                Ok(r) => Ok(r),
                Err(e) => {
                    tracing::error!("Error building response: {}", e);
                    Ok(Response::builder()
                        .status(500)
                        .body(StreamingBody::new(
                            bytes::Bytes::from_static(b"Internal Server Error"),
                            None,
                        ))
                        .unwrap())
                }
            }
        })
    }
}
