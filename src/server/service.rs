use super::AppState;
use super::body::StreamingBody;
use super::handler::{RequestClientContext, RequestContext, handle_request};
use super::middleware::session::SessionHandle;
use super::peer::SessionOrigin;
use super::request::{self, RequestType};
use hyper::body::Incoming;
use hyper::header;
use hyper::service::Service;
use hyper::{Request, Response};
use std::future::Future;
use std::pin::Pin;

pub(crate) struct HathService {
    pub(crate) state: AppState,
    pub(crate) origin: SessionOrigin,
    pub(crate) session: SessionHandle,
}

impl Service<Request<Incoming>> for HathService {
    type Response = Response<StreamingBody>;
    type Error = hyper::Error;
    type Future =
        Pin<Box<dyn Future<Output = std::result::Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let state = self.state.clone();
        let origin = self.origin;
        let session = self.session.clone();

        Box::pin(async move {
            // Load config once for this request (owned Arc, safe across .await)
            let config = state.config.load_full();

            // Determine bandwidth monitor for this request.
            // Java: Only local connections skip throttling.
            // RPC servers on non-local IPs are still throttled.
            let bwm_for_request = if origin.is_local() {
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
                origin.peer_ip(),
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
                    client: RequestClientContext::new(origin.is_local(), bwm_for_request),
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
                if !origin.is_local() {
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
