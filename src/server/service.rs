use super::AppState;
use super::body::StreamingBody;
use super::handler::{RequestContext, handle_request};
use super::middleware::session::SessionHandle;
use super::peer::SessionOrigin;
use super::request::{self, RequestType};
use super::response::ResponsePolicy;
use hyper::body::Incoming;
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

            let response = handle_request(
                request_type,
                RequestContext {
                    state: state.clone(),
                    config: config.clone(),
                },
            )
            .await;
            Ok(
                ResponsePolicy::new(origin.is_local(), bwm_for_request, state.stats.clone())
                    .finalize(response)
                    .await,
            )
        })
    }
}
