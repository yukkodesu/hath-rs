mod file;
mod servercmd;
mod speedtest;

use super::AppState;
use super::body::StreamingBody;
use super::request::RequestType;
use super::response;
use crate::bandwidth::BandwidthMonitor;
use crate::config::Config;
use crate::error::Result;
use hyper::Response;
use std::sync::Arc;

use self::file::handle_file_serve;
use self::servercmd::handle_server_command;
use self::speedtest::handle_speedtest;

#[derive(Clone)]
pub(crate) struct RequestClientContext {
    pub(crate) is_local: bool,
    pub(crate) bwm: Option<Arc<BandwidthMonitor>>,
}

impl RequestClientContext {
    pub(crate) fn new(is_local: bool, bwm: Option<Arc<BandwidthMonitor>>) -> Self {
        Self { is_local, bwm }
    }

    pub(crate) fn records_traffic(&self) -> bool {
        !self.is_local
    }
}

pub(crate) struct RequestContext {
    pub(crate) state: AppState,
    pub(crate) config: Arc<Config>,
    pub(crate) client: RequestClientContext,
}

pub(crate) async fn handle_request(
    request_type: RequestType,
    ctx: RequestContext,
) -> Result<Response<StreamingBody>> {
    match request_type {
        RequestType::FileServe {
            fileid,
            hv_file,
            additional,
            keystamp_valid,
            head_only,
        } => handle_file_serve(fileid, hv_file, additional, keystamp_valid, head_only, ctx).await,
        RequestType::ServerCommand {
            command,
            additional,
            valid,
        } => {
            if valid {
                handle_server_command(&command, &additional, &ctx.state, ctx.client.bwm).await
            } else {
                response::forbidden_response()
            }
        }
        RequestType::SpeedTest {
            testsize,
            valid,
            forbidden,
            head_only,
            ..
        } => handle_speedtest(testsize, valid, forbidden, head_only, ctx),
        RequestType::Favicon => response::redirect_response("https://e-hentai.org/favicon.ico"),
        RequestType::Robots => response::robots_response(),
        RequestType::BadRequest => response::bad_request_response(),
        RequestType::MethodNotAllowed => response::method_not_allowed_response(),
        RequestType::NotFound => response::not_found_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn traffic_policy_exempts_only_local_peers() {
        assert!(RequestClientContext::new(false, None).records_traffic());
        assert!(!RequestClientContext::new(true, None).records_traffic());
    }
}
