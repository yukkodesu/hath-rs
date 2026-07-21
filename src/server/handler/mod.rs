mod file;
mod servercmd;
mod speedtest;

use super::AppState;
use super::request::RequestType;
use super::response::{self, ResponseSpec};
use crate::config::Config;
use crate::error::Result;
use std::sync::Arc;

use self::file::handle_file_serve;
use self::servercmd::handle_server_command;
use self::speedtest::handle_speedtest;

pub(crate) struct RequestContext {
    pub(crate) state: AppState,
    pub(crate) config: Arc<Config>,
}

pub(crate) async fn handle_request(
    request_type: RequestType,
    ctx: RequestContext,
) -> Result<ResponseSpec> {
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
                handle_server_command(&command, &additional, &ctx.state).await
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
        } => handle_speedtest(testsize, valid, forbidden, head_only),
        RequestType::Favicon => response::redirect_response("https://e-hentai.org/favicon.ico"),
        RequestType::Robots => response::robots_response(),
        RequestType::BadRequest => response::bad_request_response(),
        RequestType::MethodNotAllowed => response::method_not_allowed_response(),
        RequestType::NotFound => response::not_found_response(),
    }
}
