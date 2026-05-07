use super::super::body::StreamingBody;
use super::super::response;
use super::RequestContext;
use crate::error::Result;
use hyper::Response;

pub(super) fn handle_speedtest(
    testsize: u32,
    valid: bool,
    forbidden: bool,
    head_only: bool,
    ctx: RequestContext,
) -> Result<Response<StreamingBody>> {
    if valid {
        if !head_only && ctx.client.is_normal_hath_connection() {
            ctx.state.stats.record_bytes_sent(testsize as u64);
        }
        if head_only {
            // Java: speedtest inherits CONTENT_TYPE_DEFAULT = text/html
            response::head_response("text/html; charset=iso-8859-1", testsize as usize)
        } else {
            response::speedtest_response(testsize as usize, ctx.client.bwm)
        }
    } else if forbidden {
        // Java: responseStatusCode = 403 for expired or invalid key
        response::forbidden_response()
    } else {
        // Java: responseStatusCode = 400 for malformed URL (< 5 parts)
        response::bad_request_response()
    }
}
