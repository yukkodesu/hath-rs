use super::super::response::{self, ResponseSpec};
use crate::error::Result;

pub(super) fn handle_speedtest(
    testsize: u32,
    valid: bool,
    forbidden: bool,
    head_only: bool,
) -> Result<ResponseSpec> {
    if valid {
        if head_only {
            // Java: speedtest inherits CONTENT_TYPE_DEFAULT = text/html
            response::head_response("text/html; charset=iso-8859-1", testsize as usize)
        } else {
            response::speedtest_response(testsize as usize)
        }
    } else if forbidden {
        // Java: responseStatusCode = 403 for expired or invalid key
        response::forbidden_response()
    } else {
        // Java: responseStatusCode = 400 for malformed URL (< 5 parts)
        response::bad_request_response()
    }
}
