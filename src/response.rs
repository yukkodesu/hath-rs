use crate::error::{HathError, Result};
use crate::hvfile::HVFile;
use hyper::{Response, StatusCode, header};
use http_body_util::Full;
use bytes::Bytes;
use std::path::Path;
use rand::Rng;

/// Build a Hyper Response with proper headers.
/// Java: Cache-Control + Content-Length only added when contentLength > 0.
/// Server and Date headers are set at the Hyper service layer.
pub fn ok_response(body: Vec<u8>, content_type: &str) -> Result<Response<Full<Bytes>>> {
    let len = body.len();
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONNECTION, "close");
    if len > 0 {
        builder = builder
            .header(header::CACHE_CONTROL, "public, max-age=31536000")
            .header(header::CONTENT_LENGTH, len);
    }
    builder
        .body(Full::new(Bytes::from(body)))
        .map_err(HathError::Http)
}

pub fn not_found_response() -> Result<Response<Full<Bytes>>> {
    text_response(StatusCode::NOT_FOUND, "Not Found")
}

pub fn forbidden_response() -> Result<Response<Full<Bytes>>> {
    text_response(StatusCode::FORBIDDEN, "Permission Denied")
}

pub fn bad_request_response() -> Result<Response<Full<Bytes>>> {
    text_response(StatusCode::BAD_REQUEST, "Bad Request")
}

pub fn text_response(status: StatusCode, text: &str) -> Result<Response<Full<Bytes>>> {
    let len = text.len();
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "text/html; charset=iso-8859-1")
        .header(header::CONNECTION, "close");
    if len > 0 {
        builder = builder
            .header(header::CACHE_CONTROL, "public, max-age=31536000")
            .header(header::CONTENT_LENGTH, len);
    }
    builder
        .body(Full::new(Bytes::from(text.as_bytes().to_vec())))
        .map_err(HathError::Http)
}

pub fn redirect_response(location: &str) -> Result<Response<Full<Bytes>>> {
    // Java: empty body with 301 + Location header, no Content-Length
    Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header(header::LOCATION, location)
        .header(header::CONNECTION, "close")
        .body(Full::new(Bytes::new()))
        .map_err(HathError::Http)
}

pub fn robots_response() -> Result<Response<Full<Bytes>>> {
    let body = b"User-agent: *\nDisallow: /";
    let len = body.len();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain")
        .header(header::CONTENT_LENGTH, len)
        .header(header::CONNECTION, "close")
        .body(Full::new(Bytes::from(body.to_vec())))
        .map_err(HathError::Http)
}

pub fn speedtest_response(size: usize) -> Result<Response<Full<Bytes>>> {
    // Java: HTTPResponseProcessorSpeedtest inherits getContentType() from
    // HTTPResponseProcessor → Settings.CONTENT_TYPE_DEFAULT = "text/html; charset=iso-8859-1"
    let mut data = vec![0u8; size];
    rand::rng().fill_bytes(&mut data);
    ok_response(data, "text/html; charset=iso-8859-1")
}

pub async fn file_response(hv_file: &HVFile, cache_dir: &Path) -> Result<Response<Full<Bytes>>> {
    let path = hv_file.cache_path(cache_dir);
    let data = tokio::fs::read(&path).await
        .map_err(|e| HathError::Cache(format!("cannot read {}: {}", path.display(), e)))?;

    if data.len() != hv_file.size as usize {
        return Err(HathError::Cache(format!(
            "file size mismatch for {}: expected {}, got {}",
            hv_file.fileid(), hv_file.size, data.len()
        )));
    }

    ok_response(data, hv_file.mime_type())
}
