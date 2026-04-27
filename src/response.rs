use crate::bandwidth::BandwidthMonitor;
use crate::body::StreamingBody;
use crate::error::{HathError, Result};
use crate::hvfile::HVFile;
use bytes::Bytes;
use hyper::{Response, StatusCode, header};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::Notify;

/// Build a Hyper Response with proper headers.
/// Java: Cache-Control + Content-Length only added when contentLength > 0.
/// Server and Date headers are set at the Hyper service layer.
pub fn ok_response(body: Bytes, content_type: &str) -> Result<Response<StreamingBody>> {
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
        .body(StreamingBody::new(body, None))
        .map_err(HathError::Http)
}

pub fn not_found_response() -> Result<Response<StreamingBody>> {
    text_response(StatusCode::NOT_FOUND, "Not Found")
}

pub fn forbidden_response() -> Result<Response<StreamingBody>> {
    text_response(StatusCode::FORBIDDEN, "Permission Denied")
}

pub fn bad_request_response() -> Result<Response<StreamingBody>> {
    text_response(StatusCode::BAD_REQUEST, "Bad Request")
}

pub fn method_not_allowed_response() -> Result<Response<StreamingBody>> {
    Response::builder()
        .status(StatusCode::METHOD_NOT_ALLOWED)
        .header(header::ALLOW, "GET, HEAD")
        .header(header::CONTENT_TYPE, "text/html; charset=iso-8859-1")
        .header(header::CONNECTION, "close")
        .body(StreamingBody::new(Bytes::from_static(b"Method Not Allowed"), None))
        .map_err(HathError::Http)
}

pub fn text_response(status: StatusCode, text: &str) -> Result<Response<StreamingBody>> {
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
        .body(StreamingBody::new(Bytes::copy_from_slice(text.as_bytes()), None))
        .map_err(HathError::Http)
}

pub fn redirect_response(location: &str) -> Result<Response<StreamingBody>> {
    // Java: HTTPSession always sends Content-Type regardless of status code.
    // Content-Type: text/html; charset=iso-8859-1 even on 301 redirect.
    Response::builder()
        .status(StatusCode::MOVED_PERMANENTLY)
        .header(header::CONTENT_TYPE, "text/html; charset=iso-8859-1")
        .header(header::LOCATION, location)
        .header(header::CONNECTION, "close")
        .body(StreamingBody::empty())
        .map_err(HathError::Http)
}

pub fn robots_response() -> Result<Response<StreamingBody>> {
    let body = b"User-agent: *\nDisallow: /";
    let len = body.len();
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/plain; charset=iso-8859-1")
        .header(header::CONTENT_LENGTH, len)
        .header(header::CONNECTION, "close")
        .body(StreamingBody::new(Bytes::from_static(body), None))
        .map_err(HathError::Http)
}

/// HEAD response: headers-only, no body. Java constructs identical headers
/// for HEAD but skips body writing in HTTPSession.write().
pub fn head_response(content_type: &str, total_size: usize) -> Result<Response<StreamingBody>> {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONNECTION, "close");
    if total_size > 0 {
        builder = builder
            .header(header::CACHE_CONTROL, "public, max-age=31536000")
            .header(header::CONTENT_LENGTH, total_size);
    }
    builder
        .body(StreamingBody::empty())
        .map_err(HathError::Http)
}

/// Build a speedtest response with optional bandwidth throttling.
/// Java: HTTPResponseProcessorSpeedtest inherits getContentType() from
/// HTTPResponseProcessor → Settings.CONTENT_TYPE_DEFAULT = "text/html; charset=iso-8859-1"
pub fn speedtest_response(
    size: usize,
    bwm: Option<Arc<BandwidthMonitor>>,
) -> Result<Response<StreamingBody>> {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/html; charset=iso-8859-1")
        .header(header::CONNECTION, "close");
    if size > 0 {
        builder = builder
            .header(header::CACHE_CONTROL, "public, max-age=31536000")
            .header(header::CONTENT_LENGTH, size);
    }
    builder
        .body(StreamingBody::new_random(size, bwm))
        .map_err(HathError::Http)
}

/// Build a response for a proxy file download in progress.
/// The body will stream data from the temp file as the background download
/// task writes to it. Java: HTTPResponseProcessorProxy + ProxyFileDownloader.
pub fn proxy_response(
    content_type: &str,
    total_size: usize,
    temp_file: PathBuf,
    write_offset: Arc<AtomicU64>,
    notify: Arc<Notify>,
    body_done_notify: Arc<Notify>,
    download_done: Arc<std::sync::atomic::AtomicBool>,
    bwm: Option<Arc<BandwidthMonitor>>,
) -> Result<Response<StreamingBody>> {
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, content_type)
        .header(header::CONNECTION, "close");
    if total_size > 0 {
        builder = builder
            .header(header::CACHE_CONTROL, "public, max-age=31536000")
            .header(header::CONTENT_LENGTH, total_size);
    }
    let body = StreamingBody::new_proxy(
        total_size,
        temp_file,
        write_offset,
        notify,
        body_done_notify,
        download_done,
        bwm,
    ).map_err(HathError::Io)?;
    builder.body(body).map_err(HathError::Http)
}

/// Serve a cached file with optional bandwidth throttling and inline integrity
/// check. Java: HTTPResponseProcessorFile streams from disk via FileChannel,
/// computes SHA1 incrementally, deletes corrupt file in cleanup() after send.
pub async fn file_response(
    hv_file: &HVFile,
    cache_dir: &Path,
    bwm: Option<Arc<BandwidthMonitor>>,
    verify: bool,
    cache_handler: Option<Arc<crate::cache::CacheHandler>>,
) -> Result<Response<StreamingBody>> {
    let path = hv_file.cache_path(cache_dir);
    let size = hv_file.size as usize;

    // Quick size check via metadata (avoids loading entire file into memory)
    let actual_len = tokio::fs::metadata(&path).await
        .map_err(|e| HathError::Cache(format!("cannot stat {}: {}", path.display(), e)))?
        .len() as usize;
    if actual_len != size {
        return Err(HathError::Cache(format!(
            "file size mismatch for {}: expected {}, got {}",
            hv_file.fileid(), size, actual_len
        )));
    }

    let mime = hv_file.mime_type().to_string();
    let mut builder = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, mime)
        .header(header::CONNECTION, "close");
    if size > 0 {
        builder = builder
            .header(header::CACHE_CONTROL, "public, max-age=31536000")
            .header(header::CONTENT_LENGTH, size);
    }
    let body = StreamingBody::new_file(
        path,
        size,
        hv_file.hash.to_string(),
        verify,
        cache_handler,
        bwm,
    ).map_err(|e| HathError::Cache(format!("cannot open file: {}", e)))?;
    builder.body(body).map_err(HathError::Http)
}
