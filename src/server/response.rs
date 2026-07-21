use super::body::{FileBodyParams, StreamingBody};
use crate::bandwidth::BandwidthMonitor;
use crate::error::{HathError, Result};
use crate::hvfile::HVFile;
use crate::stats::Stats;
use bytes::Bytes;
use hyper::{Response, StatusCode, header};
use std::path::Path;
use std::sync::Arc;

const DEFAULT_CONTENT_TYPE: &str = "text/html; charset=iso-8859-1";
const CACHE_CONTROL_VALUE: &str = "public, max-age=31536000";
const SERVER_VALUE: &str = "Genetic Lifeform and Distributed Open Server 1.6.5";

/// A response before common HTTP policy has been applied.
pub(crate) struct ResponseSpec {
    status: StatusCode,
    content_type: String,
    declared_length: usize,
    body: StreamingBody,
    headers: hyper::HeaderMap,
}

impl ResponseSpec {
    fn new(
        status: StatusCode,
        content_type: impl Into<String>,
        declared_length: usize,
        body: StreamingBody,
    ) -> Self {
        Self {
            status,
            content_type: content_type.into(),
            declared_length,
            body,
            headers: hyper::HeaderMap::new(),
        }
    }

    fn with_header(mut self, name: header::HeaderName, value: &str) -> Result<Self> {
        let value = value
            .parse()
            .map_err(|e| HathError::Parse(format!("invalid response header value: {}", e)))?;
        self.headers.insert(name, value);
        Ok(self)
    }

    fn internal_error() -> Self {
        let body = Bytes::from_static(b"Internal Server Error");
        Self::new(
            StatusCode::INTERNAL_SERVER_ERROR,
            DEFAULT_CONTENT_TYPE,
            body.len(),
            StreamingBody::new(body),
        )
    }
}

/// Applies common Hyper response policy after request handling has selected a
/// semantic outcome. Hyper remains responsible for final wire serialization.
pub(crate) struct ResponsePolicy {
    bwm: Option<Arc<BandwidthMonitor>>,
    traffic_stats: Option<Arc<Stats>>,
}

impl ResponsePolicy {
    pub(crate) fn new(
        is_local: bool,
        bwm: Option<Arc<BandwidthMonitor>>,
        stats: Arc<Stats>,
    ) -> Self {
        Self {
            bwm,
            traffic_stats: (!is_local).then_some(stats),
        }
    }

    pub(crate) async fn finalize(self, response: Result<ResponseSpec>) -> Response<StreamingBody> {
        let spec = match response {
            Ok(spec) => spec,
            Err(error) => {
                tracing::error!("Error building response: {}", error);
                ResponseSpec::internal_error()
            }
        };

        let ResponseSpec {
            status,
            content_type,
            declared_length,
            body,
            headers: extra_headers,
        } = spec;
        let mut response = Response::new(body);
        *response.status_mut() = status;
        response.headers_mut().extend(extra_headers);
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            content_type
                .parse()
                .expect("response content types are static or derive from HVFile"),
        );
        response.headers_mut().insert(
            header::CONNECTION,
            header::HeaderValue::from_static("close"),
        );
        let date = chrono::Utc::now()
            .format("%a, %d %b %Y %H:%M:%S GMT")
            .to_string();
        response.headers_mut().insert(
            header::DATE,
            date.parse()
                .expect("HTTP date is always a valid header value"),
        );
        response.headers_mut().insert(
            header::SERVER,
            header::HeaderValue::from_static(SERVER_VALUE),
        );
        if declared_length > 0 {
            response.headers_mut().insert(
                header::CACHE_CONTROL,
                header::HeaderValue::from_static(CACHE_CONTROL_VALUE),
            );
            response.headers_mut().insert(
                header::CONTENT_LENGTH,
                declared_length
                    .to_string()
                    .parse()
                    .expect("decimal content length is a valid header value"),
            );
        }

        let header_bytes = header_bytes(&response);
        if let Some(bwm) = &self.bwm {
            bwm.wait_for_quota(header_bytes).await;
            response.body_mut().enable_flow_control(bwm.clone());
        }
        if let Some(stats) = self.traffic_stats {
            stats.record_bytes_sent(header_bytes as u64);
            response.body_mut().enable_traffic_accounting(stats);
        }

        response
    }
}

/// Best-effort HTTP/1.1 header bytes for Hyper's final response model.
pub(crate) fn header_bytes(response: &Response<StreamingBody>) -> usize {
    let reason_len = response.status().canonical_reason().map_or(0, str::len);
    let status_line_len = "HTTP/1.1 ".len() + response.status().as_str().len() + 1 + reason_len + 2;
    let headers_len: usize = response
        .headers()
        .iter()
        .map(|(name, value)| name.as_str().len() + 2 + value.as_bytes().len() + 2)
        .sum();
    status_line_len + headers_len + 2
}

pub(crate) fn not_found_response() -> Result<ResponseSpec> {
    text_response(StatusCode::NOT_FOUND, "Not Found")
}

pub(crate) fn forbidden_response() -> Result<ResponseSpec> {
    text_response(StatusCode::FORBIDDEN, "Permission Denied")
}

pub(crate) fn bad_request_response() -> Result<ResponseSpec> {
    text_response(StatusCode::BAD_REQUEST, "Bad Request")
}

pub(crate) fn method_not_allowed_response() -> Result<ResponseSpec> {
    let body = Bytes::from_static(b"Method Not Allowed");
    ResponseSpec::new(
        StatusCode::METHOD_NOT_ALLOWED,
        DEFAULT_CONTENT_TYPE,
        body.len(),
        StreamingBody::new(body),
    )
    .with_header(header::ALLOW, "GET, HEAD")
}

pub(crate) fn text_response(status: StatusCode, text: &str) -> Result<ResponseSpec> {
    let body = Bytes::copy_from_slice(text.as_bytes());
    Ok(ResponseSpec::new(
        status,
        DEFAULT_CONTENT_TYPE,
        body.len(),
        StreamingBody::new(body),
    ))
}

pub(crate) fn redirect_response(location: &str) -> Result<ResponseSpec> {
    ResponseSpec::new(
        StatusCode::MOVED_PERMANENTLY,
        DEFAULT_CONTENT_TYPE,
        0,
        StreamingBody::empty(),
    )
    .with_header(header::LOCATION, location)
}

pub(crate) fn robots_response() -> Result<ResponseSpec> {
    let body = Bytes::from_static(b"User-agent: *\nDisallow: /");
    Ok(ResponseSpec::new(
        StatusCode::OK,
        "text/plain; charset=iso-8859-1",
        body.len(),
        StreamingBody::new(body),
    ))
}

/// HEAD response: declared length is retained while the body stays empty.
pub(crate) fn head_response(content_type: &str, total_size: usize) -> Result<ResponseSpec> {
    Ok(ResponseSpec::new(
        StatusCode::OK,
        content_type,
        total_size,
        StreamingBody::empty(),
    ))
}

pub(crate) fn proxy_body_response(
    content_type: &str,
    total_size: usize,
    body: StreamingBody,
) -> Result<ResponseSpec> {
    Ok(ResponseSpec::new(
        StatusCode::OK,
        content_type,
        total_size,
        body,
    ))
}

pub(crate) fn speedtest_response(size: usize) -> Result<ResponseSpec> {
    Ok(ResponseSpec::new(
        StatusCode::OK,
        DEFAULT_CONTENT_TYPE,
        size,
        StreamingBody::new_random(size),
    ))
}

pub(crate) fn file_response(
    hv_file: &HVFile,
    cache_dir: &Path,
    head_only: bool,
    file_stats: Arc<Stats>,
    verify: bool,
    cache_handler: Option<Arc<crate::cache::CacheHandler>>,
) -> Result<ResponseSpec> {
    let path = hv_file.cache_path(cache_dir);
    let expected_size = hv_file.size as usize;
    let file = std::fs::File::open(&path)
        .map_err(|e| HathError::Cache(format!("cannot open {}: {}", path.display(), e)))?;
    let actual_len = file
        .metadata()
        .map_err(|e| HathError::Cache(format!("cannot stat {}: {}", path.display(), e)))?
        .len() as usize;
    if actual_len != expected_size {
        return Err(HathError::Cache(format!(
            "file size mismatch for {}: expected {}, got {}",
            hv_file.fileid(),
            expected_size,
            actual_len
        )));
    }

    file_stats.record_file_sent();
    if head_only {
        return head_response(hv_file.mime_type(), expected_size);
    }

    let body = StreamingBody::new_file(FileBodyParams {
        file,
        path,
        total_size: expected_size,
        expected_hash: hv_file.hash.to_string(),
        verify,
        cache_handler,
    });
    Ok(ResponseSpec::new(
        StatusCode::OK,
        hv_file.mime_type(),
        expected_size,
        body,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use std::sync::atomic::Ordering;

    const TCP_PACKET_SIZE: usize = 1460;

    #[tokio::test]
    async fn finalization_applies_common_headers_flow_control_and_traffic_accounting() {
        let stats = Arc::new(Stats::new());
        let mut response = ResponsePolicy::new(
            false,
            Some(Arc::new(BandwidthMonitor::new(10_000_000))),
            stats.clone(),
        )
        .finalize(text_response(
            StatusCode::OK,
            &"x".repeat(TCP_PACKET_SIZE * 2),
        ))
        .await;

        assert_eq!(
            response.headers()[header::CONTENT_LENGTH],
            (TCP_PACKET_SIZE * 2).to_string()
        );
        assert_eq!(response.headers()[header::CONNECTION], "close");
        assert!(response.headers().contains_key(header::DATE));
        assert!(response.headers().contains_key(header::SERVER));
        assert_eq!(
            header_bytes(&response),
            15 + "OK".len()
                + response
                    .headers()
                    .iter()
                    .map(|(name, value)| name.as_str().len() + 2 + value.as_bytes().len() + 2)
                    .sum::<usize>()
                + 2
        );
        assert_eq!(
            stats.bytes_sent.load(Ordering::Relaxed),
            header_bytes(&response) as u64
        );

        let frame = response.body_mut().frame().await.unwrap().unwrap();
        assert_eq!(frame.into_data().unwrap().len(), TCP_PACKET_SIZE);
        assert_eq!(
            stats.bytes_sent.load(Ordering::Relaxed),
            header_bytes(&response) as u64 + TCP_PACKET_SIZE as u64
        );
    }

    #[tokio::test]
    async fn fallback_uses_the_same_common_header_policy() {
        let stats = Arc::new(Stats::new());
        let response = ResponsePolicy::new(true, None, stats.clone())
            .finalize(Err(HathError::Cache("unavailable".into())))
            .await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        assert!(response.headers().contains_key(header::CONTENT_TYPE));
        assert!(response.headers().contains_key(header::DATE));
        assert!(response.headers().contains_key(header::SERVER));
        assert_eq!(response.headers()[header::CONNECTION], "close");
        assert_eq!(stats.bytes_sent.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn head_response_counts_headers_but_not_body_bytes() {
        let stats = Arc::new(Stats::new());
        let mut response = ResponsePolicy::new(false, None, stats.clone())
            .finalize(head_response(DEFAULT_CONTENT_TYPE, 1024))
            .await;

        assert_eq!(response.headers()[header::CONTENT_LENGTH], "1024");
        let header_bytes = header_bytes(&response) as u64;
        assert!(response.body_mut().frame().await.is_none());
        assert_eq!(stats.bytes_sent.load(Ordering::Relaxed), header_bytes);
    }
}
