use bytes::Bytes;
use http_body::{Body, Frame, SizeHint};
use hyper::body::Incoming;
use hyper::header;
use hyper::service::Service;
use hyper::{Request, Response};
use std::convert::Infallible;
use std::fmt::Write;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use super::body::{BodyCompletion, BodyCompletionSnapshot, BodyCompletionStatus};

pub fn access_log_prefix(conn_id: u32, client_ip: IpAddr) -> String {
    format!("{{{}{:<17} ", conn_id, format!("/{}}}", client_ip))
}

pub fn access_log_request_info(
    prefix: &str,
    status_code: u16,
    content_length: Option<usize>,
    head_only: bool,
) -> String {
    if head_only {
        format!("{}Code={} ", prefix, status_code)
    } else {
        format!(
            "{}Code={} Bytes={:<8} ",
            prefix,
            status_code,
            content_length.unwrap_or(0)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessLogCompletion {
    Finished,
    Incomplete {
        reason: &'static str,
        offset: usize,
        total_size: usize,
    },
    Aborted {
        offset: usize,
        total_size: usize,
    },
}

impl AccessLogCompletion {
    fn label(self) -> &'static str {
        match self {
            Self::Finished => "Finished",
            Self::Incomplete { .. } => "Incomplete",
            Self::Aborted { .. } => "Aborted",
        }
    }

    fn write_suffix(self, line: &mut String) {
        match self {
            Self::Finished => {}
            Self::Incomplete {
                reason,
                offset,
                total_size,
            } => {
                let _ = write!(
                    line,
                    " reason={} offset={} expected={}",
                    reason, offset, total_size
                );
            }
            Self::Aborted { offset, total_size } => {
                let _ = write!(line, " offset={} expected={}", offset, total_size);
            }
        }
    }
}

pub fn access_log_completion_line(
    info: &str,
    content_length: usize,
    elapsed: Duration,
    completion: AccessLogCompletion,
) -> String {
    let seconds = elapsed.as_secs_f64();
    let elapsed_ms = elapsed.as_millis();
    let speed = if elapsed_ms >= 10 {
        format!(" ({:.2} KB/s)", content_length as f64 / elapsed_ms as f64)
    } else {
        String::new()
    };
    let mut line = format!(
        "{}{} processing request in {:.2} seconds{}",
        info,
        completion.label(),
        seconds,
        speed
    );
    completion.write_suffix(&mut line);
    line
}

fn access_log_completion_from_body(snapshot: BodyCompletionSnapshot) -> AccessLogCompletion {
    match snapshot.completion {
        BodyCompletion::Complete => AccessLogCompletion::Finished,
        BodyCompletion::Incomplete(reason) => AccessLogCompletion::Incomplete {
            reason: reason.as_str(),
            offset: snapshot.offset,
            total_size: snapshot.total_size,
        },
        BodyCompletion::Running => AccessLogCompletion::Incomplete {
            reason: "body_returned_eof_without_completion",
            offset: snapshot.offset,
            total_size: snapshot.total_size,
        },
    }
}

pub struct AccessLogService<S> {
    inner: S,
    conn_id: u32,
    remote_addr: SocketAddr,
}

impl<S> AccessLogService<S> {
    pub fn new(inner: S, conn_id: u32, remote_addr: SocketAddr) -> Self {
        Self {
            inner,
            conn_id,
            remote_addr,
        }
    }
}

impl<S, B> Service<Request<Incoming>> for AccessLogService<S>
where
    S: Service<Request<Incoming>, Response = Response<B>, Error = hyper::Error>,
    S::Future: Send + 'static,
    B: Body<Data = Bytes, Error = Infallible> + Unpin + BodyCompletionStatus,
{
    type Response = Response<AccessLogBody<B>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let method = req.method().to_string();
        let path = req
            .uri()
            .path_and_query()
            .map(|p| p.as_str().to_string())
            .unwrap_or_else(|| "/".to_string());
        let version = format!("{:?}", req.version());
        let head_only = req.method() == hyper::Method::HEAD;
        let prefix = access_log_prefix(
            self.conn_id,
            crate::utils::normalize_ip(self.remote_addr.ip()),
        );
        let start = Instant::now();
        let fut = self.inner.call(req);

        Box::pin(async move {
            let resp = fut.await?;
            let status = resp.status().as_u16();
            let content_length = resp
                .headers()
                .get(header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<usize>().ok());
            let info = access_log_request_info(&prefix, status, content_length, head_only);
            tracing::info!("{}{} {} {}", info, method, path, version);

            let (parts, body) = resp.into_parts();
            let body = if head_only {
                AccessLogBody::new_without_completion(body)
            } else {
                AccessLogBody::new(body, info, content_length.unwrap_or(0), start)
            };
            Ok(Response::from_parts(parts, body))
        })
    }
}

pub struct AccessLogBody<B: BodyCompletionStatus> {
    inner: B,
    info: Option<String>,
    content_length: usize,
    start: Instant,
}

impl<B: BodyCompletionStatus> AccessLogBody<B> {
    pub fn new(inner: B, info: String, content_length: usize, start: Instant) -> Self {
        Self {
            inner,
            info: Some(info),
            content_length,
            start,
        }
    }

    pub fn new_without_completion(inner: B) -> Self {
        Self {
            inner,
            info: None,
            content_length: 0,
            start: Instant::now(),
        }
    }

    fn log_completion(&mut self, completion: AccessLogCompletion) {
        if let Some(info) = self.info.take() {
            let line = access_log_completion_line(
                &info,
                self.content_length,
                self.start.elapsed(),
                completion,
            );
            match completion {
                AccessLogCompletion::Incomplete { .. } => tracing::warn!("{}", line),
                AccessLogCompletion::Finished | AccessLogCompletion::Aborted { .. } => {
                    tracing::info!("{}", line);
                }
            }
        }
    }
}

impl<B: BodyCompletionStatus> Drop for AccessLogBody<B> {
    fn drop(&mut self) {
        if self.info.is_some() {
            let snapshot = self.inner.completion_status();
            let completion = if snapshot.offset >= snapshot.total_size {
                AccessLogCompletion::Finished
            } else {
                AccessLogCompletion::Aborted {
                    offset: snapshot.offset,
                    total_size: snapshot.total_size,
                }
            };
            self.log_completion(completion);
        }
    }
}

impl<B> Body for AccessLogBody<B>
where
    B: Body<Data = Bytes, Error = Infallible> + Unpin + BodyCompletionStatus,
{
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match Pin::new(&mut self.inner).poll_frame(cx) {
            Poll::Ready(None) => {
                let snapshot = self.inner.completion_status();
                self.log_completion(access_log_completion_from_body(snapshot));
                Poll::Ready(None)
            }
            other => other,
        }
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_access_log_lines_match_java_shape() {
        let prefix = access_log_prefix(12, "222.135.74.27".parse().unwrap());
        let info = access_log_request_info(&prefix, 200, Some(145678), false);

        assert_eq!(
            format!("{}GET /h/file HTTP/1.1", info),
            "{12/222.135.74.27}   Code=200 Bytes=145678   GET /h/file HTTP/1.1"
        );
        assert_eq!(
            access_log_completion_line(
                &info,
                145678,
                Duration::from_millis(750),
                AccessLogCompletion::Finished,
            ),
            "{12/222.135.74.27}   Code=200 Bytes=145678   Finished processing request in 0.75 seconds (194.24 KB/s)"
        );
        assert_eq!(
            access_log_completion_line(
                &info,
                145678,
                Duration::from_millis(750),
                AccessLogCompletion::Incomplete {
                    reason: "proxy_timeout",
                    offset: 4096,
                    total_size: 145678,
                },
            ),
            "{12/222.135.74.27}   Code=200 Bytes=145678   Incomplete processing request in 0.75 seconds (194.24 KB/s) reason=proxy_timeout offset=4096 expected=145678"
        );
        assert_eq!(
            access_log_completion_line(
                &info,
                145678,
                Duration::from_millis(750),
                AccessLogCompletion::Aborted {
                    offset: 4096,
                    total_size: 145678,
                },
            ),
            "{12/222.135.74.27}   Code=200 Bytes=145678   Aborted processing request in 0.75 seconds (194.24 KB/s) offset=4096 expected=145678"
        );
    }
}
