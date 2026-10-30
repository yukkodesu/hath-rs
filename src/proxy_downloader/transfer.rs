//! Upstream selection and append-only recovery, independent of HTTP serving.
use super::{DownloadFailReason, DownloadState};
use crate::config::Config;
use crate::error::HathError;
use crate::hvfile::HVFile;
use crate::stats::Stats;
use crate::utils;
use reqwest::{Client, Response, StatusCode, Url};
use sha1::Digest;
use std::error::Error;
use std::fmt;
use std::io;
use std::path::Path;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::watch;
use tokio::time::{Instant, timeout_at};

const MAX_ATTEMPTS: usize = 3;

/// Failure to obtain a usable response from any upstream source.
#[derive(Debug, thiserror::Error)]
pub(super) enum ConnectError {
    #[error("no upstream sources supplied")]
    NoSources,
    #[error("upstream HTTP transfer failed")]
    Network(#[source] reqwest::Error),
    #[error("upstream returned {0}")]
    Status(StatusCode),
    #[error("missing Content-Length")]
    MissingLength,
    #[error("contentLength {length} exceeds max allowed filesize {max}")]
    TooLarge { length: u64, max: u64 },
    #[error("size mismatch: expected {expected}, got {actual}")]
    LengthMismatch { expected: u64, actual: u64 },
    #[error("download deadline exceeded")]
    Deadline,
}

impl ConnectError {
    fn reason(&self) -> DownloadFailReason {
        match self {
            Self::Deadline => DownloadFailReason::Timeout,
            Self::Network(e) if e.is_timeout() => DownloadFailReason::Timeout,
            _ => DownloadFailReason::NetworkError,
        }
    }

    fn retryable(&self) -> bool {
        !matches!(self, Self::Deadline)
    }
}

impl From<reqwest::Error> for ConnectError {
    fn from(error: reqwest::Error) -> Self {
        // Signed source URLs must never appear in diagnostics or downstream errors.
        Self::Network(error.without_url())
    }
}

impl From<ConnectError> for HathError {
    fn from(error: ConnectError) -> Self {
        let status = match &error {
            ConnectError::Deadline => 504,
            ConnectError::Network(e) if e.is_timeout() => 504,
            ConnectError::Network(_) => 500,
            ConnectError::NoSources
            | ConnectError::Status(_)
            | ConnectError::MissingLength
            | ConnectError::TooLarge { .. }
            | ConnectError::LengthMismatch { .. } => 502,
        };
        HathError::ProxyDownloader {
            status,
            message: ErrorChain(&error).to_string(),
        }
    }
}

/// Failure after streaming has started; downstream already has the headers.
#[derive(Debug, thiserror::Error)]
pub(super) enum DownloadError {
    #[error(transparent)]
    Connect(#[from] ConnectError),
    #[error("upstream HTTP transfer failed")]
    Body(#[source] reqwest::Error),
    #[error("incomplete response: {received} of {expected} bytes")]
    PrematureEof { received: u64, expected: u64 },
    #[error("response exceeds expected size {0}")]
    SizeExceeded(u64),
    #[error("response does not match the retained prefix")]
    PrefixMismatch,
    #[error("SHA1 mismatch")]
    Sha1Mismatch,
    #[error("download deadline exceeded")]
    Deadline,
    #[error("temporary file I/O failed")]
    Io(#[from] io::Error),
}

impl DownloadError {
    pub(super) fn reason(&self) -> DownloadFailReason {
        match self {
            Self::Connect(error) => error.reason(),
            Self::Body(e) if e.is_timeout() => DownloadFailReason::Timeout,
            Self::Body(_) | Self::SizeExceeded(_) => DownloadFailReason::NetworkError,
            Self::PrematureEof { .. } => DownloadFailReason::PrematureEof,
            Self::PrefixMismatch | Self::Sha1Mismatch => DownloadFailReason::Sha1Mismatch,
            Self::Deadline => DownloadFailReason::Timeout,
            Self::Io(_) => DownloadFailReason::DiskWriteError,
        }
    }

    fn retryable(&self) -> bool {
        !matches!(self, Self::Deadline | Self::Io(_) | Self::Sha1Mismatch)
    }
}

impl From<reqwest::Error> for DownloadError {
    fn from(error: reqwest::Error) -> Self {
        // Signed source URLs must never appear in diagnostics or downstream errors.
        Self::Body(error.without_url())
    }
}

struct ErrorChain<'a>(&'a (dyn Error + 'static));

impl fmt::Display for ErrorChain<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)?;
        let mut source = self.0.source();
        while let Some(error) = source {
            write!(f, ": {error}")?;
            source = error.source();
        }
        Ok(())
    }
}

/// One file's download: source rotation, the shared attempt budget and deadline.
pub(super) struct Upstream {
    client: Client,
    sources: Vec<Url>,
    hath_request: String,
    hv_file: HVFile,
    max_allowed_filesize: u64,
    started: Instant,
    deadline: Instant,
    attempts: usize,
    retained: u64,
}

impl Upstream {
    /// Starts the attempt budget and the deadline.
    pub(super) fn new(
        client: Client,
        sources: &[Url],
        hv_file: HVFile,
        config: &Config,
        timeout: Duration,
    ) -> Result<Self, ConnectError> {
        if sources.is_empty() {
            return Err(ConnectError::NoSources);
        }
        let started = Instant::now();
        Ok(Self {
            client,
            sources: sources.to_vec(),
            hath_request: format!(
                "{}-{}",
                config.client_id.0,
                utils::sha1_string(&format!(
                    "{}{}",
                    config.client_key.as_str(),
                    hv_file.fileid()
                ))
            ),
            hv_file,
            max_allowed_filesize: config.max_allowed_filesize,
            started,
            deadline: started + timeout,
            attempts: 0,
            retained: 0,
        })
    }

    /// Requires at least one attempt left in the budget.
    pub(super) async fn connect(&mut self) -> Result<Response, ConnectError> {
        loop {
            let error = match self.try_next_source().await {
                Ok(response) => return Ok(response),
                Err(error) => error,
            };
            self.log_attempt_failure(error.reason(), &error, 0);
            if !error.retryable() || self.attempts >= MAX_ATTEMPTS {
                return Err(error);
            }
        }
    }

    async fn try_next_source(&mut self) -> Result<Response, ConnectError> {
        let source = self.sources[self.attempts % self.sources.len()].clone();
        self.attempts += 1;
        let request = async {
            let response = self
                .client
                .get(source)
                .header("Hath-Request", &self.hath_request)
                .send()
                .await?;
            validate_proxy_response(&response, self.max_allowed_filesize, self.expected_size())?;
            Ok(response)
        };
        timeout_at(self.deadline, request)
            .await
            .unwrap_or(Err(ConnectError::Deadline))
    }

    pub(super) async fn download(
        &mut self,
        response: Response,
        path: &Path,
        progress: &watch::Sender<DownloadState>,
        stats: &Stats,
    ) -> Result<(), DownloadError> {
        let mut file = PartialFile::open(path, self.expected_size()).await?;
        let result = timeout_at(
            self.deadline,
            self.recover(&mut file, response, progress, stats),
        )
        .await
        .unwrap_or(Err(DownloadError::Deadline));
        self.retained = file.written;
        result
    }

    async fn recover(
        &mut self,
        file: &mut PartialFile,
        mut response: Response,
        progress: &watch::Sender<DownloadState>,
        stats: &Stats,
    ) -> Result<(), DownloadError> {
        loop {
            match file.receive(&mut response, progress, stats).await {
                Ok(()) => return file.finish(self.hv_file.hash.as_str()).await,
                Err(error) => {
                    self.retained = file.written;
                    self.log_attempt_failure(error.reason(), &error, file.received);
                    if !error.retryable() || self.attempts >= MAX_ATTEMPTS {
                        return Err(error);
                    }
                    response = self.connect().await?;
                }
            }
        }
    }

    pub(super) fn log_failed(&self, error: &DownloadError) {
        tracing::warn!(
            fileid = %self.hv_file.fileid(), source_host = self.source_host(), attempts = self.attempts,
            downloaded = self.retained, expected = self.expected_size(), elapsed_ms = self.elapsed_ms(),
            reason = ?error.reason(), error = %ErrorChain(error),
            "Proxy download failed"
        );
    }

    pub(super) fn log_complete(&self) {
        tracing::info!(
            fileid = %self.hv_file.fileid(), source_host = self.source_host(), attempts = self.attempts,
            downloaded = self.retained, elapsed_ms = self.elapsed_ms(),
            "Proxy download complete"
        );
    }

    fn log_attempt_failure(
        &self,
        reason: DownloadFailReason,
        error: &(dyn Error + 'static),
        received: u64,
    ) {
        tracing::warn!(
            fileid = %self.hv_file.fileid(), source_host = self.source_host(), attempt = self.attempts, max_attempts = MAX_ATTEMPTS,
            received, retained = self.retained, expected = self.expected_size(),
            elapsed_ms = self.elapsed_ms(),
            reason = ?reason, error = %ErrorChain(error),
            "Proxy download attempt failed"
        );
    }

    fn source_host(&self) -> &str {
        self.sources[self.attempts.saturating_sub(1) % self.sources.len()]
            .host_str()
            .unwrap_or("unknown")
    }

    fn expected_size(&self) -> u64 {
        self.hv_file.size as u64
    }

    fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis() as u64
    }
}

/// Only appends published bytes. Each replay must prove it has the same prefix
/// before any new bytes can become visible to the downstream reader.
struct PartialFile {
    file: tokio::fs::File,
    written: u64,
    received: u64,
    expected_size: u64,
    sha1: sha1::Sha1,
}

impl PartialFile {
    async fn open(path: &Path, expected_size: u64) -> Result<Self, DownloadError> {
        Ok(Self {
            file: tokio::fs::OpenOptions::new().write(true).open(path).await?,
            written: 0,
            received: 0,
            expected_size,
            sha1: sha1::Sha1::new(),
        })
    }

    async fn receive(
        &mut self,
        response: &mut Response,
        progress: &watch::Sender<DownloadState>,
        stats: &Stats,
    ) -> Result<(), DownloadError> {
        self.received = 0;
        let prefix_len = self.written;
        let prefix_hash = self.sha1.clone().finalize();
        let mut replay_hash = sha1::Sha1::new();
        while let Some(chunk) = response.chunk().await? {
            stats.record_bytes_rcvd(chunk.len() as u64);
            let offset = self.received;
            self.received += chunk.len() as u64;
            if self.received > self.expected_size {
                return Err(DownloadError::SizeExceeded(self.expected_size));
            }
            let replay_len = prefix_len.saturating_sub(offset).min(chunk.len() as u64) as usize;
            if replay_len > 0 {
                replay_hash.update(&chunk[..replay_len]);
                if offset + replay_len as u64 == prefix_len
                    && replay_hash.clone().finalize() != prefix_hash
                {
                    return Err(DownloadError::PrefixMismatch);
                }
            }
            let suffix = &chunk[replay_len..];
            if !suffix.is_empty() {
                self.file.write_all(suffix).await?;
                // Tokio writes can still be pending when write_all returns.
                self.file.flush().await?;
                self.sha1.update(suffix);
                self.written += suffix.len() as u64;
                progress.send_replace(DownloadState::InProgress(self.written));
            }
        }
        if self.received != self.expected_size {
            return Err(DownloadError::PrematureEof {
                received: self.received,
                expected: self.expected_size,
            });
        }
        Ok(())
    }

    async fn finish(&mut self, expected_hash: &str) -> Result<(), DownloadError> {
        self.file.flush().await?;
        if utils::hex_encode(&self.sha1.clone().finalize()) != expected_hash {
            return Err(DownloadError::Sha1Mismatch);
        }
        Ok(())
    }
}

fn validate_proxy_response(
    response: &Response,
    max_allowed_filesize: u64,
    expected_size: u64,
) -> Result<(), ConnectError> {
    if response.status() != StatusCode::OK {
        return Err(ConnectError::Status(response.status()));
    }
    let length = response
        .content_length()
        .ok_or(ConnectError::MissingLength)?;
    if length > max_allowed_filesize {
        return Err(ConnectError::TooLarge {
            length,
            max: max_allowed_filesize,
        });
    }
    if length != expected_size {
        return Err(ConnectError::LengthMismatch {
            expected: expected_size,
            actual: length,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{FakeHttpServer, FakeResponse};

    /// reqwest reports the length of a converted response from its body.
    fn test_response(status: u16, content_length: usize) -> Response {
        http::Response::builder()
            .status(status)
            .body(vec![0; content_length])
            .unwrap()
            .into()
    }

    struct UnknownLengthBody;

    impl http_body::Body for UnknownLengthBody {
        type Data = bytes::Bytes;
        type Error = std::convert::Infallible;

        fn poll_frame(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
            std::task::Poll::Ready(None)
        }
    }

    #[test]
    fn response_validation_names_each_rejection() {
        let validate = |response| validate_proxy_response(&response, 100, 20);
        assert!(matches!(
            validate(test_response(500, 20)),
            Err(ConnectError::Status(StatusCode::INTERNAL_SERVER_ERROR))
        ));
        assert!(matches!(
            validate(http::Response::new(reqwest::Body::wrap(UnknownLengthBody)).into()),
            Err(ConnectError::MissingLength)
        ));
        assert!(matches!(
            validate(test_response(200, 101)),
            Err(ConnectError::TooLarge {
                length: 101,
                max: 100
            })
        ));
        assert!(matches!(
            validate(test_response(200, 12)),
            Err(ConnectError::LengthMismatch {
                expected: 20,
                actual: 12
            })
        ));
        assert!(validate(test_response(200, 20)).is_ok());
    }

    #[tokio::test]
    async fn read_timeout_is_classified_and_error_chain_is_preserved() {
        let server = FakeHttpServer::start(vec![
            FakeResponse::ok(b"body".to_vec()).delay_before_body(Duration::from_secs(1)),
        ])
        .await;
        let client = crate::downloader::build_direct_client(
            Duration::from_secs(1),
            Duration::from_millis(50),
        )
        .unwrap();
        let mut response = client
            .get(server.url("/image?token=must-not-appear"))
            .send()
            .await
            .unwrap();
        let error = DownloadError::from(response.chunk().await.unwrap_err());
        assert_eq!(error.reason(), DownloadFailReason::Timeout);
        let chain = ErrorChain(&error).to_string();
        assert!(chain.contains("error decoding response body"), "{chain}");
        assert!(chain.contains("timed out"), "{chain}");
        assert!(!chain.contains("must-not-appear"), "{chain}");
    }

    #[tokio::test]
    async fn connection_error_redacts_signed_url_from_diagnostics_and_http_error() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let error =
            crate::downloader::build_direct_client(Duration::from_secs(1), Duration::from_secs(1))
                .unwrap()
                .get(format!(
                    "http://{address}/private-path?token=must-not-appear"
                ))
                .send()
                .await
                .unwrap_err();
        let error = ConnectError::from(error);
        let chain = ErrorChain(&error).to_string();
        assert!(!chain.contains("must-not-appear"), "{chain}");
        assert!(!chain.contains("private-path"), "{chain}");
        assert!(
            !HathError::from(error)
                .to_string()
                .contains("must-not-appear")
        );
    }

    #[tokio::test]
    async fn header_timeout_maps_to_gateway_timeout() {
        let server = FakeHttpServer::start(vec![
            FakeResponse::ok(b"body".to_vec()).delay_before_headers(Duration::from_secs(1)),
        ])
        .await;
        let error = crate::downloader::build_direct_client(
            Duration::from_secs(1),
            Duration::from_millis(50),
        )
        .unwrap()
        .get(server.url("/image"))
        .send()
        .await
        .unwrap_err();
        assert!(matches!(
            HathError::from(ConnectError::from(error)),
            HathError::ProxyDownloader { status: 504, .. }
        ));
    }

    #[tokio::test]
    async fn final_sha1_mismatch_is_reported_after_a_complete_body() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial");
        std::fs::write(&path, []).unwrap();
        let mut file = PartialFile::open(&path, 4).await.unwrap();
        let (progress, _) = watch::channel(DownloadState::InProgress(0));
        let mut response: Response = http::Response::new(b"body".to_vec()).into();
        file.receive(&mut response, &progress, &Stats::new())
            .await
            .unwrap();
        assert!(matches!(
            file.finish(&utils::sha1_bytes(b"other")).await,
            Err(DownloadError::Sha1Mismatch)
        ));
    }

    #[tokio::test]
    async fn oversized_chunk_is_rejected_before_writing_or_publishing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial");
        std::fs::write(&path, []).unwrap();
        let mut file = PartialFile::open(&path, 4).await.unwrap();
        let mut response: Response = http::Response::builder()
            .status(200)
            .body(b"too long".to_vec())
            .unwrap()
            .into();
        let (progress, rx) = watch::channel(DownloadState::InProgress(0));
        assert!(matches!(
            file.receive(&mut response, &progress, &Stats::new()).await,
            Err(DownloadError::SizeExceeded(4))
        ));
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        assert!(matches!(*rx.borrow(), DownloadState::InProgress(0)));
    }

    #[tokio::test]
    async fn short_replay_keeps_original_prefix_and_hash() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("partial");
        std::fs::write(&path, []).unwrap();
        let mut file = PartialFile::open(&path, 10).await.unwrap();
        let (progress, _) = watch::channel(DownloadState::InProgress(0));
        for body in [b"0123456".as_slice(), b"012"] {
            let mut response: Response = http::Response::new(body.to_vec()).into();
            assert!(matches!(
                file.receive(&mut response, &progress, &Stats::new()).await,
                Err(DownloadError::PrematureEof { .. })
            ));
            assert_eq!(file.written, 7);
            assert_eq!(std::fs::read(&path).unwrap(), b"0123456");
        }
        let mut response: Response = http::Response::new(b"0123456789".to_vec()).into();
        file.receive(&mut response, &progress, &Stats::new())
            .await
            .unwrap();
        file.finish(&utils::sha1_bytes(b"0123456789"))
            .await
            .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"0123456789");
    }
}
