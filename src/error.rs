use thiserror::Error;

#[derive(Error, Debug)]
pub enum HathError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] http::Error),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("Hyper error: {0}")]
    Hyper(#[from] hyper::Error),

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("Config error: {0}")]
    Config(String),

    #[error("Cache error: {0}")]
    Cache(String),

    #[error("Parse error: {0}")]
    Parse(String),

    #[error("Network error: {0}")]
    Network(String),

    /// Proxy download error carrying the HTTP status code to return.
    /// Java: ProxyFileDownloader.initialize() returns 502 for bad source
    /// (missing Content-Length, oversized, size mismatch) or 500 for
    /// connection failure after exhausting all sources.
    #[error("Proxy download error ({status}): {message}")]
    ProxyDownloader { status: u16, message: String },

    #[error("Shutdown requested")]
    Shutdown,

    #[error("Certificate expired")]
    CertExpired,

    #[error("{0}")]
    Fatal(String),
}

impl From<openssl::error::ErrorStack> for HathError {
    fn from(e: openssl::error::ErrorStack) -> Self {
        HathError::Tls(e.to_string())
    }
}

impl From<openssl::ssl::Error> for HathError {
    fn from(e: openssl::ssl::Error) -> Self {
        HathError::Tls(e.to_string())
    }
}

impl From<rustls::Error> for HathError {
    fn from(e: rustls::Error) -> Self {
        HathError::Tls(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, HathError>;
