use thiserror::Error;

#[derive(Error, Debug)]
pub enum HathError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    Http(#[from] http::Error),

    #[error("TLS error: {0}")]
    Tls(#[from] rustls::Error),

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

    #[error("Shutdown requested")]
    Shutdown,

    #[error("Certificate expired")]
    CertExpired,

    #[error("{0}")]
    Fatal(String),
}

pub type Result<T> = std::result::Result<T, HathError>;
