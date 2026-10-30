//! TLS backend selected at compile time. Both backends expose the same items.

#[cfg(all(feature = "tls-rustls", feature = "tls-openssl"))]
compile_error!("features `tls-rustls` and `tls-openssl` are mutually exclusive");
#[cfg(not(any(feature = "tls-rustls", feature = "tls-openssl")))]
compile_error!("enable one of the features `tls-rustls` or `tls-openssl`");

#[cfg(feature = "tls-openssl")]
mod openssl_backend;
#[cfg(feature = "tls-rustls")]
mod rustls_backend;

#[cfg(feature = "tls-openssl")]
pub(crate) use openssl_backend::*;
#[cfg(feature = "tls-rustls")]
pub(crate) use rustls_backend::*;
