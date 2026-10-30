use crate::error::{HathError, Result};
use reqwest::ClientBuilder;
use rustls::{
    ClientConfig, RootCertStore,
    crypto::{CryptoProvider, aws_lc_rs},
    pki_types::{CertificateDer, PrivateKeyDer},
};
use std::io;
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite};

pub(crate) type TlsAcceptor = tokio_rustls::TlsAcceptor;
pub(crate) type TlsStream<S> = tokio_rustls::server::TlsStream<S>;

pub(crate) fn aws_lc_provider() -> Arc<CryptoProvider> {
    Arc::new(aws_lc_rs::default_provider())
}

pub(crate) fn build_client_config() -> Result<ClientConfig> {
    let native = rustls_native_certs::load_native_certs();
    let mut roots = RootCertStore::empty();
    roots.add_parsable_certificates(native.certs);
    for error in native.errors {
        tracing::warn!("failed to load a native root certificate: {error}");
    }
    build_client_config_from_roots(roots)
}

fn build_client_config_from_roots(roots: RootCertStore) -> Result<ClientConfig> {
    if roots.is_empty() {
        return Err(HathError::Tls(
            "no native root certificates were loaded".into(),
        ));
    }

    Ok(ClientConfig::builder_with_provider(aws_lc_provider())
        .with_safe_default_protocol_versions()
        .map_err(|error| HathError::Tls(error.to_string()))?
        .with_root_certificates(roots)
        .with_no_client_auth())
}

pub(crate) fn configure_reqwest(builder: ClientBuilder) -> Result<ClientBuilder> {
    Ok(builder.tls_backend_preconfigured(build_client_config()?))
}

/// DER-encoded SubjectPublicKeyInfo of the private key.
pub(crate) fn private_key_spki(private_key: &PrivateKeyDer<'_>) -> Result<Vec<u8>> {
    let signing_key = aws_lc_provider()
        .key_provider
        .load_private_key(private_key.clone_key())
        .map_err(|error| HathError::Tls(format!("invalid PKCS12 private key: {error}")))?;
    let spki = signing_key.public_key().ok_or_else(|| {
        HathError::Tls("could not determine PKCS12 private key public key".into())
    })?;
    Ok(spki.as_ref().to_vec())
}

pub(crate) fn is_signed_by(child: &CertificateDer<'_>, issuer: &CertificateDer<'_>) -> bool {
    let (Ok((_, child)), Ok((_, issuer))) = (
        x509_parser::prelude::parse_x509_certificate(child.as_ref()),
        x509_parser::prelude::parse_x509_certificate(issuer.as_ref()),
    ) else {
        return false;
    };
    child.verify_signature(Some(issuer.public_key())).is_ok()
}

pub(crate) fn build_server_acceptor(
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
) -> Result<TlsAcceptor> {
    let config = rustls::ServerConfig::builder_with_provider(aws_lc_provider())
        .with_safe_default_protocol_versions()
        .map_err(|error| HathError::Tls(error.to_string()))?
        .with_no_client_auth()
        .with_single_cert(cert_chain, private_key)
        .map_err(|error| HathError::Tls(format!("failed to configure TLS certificate: {error}")))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

pub(crate) async fn accept<S>(acceptor: &TlsAcceptor, stream: S) -> io::Result<TlsStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    acceptor.accept(stream).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_an_empty_root_store() {
        assert!(build_client_config_from_roots(RootCertStore::empty()).is_err());
    }
}
