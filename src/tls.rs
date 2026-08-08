use crate::error::{HathError, Result};
use reqwest::ClientBuilder;
use rustls::{
    ClientConfig, RootCertStore,
    crypto::{CryptoProvider, aws_lc_rs},
};
use std::sync::Arc;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_a_reqwest_client_with_aws_lc_and_an_injected_root() {
        let pfx = p12::PFX::parse(include_bytes!("../tests/fixtures/legacy-rc2.p12")).unwrap();
        let mut roots = RootCertStore::empty();
        roots.add_parsable_certificates(
            pfx.cert_x509_bags("test-client-key")
                .unwrap()
                .into_iter()
                .map(rustls::pki_types::CertificateDer::from),
        );

        reqwest::Client::builder()
            .tls_backend_preconfigured(
                build_client_config_from_roots(roots).expect("TLS config should be created"),
            )
            .build()
            .expect("reqwest client should build");
    }

    #[test]
    fn rejects_an_empty_root_store() {
        assert!(build_client_config_from_roots(RootCertStore::empty()).is_err());
    }
}
