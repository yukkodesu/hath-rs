use crate::error::{HathError, Result};
use openssl::pkey::PKey;
use openssl::ssl::{Ssl, SslAcceptor, SslMethod, SslOptions};
use openssl::x509::X509;
use reqwest::ClientBuilder;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use std::io;
use std::pin::Pin;
use tokio::io::{AsyncRead, AsyncWrite};

// Union of the JDK 21 defaults and Mozilla's intermediate profile.
const TLS12_CIPHERS: &str = "ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-ECDSA-AES128-GCM-SHA256:\
    ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-RSA-CHACHA20-POLY1305:\
    ECDHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384:DHE-RSA-CHACHA20-POLY1305:\
    DHE-DSS-AES256-GCM-SHA384:DHE-RSA-AES128-GCM-SHA256:DHE-DSS-AES128-GCM-SHA256:\
    ECDHE-ECDSA-AES256-SHA384:ECDHE-RSA-AES256-SHA384:ECDHE-ECDSA-AES128-SHA256:\
    ECDHE-RSA-AES128-SHA256:DHE-RSA-AES256-SHA256:DHE-DSS-AES256-SHA256:DHE-RSA-AES128-SHA256:\
    DHE-DSS-AES128-SHA256:ECDHE-ECDSA-AES256-SHA:ECDHE-RSA-AES256-SHA:ECDHE-ECDSA-AES128-SHA:\
    ECDHE-RSA-AES128-SHA:DHE-RSA-AES256-SHA:DHE-DSS-AES256-SHA:DHE-RSA-AES128-SHA:\
    DHE-DSS-AES128-SHA";
const TLS13_CIPHERS: &str =
    "TLS_AES_256_GCM_SHA384:TLS_AES_128_GCM_SHA256:TLS_CHACHA20_POLY1305_SHA256";
const TLS_GROUPS: &str = "X25519MLKEM768:X25519:P-256:P-384:P-521:X448:ffdhe2048:ffdhe3072:ffdhe4096:ffdhe6144:ffdhe8192";

pub(crate) type TlsAcceptor = SslAcceptor;
pub(crate) type TlsStream<S> = tokio_openssl::SslStream<S>;

pub(crate) fn configure_reqwest(builder: ClientBuilder) -> Result<ClientBuilder> {
    Ok(builder.tls_backend_native())
}

/// DER-encoded SubjectPublicKeyInfo of the private key.
pub(crate) fn private_key_spki(private_key: &PrivateKeyDer<'_>) -> Result<Vec<u8>> {
    PKey::private_key_from_der(private_key.secret_der())
        .and_then(|key| key.public_key_to_der())
        .map_err(|error| HathError::Tls(format!("invalid PKCS12 private key: {error}")))
}

pub(crate) fn is_signed_by(child: &CertificateDer<'_>, issuer: &CertificateDer<'_>) -> bool {
    let verify = || {
        let issuer_key = X509::from_der(issuer)?.public_key()?;
        X509::from_der(child)?.verify(&issuer_key)
    };
    verify().unwrap_or(false)
}

pub(crate) fn build_server_acceptor(
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
) -> Result<TlsAcceptor> {
    let configure = || {
        let mut acceptor = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls_server())?;
        acceptor.set_cipher_list(TLS12_CIPHERS)?;
        acceptor.set_ciphersuites(TLS13_CIPHERS)?;
        acceptor.set_groups_list(TLS_GROUPS)?;
        acceptor.set_options(SslOptions::CIPHER_SERVER_PREFERENCE);
        let mut certificates = cert_chain.iter();
        if let Some(leaf) = certificates.next() {
            let leaf = X509::from_der(leaf)?;
            acceptor.set_certificate(&leaf)?;
        }
        for certificate in certificates {
            acceptor.add_extra_chain_cert(X509::from_der(certificate)?)?;
        }
        let private_key = PKey::private_key_from_der(private_key.secret_der())?;
        acceptor.set_private_key(&private_key)?;
        acceptor.check_private_key()?;
        Ok::<_, openssl::error::ErrorStack>(acceptor.build())
    };
    configure()
        .map_err(|error| HathError::Tls(format!("failed to configure TLS certificate: {error}")))
}

pub(crate) async fn accept<S>(acceptor: &TlsAcceptor, stream: S) -> io::Result<TlsStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let ssl = Ssl::new(acceptor.context()).map_err(io::Error::other)?;
    let mut stream = TlsStream::new(ssl, stream).map_err(io::Error::other)?;
    Pin::new(&mut stream)
        .accept()
        .await
        .map_err(|error| error.into_io_error().unwrap_or_else(io::Error::other))?;
    Ok(stream)
}

#[cfg(test)]
mod tests {
    use super::*;
    use openssl::ssl::{SslConnector, SslVerifyMode, SslVersion};
    use rustls_pki_types::PrivatePkcs8KeyDer;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    async fn handshake_with(version: SslVersion) -> String {
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        let acceptor = build_server_acceptor(
            vec![identity.cert.der().clone()],
            PrivatePkcs8KeyDer::from(identity.signing_key.serialize_der()).into(),
        )
        .unwrap();
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = accept(&acceptor, stream).await.unwrap();
            let mut request = [0; 4];
            stream.read_exact(&mut request).await.unwrap();
            stream.write_all(&request).await.unwrap();
            stream.shutdown().await.unwrap();
        });

        let mut connector = SslConnector::builder(SslMethod::tls_client()).unwrap();
        connector.set_verify(SslVerifyMode::NONE);
        connector.set_min_proto_version(Some(version)).unwrap();
        connector.set_max_proto_version(Some(version)).unwrap();
        let ssl = connector
            .build()
            .configure()
            .unwrap()
            .into_ssl("localhost")
            .unwrap();
        let tcp = TcpStream::connect(address).await.unwrap();
        let mut client = TlsStream::new(ssl, tcp).unwrap();
        Pin::new(&mut client).connect().await.unwrap();
        client.write_all(b"ping").await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        assert_eq!(response, b"ping");
        server.await.unwrap();
        client.ssl().version_str().to_owned()
    }

    #[tokio::test]
    async fn serves_tls12_and_tls13() {
        assert_eq!(handshake_with(SslVersion::TLS1_2).await, "TLSv1.2");
        assert_eq!(handshake_with(SslVersion::TLS1_3).await, "TLSv1.3");
    }
}
