use crate::config::Config;
use crate::error::{HathError, Result};
use crate::rpc::{self, Action};
use crate::utils;

use openssl::asn1::Asn1Time;
use openssl::hash::MessageDigest;
use openssl::nid::Nid;
use openssl::pkcs12::Pkcs12;
use openssl::provider::Provider;
use openssl::ssl::{SslContext, SslMethod};
use openssl::x509::{X509NameRef, X509Ref};
use std::fmt::Write;
use std::time::{SystemTime, UNIX_EPOCH};

const SECS_PER_DAY: i64 = 86400;
const CERT_RENEWAL_WINDOW_SECS: i64 = SECS_PER_DAY;

fn unix_time_secs() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}

pub(super) fn is_cert_expired(cert_expiry_unix: i64) -> bool {
    cert_expiry_unix < unix_time_secs().saturating_add(CERT_RENEWAL_WINDOW_SECS)
}

/// Build an OpenSSL SslContext from the PKCS12 certificate.
/// Uses OpenSSL for both PKCS12 parsing and TLS context building.
/// If `force_download` is true, always re-download the cert from the RPC server.
/// Returns (context, cert_expiry_unix_seconds).
pub(super) async fn build_tls_acceptor(
    config: &Config,
    force_download: bool,
) -> Result<(SslContext, i64)> {
    let cert_path = config.data_dir.join("hathcert.p12");

    if force_download || !cert_path.exists() {
        let cert_url = rpc::make_rpc_url(
            Action::GetCertificate,
            "",
            config,
            &crate::rpc_client::RpcState::default(),
        )?;
        let downloader = crate::downloader::FileDownloader::new(
            cert_url,
            10000,
            300000,
            crate::downloader::DownloadMode::File(cert_path.clone()),
            false,
        );
        downloader.download().await?;
    }

    let cert_data = std::fs::read(&cert_path)?;
    let pkcs12 = Pkcs12::from_der(&cert_data)?;
    // OpenSSL 3.x disables legacy algorithms (including RC2-40-CBC used by
    // the H@H server's PKCS12 certificate bags) unless the legacy provider is
    // explicitly loaded. retain_fallbacks=true keeps the default provider active.
    let _legacy = Provider::try_load(None, "legacy", true)?;
    let pkcs12 = pkcs12.parse2(config.client_key.as_str())?;

    let cert = pkcs12
        .cert
        .as_ref()
        .ok_or_else(|| HathError::Tls("no certificate in PKCS12".into()))?;
    let key = pkcs12
        .pkey
        .as_ref()
        .ok_or_else(|| HathError::Tls("no private key in PKCS12".into()))?;

    let not_after = cert.not_after();

    // Compute cert expiry as a Unix timestamp for runtime checks.
    // ASN1_TIME_diff(from, to) returns to-from, so now.diff(not_after) gives
    // the remaining seconds until expiry (positive while cert is still valid).
    let now_asn1 = Asn1Time::days_from_now(0)?;
    let diff = now_asn1.diff(not_after)?;
    let cert_expiry_unix = unix_time_secs() + diff.days as i64 * SECS_PER_DAY + diff.secs as i64;

    // Java: isCertExpired() returns true when the certificate expires within
    // the next 24 hours.
    if is_cert_expired(cert_expiry_unix) {
        tracing::error!(
            "The retrieved certificate is expired, or the system time is off by more than a day. \
             Correct the system time and try again."
        );
        return Err(HathError::CertExpired);
    }
    // Java builds SSLContext.getInstance("TLS"), leaves cipher suites and other
    // TLS parameters at provider defaults, then enables TLSv1.3/TLSv1.2 (or
    // TLSv1.2 when TLSv1.3 is unavailable). Mirror that by only rejecting
    // protocols below TLSv1.2; OpenSSL keeps its default cipher/group/session
    // policy and default maximum protocol version.
    let mut ctx_builder = SslContext::builder(SslMethod::tls_server())
        .map_err(|e| HathError::Tls(format!("Failed to create SslContextBuilder: {}", e)))?;
    ctx_builder
        .set_min_proto_version(Some(openssl::ssl::SslVersion::TLS1_2))
        .map_err(|e| HathError::Tls(format!("Failed to set min protocol: {}", e)))?;

    ctx_builder
        .set_certificate(cert)
        .map_err(|e| HathError::Tls(format!("Failed to set certificate: {}", e)))?;
    ctx_builder
        .set_private_key(key)
        .map_err(|e| HathError::Tls(format!("Failed to set private key: {}", e)))?;
    ctx_builder
        .check_private_key()
        .map_err(|e| HathError::Tls(format!("Certificate/key mismatch: {}", e)))?;
    if let Some(chain) = pkcs12.ca {
        for ca in chain {
            ctx_builder
                .add_extra_chain_cert(ca)
                .map_err(|e| HathError::Tls(format!("Failed to add chain cert: {}", e)))?;
        }
    }
    Ok((ctx_builder.build(), cert_expiry_unix))
}
