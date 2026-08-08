use crate::config::Config;
use crate::error::{HathError, Result};
use crate::rpc::{self, Action};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use crate::rpc_client::RpcClient;
use crate::server::{AppState, start_server, stop_server, tls};
use arc_swap::ArcSwap;
use p12::PFX;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::task::JoinHandle;

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

struct ParsedP12Certificate {
    cert_chain: Vec<CertificateDer<'static>>,
    private_key: PrivateKeyDer<'static>,
}

fn parse_p12_certificate(data: &[u8], password: &str) -> Result<ParsedP12Certificate> {
    let pfx = PFX::parse(data)
        .map_err(|error| HathError::Tls(format!("invalid PKCS12 archive: {error}")))?;
    if !pfx.verify_mac(password) {
        return Err(HathError::Tls(
            "PKCS12 archive integrity check failed".into(),
        ));
    }
    let private_key = pfx.key_bags(password).map_err(|error| {
        HathError::Tls(format!("failed to decrypt PKCS12 private key: {error}"))
    })?;
    let cert_chain = pfx
        .cert_x509_bags(password)
        .map_err(|error| HathError::Tls(format!("failed to decrypt PKCS12 certificates: {error}")))?
        .into_iter()
        .map(CertificateDer::from)
        .collect::<Vec<_>>();
    let private_key = select_private_key(private_key)?;

    Ok(ParsedP12Certificate {
        cert_chain: order_certificate_chain(cert_chain, &private_key)?,
        private_key,
    })
}

fn select_private_key(mut private_keys: Vec<Vec<u8>>) -> Result<PrivateKeyDer<'static>> {
    if private_keys.len() != 1 {
        return Err(HathError::Tls(
            "PKCS12 archive must contain exactly one private key".into(),
        ));
    }
    Ok(PrivatePkcs8KeyDer::from(private_keys.pop().unwrap()).into())
}

fn order_certificate_chain(
    mut certificates: Vec<CertificateDer<'static>>,
    private_key: &PrivateKeyDer<'static>,
) -> Result<Vec<CertificateDer<'static>>> {
    let provider = crate::tls::aws_lc_provider();
    let signing_key = provider
        .key_provider
        .load_private_key(private_key.clone_key())
        .map_err(|error| HathError::Tls(format!("invalid PKCS12 private key: {error}")))?;
    let key_spki = signing_key.public_key().ok_or_else(|| {
        HathError::Tls("could not determine PKCS12 private key public key".into())
    })?;

    let mut leaf_index = None;
    for (index, certificate) in certificates.iter().enumerate() {
        let (_, parsed) = x509_parser::prelude::parse_x509_certificate(certificate.as_ref())
            .map_err(|error| HathError::Tls(format!("invalid PKCS12 certificate: {error}")))?;
        if parsed.public_key().raw == key_spki.as_ref() {
            if leaf_index.replace(index).is_some() {
                return Err(HathError::Tls(
                    "multiple PKCS12 certificates match the private key".into(),
                ));
            }
        }
    }
    let leaf_index = leaf_index
        .ok_or_else(|| HathError::Tls("no PKCS12 certificate matches the private key".into()))?;

    let mut chain = vec![certificates.remove(leaf_index)];
    while !certificates.is_empty() {
        let (_, child) =
            x509_parser::prelude::parse_x509_certificate(chain.last().unwrap().as_ref())
                .map_err(|error| HathError::Tls(format!("invalid PKCS12 certificate: {error}")))?;
        let mut issuer_index = None;
        for (index, certificate) in certificates.iter().enumerate() {
            let (_, candidate) = x509_parser::prelude::parse_x509_certificate(certificate.as_ref())
                .map_err(|error| HathError::Tls(format!("invalid PKCS12 certificate: {error}")))?;
            if is_valid_issuer(&child, &candidate)? {
                if issuer_index.replace(index).is_some() {
                    return Err(HathError::Tls(
                        "multiple PKCS12 certificates can issue the same child certificate".into(),
                    ));
                }
            }
        }
        let issuer_index = issuer_index.ok_or_else(|| {
            HathError::Tls("PKCS12 certificate chain is incomplete or invalid".into())
        })?;
        chain.push(certificates.remove(issuer_index));
    }

    Ok(chain)
}

fn is_valid_issuer(
    child: &x509_parser::certificate::X509Certificate<'_>,
    candidate: &x509_parser::certificate::X509Certificate<'_>,
) -> Result<bool> {
    if child.issuer() != candidate.subject() {
        return Ok(false);
    }
    let basic_constraints = candidate
        .basic_constraints()
        .map_err(|error| HathError::Tls(format!("invalid issuer basic constraints: {error}")))?;
    if !basic_constraints.is_some_and(|extension| extension.value.ca) {
        return Ok(false);
    }
    let key_usage = candidate
        .key_usage()
        .map_err(|error| HathError::Tls(format!("invalid issuer key usage: {error}")))?;
    if key_usage.is_some_and(|extension| !extension.value.key_cert_sign()) {
        return Ok(false);
    }
    Ok(child.verify_signature(Some(candidate.public_key())).is_ok())
}

fn certificate_expiry_unix(cert: &CertificateDer<'_>) -> Result<i64> {
    let (_, certificate) = x509_parser::prelude::parse_x509_certificate(cert.as_ref())
        .map_err(|error| HathError::Tls(format!("invalid leaf certificate: {error}")))?;
    Ok(certificate.validity().not_after.timestamp())
}

fn build_server_acceptor(parsed: ParsedP12Certificate) -> Result<tokio_rustls::TlsAcceptor> {
    let config = rustls::ServerConfig::builder_with_provider(crate::tls::aws_lc_provider())
        .with_safe_default_protocol_versions()
        .map_err(|error| HathError::Tls(error.to_string()))?
        .with_no_client_auth()
        .with_single_cert(parsed.cert_chain, parsed.private_key)
        .map_err(|error| HathError::Tls(format!("failed to configure TLS certificate: {error}")))?;
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(config)))
}

/// Build a rustls acceptor from the PKCS12 certificate.
/// If `force_download` is true, always re-download the cert from the RPC server.
/// Returns (acceptor, cert_expiry_unix_seconds).
pub(super) async fn build_tls_acceptor(
    config: &Config,
    force_download: bool,
) -> Result<(tokio_rustls::TlsAcceptor, i64)> {
    let cert_path = config.data_dir.join("hathcert.p12");

    if force_download || !cert_path.exists() {
        let host = rpc::default_rpc_host(config);
        let cert_url = rpc::make_rpc_url(Action::GetCertificate, "", config, &host)?;
        let downloader = crate::downloader::FileDownloader::new(
            cert_url,
            10000,
            300000,
            crate::downloader::DownloadMode::File(cert_path.clone()),
        );
        downloader.download().await?;
    }

    let parsed = parse_p12_certificate(&std::fs::read(&cert_path)?, config.client_key.as_str())?;
    let cert_expiry_unix = certificate_expiry_unix(&parsed.cert_chain[0])?;

    // Java: isCertExpired() returns true when the certificate expires within
    // the next 24 hours.
    if is_cert_expired(cert_expiry_unix) {
        tracing::error!(
            "The retrieved certificate is expired, or the system time is off by more than a day. \
             Correct the system time and try again."
        );
        return Err(HathError::CertExpired);
    }
    Ok((build_server_acceptor(parsed)?, cert_expiry_unix))
}

/// Spawn the certificate refresh watcher.
/// Watches for refresh_certs RPC commands and performs a full server restart
/// (suspend → reject new connections → shutdown old listener → drain → restart → resume).
pub fn spawn_cert_refresh_watcher(
    state: AppState,
    rpc_client: Arc<RpcClient>,
    shutdown: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = state.cert_refresh_notify.notified() => {},
                _ = shutdown.cancelled() => break,
            }
            if !state.do_cert_refresh.load(Ordering::Acquire) {
                continue;
            }
            tracing::info!("Starting certificate refresh (full server restart)...");

            // 1. Suspend traffic
            match rpc_client.client_suspend().await {
                Ok(resp) if resp.status == rpc::ResponseStatus::Ok => {
                    tracing::info!("Suspend notification successful");
                }
                _ => {
                    tracing::warn!(
                        "Failed to contact server to suspend client traffic; will retry"
                    );
                    tokio::time::sleep(Duration::from_secs(10)).await;
                    state.cert_refresh_notify.notify_one();
                    continue;
                }
            }

            // Java waits 5s before httpServerShutdown(true), and that helper
            // waits another 5s before closing the listener.
            tokio::time::sleep(Duration::from_secs(5)).await;

            // 2-5. Reject non-RPC traffic, stop the listener, drain active
            // requests, and wait for the old server generation to terminate.
            stop_server(&state).await;

            // 6. Wait 1s
            tokio::time::sleep(Duration::from_secs(1)).await;

            if shutdown.is_cancelled() {
                break;
            }

            // 7. Start a fresh server generation with a new shutdown token.
            let (ready_rx, new_shutdown) = start_server(state.clone());
            state
                .server_shutdown_token
                .store(Some(Arc::new(new_shutdown)));

            // 8. Wait for new server to bind
            match ready_rx.await {
                Ok(Ok(port)) => {
                    tracing::info!("Server restarted successfully on port {}", port);
                }
                Ok(Err(e)) => {
                    tracing::error!("Server restart failed to bind: {}", e);
                    shutdown.cancel();
                    break;
                }
                Err(_) => {
                    tracing::error!("Server restart failed unexpectedly (oneshot dropped)");
                    shutdown.cancel();
                    break;
                }
            }

            // 9. Re-allow connections
            state.allow_normal_connections.store(true, Ordering::SeqCst);

            // 10. Resume traffic
            match rpc_client.still_alive(true).await {
                Ok(resp) if resp.status == rpc::ResponseStatus::Ok => {
                    tracing::info!("Resume notification successful");
                }
                Ok(resp) => {
                    let code = resp.fail_code.unwrap_or_default();
                    // Java: TERM_BAD_NETWORK → dieWithError (terminate client)
                    if code.starts_with("TERM_BAD_NETWORK") {
                        tracing::error!(
                            "Client is shutting down since the network is misconfigured; \
                             correct firewall/forwarding settings then restart the client."
                        );
                        shutdown.cancel();
                        break;
                    } else {
                        tracing::warn!("Failed stillAlive test: ({}) - will retry later", code);
                    }
                }
                Err(e) => {
                    tracing::warn!("Still-alive request failed: {}", e);
                }
            }

            state.do_cert_refresh.store(false, Ordering::Release);
            tracing::info!("Certificate refresh completed successfully");
        }
    })
}

/// Spawn periodic time check + cert expiry check (5min interval).
/// If time drift >24h or cert expires within 24h, triggers global shutdown.
pub fn spawn_time_cert_check(
    config: Arc<ArcSwap<Config>>,
    state: AppState,
    shutdown: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    let global_shutdown = shutdown.clone();
    tokio::spawn(crate::utils::tick_every(
        shutdown,
        Duration::from_secs(300),
        move || {
            let config = config.clone();
            let state = state.clone();
            let shutdown_signal = global_shutdown.clone();
            async move {
                if config.load().server_time_delta.abs() > 86400 {
                    tracing::warn!("System time off by >24h. Correct your system clock.");
                }
                if let Some(expiry) = *state.cert_expiry.lock().await
                    && tls::is_cert_expired(expiry)
                {
                    tracing::error!(
                        "Either the system clock is significantly wrong, or something has \
                         gone wrong with certificate renewal. Check your system clock and \
                         internet connection, then restart the client manually."
                    );
                    shutdown_signal.cancel();
                }
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn imports_an_rc2_40_pkcs12_archive() {
        let parsed = parse_p12_certificate(
            include_bytes!("../../tests/fixtures/legacy-rc2.p12"),
            "test-client-key",
        )
        .unwrap();
        assert!(!parsed.cert_chain.is_empty());
        assert!(matches!(
            parsed.private_key,
            rustls::pki_types::PrivateKeyDer::Pkcs8(_)
        ));
    }

    #[test]
    fn rejects_a_wrong_pkcs12_password() {
        assert!(
            parse_p12_certificate(
                include_bytes!("../../tests/fixtures/legacy-rc2.p12"),
                "wrong-password",
            )
            .is_err()
        );
    }

    #[test]
    fn rejects_a_pkcs12_archive_with_a_tampered_mac() {
        let mut archive = include_bytes!("../../tests/fixtures/legacy-rc2.p12").to_vec();
        *archive.last_mut().unwrap() ^= 1;

        assert!(parse_p12_certificate(&archive, "test-client-key").is_err());
    }

    #[test]
    fn rejects_corrupt_pkcs12_data() {
        assert!(parse_p12_certificate(b"not a p12 archive", "test-client-key").is_err());
    }

    #[test]
    fn orders_an_unordered_multi_certificate_pkcs12_chain_with_the_leaf_first() {
        let pfx = PFX::parse(include_bytes!("../../tests/fixtures/legacy-rc2-chain.p12")).unwrap();
        let private_key: PrivateKeyDer<'static> =
            PrivatePkcs8KeyDer::from(pfx.key_bags("test-client-key").unwrap().pop().unwrap())
                .into();
        let mut certificates = pfx
            .cert_x509_bags("test-client-key")
            .unwrap()
            .into_iter()
            .map(CertificateDer::from)
            .collect::<Vec<_>>();
        certificates.reverse();

        let cert_chain = order_certificate_chain(certificates, &private_key).unwrap();
        let (_, leaf) =
            x509_parser::prelude::parse_x509_certificate(cert_chain[0].as_ref()).unwrap();
        let (_, issuer) =
            x509_parser::prelude::parse_x509_certificate(cert_chain[1].as_ref()).unwrap();

        assert_eq!(leaf.subject().to_string(), "CN=hath-rs-test-leaf.invalid");
        assert_eq!(issuer.subject().to_string(), "CN=hath-rs-test-root.invalid");
        assert_eq!(leaf.issuer(), issuer.subject());
        build_server_acceptor(ParsedP12Certificate {
            cert_chain,
            private_key,
        })
        .expect("ordered chain must configure rustls");
    }

    #[test]
    fn rejects_a_same_subject_candidate_that_did_not_sign_the_child_certificate() {
        let pfx = PFX::parse(include_bytes!("../../tests/fixtures/legacy-rc2-chain.p12")).unwrap();
        let private_key: PrivateKeyDer<'static> =
            PrivatePkcs8KeyDer::from(pfx.key_bags("test-client-key").unwrap().pop().unwrap())
                .into();
        let mut certificates = pfx
            .cert_x509_bags("test-client-key")
            .unwrap()
            .into_iter()
            .map(CertificateDer::from)
            .collect::<Vec<_>>();
        certificates.push(CertificateDer::from(
            include_bytes!("../../tests/fixtures/same-subject-wrong-issuer.der").to_vec(),
        ));
        certificates.reverse();

        assert!(order_certificate_chain(certificates, &private_key).is_err());
    }

    #[test]
    fn rejects_multiple_pkcs12_private_keys() {
        let pfx = PFX::parse(include_bytes!("../../tests/fixtures/legacy-rc2.p12")).unwrap();
        let private_key = pfx.key_bags("test-client-key").unwrap().pop().unwrap();

        assert!(select_private_key(vec![private_key.clone(), private_key]).is_err());
    }

    #[test]
    fn expiry_window_matches_the_existing_24_hour_rule() {
        assert!(!is_cert_expired(
            unix_time_secs() + CERT_RENEWAL_WINDOW_SECS + 1
        ));
        assert!(is_cert_expired(
            unix_time_secs() + CERT_RENEWAL_WINDOW_SECS - 1
        ));
    }

    #[test]
    fn imported_fixture_builds_a_rustls_server_config() {
        let parsed = parse_p12_certificate(
            include_bytes!("../../tests/fixtures/legacy-rc2.p12"),
            "test-client-key",
        )
        .unwrap();
        build_server_acceptor(parsed).expect("fixture must configure rustls");
    }
}
