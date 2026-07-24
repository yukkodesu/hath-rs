use super::super::AppState;
use super::super::response::{self, ResponseSpec};
use super::super::threaded_proxy::run_threaded_proxy_test;
use crate::bandwidth::BandwidthMonitor;
use crate::config::Config;
use crate::error::Result;
use crate::utils;
use hyper::StatusCode;
use std::sync::Arc;

/// Helper for `threaded_proxy_test`: extract required params from Additional,
/// returning `INVALID_COMMAND` on missing/illegal values (matching Java's
/// NumberFormatException -> catch -> INVALID_COMMAND flow).
macro_rules! required_param {
    ($add:expr, $field:ident) => {
        match $add.$field.as_deref() {
            Some(v) => v,
            None => return response::text_response(StatusCode::OK, "INVALID_COMMAND"),
        }
    };
    ($add:expr, $field:ident, $T:ty) => {
        match $add.$field.as_deref().and_then(|v| v.parse::<$T>().ok()) {
            Some(v) => v,
            None => return response::text_response(StatusCode::OK, "INVALID_COMMAND"),
        }
    };
    ($add:expr, $field:ident, $T:ty, default $default:expr) => {
        match $add.$field.as_deref() {
            Some(v) => match v.parse::<$T>() {
                Ok(n) => n,
                Err(_) => return response::text_response(StatusCode::OK, "INVALID_COMMAND"),
            },
            None => $default,
        }
    };
}

/// Handle servercmd API commands. Must support all Java commands.
pub(super) async fn handle_server_command(
    command: &str,
    additional: &str,
    state: &AppState,
) -> Result<ResponseSpec> {
    match command.to_lowercase().as_str() {
        "still_alive" => {
            response::text_response(StatusCode::OK, "I feel FANTASTIC and I'm still alive")
        }
        "threaded_proxy_test" => {
            // Java: Integer.parseInt on missing/illegal params throws NFE,
            // caught by processRemoteAPICommand -> returns "INVALID_COMMAND".
            let add = utils::parse_additional(additional);
            let hostname = required_param!(add, hostname);
            let protocol = required_param!(add, protocol);
            let port: u16 = required_param!(add, port, u16);
            let testsize: u64 = required_param!(add, testsize, u64);
            let testcount: u32 = required_param!(add, testcount, u32);
            let testtime: u32 = required_param!(add, testtime, u32);
            let testkey = add.testkey.as_deref().unwrap_or("");

            tracing::debug!(
                "Running speedtest against hostname={} protocol={} port={} testsize={} testcount={} testtime={} testkey={}",
                hostname,
                protocol,
                port,
                testsize,
                testcount,
                testtime,
                testkey
            );

            let result = run_threaded_proxy_test(
                hostname, protocol, port, testsize, testcount, testtime, testkey,
            )
            .await;

            tracing::debug!(
                "Ran speedtest against hostname={} testsize={} testcount={}, reporting successfulTests={} totalTimeMillis={}",
                hostname,
                testsize,
                testcount,
                result.0,
                result.1
            );

            response::text_response(StatusCode::OK, &format!("OK:{}-{}", result.0, result.1))
        }
        "speed_test" => {
            // Java: additional is parsed as key=value pairs via Tools.parseAdditional();
            // testsize is read from addTable with default 1_000_000. No upper limit.
            let add = utils::parse_additional(additional);
            let testsize: usize = required_param!(add, testsize, usize, default 1_000_000);
            response::speedtest_response(testsize)
        }
        "refresh_settings" => {
            match state.rpc_client.refresh_settings().await {
                Ok(sr) if sr.status == crate::rpc::ResponseStatus::Ok => {
                    Config::apply_server_response(&state.config, &sr);
                    // Recreate bandwidth monitor if throttle_bytes changed
                    let cfg = state.config.load_full();
                    if cfg.throttle_bytes > 0 && !cfg.disable_bwm {
                        state
                            .bandwidth_monitor
                            .store(Some(Arc::new(BandwidthMonitor::new(cfg.throttle_bytes))));
                    } else {
                        state.bandwidth_monitor.store(None);
                    }
                    response::text_response(StatusCode::OK, "")
                }
                _ => response::text_response(StatusCode::OK, ""),
            }
        }
        "start_downloader" => {
            match state.gallery_downloader.start() {
                crate::gallery_downloader::StartOutcome::Started => {
                    tracing::info!("Started gallery downloader");
                }
                crate::gallery_downloader::StartOutcome::AlreadyRunning => {
                    tracing::debug!("Gallery downloader is already running");
                }
            }
            response::text_response(StatusCode::OK, "")
        }
        "refresh_certs" => {
            // Java: client.setCertRefresh() — just set the flag, main loop does
            // the actual work (suspend -> shutdown -> restart -> resume).
            state
                .do_cert_refresh
                .store(true, std::sync::atomic::Ordering::Release);
            state.cert_refresh_notify.notify_one();
            response::text_response(StatusCode::OK, "")
        }
        _ => response::text_response(StatusCode::OK, "INVALID_COMMAND"),
    }
}
