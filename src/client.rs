use crate::config::{Config, CliArgs};
use crate::error::{HathError, Result};
use crate::cache::CacheHandler;
use crate::rpc::{self, ResponseStatus};
use crate::rpc_client::RpcClient;
use crate::server::{self, AppState, prune_flood_control};
use crate::stats::Stats;

use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::Mutex;

/// Run `f` on each tick of an interval, until `shutdown` fires.
async fn tick_every<F, Fut>(shutdown: tokio_util::sync::CancellationToken, every: Duration, mut f: F)
where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let mut tick = tokio::time::interval(every);
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tick.tick() => f().await,
        }
    }
}

/// Main client entry point. Follows Java HentaiAtHomeClient.run() lifecycle.
pub async fn run() -> Result<()> {
    let args = CliArgs::parse();
    let config = Config::load(args)?;

    // 1. Initialize directories
    config.initialize_directories()?;

    // 2. Start logging
    crate::logging::init_logging(&config.log_dir, !config.disable_logging)?;

    tracing::info!("Hentai@Home {} (Build {}) starting up", rpc::CLIENT_VERSION, rpc::CLIENT_BUILD);

    let shutdown = tokio_util::sync::CancellationToken::new();

    // 3. Save client_login if newly provided via CLI
    if config.client_id.0 > 0 && !config.client_key.as_str().is_empty() {
        let _ = config.save_client_login();
    }

    // Validate credentials
    if config.client_id.0 < 1000 || config.client_key.as_str().len() != 20 {
        return Err(HathError::Config("Invalid credentials".into()));
    }

    // 4. Server stat: get time and min build
    let config = Arc::new(config);
    let rpc_client = Arc::new(RpcClient::new(config.clone())?);
    tracing::info!("Getting initial stat from server...");

    let stat_resp = rpc_client.server_stat().await?;
    if stat_resp.status != ResponseStatus::Ok {
        return Err(HathError::Rpc("Failed to get initial stat from server".into()));
    }

    // 5. Client login: get full settings
    tracing::info!("Reading client settings from server...");
    let login_resp = rpc_client.client_login().await?;
    if login_resp.status != ResponseStatus::Ok {
        let code = login_resp.fail_code.unwrap_or_default();
        return Err(HathError::Rpc(format!("Login failed: {}", code)));
    }

    // Apply server settings (unsafe save my life)
    {
        let config_ptr = Arc::as_ptr(&config) as *mut Config;
        unsafe { &mut *config_ptr }.apply_server_settings(&login_resp.lines);
    }

    // 6. Init cache
    let stats = Arc::new(Stats::new());
    let cache = Arc::new(Mutex::new(CacheHandler::new(config.clone(), stats.clone())?));

    // 7. Download cert + start HTTP server
    let allow_connections = Arc::new(AtomicBool::new(false));
    let flood_control = Arc::new(Mutex::new(HashMap::new()));

    let app_state = AppState {
        config: config.clone(),
        stats: stats.clone(),
        cache: cache.clone(),
        rpc_client: rpc_client.clone(),
        allow_normal_connections: allow_connections.clone(),
        flood_control: flood_control.clone(),
    };

    let server_shutdown = shutdown.clone();
    let server_state = app_state.clone();
    tokio::spawn(async move {
        if let Err(e) = server::start_server(server_state, server_shutdown).await {
            tracing::error!("Server error: {}", e);
        }
    });

    // 8. notifyStart: tell server we're ready (this triggers connectivity test)
    let start_resp = rpc_client.client_start().await?;
    if start_resp.status != ResponseStatus::Ok {
        let code = start_resp.fail_code.unwrap_or_default();
        tracing::error!("Startup failure: {}", code);
        if code.starts_with("FAIL_OTHER_CLIENT_CONNECTED") || code.starts_with("FAIL_CID_IN_USE") {
            return Err(HathError::Fatal(code));
        }
    }

    // 9. Allow normal connections
    allow_connections.store(true, Ordering::SeqCst);
    stats.program_started();

    tracing::info!("Startup completed successfully. Starting normal operation");

    // 10. Spawn periodic background tasks

    // 10s: LRU cycle + shift stats
    {
        let cache = cache.clone();
        let stats = stats.clone();
        tokio::spawn(tick_every(shutdown.clone(), Duration::from_secs(10), move || {
            let cache = cache.clone();
            let stats = stats.clone();
            async move {
                if let Ok(mut c) = cache.try_lock() {
                    c.cycle_lru_cache_table();
                }
                stats.shift_bytes_sent_history();
            }
        }));
    }

    // 60s: prune flood control
    {
        let app_state = app_state.clone();
        tokio::spawn(tick_every(shutdown.clone(), Duration::from_secs(60), move || {
            let app_state = app_state.clone();
            async move { prune_flood_control(&app_state).await }
        }));
    }

    // 110s: still_alive
    {
        let rpc_client = rpc_client.clone();
        let stats = stats.clone();
        tokio::spawn(tick_every(shutdown.clone(), Duration::from_secs(110), move || {
            let rpc_client = rpc_client.clone();
            let stats = stats.clone();
            async move {
                if let Err(e) = rpc_client.still_alive(false).await {
                    tracing::warn!("Still-alive failed: {}", e);
                } else {
                    stats.record_server_contact();
                }
            }
        }));
    }

    // 5min: time check
    {
        let config = config.clone();
        tokio::spawn(tick_every(shutdown.clone(), Duration::from_secs(300), move || {
            let config = config.clone();
            async move {
                if config.server_time_delta.abs() > 86400 {
                    tracing::warn!("System time off by >24h. Correct your system clock.");
                }
            }
        }));
    }

    // 4h: clear RPC server failure
    {
        let config = config.clone();
        tokio::spawn(tick_every(shutdown.clone(), Duration::from_secs(14400), move || {
            let config = config.clone();
            async move { config.clear_rpc_server_failure() }
        }));
    }

    // 6h: fetch blacklist
    {
        let rpc_client = rpc_client.clone();
        let cache = cache.clone();
        tokio::spawn(tick_every(shutdown.clone(), Duration::from_secs(21600), move || {
            let rpc_client = rpc_client.clone();
            let cache = cache.clone();
            async move {
                if let Ok(resp) = rpc_client.get_blacklist(43200).await
                    && resp.status == ResponseStatus::Ok {
                        for fileid in &resp.lines {
                            if let Ok(mut c) = cache.try_lock() {
                                let _ = c.delete_file_from_cache(fileid);
                            }
                        }
                    }
            }
        }));
    }

    // Wait for shutdown signal
    shutdown.cancelled().await;

    // Graceful shutdown
    tracing::info!("Shutting down...");
    rpc_client.client_stop().await.ok();
    config.save_client_login().ok();

    Ok(())
}
