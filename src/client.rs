use crate::cache::{self, CacheHandler};
use crate::config::{CliArgs, Config};
use crate::error::{HathError, Result};
use crate::rpc::{self, ResponseStatus};
use crate::rpc_client::{self, RpcClient};
use crate::server::{self, AppState};
use crate::stats::Stats;

use arc_swap::{ArcSwap, ArcSwapOption};
use clap::Parser;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use tokio::sync::{Mutex, Notify};

/// Main client entry point. Follows Java HentaiAtHomeClient.run() lifecycle.
pub async fn run() -> Result<()> {
    let args = CliArgs::parse();
    let config = Config::load(args)?;

    // 1. Initialize directories
    config.initialize_directories()?;

    // 2. Start logging
    crate::logging::init_logging(&config.log_dir, !config.disable_logging)?;

    tracing::info!(
        "Hentai@Home {} (Build {}) starting up",
        rpc::CLIENT_VERSION,
        rpc::CLIENT_BUILD
    );
    tracing::info!("Copyright (c) 2008-2026, E-Hentai.org - all rights reserved.");
    tracing::info!(
        "This software comes with ABSOLUTELY NO WARRANTY. This is free software, and you are welcome to modify and redistribute it under the GPL v3 license."
    );

    let shutdown = tokio_util::sync::CancellationToken::new();

    // Handle Ctrl+C / SIGTERM for graceful shutdown (Java: ShutdownHook)
    {
        let s = shutdown.clone();
        tokio::spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            tracing::info!("Interrupt received, shutting down gracefully...");
            s.cancel();
        });
    }

    // 3. Save client_login if newly provided via CLI
    if config.client_id.0 > 0 && !config.client_key.as_str().is_empty() {
        let _ = config.save_client_login();
    }

    // Validate credentials
    if config.client_id.0 < 1000 || config.client_key.as_str().len() != 20 {
        return Err(HathError::Config("Invalid credentials".into()));
    }

    // 4. Wrap config in ArcSwap for sharing
    let config = Arc::new(ArcSwap::from(Arc::new(config)));

    // 5. Server stat: get time and min build
    let rpc_client = Arc::new(RpcClient::new(config.clone())?);
    tracing::info!("Getting initial stat from server...");

    let stat_resp = rpc_client.server_stat().await?;
    if stat_resp.status != ResponseStatus::Ok {
        return Err(HathError::Rpc(
            "Failed to get initial stat from server".into(),
        ));
    }

    // 6. Client login: get full settings
    tracing::info!("Reading client settings from server...");
    let login_resp = rpc_client.client_login().await?;
    if login_resp.status != ResponseStatus::Ok {
        let code = login_resp.fail_code.unwrap_or_default();
        return Err(HathError::Rpc(format!("Login failed: {}", code)));
    }

    // Apply server settings via rcu (clone → modify → atomic swap, zero unsafe)
    config.rcu(|current| {
        let mut new = (**current).clone();
        for line in &login_resp.lines {
            if let Some((key, value)) = line.split_once('=') {
                new.apply_setting(&key.to_lowercase(), value);
            }
        }
        Arc::new(new)
    });

    // 7. Init cache
    let stats = Arc::new(Stats::new());
    let cache = Arc::new(CacheHandler::new(config.clone(), stats.clone())?);

    // 8. Build AppState and spawn HTTP server
    let allow_connections = Arc::new(AtomicBool::new(false));
    let flood_control = Arc::new(Mutex::new(HashMap::new()));

    let app_state = AppState {
        config: config.clone(),
        stats: stats.clone(),
        cache: cache.clone(),
        rpc_client: rpc_client.clone(),
        allow_normal_connections: allow_connections.clone(),
        flood_control: flood_control.clone(),
        tls_acceptor: Arc::new(ArcSwapOption::const_empty()),
        cert_expiry: Arc::new(Mutex::new(None)),
        bandwidth_monitor: Arc::new(ArcSwapOption::const_empty()),
        active_connections: Arc::new(AtomicU32::new(0)),
        last_overload_notification: Arc::new(Mutex::new(None)),
        do_cert_refresh: Arc::new(AtomicBool::new(false)),
        cert_refresh_notify: Arc::new(Notify::new()),
        server_restart_token: Arc::new(ArcSwapOption::const_empty()),
        server_terminated: Arc::new(AtomicBool::new(false)),
    };

    let (ready_rx, restart_token) = server::spawn_server(app_state.clone(), shutdown.clone());
    app_state
        .server_restart_token
        .store(Some(Arc::new(restart_token)));

    // Wait for server to bind before notifying the RPC server
    match ready_rx.await {
        Ok(Ok(_port)) => {}
        Ok(Err(e)) => {
            return Err(HathError::Config(format!("Server startup failed: {}", e)));
        }
        Err(_) => {
            return Err(HathError::Config(
                "Server startup failed unexpectedly".into(),
            ));
        }
    }

    // 9. notifyStart: tell server we're ready (this triggers connectivity test)
    let start_resp = rpc_client.client_start().await?;
    if start_resp.status != ResponseStatus::Ok {
        let code = start_resp.fail_code.unwrap_or_default();
        tracing::error!("Startup failure: {}", code);
        if code.starts_with("FAIL_OTHER_CLIENT_CONNECTED") || code.starts_with("FAIL_CID_IN_USE") {
            tracing::error!(
                "Another client with the same ID ({}) is already connected to the server. \
                 This can happen if the Java client is still running. \
                 If you are switching from the Java client, make sure to stop it first, \
                 then wait 5-10 minutes before starting this client.",
                config.load().client_id.0
            );
            return Err(HathError::Fatal(code));
        }
        // Java: FAIL_CONNECT_TEST is non-fatal — prints troubleshooting info and keeps running
        if code.starts_with("FAIL_CONNECT_TEST") {
            tracing::error!(
                "FAIL_CONNECT_TEST: The server was unable to verify your connection. \
                 This usually means your port forwarding or firewall settings are incorrect. \
                 Please ensure port {} is accessible from the internet.",
                config.load().client_port
            );
        } else {
            return Err(HathError::Fatal(format!(
                "Unexpected client_start failure: {}",
                code
            )));
        }
    }

    // 10. Allow normal connections
    allow_connections.store(true, Ordering::SeqCst);
    stats.program_started();

    // Refresh settings after notifyStart
    match rpc_client.refresh_settings().await {
        Ok(refresh_resp) if refresh_resp.status == ResponseStatus::Ok => {
            config.rcu(|current| {
                let mut new = (**current).clone();
                for line in &refresh_resp.lines {
                    if let Some((key, value)) = line.split_once('=') {
                        new.apply_setting(&key.to_lowercase(), value);
                    }
                }
                Arc::new(new)
            });
        }
        Ok(_) => {
            tracing::warn!("refresh_settings returned non-OK status after startup");
        }
        Err(e) => {
            tracing::warn!("Failed to refresh settings after startup: {}", e);
        }
    }

    // Initial blacklist fetch (3-day delta, synchronous)
    cache::fetch_initial_blacklist(&rpc_client, &cache).await;

    tracing::info!("Startup completed successfully. Starting normal operation");

    // 11. Spawn periodic background tasks
    cache::spawn_pruner(cache.clone(), config.clone(), shutdown.clone());
    cache::spawn_periodic_stats(cache.clone(), stats.clone(), shutdown.clone());
    server::spawn_flood_control_pruner(app_state.clone(), shutdown.clone());
    rpc_client::spawn_still_alive_heartbeat(rpc_client.clone(), stats.clone(), shutdown.clone());
    server::spawn_time_cert_check(config.clone(), app_state.clone(), shutdown.clone());
    rpc_client::spawn_rpc_failure_clearer(config.clone(), shutdown.clone());
    cache::spawn_blacklist_fetcher(rpc_client.clone(), cache.clone(), shutdown.clone());
    server::spawn_cert_refresh_watcher(app_state.clone(), rpc_client.clone(), shutdown.clone());

    // Wait for shutdown signal
    shutdown.cancelled().await;

    // Graceful shutdown (Java order: client_stop → drain connections → save data)
    tracing::info!("Shutting down...");
    rpc_client.client_stop().await.ok();
    cache.save_persistent_data();
    {
        let cfg = config.load();
        cfg.save_client_login().ok();
    }

    Ok(())
}
