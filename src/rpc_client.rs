use crate::config::Config;
use crate::error::{HathError, Result};
use crate::rpc::{self, ServerResponse, ResponseStatus, Action};
use crate::stats::Stats;
use arc_swap::ArcSwap;
use reqwest::Client;
use std::sync::Arc;
use std::time::Duration;

/// Shared HTTP client for RPC calls.
pub struct RpcClient {
    http: Client,
    config: Arc<ArcSwap<Config>>,
}

impl RpcClient {
    pub fn new(config: Arc<ArcSwap<Config>>) -> Result<Self> {
        let http = Client::builder()
            .user_agent(format!("Hentai@Home {}", rpc::CLIENT_VERSION))
            .build()
            .map_err(|e| HathError::Network(e.to_string()))?;
        Ok(Self { http, config })
    }

    /// Execute an RPC call and return the parsed response.
    /// Java: ServerResponse.getServerResponse() — handles KEY_EXPIRED by
    /// refreshing server_stat and retrying with corrected time.
    pub async fn call(&self, act: Action, add: &str) -> Result<ServerResponse> {
        let mut key_expired_retries = 0u32;
        loop {
            let cfg = self.config.load();
            let url = rpc::make_rpc_url(act, add, &cfg)?;
            let host = url.host_str().unwrap_or("unknown").to_string();

            let resp = self.http.get(url).send().await
                .map_err(|e| HathError::Rpc(format!("request failed: {}", e)))?;

            let body = resp.text().await
                .map_err(|e| HathError::Rpc(format!("read failed: {}", e)))?;

            let parsed = rpc::parse_server_response(&body, &host);

            // Java: KEY_EXPIRED triggers refreshServerStat() and retry (max 2 retries)
            if parsed.fail_code.as_deref() == Some("KEY_EXPIRED") && key_expired_retries < 2 {
                key_expired_retries += 1;
                tracing::info!("KEY_EXPIRED received, refreshing server stat and retrying ({}/{})...", key_expired_retries, 2);
                // Inline server_stat to avoid recursive call()
                if let Ok(stat_resp) = self.call_stat_inner().await
                    && stat_resp.status == ResponseStatus::Ok {
                        crate::config::Config::apply_server_response(&self.config, &stat_resp);
                    }
                continue; // retry the original request with corrected time
            }

            if parsed.status == ResponseStatus::Null {
                let fail_host = parsed.fail_host.as_deref().unwrap_or(&host);
                let mut new = (**cfg).clone();
                new.rpc_last_failed = Some(fail_host.to_string());
                // Don't clear rpc_current — get_rpc_host() checks last_failed
                // against cached host and will skip it if it matches.
                self.config.store(Arc::new(new));
            } else {
                // Java: persist the selected RPC host so subsequent calls reuse it.
                // rpcServerCurrent is cached until cleared on failure or periodic reset.
                let mut new = (**cfg).clone();
                new.rpc_current = Some(host.clone());
                self.config.store(Arc::new(new));
            }

            return Ok(parsed);
        }
    }

    /// Inline server_stat HTTP call (not recursive through call()).
    async fn call_stat_inner(&self) -> Result<ServerResponse> {
        let cfg = self.config.load();
        let url = rpc::make_rpc_url(Action::ServerStat, "", &cfg)?;
        let host = url.host_str().unwrap_or("unknown").to_string();
        let resp = self.http.get(url).send().await
            .map_err(|e| HathError::Rpc(format!("stat request failed: {}", e)))?;
        let body = resp.text().await
            .map_err(|e| HathError::Rpc(format!("stat read failed: {}", e)))?;
        Ok(rpc::parse_server_response(&body, &host))
    }

    /// Server stat: get server time and minimum build.
    pub async fn server_stat(&self) -> Result<ServerResponse> {
        self.call(Action::ServerStat, "").await
    }

    /// Client login: authenticate and get full settings.
    pub async fn client_login(&self) -> Result<ServerResponse> {
        self.call(Action::ClientLogin, "").await
    }

    /// Client start: notify server we're ready, triggers connectivity test.
    pub async fn client_start(&self) -> Result<ServerResponse> {
        self.call(Action::ClientStart, "").await
    }

    /// Client stop: notify server we're shutting down.
    pub async fn client_stop(&self) -> Result<ServerResponse> {
        self.call(Action::ClientStop, "").await
    }

    /// Client suspend.
    pub async fn client_suspend(&self) -> Result<ServerResponse> {
        self.call(Action::ClientSuspend, "").await
    }

    /// Client resume.
    pub async fn client_resume(&self) -> Result<ServerResponse> {
        self.call(Action::ClientResume, "").await
    }

    /// Still-alive heartbeat. If resume=true, also notifies resume.
    pub async fn still_alive(&self, resume: bool) -> Result<ServerResponse> {
        let add = if resume { "resume" } else { "" };
        self.call(Action::StillAlive, add).await
    }

    /// Refresh settings from server.
    pub async fn refresh_settings(&self) -> Result<ServerResponse> {
        self.call(Action::ClientSettings, "").await
    }

    /// Get blacklisted files since `deltatime` seconds ago.
    pub async fn get_blacklist(&self, deltatime: u64) -> Result<ServerResponse> {
        self.call(Action::GetBlacklist, &deltatime.to_string()).await
    }

    /// Notify server of overload.
    pub async fn notify_overload(&self) -> Result<ServerResponse> {
        self.call(Action::Overload, "").await
    }

    /// Fetch download URLs for a static range file.
    pub async fn static_range_fetch(&self, fileindex: &str, xres: &str, fileid: &str) -> Result<ServerResponse> {
        let add = format!("{};{};{}", fileindex, xres, fileid);
        self.call(Action::StaticRangeFetch, &add).await
    }
}

/// Spawn periodic still_alive heartbeat (110s interval).
pub fn spawn_still_alive_heartbeat(
    rpc_client: Arc<RpcClient>,
    stats: Arc<Stats>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(crate::utils::tick_every(shutdown, Duration::from_secs(110), move || {
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

/// Spawn periodic RPC server failure clearer (4h interval).
pub fn spawn_rpc_failure_clearer(
    config: Arc<ArcSwap<Config>>,
    shutdown: tokio_util::sync::CancellationToken,
) {
    tokio::spawn(crate::utils::tick_every(shutdown, Duration::from_secs(14400), move || {
        let config = config.clone();
        async move {
            let cfg = config.load();
            if cfg.rpc_last_failed.is_some() {
                let mut new = (**cfg).clone();
                new.rpc_last_failed = None;
                new.rpc_current = None;
                config.store(Arc::new(new));
            }
        }
    }));
}
