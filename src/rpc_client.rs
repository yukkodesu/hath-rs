use crate::config::Config;
use crate::error::{HathError, Result};
use crate::rpc::{self, ServerResponse, ResponseStatus, Action};
use arc_swap::ArcSwap;
use reqwest::Client;
use std::sync::Arc;

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
    pub async fn call(&self, act: Action, add: &str) -> Result<ServerResponse> {
        let cfg = self.config.load();
        let url = rpc::make_rpc_url(act, add, &cfg)?;
        let host = url.host_str().unwrap_or("unknown").to_string();

        let resp = self.http.get(url).send().await
            .map_err(|e| HathError::Rpc(format!("request failed: {}", e)))?;

        let body = resp.text().await
            .map_err(|e| HathError::Rpc(format!("read failed: {}", e)))?;

        let parsed = rpc::parse_server_response(&body, &host);

        if parsed.status == ResponseStatus::Null {
            let fail_host = parsed.fail_host.as_deref().unwrap_or(&host);
            let mut new = (**cfg).clone();
            new.rpc_last_failed = Some(fail_host.to_string());
            new.rpc_current = None;
            self.config.store(Arc::new(new));
        }

        Ok(parsed)
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
