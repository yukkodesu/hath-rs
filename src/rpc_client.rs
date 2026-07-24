use crate::config::Config;
use crate::error::{HathError, Result};
use crate::rpc::{self, Action, ResponseStatus, ServerResponse};
use crate::stats::Stats;
use arc_swap::ArcSwap;
use rand::Rng;
use reqwest::{Client, StatusCode, Url};
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::task::JoinHandle;

const KEY_EXPIRED_RETRY_LIMIT: u32 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GalleryAck {
    pub(crate) gid: u32,
    pub(crate) minxres: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct GalleryFileRequest {
    pub(crate) gid: u32,
    pub(crate) page: u32,
    pub(crate) fileindex: u32,
    pub(crate) xres: String,
    pub(crate) attempt: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum GalleryQueueReply {
    NoPendingDownloads,
    InvalidRequest,
    Metadata(String),
}

#[derive(Debug)]
struct RpcRequestError {
    message: String,
}

impl RpcRequestError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for RpcRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.message.fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RpcStatusAction {
    ReadBody,
    Retry,
    Abort,
}

fn rpc_status_action(status: StatusCode) -> RpcStatusAction {
    if status == StatusCode::NOT_FOUND {
        RpcStatusAction::Abort
    } else if status.is_client_error() || status.is_server_error() {
        RpcStatusAction::Retry
    } else {
        RpcStatusAction::ReadBody
    }
}

/// Mutable RPC routing state, separated from Config to avoid cloning the
/// entire Config (including static_ranges HashMap) on every RPC call.
/// Java: Settings.rpcServerCurrent / rpcServerLastFailed (static fields).
#[derive(Default)]
struct RpcState {
    /// Currently selected RPC host. Cached until failure or periodic reset.
    /// Java: Settings.rpcServerCurrent
    rpc_current: Option<String>,
    /// Last failed RPC host, skipped during host selection.
    /// Java: Settings.rpcServerLastFailed
    rpc_last_failed: Option<String>,
}

#[derive(Default)]
struct RpcRouter {
    state: Mutex<RpcState>,
}

impl RpcRouter {
    fn select_host(&self, config: &Config) -> String {
        let mut state = self.state.lock().unwrap();

        if let Some(ref current) = state.rpc_current
            && !Self::host_is_configured(config, current)
        {
            state.rpc_current = None;
        }

        if let Some(ref current) = state.rpc_current {
            if let Some(ref failed) = state.rpc_last_failed
                && current == failed
            {
                tracing::debug!("{} was marked as last failed (from cache)", failed);
            } else {
                return current.clone();
            }
        }

        Self::choose_host(config, state.rpc_last_failed.as_deref())
    }

    fn mark_failed(&self, host: String) {
        self.state.lock().unwrap().rpc_last_failed = Some(host);
    }

    fn mark_success(&self, host: String) {
        self.state.lock().unwrap().rpc_current = Some(host);
    }

    fn clear(&self) {
        let mut state = self.state.lock().unwrap();
        state.rpc_last_failed = None;
        state.rpc_current = None;
    }

    fn host_is_configured(config: &Config, host: &str) -> bool {
        config
            .rpc_servers
            .iter()
            .any(|server| server.to_string().to_lowercase() == host)
    }

    fn choose_host(config: &Config, last_failed: Option<&str>) -> String {
        if config.rpc_servers.is_empty() {
            return "rpc.hentaiathome.net".to_string();
        }
        if config.rpc_servers.len() == 1 {
            return config.rpc_servers[0].to_string().to_lowercase();
        }

        let mut rng = rand::rng();
        let mut idx: isize = (rng.next_u32() as usize % config.rpc_servers.len()) as isize;
        let dir: isize = if rng.next_u32() & 1 == 0 { -1 } else { 1 };
        let len = config.rpc_servers.len() as isize;
        loop {
            let candidate = config.rpc_servers[((len + idx) % len) as usize]
                .to_string()
                .to_lowercase();
            if let Some(failed) = last_failed
                && candidate == failed
            {
                tracing::debug!("{} was marked as last failed", failed);
                idx += dir;
                continue;
            }
            tracing::debug!("Selected rpcServerCurrent={}", candidate);
            break candidate;
        }
    }
}

/// Shared HTTP client for RPC calls.
pub struct RpcClient {
    http: Client,
    config: Arc<ArcSwap<Config>>,
    router: RpcRouter,
}

impl RpcClient {
    pub fn new(config: Arc<ArcSwap<Config>>) -> Result<Self> {
        // Java: http.keepAlive=false (global), FileDownloader uses connect/read
        // timeout. Long timeouts for slow RPC responses.
        let http = Client::builder()
            .user_agent(format!("Hentai@Home {}", rpc::CLIENT_VERSION))
            // Java: setConnectTimeout(5000), FileDownloader(timeout=3600000)
            .connect_timeout(Duration::from_secs(5))
            .read_timeout(Duration::from_secs(3600))
            .build()
            .map_err(|e| HathError::Network(e.to_string()))?;
        Ok(Self {
            http,
            config,
            router: RpcRouter::default(),
        })
    }

    /// Execute an RPC call and return the parsed response.
    /// Java: ServerResponse.getServerResponse() — handles KEY_EXPIRED by
    /// refreshing server_stat and retrying with corrected time.
    pub async fn call(&self, act: Action, add: &str) -> Result<ServerResponse> {
        let mut key_expired_retries = 0u32;
        loop {
            let cfg = self.config.load();
            let host = self.router.select_host(&cfg);
            let url = rpc::make_rpc_url(act, add, &cfg, &host)?;
            let host = url.host_str().unwrap_or("unknown").to_string();

            // Java: FileDownloader retries up to 3 times on network failure.
            // 404 (FileNotFoundException) is not retried; HTTP-level errors are.
            let body = match Self::send_rpc_request(&self.http, url.clone()).await {
                Ok(b) => b,
                Err(e) => {
                    self.router.mark_failed(host.clone());
                    return Err(HathError::Rpc(format!("request failed: {}", e)));
                }
            };

            let parsed = rpc::parse_server_response(&body, &host);

            // Java: KEY_EXPIRED retry only for string-act calls with non-null retryact.
            // URL/add-based calls (still_alive, get_blacklist, srfetch) do not retry.
            if parsed.fail_code.as_deref() == Some("KEY_EXPIRED")
                && key_expired_retries < KEY_EXPIRED_RETRY_LIMIT
                && act.supports_key_expired_retry()
            {
                key_expired_retries += 1;
                tracing::info!(
                    "KEY_EXPIRED received, refreshing server stat and retrying ({}/{})...",
                    key_expired_retries,
                    KEY_EXPIRED_RETRY_LIMIT
                );
                match self.call_stat_inner().await {
                    Ok(stat_resp) if stat_resp.status == ResponseStatus::Ok => {
                        Config::apply_server_response(&self.config, &stat_resp);
                    }
                    Ok(stat_resp) if stat_resp.status == ResponseStatus::Null => {
                        let fail_host = stat_resp.fail_host.unwrap_or_else(|| "unknown".into());
                        self.router.mark_failed(fail_host);
                    }
                    Ok(_) => {}
                    Err(_) => {}
                }
                continue;
            }

            if parsed.status == ResponseStatus::Null {
                let fail_host = parsed.fail_host.as_deref().unwrap_or(&host).to_string();
                self.router.mark_failed(fail_host);
            } else {
                self.router.mark_success(host.clone());
            }

            return Ok(parsed);
        }
    }

    /// HTTP request with 3-attempt retry matching Java FileDownloader.run().
    /// Network/read errors are retried. HTTP 404 is not retried
    /// (FileNotFoundException); other 4xx/5xx responses are retried.
    async fn send_rpc_request(
        http: &Client,
        url: Url,
    ) -> std::result::Result<String, RpcRequestError> {
        let mut last_err = None;
        for attempt in 0..3 {
            match http
                .get(url.clone())
                .header("Connection", "close")
                .send()
                .await
            {
                Ok(r) => {
                    let status = r.status();
                    match rpc_status_action(status) {
                        RpcStatusAction::ReadBody => match r.text().await {
                            Ok(b) => return Ok(b),
                            Err(e) => last_err = Some(RpcRequestError::new(e.to_string())),
                        },
                        RpcStatusAction::Retry => {
                            last_err =
                                Some(RpcRequestError::new(format!("server returned {}", status)));
                        }
                        RpcStatusAction::Abort => {
                            return Err(RpcRequestError::new(format!(
                                "server returned {}",
                                status
                            )));
                        }
                    }
                }
                Err(e) => last_err = Some(RpcRequestError::new(e.to_string())),
            }
            if attempt < 2 {
                tracing::debug!("RPC request attempt {} failed, retrying...", attempt + 1);
            }
        }
        Err(last_err.unwrap())
    }

    /// Inline server_stat HTTP call (not recursive through call()).
    async fn call_stat_inner(&self) -> Result<ServerResponse> {
        let cfg = self.config.load();
        let host = self.router.select_host(&cfg);
        let url = rpc::make_rpc_url(Action::ServerStat, "", &cfg, &host)?;
        let host = url.host_str().unwrap_or("unknown").to_string();
        let body = match Self::send_rpc_request(&self.http, url.clone()).await {
            Ok(b) => b,
            Err(e) => {
                self.router.mark_failed(host.clone());
                return Err(HathError::Rpc(format!("stat request failed: {}", e)));
            }
        };
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
        self.call(Action::GetBlacklist, &deltatime.to_string())
            .await
    }

    /// Notify server of overload.
    pub async fn notify_overload(&self) -> Result<ServerResponse> {
        self.call(Action::Overload, "").await
    }

    /// Fetch download URLs for a static range file.
    pub async fn static_range_fetch(
        &self,
        fileindex: &str,
        xres: &str,
        fileid: &str,
    ) -> Result<ServerResponse> {
        let add = format!("{};{};{}", fileindex, xres, fileid);
        self.call(Action::StaticRangeFetch, &add).await
    }

    /// Fetch the next gallery metadata document, optionally acknowledging the
    /// preceding gallery. This endpoint returns plain text rather than the
    /// normal "OK\n..." RPC envelope.
    pub(crate) async fn fetch_gallery_queue(
        &self,
        ack: Option<&GalleryAck>,
    ) -> Result<GalleryQueueReply> {
        let add = ack
            .map(|ack| format!("{};{}", ack.gid, ack.minxres))
            .unwrap_or_default();
        let cfg = self.config.load();
        let selected_host = self.router.select_host(&cfg);
        let url = rpc::make_gallery_queue_url(&add, &cfg, &selected_host)?;
        let host = url.host_str().unwrap_or("unknown").to_string();

        let body = match Self::send_rpc_request(&self.http, url).await {
            Ok(body) => body,
            Err(e) => {
                self.router.mark_failed(host.clone());
                return Err(HathError::Rpc(format!(
                    "gallery queue request failed: {}",
                    e
                )));
            }
        };
        self.router.mark_success(host);
        match body.trim_end_matches(['\r', '\n']) {
            "NO_PENDING_DOWNLOADS" => Ok(GalleryQueueReply::NoPendingDownloads),
            "INVALID_REQUEST" => Ok(GalleryQueueReply::InvalidRequest),
            _ => Ok(GalleryQueueReply::Metadata(body)),
        }
    }

    /// Request the one-time source URL for a gallery file.
    pub(crate) async fn fetch_gallery_file_url(&self, request: &GalleryFileRequest) -> Result<Url> {
        let add = format!(
            "{};{};{};{};{}",
            request.gid, request.page, request.fileindex, request.xres, request.attempt
        );
        let response = self.call(Action::DownloaderFetch, &add).await?;
        if response.status != ResponseStatus::Ok {
            return Err(HathError::Rpc(format!(
                "dlfetch failed: {}",
                response.fail_code.unwrap_or_default()
            )));
        }
        let source = response
            .lines
            .iter()
            .find(|line| !line.is_empty())
            .ok_or_else(|| HathError::Rpc("dlfetch returned no URL".into()))?;
        Url::parse(source).map_err(|e| HathError::Rpc(format!("invalid dlfetch URL: {}", e)))
    }

    /// Best-effort report of failed gallery source hosts. The caller enforces
    /// de-duplication and the protocol's 50-entry cap.
    pub(crate) async fn report_gallery_failures(&self, failures: &[String]) -> Result<()> {
        if failures.is_empty() {
            return Ok(());
        }
        let response = self
            .call(Action::DownloaderFailreport, &failures.join(";"))
            .await?;
        if response.status == ResponseStatus::Ok {
            Ok(())
        } else {
            Err(HathError::Rpc(format!(
                "dlfails failed: {}",
                response.fail_code.unwrap_or_default()
            )))
        }
    }
}

/// Spawn periodic still_alive heartbeat (110s interval).
pub fn spawn_still_alive_heartbeat(
    rpc_client: Arc<RpcClient>,
    stats: Arc<Stats>,
    shutdown: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(crate::utils::tick_every(
        shutdown.clone(),
        Duration::from_secs(110),
        move || {
            let rpc_client = rpc_client.clone();
            let stats = stats.clone();
            let shutdown = shutdown.clone();
            async move {
                match rpc_client.still_alive(false).await {
                    Ok(resp) if resp.status == ResponseStatus::Ok => {
                        stats.record_server_contact();
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
                        } else {
                            tracing::warn!("Failed stillAlive test: ({}) - will retry later", code);
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Still-alive request failed: {}", e);
                    }
                }
            }
        },
    ))
}

/// Spawn periodic RPC server failure clearer (4h interval).
pub fn spawn_rpc_failure_clearer(
    rpc_client: Arc<RpcClient>,
    shutdown: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(crate::utils::tick_every(
        shutdown,
        Duration::from_secs(14400),
        move || {
            let rpc_client = rpc_client.clone();
            async move {
                rpc_client.router.clear();
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{FakeHttpServer, FakeResponse, FixtureDirs};
    use arc_swap::ArcSwap;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn key_expired_retries_once_for_retryact_calls() {
        assert_eq!(KEY_EXPIRED_RETRY_LIMIT, 1);
        assert!(Action::ClientStart.supports_key_expired_retry());
        assert!(!Action::StillAlive.supports_key_expired_retry());
        assert!(!Action::StaticRangeFetch.supports_key_expired_retry());
    }

    #[test]
    fn rpc_http_statuses_follow_file_downloader_retry_shape() {
        assert_eq!(rpc_status_action(StatusCode::OK), RpcStatusAction::ReadBody);
        assert_eq!(
            rpc_status_action(StatusCode::NOT_FOUND),
            RpcStatusAction::Abort
        );
        assert_eq!(
            rpc_status_action(StatusCode::FORBIDDEN),
            RpcStatusAction::Retry
        );
        assert_eq!(
            rpc_status_action(StatusCode::INTERNAL_SERVER_ERROR),
            RpcStatusAction::Retry
        );
    }

    fn config_with_rpc_servers(servers: &str) -> Config {
        let fixture = FixtureDirs::new();
        let mut config = fixture.config();
        config.apply_setting("rpc_server_ip", servers);
        config
    }

    #[test]
    fn rpc_router_reuses_valid_current_host() {
        let config = config_with_rpc_servers("192.0.2.1;192.0.2.2");
        let router = RpcRouter::default();
        router.mark_success("192.0.2.1".to_string());

        assert_eq!(router.select_host(&config), "192.0.2.1");
    }

    #[test]
    fn rpc_router_invalidates_current_host_after_server_list_changes() {
        let mut config = config_with_rpc_servers("192.0.2.1");
        let router = RpcRouter::default();
        router.mark_success("192.0.2.1".to_string());

        config.apply_setting("rpc_server_ip", "192.0.2.2");

        assert_eq!(router.select_host(&config), "192.0.2.2");
        assert!(router.state.lock().unwrap().rpc_current.is_none());
    }

    #[test]
    fn rpc_router_skips_last_failed_host_when_alternates_exist() {
        let config = config_with_rpc_servers("192.0.2.1;192.0.2.2");
        let router = RpcRouter::default();
        router.mark_failed("192.0.2.1".to_string());

        assert_eq!(router.select_host(&config), "192.0.2.2");
    }

    fn rpc_client_for_fake_server(server: &FakeHttpServer) -> RpcClient {
        let fixture = FixtureDirs::new();
        let mut config = fixture.config();
        config.rpc_servers = vec![IpAddr::V4(Ipv4Addr::LOCALHOST)];
        config.rpc_port = server.port();
        config.rpc_path = "rpc".to_string();
        RpcClient::new(Arc::new(ArcSwap::from_pointee(config))).unwrap()
    }

    #[tokio::test]
    async fn key_expired_refreshes_stat_and_retries_retryact_rpc() {
        let now = chrono::Utc::now().timestamp();
        let server = FakeHttpServer::start(vec![
            FakeResponse::ok("KEY_EXPIRED"),
            FakeResponse::ok(format!("OK\nserver_time={}", now)),
            FakeResponse::ok("OK\n"),
        ])
        .await;
        let client = rpc_client_for_fake_server(&server);

        let response = client.client_start().await.unwrap();

        assert_eq!(response.status, ResponseStatus::Ok);
        let requests = server.requests();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].contains("act=client_start"));
        assert!(requests[1].contains("act=server_stat"));
        assert!(requests[2].contains("act=client_start"));
    }

    #[tokio::test]
    async fn rpc_http_500_retries_against_fake_rpc_server() {
        let server = FakeHttpServer::start(vec![
            FakeResponse::status(500, "Internal Server Error", "try again"),
            FakeResponse::ok("OK\n"),
        ])
        .await;
        let client = rpc_client_for_fake_server(&server);

        let response = client.server_stat().await.unwrap();

        assert_eq!(response.status, ResponseStatus::Ok);
        let requests = server.requests();
        assert_eq!(requests.len(), 2);
        assert!(requests.iter().all(|r| r.contains("act=server_stat")));
    }

    #[tokio::test]
    async fn gallery_queue_uses_fixed_dl_path_and_acknowledges_previous_gallery() {
        let server = FakeHttpServer::start(vec![FakeResponse::ok("NO_PENDING_DOWNLOADS")]).await;
        let client = rpc_client_for_fake_server(&server);

        let reply = client
            .fetch_gallery_queue(Some(&GalleryAck {
                gid: 42,
                minxres: "org".to_string(),
            }))
            .await
            .unwrap();

        assert_eq!(reply, GalleryQueueReply::NoPendingDownloads);
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].starts_with("GET /15/dl?"));
        assert!(requests[0].contains("act=fetchqueue"));
        assert!(requests[0].contains("add=42;org"));
    }

    #[tokio::test]
    async fn gallery_file_url_requires_a_valid_rpc_url() {
        let server =
            FakeHttpServer::start(vec![FakeResponse::ok("OK\nhttp://example.test/file")]).await;
        let client = rpc_client_for_fake_server(&server);
        let url = client
            .fetch_gallery_file_url(&GalleryFileRequest {
                gid: 1,
                page: 2,
                fileindex: 3,
                xres: "org".to_string(),
                attempt: 1,
            })
            .await
            .unwrap();

        assert_eq!(url.as_str(), "http://example.test/file");
        assert!(server.requests()[0].contains("act=dlfetch"));
        assert!(server.requests()[0].contains("add=1;2;3;org;1"));
    }
}
