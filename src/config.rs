use crate::error::{HathError, Result};
use crate::types::{ClientId, ClientKey};
use clap::Parser;
use rand::Rng;
use std::collections::HashMap;
use std::net::IpAddr;
use std::path::PathBuf;
use std::sync::Arc;

#[derive(Parser, Debug)]
#[command(name = "hath-rs", version = "1.6.5")]
pub struct CliArgs {
    #[arg(long, env = "HATH_CLIENT_ID")]
    pub client_id: Option<u32>,
    #[arg(long, env = "HATH_CLIENT_KEY")]
    pub client_key: Option<String>,
    #[arg(long, env = "HATH_DATA_DIR", default_value = "data")]
    pub data_dir: String,
    #[arg(long, env = "HATH_LOG_DIR", default_value = "log")]
    pub log_dir: String,
    #[arg(long, env = "HATH_CACHE_DIR", default_value = "cache")]
    pub cache_dir: String,
    #[arg(long, env = "HATH_TEMP_DIR", default_value = "tmp")]
    pub temp_dir: String,
    #[arg(long, env = "HATH_DOWNLOAD_DIR", default_value = "download")]
    pub download_dir: String,
    #[arg(long, env = "HATH_PORT")]
    pub port: Option<u16>,
    #[arg(long, env = "HATH_VERIFY_CACHE")]
    pub verify_cache: Option<bool>,
    #[arg(long, env = "HATH_USE_LESS_MEMORY")]
    pub use_less_memory: Option<bool>,
    #[arg(long, env = "HATH_DISABLE_LOGGING")]
    pub disable_logging: Option<bool>,
    #[arg(long, env = "HATH_DISABLE_BWM")]
    pub disable_bwm: Option<bool>,
    #[arg(long, env = "HATH_DISABLE_DOWNLOAD_BWM")]
    pub disable_download_bwm: Option<bool>,
    #[arg(long, env = "HATH_DISABLE_FILE_VERIFICATION")]
    pub disable_file_verification: Option<bool>,
    #[arg(long, env = "HATH_DISABLE_IP_ORIGIN_CHECK")]
    pub disable_ip_origin_check: Option<bool>,
    #[arg(long, env = "HATH_DISABLE_FLOOD_CONTROL")]
    pub disable_flood_control: Option<bool>,
    #[arg(long, env = "HATH_SKIP_FREE_SPACE_CHECK")]
    pub skip_free_space_check: Option<bool>,
    #[arg(long, env = "HATH_FLUSH_LOGS")]
    pub flush_logs: Option<bool>,
    #[arg(long, env = "HATH_MAX_CONNECTIONS")]
    pub max_connections: Option<u32>,
    #[arg(long, env = "HATH_FILESYSTEM_BLOCKSIZE")]
    pub filesystem_blocksize: Option<u64>,
    #[arg(long, env = "HATH_IMAGE_PROXY_TYPE")]
    pub image_proxy_type: Option<String>,
    #[arg(long, env = "HATH_IMAGE_PROXY_HOST")]
    pub image_proxy_host: Option<String>,
    #[arg(long, env = "HATH_IMAGE_PROXY_PORT")]
    pub image_proxy_port: Option<u16>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub client_id: ClientId,
    pub client_key: ClientKey,
    pub data_dir: PathBuf,
    pub log_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub temp_dir: PathBuf,
    pub download_dir: PathBuf,
    pub client_port: u16,

    pub server_time_delta: i64,
    pub throttle_bytes: u32,
    pub disklimit_bytes: u64,
    pub diskremaining_bytes: u64,
    pub filesystem_blocksize: u64,
    pub max_allowed_filesize: u64,
    pub max_filename_length: u32,

    pub rpc_servers: Vec<IpAddr>,
    pub rpc_port: u16,
    pub rpc_path: String,

    /// Java (build 174+): static ranges are only sent on startup via client_login,
    /// as the client only needs them for startup cache pruning. Subsequent RPC
    /// responses (still_alive, refresh_settings, etc.) do not include this field.
    pub static_ranges: HashMap<String, u8>,
    /// Updated independently of static_ranges to reflect the current assignment
    /// count without re-sending the full range list on every settings refresh.
    pub static_range_count: u32,

    pub verify_cache: bool,
    pub rescan_cache: bool,
    pub use_less_memory: bool,
    pub disable_logging: bool,
    pub disable_bwm: bool,
    pub disable_download_bwm: bool,
    pub disable_file_verification: bool,
    pub disable_ip_origin_check: bool,
    pub disable_flood_control: bool,
    pub skip_free_space_check: bool,
    pub flush_logs: bool,
    pub warn_new_client: bool,

    pub image_proxy_type: Option<String>,
    pub image_proxy_host: Option<String>,
    pub image_proxy_port: Option<u16>,

    pub client_host: String,
    pub override_conns: u32,
}

impl Config {
    /// Load config from CLI args, env vars, and client_login file.
    /// Priority: CLI args > env vars > client_login file.
    pub fn load(args: CliArgs) -> Result<Self> {
        // Resolve credentials: CLI/env > client_login file
        let (client_id, client_key) = if let (Some(id), Some(ref key_str)) =
            (args.client_id, args.client_key)
        {
            let key = ClientKey::new(key_str).ok_or_else(|| {
                HathError::Config("client key must be exactly 20 alphanumeric characters".into())
            })?;
            (ClientId(id), key)
        } else {
            // Fallback: try client_login file in data dir
            let login_file = PathBuf::from(&args.data_dir).join("client_login");
            if login_file.exists() {
                let content = std::fs::read_to_string(&login_file)
                    .map_err(|e| HathError::Config(format!("cannot read client_login: {}", e)))?;
                if let Some((id_str, key_str)) = content.trim().split_once('-') {
                    let id: u32 = id_str.parse().map_err(|_| {
                        HathError::Config("invalid client ID in client_login".into())
                    })?;
                    let key = ClientKey::new(key_str.trim()).ok_or_else(|| {
                        HathError::Config("invalid client key in client_login".into())
                    })?;
                    (ClientId(id), key)
                } else {
                    return Err(HathError::Config("malformed client_login file".into()));
                }
            } else {
                return Err(HathError::Config(
                    "No credentials found. Provide --client-id/--client-key or place client_login file in data dir.".into()
                ));
            }
        };

        Ok(Self {
            client_id,
            client_key,
            data_dir: PathBuf::from(&args.data_dir),
            log_dir: PathBuf::from(&args.log_dir),
            cache_dir: PathBuf::from(&args.cache_dir),
            temp_dir: PathBuf::from(&args.temp_dir),
            download_dir: PathBuf::from(&args.download_dir),
            client_port: args.port.unwrap_or(0),
            server_time_delta: 0,
            throttle_bytes: 0,
            disklimit_bytes: 0,
            diskremaining_bytes: 0,
            filesystem_blocksize: args.filesystem_blocksize.unwrap_or(4096),
            max_allowed_filesize: 1073741824,
            max_filename_length: 125,
            rpc_servers: Vec::new(),
            rpc_port: 80,
            rpc_path: "15/rpc?".to_string(),
            static_ranges: HashMap::new(),
            static_range_count: 0,
            verify_cache: args.verify_cache.unwrap_or(false),
            rescan_cache: args.verify_cache.unwrap_or(false),
            use_less_memory: args.use_less_memory.unwrap_or(false),
            disable_logging: args.disable_logging.unwrap_or(false),
            disable_bwm: args.disable_bwm.unwrap_or(false),
            disable_download_bwm: args.disable_download_bwm.unwrap_or(false),
            disable_file_verification: args.disable_file_verification.unwrap_or(false),
            disable_ip_origin_check: args.disable_ip_origin_check.unwrap_or(false),
            disable_flood_control: args.disable_flood_control.unwrap_or(false),
            skip_free_space_check: args.skip_free_space_check.unwrap_or(false),
            flush_logs: args.flush_logs.unwrap_or(false),
            warn_new_client: false,
            image_proxy_type: args.image_proxy_type,
            image_proxy_host: args.image_proxy_host,
            image_proxy_port: args.image_proxy_port,
            client_host: String::new(),
            override_conns: args.max_connections.unwrap_or(0),
        })
    }

    pub fn server_time(&self) -> i64 {
        chrono::Utc::now().timestamp() + self.server_time_delta
    }

    pub fn max_connections(&self) -> u32 {
        if self.override_conns > 0 {
            self.override_conns
        } else {
            20 + (self.throttle_bytes / 10000).min(480)
        }
    }

    /// Pick an RPC host using routing state from RpcClient.
    /// Uses cached `state.rpc_current` if set and not last-failed,
    /// otherwise selects a random server from `rpc_servers`.
    pub fn get_rpc_host(&self, state: &crate::rpc_client::RpcState) -> String {
        let host = if let Some(ref host) = state.rpc_current {
            if let Some(ref failed) = state.rpc_last_failed
                && *host == *failed
            {
                tracing::debug!("{} was marked as last failed (from cache)", failed);
                None
            } else {
                Some(host.clone())
            }
        } else {
            None
        };

        host.unwrap_or_else(|| {
            if self.rpc_servers.is_empty() {
                return "rpc.hentaiathome.net".to_string();
            }
            if self.rpc_servers.len() == 1 {
                return self.rpc_servers[0].to_string().to_lowercase();
            }
            let mut rng = rand::rng();
            let mut idx: isize = (rng.next_u32() as usize % self.rpc_servers.len()) as isize;
            let dir: isize = if rng.next_u32() & 1 == 0 { -1 } else { 1 };
            let len = self.rpc_servers.len() as isize;
            loop {
                let candidate = self.rpc_servers[((len + idx) % len) as usize]
                    .to_string()
                    .to_lowercase();
                if let Some(ref failed) = state.rpc_last_failed
                    && candidate == *failed
                {
                    tracing::debug!("{} was marked as last failed", failed);
                    idx += dir;
                    continue;
                }
                tracing::debug!("Selected rpcServerCurrent={}", candidate);
                break candidate;
            }
        })
    }

    pub fn is_static_range(&self, range: &str) -> bool {
        self.static_ranges.contains_key(range)
    }

    pub fn apply_setting(&mut self, setting: &str, value: &str) {
        // Java: replace '-' with '_' before matching (e.g. rpc-server-ip → rpc_server_ip)
        let setting = &setting.replace('-', "_");
        match setting.as_str() {
            "min_client_build" => {
                if let Ok(build) = value.parse::<i32>()
                    && build > crate::rpc::CLIENT_BUILD
                {
                    tracing::error!(
                        "Your client is too old to connect to the Hentai@Home Network. \
                             Required build: {}, our build: {}. Please download a newer version.",
                        build,
                        crate::rpc::CLIENT_BUILD
                    );
                    std::process::exit(1);
                }
            }
            "cur_client_build" => {
                if let Ok(build) = value.parse::<i32>()
                    && build > 178
                {
                    self.warn_new_client = true;
                }
            }
            "server_time" => {
                if let Ok(st) = value.parse::<i64>() {
                    self.server_time_delta = st - chrono::Utc::now().timestamp();
                }
            }
            "rpc_server_port" => self.rpc_port = value.parse().unwrap_or(80),
            "rpc_server_ip" => {
                self.rpc_servers = value
                    .split(';')
                    .filter_map(|s| s.trim().parse::<IpAddr>().ok())
                    .map(crate::utils::normalize_ip)
                    .collect();
                // Java: clear cached host if it's no longer in the new server list.
                // rpc_current is now in RpcState inside RpcClient; the equivalent
                // staleness check runs at the top of RpcClient::call() each time.
            }
            "rpc_path" => self.rpc_path = value.to_string(),
            "host" => {
                // Normalize IPv4-mapped IPv6 (::ffff:x.x.x.x) to plain IPv4
                // so per-connection comparisons don't need String::replace.
                self.client_host = value
                    .parse::<IpAddr>()
                    .map(|ip| crate::utils::normalize_ip(ip).to_string())
                    .unwrap_or_else(|_| value.to_string());
            }
            "port" => {
                if self.client_port == 0 {
                    self.client_port = value.parse().unwrap_or(0);
                }
            }
            "throttle_bytes" => self.throttle_bytes = value.parse().unwrap_or(0),
            "disklimit_bytes" => {
                let new_limit: u64 = value.parse().unwrap_or(0);
                if new_limit >= self.disklimit_bytes {
                    self.disklimit_bytes = new_limit;
                }
            }
            "diskremaining_bytes" => self.diskremaining_bytes = value.parse().unwrap_or(0),
            "filesystem_blocksize" => {
                let bs: u64 = value.parse().unwrap_or(4096);
                self.filesystem_blocksize = bs.clamp(1, 65536);
            }
            "rescan_cache" => self.rescan_cache = value == "true",
            "verify_cache" => {
                self.verify_cache = value == "true";
                self.rescan_cache = value == "true";
            }
            "use_less_memory" => self.use_less_memory = value == "true",
            "disable_logging" => self.disable_logging = value == "true",
            "disable_bwm" => {
                self.disable_bwm = value == "true";
                self.disable_download_bwm = value == "true";
            }
            "disable_download_bwm" => self.disable_download_bwm = value == "true",
            "disable_file_verification" => self.disable_file_verification = value == "true",
            "disable_ip_origin_check" => self.disable_ip_origin_check = value == "true",
            "disable_flood_control" => self.disable_flood_control = value == "true",
            "skip_free_space_check" => self.skip_free_space_check = value == "true",
            "flush_logs" => self.flush_logs = value == "true",
            "max_connections" => self.override_conns = value.parse().unwrap_or(0),
            "max_allowed_filesize" => {
                self.max_allowed_filesize = value.parse().unwrap_or(1073741824)
            }
            "max_filename_length" => self.max_filename_length = value.parse().unwrap_or(125),
            "static_ranges" => {
                self.static_ranges.clear();
                for s in value.split(';') {
                    if s.len() == 4 {
                        self.static_ranges.insert(s.to_string(), 1);
                    }
                }
            }
            "static_range_count" => {
                self.static_range_count = value.parse().unwrap_or(self.static_range_count)
            }
            "cache_dir" => self.cache_dir = PathBuf::from(value),
            "temp_dir" => self.temp_dir = PathBuf::from(value),
            "data_dir" => self.data_dir = PathBuf::from(value),
            "log_dir" => self.log_dir = PathBuf::from(value),
            "download_dir" => self.download_dir = PathBuf::from(value),
            "image_proxy_type" => self.image_proxy_type = Some(value.to_lowercase()),
            "image_proxy_host" => self.image_proxy_host = Some(value.to_lowercase()),
            "image_proxy_port" => self.image_proxy_port = value.parse().ok(),
            _ => tracing::warn!("Unknown setting {} = {}", setting, value),
        }
        tracing::debug!("Setting altered: {}={}", setting, value);
    }

    pub fn apply_server_settings(&mut self, lines: &[String]) {
        for line in lines {
            if let Some((key, value)) = line.split_once('=') {
                self.apply_setting(&key.to_lowercase(), value);
            }
        }
    }

    /// Apply settings from a `ServerResponse` into an `ArcSwap<Config>` via rcu.
    pub fn apply_server_response(
        config: &arc_swap::ArcSwap<Config>,
        resp: &crate::rpc::ServerResponse,
    ) {
        config.rcu(|current| {
            let mut new = (**current).clone();
            new.apply_server_settings(&resp.lines);
            Arc::new(new)
        });
    }

    pub fn load_client_login(&self) -> Result<Option<(ClientId, ClientKey)>> {
        let login_file = self.data_dir.join("client_login");
        if !login_file.exists() {
            return Ok(None);
        }
        let content = crate::utils::read_string_file(&login_file)?;
        if let Some((id_str, key_str)) = content.trim().split_once('-') {
            let id: u32 = id_str
                .parse()
                .map_err(|_| HathError::Config("invalid client ID".into()))?;
            let key = ClientKey::new(key_str.trim())
                .ok_or_else(|| HathError::Config("invalid client key format".into()))?;
            Ok(Some((ClientId(id), key)))
        } else {
            Err(HathError::Config("malformed client_login file".into()))
        }
    }

    pub fn save_client_login(&self) -> Result<()> {
        crate::utils::ensure_dir(&self.data_dir)?;
        crate::utils::write_string_file(
            &self.data_dir.join("client_login"),
            &format!("{}-{}", self.client_id.0, self.client_key.as_str()),
        )?;
        Ok(())
    }

    pub fn initialize_directories(&self) -> Result<()> {
        for dir in [
            &self.data_dir,
            &self.log_dir,
            &self.cache_dir,
            &self.temp_dir,
            &self.download_dir,
        ] {
            crate::utils::ensure_dir(dir)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn test_cli() -> CliArgs {
        CliArgs::try_parse_from([
            "hath-rs",
            "--client-id",
            "12345",
            "--client-key",
            "abcde12345abcde12345",
        ])
        .unwrap()
    }

    #[test]
    fn test_load_valid() {
        let config = Config::load(test_cli()).unwrap();
        assert_eq!(config.client_id.0, 12345);
    }

    #[test]
    fn test_apply_server_time() {
        let mut config = Config::load(test_cli()).unwrap();
        let st = chrono::Utc::now().timestamp();
        config.apply_server_settings(&[format!("server_time={}", st)]);
        assert!(config.server_time_delta.abs() < 5);
    }

    #[test]
    fn test_static_ranges() {
        let mut config = Config::load(test_cli()).unwrap();
        config.apply_server_settings(&["static_ranges=abcd;ef01".to_string()]);
        assert!(config.is_static_range("abcd"));
        assert!(!config.is_static_range("9999"));
    }

    #[test]
    fn test_max_connections() {
        let mut config = Config::load(test_cli()).unwrap();
        config.throttle_bytes = 1_000_000;
        assert_eq!(config.max_connections(), 120);
    }

    #[test]
    fn test_get_rpc_host_normalizes_ipv4_mapped() {
        let mut config = Config::load(test_cli()).unwrap();
        // ::ffff:192.0.2.1 is IPv4-mapped IPv6 — normalize_ip strips the prefix
        config.apply_setting("rpc_server_ip", "::ffff:192.0.2.1");

        assert_eq!(
            config.get_rpc_host(&crate::rpc_client::RpcState::default()),
            "192.0.2.1"
        );
    }
}
