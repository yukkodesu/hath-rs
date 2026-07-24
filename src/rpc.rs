use crate::config::Config;
use crate::error::{HathError, Result};
use crate::utils;
use rand::Rng;
use reqwest::Url;
use std::fmt;
use std::net::IpAddr;

pub const CLIENT_BUILD: i32 = 178;
pub const CLIENT_VERSION: &str = "1.6.5";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    ServerStat,
    ClientLogin,
    ClientSettings,
    ClientStart,
    ClientSuspend,
    ClientResume,
    ClientStop,
    StillAlive,
    GetBlacklist,
    GetCertificate,
    StaticRangeFetch,
    GalleryQueueFetch,
    DownloaderFetch,
    DownloaderFailreport,
    Overload,
}

impl Action {
    /// server_stat 是第一个调用的 RPC，用于校时。
    /// 此时还没有 server_time_delta，所以不带 cid/acttime/actkey 签名。
    fn needs_signing(self) -> bool {
        !matches!(self, Self::ServerStat)
    }

    /// Java: KEY_EXPIRED retry only applies to string-act calls that pass
    /// a non-null retryact (via ServerHandler getServerResponse with act).
    /// URL/add-based calls (still_alive, get_blacklist, srfetch, etc.) do
    /// not retry on KEY_EXPIRED.
    pub fn supports_key_expired_retry(self) -> bool {
        matches!(
            self,
            Self::ClientLogin
                | Self::ClientSettings
                | Self::ClientStart
                | Self::ClientSuspend
                | Self::ClientResume
                | Self::ClientStop
                | Self::Overload
        )
    }
}

impl fmt::Display for Action {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ServerStat => write!(f, "server_stat"),
            Self::ClientLogin => write!(f, "client_login"),
            Self::ClientSettings => write!(f, "client_settings"),
            Self::ClientStart => write!(f, "client_start"),
            Self::ClientSuspend => write!(f, "client_suspend"),
            Self::ClientResume => write!(f, "client_resume"),
            Self::ClientStop => write!(f, "client_stop"),
            Self::StillAlive => write!(f, "still_alive"),
            Self::GetBlacklist => write!(f, "get_blacklist"),
            Self::GetCertificate => write!(f, "get_cert"),
            Self::StaticRangeFetch => write!(f, "srfetch"),
            Self::GalleryQueueFetch => write!(f, "fetchqueue"),
            Self::DownloaderFetch => write!(f, "dlfetch"),
            Self::DownloaderFailreport => write!(f, "dlfails"),
            Self::Overload => write!(f, "overload"),
        }
    }
}

/// Construct the signed RPC URL query string.
/// Replicates Java: actkey = SHA1("hentai@home-" + act + "-" + add + "-" + cid + "-" + time + "-" + key)
pub fn make_rpc_query(act: Action, add: &str, config: &Config) -> String {
    if !act.needs_signing() {
        return format!("clientbuild={}&act={}", CLIENT_BUILD, act);
    }
    let corrected_time = config.server_time();
    let act_str = act.to_string();
    let plain = format!(
        "hentai@home-{}-{}-{}-{}-{}",
        act_str,
        add,
        config.client_id.0,
        corrected_time,
        config.client_key.as_str()
    );
    let actkey = utils::sha1_string(&plain);
    format!(
        "clientbuild={}&act={}&add={}&cid={}&acttime={}&actkey={}",
        CLIENT_BUILD, act_str, add, config.client_id.0, corrected_time, actkey
    )
}

/// Build the full RPC URL for a given action.
pub fn make_rpc_url(act: Action, add: &str, config: &Config, host: &str) -> Result<Url> {
    let query = make_rpc_query(act, add, config);
    let mut url = Url::parse("http://rpc.hentaiathome.net/")
        .map_err(|e| HathError::Rpc(format!("invalid URL base: {}", e)))?;
    if let Ok(ip) = host.parse::<IpAddr>() {
        url.set_ip_host(ip)
            .map_err(|_| HathError::Rpc("invalid URL host".into()))?;
    } else {
        url.set_host(Some(&host))
            .map_err(|e| HathError::Rpc(format!("invalid URL host: {}", e)))?;
    }
    if config.rpc_port == 80 {
        url.set_port(None)
            .map_err(|_| HathError::Rpc("invalid URL port".into()))?;
    } else {
        url.set_port(Some(config.rpc_port))
            .map_err(|_| HathError::Rpc("invalid URL port".into()))?;
    }
    url.set_path(config.rpc_path.trim_end_matches('?'));
    url.set_query(Some(&query));
    Ok(url)
}

/// Build the signed gallery-queue URL. The queue endpoint deliberately does
/// not share the configurable RPC path: the H@H protocol fixes it at /15/dl.
pub fn make_gallery_queue_url(act_add: &str, config: &Config, host: &str) -> Result<Url> {
    let mut url = make_rpc_url(Action::GalleryQueueFetch, act_add, config, host)?;
    url.set_path("/15/dl");
    Ok(url)
}

/// Host used by one-off RPC-shaped downloads that do not participate in
/// RpcClient's Java routing state, such as certificate fetches.
pub fn default_rpc_host(config: &Config) -> String {
    if config.rpc_servers.is_empty() {
        return "rpc.hentaiathome.net".to_string();
    }
    if config.rpc_servers.len() == 1 {
        return config.rpc_servers[0].to_string().to_lowercase();
    }

    let mut rng = rand::rng();
    let idx = rng.next_u32() as usize % config.rpc_servers.len();
    config.rpc_servers[idx].to_string().to_lowercase()
}

#[derive(Debug, PartialEq, Eq)]
pub enum ResponseStatus {
    Ok,
    Fail,
    Null,
}

#[derive(Debug)]
pub struct ServerResponse {
    pub status: ResponseStatus,
    pub lines: Vec<String>,
    pub fail_code: Option<String>,
    pub fail_host: Option<String>,
}

/// Parse the raw string response from an RPC call.
pub fn parse_server_response(body: &str, request_host: &str) -> ServerResponse {
    let lines: Vec<&str> = body.lines().collect();
    if lines.is_empty() {
        return ServerResponse {
            status: ResponseStatus::Null,
            lines: vec![],
            fail_code: Some("NO_RESPONSE".into()),
            fail_host: Some(request_host.to_lowercase()),
        };
    }
    match lines[0] {
        "OK" => ServerResponse {
            status: ResponseStatus::Ok,
            lines: lines[1..].iter().map(|s| s.to_string()).collect(),
            fail_code: None,
            fail_host: None,
        },
        s if s.starts_with("TEMPORARILY_UNAVAILABLE") => ServerResponse {
            status: ResponseStatus::Null,
            lines: vec![],
            fail_code: Some(s.to_string()),
            fail_host: Some(request_host.to_lowercase()),
        },
        "KEY_EXPIRED" => ServerResponse {
            status: ResponseStatus::Fail,
            lines: vec![],
            fail_code: Some("KEY_EXPIRED".into()),
            fail_host: Some(request_host.to_lowercase()),
        },
        fail => ServerResponse {
            status: ResponseStatus::Fail,
            lines: vec![],
            fail_code: Some(fail.to_string()),
            fail_host: Some(request_host.to_lowercase()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CliArgs;
    use clap::Parser;

    fn test_config() -> Config {
        let args = CliArgs::try_parse_from([
            "hath-rs",
            "--client-id",
            "12345",
            "--client-key",
            "abcde12345abcde12345",
        ])
        .unwrap();
        Config::load(args).unwrap()
    }

    #[test]
    fn test_server_stat_unsigned() {
        let config = test_config();
        let q = make_rpc_query(Action::ServerStat, "", &config);
        assert_eq!(q, "clientbuild=178&act=server_stat");
        assert!(!q.contains("actkey="));
    }

    #[test]
    fn test_make_rpc_query() {
        let config = test_config();
        let q = make_rpc_query(Action::ClientStart, "", &config);
        assert!(q.contains("clientbuild=178"));
        assert!(q.contains("act=client_start"));
        assert!(q.contains("cid=12345"));
        assert!(q.contains("actkey="));
    }

    #[test]
    fn test_make_rpc_url_normalizes_ipv4_mapped_host() {
        let mut config = test_config();
        // ::ffff:192.0.2.1 normalizes to plain IPv4 192.0.2.1
        config.apply_setting("rpc_server_ip", "::ffff:192.0.2.1");

        let host = default_rpc_host(&config);
        let url = make_rpc_url(Action::GetCertificate, "", &config, &host).unwrap();

        assert!(
            url.as_str()
                .starts_with("http://192.0.2.1/15/rpc?clientbuild=178&act=get_cert")
        );
    }

    #[test]
    fn test_make_rpc_url_sets_non_default_rpc_port() {
        let mut config = test_config();
        config.apply_setting("rpc_server_ip", "192.0.2.1");
        config.apply_setting("rpc_server_port", "8080");

        let host = default_rpc_host(&config);
        let url = make_rpc_url(Action::GetCertificate, "", &config, &host).unwrap();

        assert!(
            url.as_str()
                .starts_with("http://192.0.2.1:8080/15/rpc?clientbuild=178&act=get_cert")
        );
    }

    #[test]
    fn test_parse_ok() {
        let r = parse_server_response("OK\nkey=val", "rpc.example.com");
        assert_eq!(r.status, ResponseStatus::Ok);
        assert_eq!(r.lines, vec!["key=val"]);
    }

    #[test]
    fn test_parse_fail() {
        let r = parse_server_response("FAIL_CODE", "rpc.example.com");
        assert_eq!(r.status, ResponseStatus::Fail);
        assert_eq!(r.fail_code.unwrap(), "FAIL_CODE");
    }

    #[test]
    fn test_parse_empty_is_null() {
        let r = parse_server_response("", "rpc.example.com");
        assert_eq!(r.status, ResponseStatus::Null);
    }

    #[test]
    fn test_parse_key_expired_is_fail() {
        let r = parse_server_response("KEY_EXPIRED", "rpc.example.com");

        assert_eq!(r.status, ResponseStatus::Fail);
        assert_eq!(r.fail_code.as_deref(), Some("KEY_EXPIRED"));
        assert_eq!(r.fail_host.as_deref(), Some("rpc.example.com"));
    }
}
