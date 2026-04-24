use crate::config::Config;
use crate::error::{HathError, Result};
use crate::utils;
use reqwest::Url;

pub const CLIENT_BUILD: i32 = 178;
pub const CLIENT_VERSION: &str = "1.6.5";

pub mod actions {
    pub const SERVER_STAT: &str = "server_stat";
    pub const CLIENT_LOGIN: &str = "client_login";
    pub const CLIENT_SETTINGS: &str = "client_settings";
    pub const CLIENT_START: &str = "client_start";
    pub const CLIENT_SUSPEND: &str = "client_suspend";
    pub const CLIENT_RESUME: &str = "client_resume";
    pub const CLIENT_STOP: &str = "client_stop";
    pub const STILL_ALIVE: &str = "still_alive";
    pub const GET_BLACKLIST: &str = "get_blacklist";
    pub const GET_CERTIFICATE: &str = "get_cert";
    pub const STATIC_RANGE_FETCH: &str = "srfetch";
    pub const DOWNLOADER_FETCH: &str = "dlfetch";
    pub const DOWNLOADER_FAILREPORT: &str = "dlfails";
    pub const OVERLOAD: &str = "overload";
}

/// Construct the signed RPC URL query string.
/// Replicates Java: actkey = SHA1("hentai@home-" + act + "-" + add + "-" + cid + "-" + time + "-" + key)
pub fn make_rpc_query(act: &str, add: &str, config: &Config) -> String {
    let corrected_time = config.server_time();
    let plain = format!(
        "hentai@home-{}-{}-{}-{}-{}",
        act, add, config.client_id.0, corrected_time, config.client_key.as_str()
    );
    let actkey = utils::sha1_string(&plain);
    format!(
        "clientbuild={}&act={}&add={}&cid={}&acttime={}&actkey={}",
        CLIENT_BUILD, act, add, config.client_id.0, corrected_time, actkey
    )
}

/// Build the full RPC URL for a given action.
pub fn make_rpc_url(act: &str, add: &str, config: &Config) -> Result<Url> {
    let host = config.get_rpc_host();
    let query = make_rpc_query(act, add, config);
    // rpc_path already ends with '?', e.g. "15/rpc?" — do not add another
    let url_str = format!("http://{}/{}{}", host, config.rpc_path, query);
    Url::parse(&url_str).map_err(|e| HathError::Rpc(format!("invalid URL: {}", e)))
}

#[derive(Debug, PartialEq, Eq)]
pub enum ResponseStatus { Ok, Fail, Null }

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
        "TEMPORARILY_UNAVAILABLE" => ServerResponse {
            status: ResponseStatus::Null,
            lines: vec![],
            fail_code: Some("TEMPORARILY_UNAVAILABLE".into()),
            fail_host: Some(request_host.to_lowercase()),
        },
        first if first.starts_with("KEY_EXPIRED") => ServerResponse {
            status: ResponseStatus::Null,
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
            "hath-rs", "--client-id", "12345", "--client-key", "abcde12345abcde12345",
        ]).unwrap();
        Config::load(args).unwrap()
    }

    #[test]
    fn test_make_rpc_query() {
        let config = test_config();
        let q = make_rpc_query(actions::CLIENT_START, "", &config);
        assert!(q.contains("clientbuild=178"));
        assert!(q.contains("act=client_start"));
        assert!(q.contains("cid=12345"));
        assert!(q.contains("actkey="));
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
}
