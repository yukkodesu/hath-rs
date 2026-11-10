use crate::config::Config;
use crate::error::{HathError, Result};
use crate::types::ClientKey;
use crate::utils;
use rand::Rng;
use reqwest::Url;
use std::fmt;
use std::net::IpAddr;

pub const CLIENT_BUILD: i32 = 181;
pub const CLIENT_VERSION: &str = "1.6.6";

/// Default RPC path; the server may override it via the `rpc_path` setting.
pub const DEFAULT_RPC_PATH: &str = "16/rpc?";
/// The gallery queue endpoint is fixed by the protocol, independent of `rpc_path`.
const GALLERY_QUEUE_PATH: &str = "/16/dl";

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
/// Since 1.6.6 every action is signed, including server_stat.
pub fn make_rpc_query(act: Action, add: &str, config: &Config) -> String {
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
        url.set_host(Some(host))
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
/// not share the configurable RPC path.
pub fn make_gallery_queue_url(act_add: &str, config: &Config, host: &str) -> Result<Url> {
    let mut url = make_rpc_url(Action::GalleryQueueFetch, act_add, config, host)?;
    url.set_path(GALLERY_QUEUE_PATH);
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

/// Integrity signature used by H@H 1.6.6 for RPC responses and gallery file
/// lists: SHA-1 of `prefix` followed by `"\n" + line` for every line. The
/// prefix always embeds the client key, so a MitM cannot forge it.
pub(crate) fn sign_lines<'a>(prefix: &str, lines: impl IntoIterator<Item = &'a str>) -> String {
    let mut plain = prefix.to_owned();
    for line in lines {
        plain.push('\n');
        plain.push_str(line);
    }
    utils::sha1_string(&plain)
}

#[derive(Debug, PartialEq, Eq)]
pub enum ResponseStatus {
    Ok,
    Fail,
    Null,
}

/// Failure codes the client reacts to. The server may append details to a
/// code, so codes are matched by prefix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailKind {
    /// No usable response: unreachable server, NO_RESPONSE, TEMPORARILY_UNAVAILABLE.
    Unavailable,
    KeyExpired,
    StartupLoop,
    UnsupportedLocation,
    OtherClientConnected,
    ConnectTest,
    BadNetwork,
    Other,
}

const FAIL_PREFIXES: &[(&str, FailKind)] = &[
    ("KEY_EXPIRED", FailKind::KeyExpired),
    ("FAIL_STARTUP_LOOP", FailKind::StartupLoop),
    ("FAIL_UNSUPPORTED_LOCATION", FailKind::UnsupportedLocation),
    (
        "FAIL_OTHER_CLIENT_CONNECTED",
        FailKind::OtherClientConnected,
    ),
    ("FAIL_CID_IN_USE", FailKind::OtherClientConnected),
    ("FAIL_CONNECT_TEST", FailKind::ConnectTest),
    ("TERM_BAD_NETWORK", FailKind::BadNetwork),
];

#[derive(Debug)]
pub struct ServerResponse {
    pub status: ResponseStatus,
    pub lines: Vec<String>,
    pub fail_code: Option<String>,
    pub fail_host: Option<String>,
}

impl ServerResponse {
    fn failed(status: ResponseStatus, code: &str, request_host: &str) -> Self {
        Self {
            status,
            lines: vec![],
            fail_code: Some(code.to_string()),
            fail_host: Some(request_host.to_lowercase()),
        }
    }

    /// `None` for a successful response.
    pub fn fail_kind(&self) -> Option<FailKind> {
        let code = self.fail_code.as_deref().unwrap_or_default();
        match self.status {
            ResponseStatus::Ok => None,
            ResponseStatus::Null => Some(FailKind::Unavailable),
            ResponseStatus::Fail => Some(
                FAIL_PREFIXES
                    .iter()
                    .find(|(prefix, _)| code.starts_with(prefix))
                    .map_or(FailKind::Other, |&(_, kind)| kind),
            ),
        }
    }

    pub fn fail_code(&self) -> &str {
        self.fail_code.as_deref().unwrap_or_default()
    }
}

/// Parse the raw string response from an RPC call.
/// Since 1.6.6 a success header is `OK-<vary>-<signature>`, signed over
/// `<vary>-<client key>` and every following line; anything else is a failure.
pub fn parse_server_response(body: &str, request_host: &str, key: &ClientKey) -> ServerResponse {
    // Trailing blank lines are not part of the signed content.
    let body = body.trim_end_matches('\n');
    if body.is_empty() {
        return ServerResponse::failed(ResponseStatus::Null, "NO_RESPONSE", request_host);
    }
    let mut lines = body.split('\n');
    let head = lines.next().unwrap_or_default();
    let lines: Vec<&str> = lines.collect();

    if head.starts_with("TEMPORARILY_UNAVAILABLE") {
        return ServerResponse::failed(ResponseStatus::Null, head, request_host);
    }
    let Some(header) = head.strip_prefix("OK-") else {
        return ServerResponse::failed(ResponseStatus::Fail, head, request_host);
    };
    let signature_valid = header.split_once('-').is_some_and(|(vary, signature)| {
        sign_lines(&format!("{vary}-{}", key.as_str()), lines.iter().copied()) == signature
    });
    if !signature_valid {
        tracing::warn!("Received an invalid response hash from the server");
        return ServerResponse::failed(ResponseStatus::Fail, "RESPONSE_HASH_FAILED", request_host);
    }
    ServerResponse {
        status: ResponseStatus::Ok,
        lines: lines.into_iter().map(str::to_string).collect(),
        fail_code: None,
        fail_host: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CliArgs;
    use crate::test_support::{TEST_CLIENT_KEY, signed_rpc_body, test_client_key};
    use clap::Parser;

    fn test_config() -> Config {
        let args = CliArgs::try_parse_from([
            "hath-rs",
            "--client-id",
            "12345",
            "--client-key",
            TEST_CLIENT_KEY,
        ])
        .unwrap();
        Config::load(args).unwrap()
    }

    #[test]
    fn test_server_stat_signed() {
        let config = test_config();
        let q = make_rpc_query(Action::ServerStat, "", &config);
        assert!(q.starts_with("clientbuild=181&act=server_stat&add=&cid=12345&acttime="));
        assert!(q.contains("actkey="));
    }

    #[test]
    fn test_make_rpc_query() {
        let config = test_config();
        let q = make_rpc_query(Action::ClientStart, "", &config);
        assert!(q.contains("clientbuild=181"));
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
                .starts_with("http://192.0.2.1/16/rpc?clientbuild=181&act=get_cert")
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
                .starts_with("http://192.0.2.1:8080/16/rpc?clientbuild=181&act=get_cert")
        );
    }

    fn parse(body: &str) -> ServerResponse {
        parse_server_response(body, "rpc.example.com", &test_client_key())
    }

    #[test]
    fn test_parse_signed_ok() {
        let r = parse(&format!("{}\n\n", signed_rpc_body(&["key=val", "other=1"])));
        assert_eq!(r.status, ResponseStatus::Ok);
        assert_eq!(r.lines, vec!["key=val", "other=1"]);
        assert_eq!(r.fail_kind(), None);
    }

    #[test]
    fn test_parse_rejects_bad_or_missing_signature() {
        let body = signed_rpc_body(&["key=val"]);
        for bad in [
            body.replace("key=val", "key=evil"),
            body.replacen("OK-1-", "OK-2-", 1),
            format!("{body}\nextra"),
            "OK-1\nkey=val".to_string(),
        ] {
            let r = parse(&bad);
            assert_eq!(r.status, ResponseStatus::Fail, "{bad}");
            assert_eq!(r.fail_code(), "RESPONSE_HASH_FAILED", "{bad}");
        }
        let r = parse("OK\nkey=val");
        assert_eq!(r.status, ResponseStatus::Fail);
        assert_eq!(r.fail_kind(), Some(FailKind::Other));
    }

    #[test]
    fn test_parse_empty_is_unavailable() {
        assert_eq!(parse("").fail_kind(), Some(FailKind::Unavailable));
        assert_eq!(
            parse("TEMPORARILY_UNAVAILABLE\n").fail_kind(),
            Some(FailKind::Unavailable)
        );
    }

    #[test]
    fn test_fail_kind_matches_code_prefixes() {
        for (body, kind) in [
            ("KEY_EXPIRED", FailKind::KeyExpired),
            ("FAIL_STARTUP_LOOP", FailKind::StartupLoop),
            (
                "FAIL_UNSUPPORTED_LOCATION XX",
                FailKind::UnsupportedLocation,
            ),
            ("FAIL_CID_IN_USE", FailKind::OtherClientConnected),
            ("TERM_BAD_NETWORK", FailKind::BadNetwork),
            ("FAIL_CODE", FailKind::Other),
        ] {
            let r = parse(body);
            assert_eq!(r.fail_kind(), Some(kind), "{body}");
            assert_eq!(r.fail_code(), body);
            assert_eq!(r.fail_host.as_deref(), Some("rpc.example.com"));
        }
    }
}
