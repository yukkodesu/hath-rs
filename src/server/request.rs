use crate::config::Config;
use crate::utils::{self, parse_additional, Additional};
use crate::hvfile::HVFile;
use std::net::IpAddr;

#[derive(Debug)]
pub enum RequestType {
    FileServe {
        fileid: String,
        hv_file: Option<HVFile>,
        additional: Additional,
        keystamp_valid: bool,
        /// HEAD requests: skip body construction (file read / proxy download).
        head_only: bool,
    },
    ServerCommand {
        command: String,
        additional: String,
        valid: bool,
    },
    SpeedTest {
        testsize: u32,
        valid: bool,
        /// Distinguishes 403 (invalid key) from 400 (malformed URL) when valid=false.
        forbidden: bool,
        /// HEAD requests: skip body generation.
        head_only: bool,
    },
    Favicon,
    Robots,
    NotFound,
    BadRequest,
    MethodNotAllowed,
}
pub fn parse_request(method: &str, path_and_query: &str, client_ip: IpAddr, config: &Config) -> RequestType {
    if !matches!(method.to_uppercase().as_str(), "GET" | "HEAD") {
        return RequestType::MethodNotAllowed;
    }

    let uri = path_and_query;

    // Strip absolute URI prefix (section 5.1.2 RFC 2616)
    let uri = if let Some(rest) = uri.strip_prefix("http://") {
        rest.find('/').map(|i| &rest[i..]).unwrap_or("/")
    } else {
        uri
    };

    // Java: HTTPResponse.parseRequest() line 151 —
    // requestParts[1].replace("%3d", "=") decodes URL-encoded equals signs
    // in the additional segment before splitting into key=value pairs.
    let uri = uri.replace("%3d", "=");

    let url_parts: Vec<&str> = uri.split('/').collect();
    if url_parts.len() < 2 || !url_parts[0].is_empty() {
        return RequestType::NotFound;
    }

    let head_only = method.eq_ignore_ascii_case("HEAD");

    match url_parts[1] {
        "h" => parse_file_serve(&url_parts, config, head_only),
        "servercmd" => parse_server_command(&url_parts, client_ip, config),
        "t" => parse_speed_test(&url_parts, config, head_only),
        _ if url_parts.len() == 2 => match url_parts[1] {
            "favicon.ico" => RequestType::Favicon,
            "robots.txt" => RequestType::Robots,
            _ => RequestType::NotFound,
        },
        _ => RequestType::NotFound,
    }
}

fn parse_file_serve(url_parts: &[&str], config: &Config, head_only: bool) -> RequestType {
    if url_parts.len() < 4 { return RequestType::BadRequest; }
    let fileid = url_parts[2].to_string();
    let hv_file = HVFile::from_fileid(&fileid);
    let additional = parse_additional(url_parts[3]);
    let keystamp_valid = validate_keystamp(&fileid, additional.keystamp.as_deref(), config);

    RequestType::FileServe { fileid, hv_file, additional, keystamp_valid, head_only }
}

fn parse_server_command(url_parts: &[&str], client_ip: IpAddr, config: &Config) -> RequestType {
    let is_from_rpc = config.rpc_servers.contains(&client_ip) || config.disable_ip_origin_check;

    if url_parts.len() < 6 {
        return RequestType::ServerCommand { command: String::new(), additional: String::new(), valid: false };
    }

    let command = url_parts[2].to_string();
    let additional = url_parts[3].to_string();
    let command_time: i64 = url_parts[4].parse().unwrap_or(0);
    let key = url_parts[5];

    let valid = is_from_rpc && validate_servercmd(&command, &additional, command_time, key, config);

    RequestType::ServerCommand { command, additional, valid }
}

fn parse_speed_test(url_parts: &[&str], config: &Config, head_only: bool) -> RequestType {
    if url_parts.len() < 5 {
        // Java: responseStatusCode = 400 when urlparts.length < 5
        return RequestType::SpeedTest { testsize: 0, valid: false, forbidden: false, head_only };
    }
    let testsize: u32 = url_parts[2].parse().unwrap_or(0);
    let testtime: i64 = url_parts[3].parse().unwrap_or(0);
    let testkey = url_parts[4].to_string();
    let valid = validate_speedtest(testsize, testtime, &testkey, config);
    // Java: responseStatusCode = 403 for expired or invalid key
    RequestType::SpeedTest { testsize, valid, forbidden: !valid, head_only }
}

/// Validate keystamp for /h/ requests.
/// Java: |serverTime - ts| < 900 && SHA1(...)[0..10].equalsIgnoreCase(provided)
pub fn validate_keystamp(fileid: &str, keystamp: Option<&str>, config: &Config) -> bool {
    let keystamp = match keystamp { Some(k) => k, None => return false };
    let (timestamp_str, provided_prefix) = match keystamp.split_once('-') {
        Some((t, p)) => (t, p),
        None => return false,
    };
    let timestamp: i64 = match timestamp_str.parse() { Ok(t) => t, Err(_) => return false };

    if (config.server_time() - timestamp).abs() >= 900 { return false; }
    if provided_prefix.len() != 10 { return false; }

    let expected = utils::sha1_string(&format!(
        "{}-{}-{}-hotlinkthis", timestamp, fileid, config.client_key.as_str()
    ));

    // Case-insensitive comparison, exactly first 10 chars
    expected[..10].eq_ignore_ascii_case(provided_prefix)
}

fn validate_servercmd(command: &str, additional: &str, time: i64, key: &str, config: &Config) -> bool {
    if (time - config.server_time()).abs() > 300 { return false; }
    let expected = utils::sha1_string(&format!(
        "hentai@home-servercmd-{}-{}-{}-{}-{}",
        command, additional, config.client_id.0, time, config.client_key.as_str()
    ));
    expected == key
}

fn validate_speedtest(testsize: u32, testtime: i64, testkey: &str, config: &Config) -> bool {
    if (testtime - config.server_time()).abs() > 300 { return false; }
    let expected = utils::sha1_string(&format!(
        "hentai@home-speedtest-{}-{}-{}-{}",
        testsize, testtime, config.client_id.0, config.client_key.as_str()
    ));
    expected == testkey
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CliArgs;
    use clap::Parser;

    fn test_config() -> Config {
        let args = CliArgs::try_parse_from([
            "hath-rs", "--client-id", "123", "--client-key", "abcde12345abcde12345",
        ]).unwrap();
        Config::load(args).unwrap()
    }

    #[test]
    fn test_favicon() {
        let c = test_config();
        assert!(matches!(parse_request("GET", "/favicon.ico", "127.0.0.1".parse().unwrap(), &c), RequestType::Favicon));
    }

    #[test]
    fn test_robots() {
        let c = test_config();
        assert!(matches!(parse_request("GET", "/robots.txt", "127.0.0.1".parse().unwrap(), &c), RequestType::Robots));
    }

    #[test]
    fn test_keystamp_too_short_rejected() {
        let c = test_config();
        // Only 5 chars in prefix (not 10)
        assert!(!validate_keystamp("test", Some("1234567890-12345"), &c));
    }

    #[test]
    fn test_keystamp_without_separator() {
        let c = test_config();
        assert!(!validate_keystamp("test", Some("noseparator"), &c));
    }
}
