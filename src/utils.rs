use sha1::{Digest, Sha1};
use std::fs;
use std::io;
use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;
use std::time::UNIX_EPOCH;
use tracing;

/// Normalize an IP address for comparison: maps IPv4-mapped IPv6 addresses
/// (e.g. `::ffff:1.2.3.4`) to plain IPv4. Needed when the server listens on
/// `[::]` (dual-stack) and receives IPv4 connections as mapped addresses.
pub fn normalize_ip(addr: IpAddr) -> IpAddr {
    match addr {
        IpAddr::V6(v6) => v6
            .to_ipv4_mapped()
            .map(IpAddr::V4)
            .unwrap_or(IpAddr::V6(v6)),
        v4 => v4,
    }
}

/// Compute SHA-1 hex digest of a string.
pub fn sha1_string(input: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(input.as_bytes());
    hex_encode(&hasher.finalize())
}

/// Compute SHA-1 hex digest of a file.
pub fn sha1_file(path: &Path) -> io::Result<String> {
    let data = fs::read(path)?;
    Ok(sha1_bytes(&data))
}

/// Compute SHA-1 hex digest of a byte slice (zero-copy, no intermediate allocation).
pub fn sha1_bytes(data: &[u8]) -> String {
    let mut hasher = Sha1::new();
    hasher.update(data);
    hex_encode(&hasher.finalize())
}

/// Convert bytes to lowercase hex string.
pub fn hex_encode(data: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(data.len() * 2);
    for b in data {
        write!(out, "{:02x}", b).unwrap();
    }
    out
}

/// All key=value fields used across H@H additional segments.
/// Covers both file-serve (/h/) and servercmd paths.
/// Parsed directly by key name — no HashMap, no hash computation.
#[derive(Debug, Default)]
pub struct Additional {
    // /h/ file-serve keys
    pub keystamp: Option<String>,
    pub fileindex: Option<String>,
    pub xres: Option<String>,
    // servercmd/threaded_proxy_test keys
    pub hostname: Option<String>,
    pub protocol: Option<String>,
    pub port: Option<String>,
    pub testsize: Option<String>,
    pub testcount: Option<String>,
    pub testtime: Option<String>,
    pub testkey: Option<String>,
}

/// Parse semicolon-separated key=value pairs into an `Additional`.
/// Replicates Java Tools.parseAdditional().
pub fn parse_additional(additional: &str) -> Additional {
    let mut out = Additional::default();
    if additional.is_empty() {
        return out;
    }
    for kv_pair in additional.trim().split(';') {
        if kv_pair.len() > 2
            && let Some((k, v)) = kv_pair.split_once('=')
        {
            let v = v.trim().to_string();
            match k.trim() {
                "keystamp" => out.keystamp = Some(v),
                "fileindex" => out.fileindex = Some(v),
                "xres" => out.xres = Some(v),
                "hostname" => out.hostname = Some(v),
                "protocol" => out.protocol = Some(v),
                "port" => out.port = Some(v),
                "testsize" => out.testsize = Some(v),
                "testcount" => out.testcount = Some(v),
                "testtime" => out.testtime = Some(v),
                "testkey" => out.testkey = Some(v),
                _ => {}
            }
        }
    }
    out
}

/// Ensure a directory exists, creating it if necessary.
pub fn ensure_dir(path: &Path) -> std::io::Result<()> {
    if path.is_file() {
        fs::remove_file(path)?;
    }
    if !path.is_dir() {
        fs::create_dir_all(path)?;
    }
    Ok(())
}

/// Read entire contents of a file as a UTF-8 string.
pub fn read_string_file(path: &Path) -> std::io::Result<String> {
    fs::read_to_string(path)
}

/// Write a string to a file, overwriting if it exists.
pub fn write_string_file(path: &Path, content: &str) -> std::io::Result<()> {
    fs::write(path, content)
}

/// List files in a directory, sorted by name. Returns empty vec if dir doesn't exist.
pub fn list_sorted_files(dir: &Path) -> Vec<std::path::PathBuf> {
    let mut files: Vec<_> = match fs::read_dir(dir) {
        Ok(entries) => entries.filter_map(|e| e.ok().map(|e| e.path())).collect(),
        Err(_) => return vec![],
    };
    files.sort();
    files
}

/// Remove a file, logging a warning on failure (never silently swallow).
pub fn remove_file(path: &Path) {
    if let Err(e) = fs::remove_file(path)
        && e.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!("Failed to remove file {}: {}", path.display(), e);
    }
}

/// Remove a directory, logging a warning on failure.
pub fn remove_dir(path: &Path) {
    if let Err(e) = fs::remove_dir(path)
        && e.kind() != io::ErrorKind::NotFound
    {
        tracing::warn!("Failed to remove dir {}: {}", path.display(), e);
    }
}

/// Get the last-modified timestamp of a file as milliseconds since Unix epoch.
/// Returns 0 if the timestamp can't be read.
pub fn modified_millis(path: &Path) -> u64 {
    path.metadata()
        .and_then(|m| m.modified())
        .map(|t| t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64)
        .unwrap_or(0)
}

/// Current time in milliseconds since UNIX epoch (matches Java System.currentTimeMillis())
pub fn millis_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Run `f` on each tick of an interval, until `shutdown` fires.
pub async fn tick_every<F, Fut>(
    shutdown: tokio_util::sync::CancellationToken,
    every: Duration,
    mut f: F,
) where
    F: FnMut() -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send,
{
    let mut tick = tokio::time::interval(every);
    // Skip the immediate first tick (Java main loop sleeps before first iteration).
    tick.tick().await;
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tick.tick() => f().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha1_string_known_value() {
        let result = sha1_string("hentai@home");
        assert_eq!(result, "1ba2bf5934ee97c0ee8d47f7acd43f774510495c");
    }

    #[test]
    fn test_hex_encode() {
        assert_eq!(hex_encode(&[0xab, 0xcd, 0xef]), "abcdef");
    }

    #[test]
    fn test_hex_encode_zero_pad() {
        assert_eq!(hex_encode(&[0x01, 0x0a, 0xff]), "010aff");
    }

    #[test]
    fn test_parse_additional_multiple() {
        let m = parse_additional("fileindex=42;xres=org");
        assert_eq!(m.fileindex.as_deref().unwrap(), "42");
        assert_eq!(m.xres.as_deref().unwrap(), "org");
    }
}
