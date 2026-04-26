use sha1::{Sha1, Digest};
use std::path::Path;
use std::collections::HashMap;
use std::fs;
use std::io;
use std::time::UNIX_EPOCH;
use std::time::Duration;
use tracing;

/// Compute SHA-1 hex digest of a string.
pub fn sha1_string(input: &str) -> String {
    let mut hasher = Sha1::new();
    hasher.update(input.as_bytes());
    hex_encode(&hasher.finalize())
}

/// Compute SHA-1 hex digest of a file.
pub fn sha1_file(path: &Path) -> io::Result<String> {
    let data = fs::read(path)?;
    let mut hasher = Sha1::new();
    hasher.update(&data);
    Ok(hex_encode(&hasher.finalize()))
}

/// Convert bytes to lowercase hex string.
pub fn hex_encode(data: &[u8]) -> String {
    data.iter().map(|b| format!("{:02x}", b)).collect()
}

/// Parse key=value pairs separated by semicolons into a HashMap.
/// Replicates Java Tools.parseAdditional().
pub fn parse_additional(additional: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if additional.is_empty() {
        return map;
    }
    for kv_pair in additional.trim().split(';') {
        if kv_pair.len() > 2
            && let Some((k, v)) = kv_pair.split_once('=') {
                map.insert(k.trim().to_string(), v.trim().to_string());
            }
    }
    map
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
        && e.kind() != io::ErrorKind::NotFound {
            tracing::warn!("Failed to remove file {}: {}", path.display(), e);
        }
}

/// Remove a directory, logging a warning on failure.
pub fn remove_dir(path: &Path) {
    if let Err(e) = fs::remove_dir(path)
        && e.kind() != io::ErrorKind::NotFound {
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
pub async fn tick_every<F, Fut>(shutdown: tokio_util::sync::CancellationToken, every: Duration, mut f: F)
where
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
        assert_eq!(m.get("fileindex").unwrap(), "42");
        assert_eq!(m.get("xres").unwrap(), "org");
    }
}
