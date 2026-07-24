use crate::bandwidth::BandwidthMonitor;
use crate::config::Config;
use crate::downloader::{
    DownloadMode, FileDownloader, build_direct_client, build_image_proxy_client,
};
use crate::error::{HathError, Result};
use crate::rpc_client::{GalleryAck, GalleryFileRequest, GalleryQueueReply, RpcClient};
use crate::stats::Stats;
use crate::types::Sha1Hash;
use arc_swap::ArcSwap;
use reqwest::Client;
use sha1::Digest;
use std::collections::HashSet;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

const MAX_GALLERY_FILES: usize = 10_000;
const MAX_GALLERY_ROUNDS: u32 = 10;
const MAX_REPORTED_FAILURES: usize = 50;
const SUCCESS_DELAY: Duration = Duration::from_secs(1);
const FAILURE_DELAY: Duration = Duration::from_secs(5);
const LOW_SPACE_DELAY: Duration = Duration::from_secs(5 * 60);
const DOWNLOAD_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const DOWNLOAD_READ_TIMEOUT: Duration = Duration::from_secs(10);

/// Result of asking the on-demand supervisor to start work.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StartOutcome {
    Started,
    AlreadyRunning,
}

/// Owns the single-flight lifecycle used by the authenticated server command.
/// The runner is deliberately independent so its Interface is also the test
/// surface for queue semantics.
pub(crate) struct GalleryDownloadSupervisor {
    runner: GalleryQueueRunner,
    active: Arc<AtomicBool>,
}

impl GalleryDownloadSupervisor {
    pub(crate) fn new(
        config: Arc<ArcSwap<Config>>,
        rpc_client: Arc<RpcClient>,
        stats: Arc<Stats>,
        shutdown: CancellationToken,
    ) -> Arc<Self> {
        Arc::new(Self {
            runner: GalleryQueueRunner {
                config,
                rpc_client,
                stats,
                shutdown,
            },
            active: Arc::new(AtomicBool::new(false)),
        })
    }

    pub(crate) fn start(self: &Arc<Self>) -> StartOutcome {
        if self
            .active
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return StartOutcome::AlreadyRunning;
        }

        let runner = self.runner.clone();
        let active = self.active.clone();
        tokio::spawn(async move {
            let _reset_active = ActiveRun(active);
            match runner.run_until_idle().await {
                Ok(report) => tracing::info!(
                    galleries_completed = report.galleries_completed,
                    galleries_failed = report.galleries_failed,
                    "Gallery downloader stopped"
                ),
                Err(HathError::Shutdown) => {
                    tracing::info!("Gallery downloader stopped for shutdown")
                }
                Err(e) => tracing::warn!("Gallery downloader stopped: {}", e),
            }
        });
        StartOutcome::Started
    }
}

struct ActiveRun(Arc<AtomicBool>);

impl Drop for ActiveRun {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

#[derive(Clone)]
struct GalleryQueueRunner {
    config: Arc<ArcSwap<Config>>,
    rpc_client: Arc<RpcClient>,
    stats: Arc<Stats>,
    shutdown: CancellationToken,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct GalleryRunReport {
    pub(crate) galleries_completed: u32,
    pub(crate) galleries_failed: u32,
}

#[derive(Debug)]
struct FinishedGallery {
    ack: GalleryAck,
    successful: bool,
    failures: Vec<String>,
}

#[derive(Debug)]
struct GalleryMetadata {
    gid: u32,
    minxres: String,
    title: String,
    information: String,
    files: Vec<GalleryFile>,
}

#[derive(Debug)]
struct GalleryFile {
    page: u32,
    fileindex: u32,
    xres: String,
    expected_sha1: Option<Sha1Hash>,
    filetype: String,
    filename: String,
}

#[derive(Debug)]
enum DownloadOne {
    Downloaded,
    AlreadyPresent,
    Failed(Option<String>),
}

impl GalleryQueueRunner {
    /// Consume galleries until the server reports an empty queue. A completed
    /// gallery is acknowledged only by the next fetchqueue request, matching
    /// the server protocol while keeping the state local to this runner.
    pub(crate) async fn run_until_idle(&self) -> Result<GalleryRunReport> {
        let mut report = GalleryRunReport::default();
        let mut previous: Option<FinishedGallery> = None;

        loop {
            self.ensure_not_shutdown()?;

            if let Some(finished) = &previous
                && !finished.failures.is_empty()
                && let Err(e) = self
                    .rpc_client
                    .report_gallery_failures(&finished.failures)
                    .await
            {
                tracing::warn!("Could not report gallery download failures: {}", e);
            }

            let ack = previous.as_ref().map(|finished| &finished.ack);
            let reply = self.rpc_client.fetch_gallery_queue(ack).await?;

            let metadata = match reply {
                GalleryQueueReply::NoPendingDownloads => return Ok(report),
                GalleryQueueReply::InvalidRequest => {
                    return Err(HathError::Rpc("gallery queue rejected request".into()));
                }
                GalleryQueueReply::Metadata(raw) => match parse_gallery_metadata(&raw) {
                    Ok(metadata) => metadata,
                    Err(e) => {
                        tracing::warn!("Refusing malformed gallery metadata: {}", e);
                        return Ok(report);
                    }
                },
            };

            tracing::info!(gid = metadata.gid, title = %metadata.title, "Starting gallery download");
            let finished = self.download_gallery(metadata).await?;
            if finished.successful {
                report.galleries_completed += 1;
            } else {
                report.galleries_failed += 1;
            }
            previous = Some(finished);
        }
    }

    async fn download_gallery(&self, metadata: GalleryMetadata) -> Result<FinishedGallery> {
        let cfg = self.config.load_full();
        let gallery_dir = create_gallery_dir(&cfg, &metadata)?;
        let direct_client = build_direct_client(DOWNLOAD_CONNECT_TIMEOUT, DOWNLOAD_READ_TIMEOUT)?;
        let proxied_client = if cfg.image_proxy_host.is_some() {
            build_image_proxy_client(&cfg, DOWNLOAD_CONNECT_TIMEOUT, DOWNLOAD_READ_TIMEOUT)?
        } else {
            direct_client.clone()
        };
        let limiter = (!cfg.disable_download_bwm && cfg.throttle_bytes > 0)
            .then(|| Arc::new(BandwidthMonitor::new(cfg.throttle_bytes)));

        let filecount = metadata.files.len();
        let mut complete = vec![false; filecount];
        let mut attempts = vec![0u32; filecount];
        let mut total_failures = 0usize;
        let mut source_failures = Vec::new();

        for _round in 0..MAX_GALLERY_ROUNDS {
            if complete.iter().all(|complete| *complete) || total_failures >= filecount * 2 {
                break;
            }

            for (index, file) in metadata.files.iter().enumerate() {
                self.ensure_not_shutdown()?;
                if complete[index] {
                    continue;
                }
                if has_low_space(&cfg) {
                    tracing::warn!("Gallery download paused because free space is too low");
                    self.delay(LOW_SPACE_DELAY).await?;
                    continue;
                }

                attempts[index] += 1;
                let client = if attempts[index] == 1 {
                    direct_client.clone()
                } else {
                    proxied_client.clone()
                };
                let outcome = self
                    .download_file(
                        &cfg,
                        metadata.gid,
                        &gallery_dir,
                        file,
                        attempts[index],
                        client,
                        limiter.clone(),
                    )
                    .await?;
                match outcome {
                    DownloadOne::Downloaded => {
                        complete[index] = true;
                        self.delay(SUCCESS_DELAY).await?;
                    }
                    DownloadOne::AlreadyPresent => complete[index] = true,
                    DownloadOne::Failed(source_failure) => {
                        total_failures += 1;
                        if let Some(failure) = source_failure
                            && source_failures.len() < MAX_REPORTED_FAILURES
                            && !source_failures.contains(&failure)
                        {
                            source_failures.push(failure);
                        }
                        self.delay(FAILURE_DELAY).await?;
                    }
                }
            }
        }

        let successful = complete.iter().all(|complete| *complete);
        if successful {
            let info_path = gallery_dir.join("galleryinfo.txt");
            if let Err(e) = tokio::fs::write(&info_path, &metadata.information).await {
                tracing::warn!(path = %info_path.display(), "Could not write galleryinfo.txt: {}", e);
            }
            tracing::info!(gid = metadata.gid, "Finished gallery download");
        } else {
            tracing::warn!(
                gid = metadata.gid,
                "Gallery download exhausted its retry budget"
            );
        }

        Ok(FinishedGallery {
            ack: GalleryAck {
                gid: metadata.gid,
                minxres: metadata.minxres,
            },
            successful,
            failures: source_failures,
        })
    }

    async fn download_file(
        &self,
        cfg: &Config,
        gid: u32,
        gallery_dir: &Path,
        file: &GalleryFile,
        attempt: u32,
        client: Client,
        limiter: Option<Arc<BandwidthMonitor>>,
    ) -> Result<DownloadOne> {
        let target = gallery_dir.join(format!("{}.{}", file.filename, file.filetype));
        if existing_file_is_valid(&target, file.expected_sha1.as_ref()).await? {
            return Ok(DownloadOne::AlreadyPresent);
        }
        remove_if_exists(&target).await;

        let source = match self
            .rpc_client
            .fetch_gallery_file_url(&GalleryFileRequest {
                gid,
                page: file.page,
                fileindex: file.fileindex,
                xres: file.xres.clone(),
                attempt,
            })
            .await
        {
            Ok(source) => source,
            Err(e) => {
                tracing::warn!(page = file.page, "Could not obtain gallery file URL: {}", e);
                return Ok(DownloadOne::Failed(None));
            }
        };

        let part = part_path(&target)?;
        remove_if_exists(&part).await;
        let mut downloader = FileDownloader::new(
            source.clone(),
            DOWNLOAD_READ_TIMEOUT.as_millis() as u64,
            300_000,
            DownloadMode::File(part.clone()),
        )
        .with_client(client);
        downloader.set_max_content_length(cfg.max_allowed_filesize);
        if let Some(limiter) = limiter {
            downloader.set_download_limiter(limiter);
        }

        let downloaded = tokio::select! {
            _ = self.shutdown.cancelled() => return Err(HathError::Shutdown),
            result = downloader.download() => result,
        };
        if let Err(e) = downloaded {
            tracing::debug!(page = file.page, "Gallery file transfer failed: {}", e);
            remove_if_exists(&part).await;
            return Ok(DownloadOne::Failed(source_failure_key(&source, file)));
        }

        let content_length = downloader.content_length.load(Ordering::Relaxed);
        let verified = content_length > 0
            && match &file.expected_sha1 {
                Some(expected) => verify_file_sha1(&part, expected.clone()).await?,
                None => true,
            };
        if !verified {
            tracing::debug!(page = file.page, "Gallery file failed integrity validation");
            remove_if_exists(&part).await;
            return Ok(DownloadOne::Failed(source_failure_key(&source, file)));
        }

        if let Err(e) = tokio::fs::rename(&part, &target).await {
            tracing::warn!(path = %target.display(), "Could not finalize gallery file: {}", e);
            remove_if_exists(&part).await;
            return Ok(DownloadOne::Failed(source_failure_key(&source, file)));
        }
        self.stats.record_file_rcvd();
        self.stats.record_bytes_rcvd(content_length as u64);
        tracing::info!(gid, page = file.page, path = %target.display(), "Finished gallery file");
        Ok(DownloadOne::Downloaded)
    }

    fn ensure_not_shutdown(&self) -> Result<()> {
        if self.shutdown.is_cancelled() {
            Err(HathError::Shutdown)
        } else {
            Ok(())
        }
    }

    async fn delay(&self, duration: Duration) -> Result<()> {
        tokio::select! {
            _ = self.shutdown.cancelled() => Err(HathError::Shutdown),
            _ = tokio::time::sleep(duration) => Ok(()),
        }
    }
}

fn parse_gallery_metadata(raw: &str) -> Result<GalleryMetadata> {
    enum State {
        Header,
        FileList,
        Information,
    }

    let mut state = State::Header;
    let mut gid = None;
    let mut filecount = None;
    let mut minxres = None;
    let mut title = None;
    let mut files: Vec<Option<GalleryFile>> = Vec::new();
    let mut filenames = HashSet::new();
    let mut information = String::new();

    for line in raw.lines() {
        match state {
            State::Header => {
                if line.is_empty() {
                    continue;
                }
                if line == "FILELIST" {
                    let count = filecount
                        .ok_or_else(|| HathError::Parse("FILELIST before FILECOUNT".into()))?;
                    files = (0..count).map(|_| None).collect();
                    state = State::FileList;
                    continue;
                }
                let (key, value) = line
                    .split_once(' ')
                    .ok_or_else(|| HathError::Parse("invalid gallery header line".into()))?;
                match key {
                    "GID" if gid.is_none() => gid = Some(parse_positive(value, "gid")?),
                    "FILECOUNT" if filecount.is_none() => {
                        let count = parse_positive(value, "filecount")? as usize;
                        if count > MAX_GALLERY_FILES {
                            return Err(HathError::Parse("gallery filecount exceeds limit".into()));
                        }
                        filecount = Some(count);
                    }
                    "MINXRES" if minxres.is_none() && valid_xres(value) => {
                        minxres = Some(value.to_string())
                    }
                    "TITLE" if title.is_none() => title = Some(sanitize_title(value)),
                    _ => {
                        return Err(HathError::Parse(format!(
                            "invalid gallery header key: {}",
                            key
                        )));
                    }
                }
            }
            State::FileList => {
                if line.is_empty() {
                    continue;
                }
                if line == "INFORMATION" {
                    state = State::Information;
                    continue;
                }
                let entry = parse_file_line(line)?;
                let count = filecount.expect("FILELIST requires FILECOUNT");
                if entry.page == 0 || entry.page as usize > count {
                    return Err(HathError::Parse("gallery page is out of range".into()));
                }
                let render_name = format!("{}.{}", entry.filename, entry.filetype);
                if !filenames.insert(render_name) {
                    return Err(HathError::Parse(
                        "gallery contains duplicate output filename".into(),
                    ));
                }
                let slot = &mut files[entry.page as usize - 1];
                if slot.replace(entry).is_some() {
                    return Err(HathError::Parse("gallery contains duplicate page".into()));
                }
            }
            State::Information => {
                information.push_str(line);
                information.push('\n');
            }
        }
    }

    let gid = gid.ok_or_else(|| HathError::Parse("gallery has no gid".into()))?;
    let minxres = minxres.ok_or_else(|| HathError::Parse("gallery has no minxres".into()))?;
    let title = title.ok_or_else(|| HathError::Parse("gallery has no title".into()))?;
    let filecount = filecount.ok_or_else(|| HathError::Parse("gallery has no filecount".into()))?;
    if !matches!(state, State::Information) {
        return Err(HathError::Parse(
            "gallery has no INFORMATION section".into(),
        ));
    }
    if files.len() != filecount || files.iter().any(Option::is_none) {
        return Err(HathError::Parse("gallery file list is incomplete".into()));
    }
    Ok(GalleryMetadata {
        gid,
        minxres,
        title,
        information,
        files: files.into_iter().flatten().collect(),
    })
}

fn parse_file_line(line: &str) -> Result<GalleryFile> {
    let mut rest = line;
    let mut fields = Vec::with_capacity(5);
    for _ in 0..5 {
        rest = rest.trim_start();
        let end = rest
            .find(char::is_whitespace)
            .ok_or_else(|| HathError::Parse("truncated gallery file entry".into()))?;
        fields.push(&rest[..end]);
        rest = &rest[end..];
    }
    let filename = rest.trim_start();
    let page = parse_positive(fields[0], "page")?;
    let fileindex = parse_positive(fields[1], "fileindex")?;
    if !valid_xres(fields[2]) || !safe_file_component(filename) || !safe_filetype(fields[4]) {
        return Err(HathError::Parse("unsafe gallery file entry".into()));
    }
    let expected_sha1 = if fields[3] == "unknown" {
        None
    } else {
        Some(
            Sha1Hash::new(fields[3])
                .ok_or_else(|| HathError::Parse("invalid gallery SHA-1".into()))?,
        )
    };
    Ok(GalleryFile {
        page,
        fileindex,
        xres: fields[2].to_string(),
        expected_sha1,
        filetype: fields[4].to_string(),
        filename: filename.to_string(),
    })
}

fn parse_positive(value: &str, field: &str) -> Result<u32> {
    value
        .parse::<u32>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| HathError::Parse(format!("invalid {}", field)))
}

fn valid_xres(value: &str) -> bool {
    value == "org" || (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
}

fn sanitize_title(value: &str) -> String {
    value
        .chars()
        .filter(|ch| !matches!(ch, '*' | '"' | '\\' | '/' | '<' | '>' | ':' | '|' | '?'))
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn safe_file_component(value: &str) -> bool {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.ends_with(['.', ' '])
        || value
            .chars()
            .any(|ch| matches!(ch, '/' | '\\' | ':' | '\0'))
    {
        return false;
    }
    let base = value.split('.').next().unwrap_or("").to_ascii_uppercase();
    !matches!(base.as_str(), "CON" | "PRN" | "AUX" | "NUL")
        && !matches!(
            base.as_str(),
            "COM1" | "COM2" | "COM3" | "COM4" | "COM5" | "COM6" | "COM7" | "COM8" | "COM9"
        )
        && !matches!(
            base.as_str(),
            "LPT1" | "LPT2" | "LPT3" | "LPT4" | "LPT5" | "LPT6" | "LPT7" | "LPT8" | "LPT9"
        )
}

fn safe_filetype(value: &str) -> bool {
    !value.is_empty() && value.len() <= 16 && value.bytes().all(|byte| byte.is_ascii_alphanumeric())
}

fn create_gallery_dir(config: &Config, metadata: &GalleryMetadata) -> Result<PathBuf> {
    let suffix = if metadata.minxres == "org" {
        format!(" [{}]", metadata.gid)
    } else {
        format!(" [{}-{}x]", metadata.gid, metadata.minxres)
    };
    let max = config.max_filename_length as usize;
    let title = if metadata.title.chars().count() + suffix.chars().count() > max {
        let keep = max.saturating_sub(suffix.chars().count() + 3);
        format!(
            "{}...",
            metadata.title.chars().take(keep).collect::<String>()
        )
    } else {
        metadata.title.clone()
    };
    let primary = config.download_dir.join(format!("{}{}", title, suffix));
    let fallback = config.download_dir.join(if metadata.minxres == "org" {
        metadata.gid.to_string()
    } else {
        format!("{}-{}x", metadata.gid, metadata.minxres)
    });

    for path in [primary, fallback] {
        if path.parent() != Some(config.download_dir.as_path()) {
            continue;
        }
        if std::fs::create_dir_all(&path).is_ok() && path.is_dir() {
            return Ok(path);
        }
    }
    Err(HathError::Io(std::io::Error::other(
        "could not create gallery directory",
    )))
}

fn has_low_space(config: &Config) -> bool {
    if config.skip_free_space_check {
        return false;
    }
    let minimum = config.diskremaining_bytes.saturating_add(1_048_576_000);
    fs2::available_space(&config.download_dir)
        .map(|space| space < minimum)
        .unwrap_or(true)
}

async fn existing_file_is_valid(path: &Path, expected_sha1: Option<&Sha1Hash>) -> Result<bool> {
    let metadata = match tokio::fs::metadata(path).await {
        Ok(metadata) if metadata.is_file() && metadata.len() > 0 => metadata,
        Ok(_) => return Ok(false),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(HathError::Io(e)),
    };
    let _ = metadata;
    match expected_sha1 {
        None => Ok(true),
        Some(expected) => verify_file_sha1(path, expected.clone()).await,
    }
}

async fn verify_file_sha1(path: &Path, expected: Sha1Hash) -> Result<bool> {
    let path = path.to_path_buf();
    tokio::task::spawn_blocking(move || {
        let mut file = std::fs::File::open(path)?;
        let mut digest = sha1::Sha1::new();
        let mut buffer = [0u8; 65_536];
        loop {
            let read = file.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        Ok::<_, std::io::Error>(crate::utils::hex_encode(&digest.finalize()) == expected.as_str())
    })
    .await
    .map_err(|e| HathError::Network(format!("SHA-1 task failed: {}", e)))?
    .map_err(HathError::Io)
}

async fn remove_if_exists(path: &Path) {
    if let Err(e) = tokio::fs::remove_file(path).await
        && e.kind() != std::io::ErrorKind::NotFound
    {
        tracing::warn!(path = %path.display(), "Could not remove stale gallery file: {}", e);
    }
}

fn part_path(target: &Path) -> Result<PathBuf> {
    let filename = target
        .file_name()
        .and_then(|filename| filename.to_str())
        .ok_or_else(|| HathError::Parse("gallery target has no UTF-8 filename".into()))?;
    Ok(target.with_file_name(format!(".{}.part", filename)))
}

fn source_failure_key(source: &reqwest::Url, file: &GalleryFile) -> Option<String> {
    source
        .host_str()
        .map(|host| format!("{}-{}-{}", host, file.fileindex, file.xres))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{FakeHttpServer, FakeResponse, FixtureDirs};
    use arc_swap::ArcSwap;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn parses_complete_metadata_and_preserves_information() {
        let metadata = parse_gallery_metadata(
            "GID 42\nFILECOUNT 2\nMINXRES org\nTITLE hello/world\nFILELIST\n1 10 org unknown jpg first page\n2 11 1280 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa png second\nINFORMATION\nline one\nline two\n",
        )
        .unwrap();
        assert_eq!(metadata.gid, 42);
        assert_eq!(metadata.title, "helloworld");
        assert_eq!(metadata.files.len(), 2);
        assert_eq!(metadata.information, "line one\nline two\n");
    }

    #[test]
    fn rejects_unsafe_or_incomplete_metadata() {
        assert!(parse_gallery_metadata(
            "GID 1\nFILECOUNT 1\nMINXRES org\nTITLE x\nFILELIST\n1 1 org unknown jpg ../escape\nINFORMATION\n"
        )
        .is_err());
        assert!(parse_gallery_metadata(
            "GID 1\nFILECOUNT 2\nMINXRES org\nTITLE x\nFILELIST\n1 1 org unknown jpg one\nINFORMATION\n"
        )
        .is_err());
    }

    #[tokio::test]
    async fn runner_downloads_acknowledges_and_writes_gallery_info() {
        let body = b"gallery image".to_vec();
        let source_server = FakeHttpServer::start(vec![FakeResponse::ok(body.clone())]).await;
        let fixture = FixtureDirs::new();
        let mut config = fixture.config();
        let rpc_server = FakeHttpServer::start(vec![
            FakeResponse::ok(format!(
                "GID 42\nFILECOUNT 1\nMINXRES org\nTITLE Gallery\nFILELIST\n1 7 org {} jpg page\nINFORMATION\nmetadata\n",
                crate::utils::sha1_bytes(&body),
            )),
            FakeResponse::ok(format!("OK\n{}", source_server.url("/image"))),
            FakeResponse::ok("NO_PENDING_DOWNLOADS"),
        ])
        .await;
        config.rpc_servers = vec![IpAddr::V4(Ipv4Addr::LOCALHOST)];
        config.rpc_port = rpc_server.port();
        config.rpc_path = "rpc".to_string();
        config.skip_free_space_check = true;
        let config = Arc::new(ArcSwap::from_pointee(config));
        let rpc_client = Arc::new(RpcClient::new(config.clone()).unwrap());
        let stats = Arc::new(Stats::new());
        let runner = GalleryQueueRunner {
            config: config.clone(),
            rpc_client,
            stats: stats.clone(),
            shutdown: CancellationToken::new(),
        };

        let report = runner.run_until_idle().await.unwrap();

        assert_eq!(report.galleries_completed, 1);
        assert_eq!(report.galleries_failed, 0);
        let download_dir = config.load().download_dir.clone();
        assert_eq!(
            std::fs::read(download_dir.join("Gallery [42]").join("page.jpg")).unwrap(),
            body
        );
        assert_eq!(
            std::fs::read_to_string(download_dir.join("Gallery [42]").join("galleryinfo.txt"))
                .unwrap(),
            "metadata\n"
        );
        assert_eq!(stats.files_rcvd.load(Ordering::Relaxed), 1);
        let requests = rpc_server.requests();
        assert_eq!(requests.len(), 3);
        assert!(requests[0].contains("act=fetchqueue"));
        assert!(requests[1].contains("act=dlfetch"));
        assert!(requests[2].contains("add=42;org"));
    }
}
