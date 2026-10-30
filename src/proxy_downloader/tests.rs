use super::*;
use crate::tasks::TransferTasks;
use crate::test_support::{FakeHttpServer, FakeResponse, FixtureDirs, wait_for_file_contents};
use tokio::sync::Semaphore;
use tokio::time::Instant;

const FINAL_BODY: &[u8] = b"0123456789abcdefghij";

fn fileid_for_body(body: &[u8]) -> String {
    format!("{}-{}-jpg", utils::sha1_bytes(body), body.len())
}

struct Harness {
    _fixture: FixtureDirs,
    config: Config,
    downloader: ProxyDownloader,
    tasks: TransferTasks,
}

impl Harness {
    fn new() -> Self {
        let fixture = FixtureDirs::new();
        let config = fixture.config();
        let downloader = ProxyDownloader::new(&config).unwrap();
        Self {
            _fixture: fixture,
            config,
            downloader,
            tasks: TransferTasks::new(crate::shutdown::Shutdown::default()),
        }
    }

    fn with_timeout(timeout: Duration) -> Self {
        let mut harness = Self::new();
        harness.downloader.timeout = timeout;
        harness
    }

    async fn start(&self, fileid: &str, sources: &[Url]) -> Result<ProxyDownload> {
        self.downloader
            .start(
                fileid,
                sources,
                &self.config,
                None,
                Arc::new(Stats::new()),
                &self.tasks,
            )
            .await
    }

    fn cache_path(&self, fileid: &str) -> PathBuf {
        HVFile::from_fileid(fileid)
            .unwrap()
            .cache_path(&self.config.cache_dir)
    }

    /// Releases downstream without reading, then returns the terminal state.
    async fn finish(&self, proxy: ProxyDownload) -> DownloadState {
        let mut rx = proxy.body.watch_rx.clone();
        drop(proxy);
        loop {
            let state = rx.borrow_and_update().clone();
            if matches!(state, DownloadState::Done | DownloadState::Failed(_)) {
                return state;
            }
            rx.changed().await.unwrap();
        }
    }
}

async fn wait_for_requests(server: &FakeHttpServer, count: usize) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.requests().len() < count {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn copy_temp_to_cache_and_delete_removes_source_after_copy() {
    let dir = tempfile::tempdir().unwrap();
    let temp_path = dir.path().join("proxy-temp");
    let cache_path = dir.path().join("cache-file");
    std::fs::write(&temp_path, b"cache me").unwrap();

    copy_temp_to_cache_and_delete(&temp_path, &cache_path)
        .await
        .unwrap();

    assert_eq!(std::fs::read(&cache_path).unwrap(), b"cache me");
    assert!(!temp_path.exists());
}

#[tokio::test]
async fn proxy_downloader_retries_body_stage_against_fake_upstream() {
    let harness = Harness::new();
    let fileid = fileid_for_body(FINAL_BODY);
    let server = FakeHttpServer::start(vec![
        FakeResponse::ok(FINAL_BODY).close_after(7),
        FakeResponse::ok(FINAL_BODY),
    ])
    .await;

    let proxy = harness
        .start(&fileid, &[server.url("/image")])
        .await
        .unwrap();
    drop(proxy);

    wait_for_file_contents(&harness.cache_path(&fileid), FINAL_BODY).await;
    assert_eq!(server.requests().len(), 2);
}

#[tokio::test]
async fn released_downstream_still_allows_successful_upstream_to_finalize() {
    let harness = Harness::new();
    let fileid = fileid_for_body(FINAL_BODY);
    let server = FakeHttpServer::start(vec![FakeResponse::ok(FINAL_BODY)]).await;

    let proxy = harness
        .start(&fileid, &[server.url("/image")])
        .await
        .unwrap();
    // Java initializes ProxyFileDownloader for HEAD, but HTTPSession skips
    // writing a body, and requestCompleted() releases the finalization gate on
    // abort paths too. Releasing downstream is the Rust equivalent for both.
    drop(proxy);

    wait_for_file_contents(&harness.cache_path(&fileid), FINAL_BODY).await;
    assert_eq!(server.requests().len(), 1);
}

#[tokio::test]
async fn retry_retains_published_prefix_while_backup_is_stalled() {
    let harness = Harness::new();
    let contents: Vec<u8> = (0..256333).map(|n| (n % 251) as u8).collect();
    let fileid = fileid_for_body(&contents);
    let primary =
        FakeHttpServer::start(vec![FakeResponse::ok(contents.clone()).close_after(130005)]).await;
    let gate = Arc::new(Semaphore::new(0));
    let backup = FakeHttpServer::start(vec![
        FakeResponse::ok(contents.clone()).with_body_gate(gate.clone()),
    ])
    .await;
    let proxy = harness
        .start(&fileid, &[primary.url("/image"), backup.url("/image")])
        .await
        .unwrap();
    wait_for_requests(&backup, 1).await;
    assert!(matches!(
        *proxy.body.watch_rx.borrow(),
        DownloadState::InProgress(130005)
    ));
    assert_eq!(
        std::fs::read(&proxy.body.temp_file).unwrap(),
        &contents[..130005]
    );
    gate.add_permits(1);
    assert!(matches!(harness.finish(proxy).await, DownloadState::Done));
    assert_eq!(
        std::fs::read(harness.cache_path(&fileid)).unwrap(),
        contents
    );
}

#[tokio::test]
async fn mismatching_replay_cannot_overwrite_prefix_and_next_source_can_recover() {
    let harness = Harness::new();
    let fileid = fileid_for_body(FINAL_BODY);
    let server = FakeHttpServer::start(vec![
        FakeResponse::ok(FINAL_BODY).close_after(7),
        FakeResponse::ok(vec![b'X'; FINAL_BODY.len()]),
        FakeResponse::ok(FINAL_BODY),
    ])
    .await;
    let proxy = harness
        .start(&fileid, &[server.url("/image")])
        .await
        .unwrap();
    assert!(matches!(harness.finish(proxy).await, DownloadState::Done));
    assert_eq!(
        std::fs::read(harness.cache_path(&fileid)).unwrap(),
        FINAL_BODY
    );
    assert_eq!(server.requests().len(), 3);
}

#[tokio::test]
async fn invalid_response_and_body_failures_share_one_attempt_budget() {
    let harness = Harness::new();
    let fileid = fileid_for_body(FINAL_BODY);
    let server = FakeHttpServer::start(vec![
        FakeResponse::status(500, "Internal Server Error", "retry"),
        FakeResponse::ok(FINAL_BODY).close_after(7),
        FakeResponse::ok(FINAL_BODY).close_after(7),
        FakeResponse::ok(FINAL_BODY),
    ])
    .await;
    let proxy = harness
        .start(&fileid, &[server.url("/image")])
        .await
        .unwrap();
    assert!(matches!(
        harness.finish(proxy).await,
        DownloadState::Failed(_)
    ));
    assert_eq!(server.requests().len(), 3);
    assert!(!harness.cache_path(&fileid).exists());
}

#[tokio::test]
async fn redirects_are_not_followed_and_count_as_failed_attempts() {
    let harness = Harness::new();
    let fileid = fileid_for_body(FINAL_BODY);
    let target = FakeHttpServer::start(vec![FakeResponse::ok(FINAL_BODY)]).await;
    let redirect = FakeResponse::status(302, "Found", "")
        .with_header("Location", target.url("/image").as_str());
    let primary = FakeHttpServer::start(vec![redirect.clone(), redirect.clone(), redirect]).await;

    let result = harness.start(&fileid, &[primary.url("/image")]).await;

    assert!(matches!(
        result,
        Err(HathError::ProxyDownloader { status: 502, .. })
    ));
    assert_eq!(primary.requests().len(), 3);
    assert!(target.requests().is_empty());
}

#[tokio::test]
async fn empty_sources_are_rejected_before_any_request() {
    let harness = Harness::new();
    let result = harness.start(&fileid_for_body(FINAL_BODY), &[]).await;
    assert!(matches!(
        result,
        Err(HathError::ProxyDownloader { status: 502, .. })
    ));
}

#[tokio::test]
async fn overall_deadline_interrupts_a_stalled_retry_and_cleans_up() {
    let harness = Harness::with_timeout(Duration::from_secs(2));
    let fileid = fileid_for_body(FINAL_BODY);
    let primary = FakeHttpServer::start(vec![FakeResponse::ok(FINAL_BODY).close_after(7)]).await;
    let backup = FakeHttpServer::start(vec![
        FakeResponse::ok(FINAL_BODY).with_body_gate(Arc::new(Semaphore::new(0))),
    ])
    .await;
    let proxy = harness
        .start(&fileid, &[primary.url("/image"), backup.url("/image")])
        .await
        .unwrap();
    let path = proxy.body.temp_file.clone();
    wait_for_requests(&backup, 1).await;
    // Both attempts have started; advancing past the original deadline must
    // cancel the body wait even though it has a 30-second read timeout.
    tokio::time::pause();
    tokio::time::advance(Duration::from_secs(3)).await;
    assert!(matches!(
        harness.finish(proxy).await,
        DownloadState::Failed(DownloadFailReason::Timeout)
    ));
    tokio::time::resume();
    assert!(
        harness
            .tasks
            .finish(Instant::now() + Duration::from_secs(2))
            .await
    );
    assert!(!path.exists());
    assert!(!harness.cache_path(&fileid).exists());
    assert_eq!(primary.requests().len(), 1);
    assert_eq!(backup.requests().len(), 1);
}

#[tokio::test]
async fn failed_transfer_cleans_temporary_file_and_never_caches_invalid_content() {
    let harness = Harness::new();
    let fileid = fileid_for_body(FINAL_BODY);
    let server = FakeHttpServer::start(vec![
        FakeResponse::ok(FINAL_BODY).close_after(7),
        FakeResponse::ok(vec![b'X'; FINAL_BODY.len()]),
        FakeResponse::ok(vec![b'X'; FINAL_BODY.len()]),
    ])
    .await;
    let proxy = harness
        .start(&fileid, &[server.url("/image")])
        .await
        .unwrap();
    let path = proxy.body.temp_file.clone();
    assert!(matches!(
        harness.finish(proxy).await,
        DownloadState::Failed(DownloadFailReason::Sha1Mismatch)
    ));
    assert!(
        harness
            .tasks
            .finish(Instant::now() + Duration::from_secs(2))
            .await
    );
    assert!(!path.exists());
    assert!(!harness.cache_path(&fileid).exists());
}

#[tokio::test]
async fn aborted_transfer_removes_temporary_file() {
    let harness = Harness::new();
    let fileid = fileid_for_body(FINAL_BODY);
    let gate = Arc::new(Semaphore::new(0));
    let server =
        FakeHttpServer::start(vec![FakeResponse::ok(FINAL_BODY).with_body_gate(gate)]).await;
    let proxy = harness
        .start(&fileid, &[server.url("/image")])
        .await
        .unwrap();
    let path = proxy.body.temp_file.clone();
    assert!(path.exists());

    // Drain times out on the stalled body, which aborts the task.
    assert!(!harness.tasks.finish(Instant::now()).await);

    assert!(!path.exists());
    assert!(!harness.cache_path(&fileid).exists());
}

#[tokio::test]
async fn refused_spawn_removes_temporary_file() {
    let harness = Harness::new();
    let fileid = fileid_for_body(FINAL_BODY);
    let server = FakeHttpServer::start(vec![FakeResponse::ok(FINAL_BODY)]).await;
    assert!(
        harness
            .tasks
            .finish(Instant::now() + Duration::from_secs(1))
            .await
    );

    let result = harness.start(&fileid, &[server.url("/image")]).await;

    assert!(matches!(result, Err(HathError::Shutdown)));
    assert_eq!(
        std::fs::read_dir(&harness.config.temp_dir).unwrap().count(),
        0
    );
}

#[tokio::test]
async fn invalid_primary_headers_fall_back_before_streaming_starts() {
    let harness = Harness::new();
    let fileid = fileid_for_body(FINAL_BODY);
    let primary = FakeHttpServer::start(vec![FakeResponse::status(
        500,
        "Internal Server Error",
        "retry",
    )])
    .await;
    let backup = FakeHttpServer::start(vec![FakeResponse::ok(FINAL_BODY)]).await;
    let proxy = harness
        .start(&fileid, &[primary.url("/image"), backup.url("/image")])
        .await
        .unwrap();
    assert!(matches!(harness.finish(proxy).await, DownloadState::Done));
    assert_eq!(primary.requests().len(), 1);
    assert_eq!(backup.requests().len(), 1);
}

#[tokio::test]
async fn overall_deadline_also_bounds_waiting_for_initial_response_headers() {
    use tokio::io::AsyncReadExt;
    let harness = Harness::with_timeout(Duration::from_secs(2));
    let fileid = fileid_for_body(FINAL_BODY);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source = Url::parse(&format!("http://{}/image", listener.local_addr().unwrap())).unwrap();
    let (requested_tx, requested_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(socket.read_u8().await.unwrap());
        }
        requested_tx.send(()).unwrap();
        std::future::pending::<()>().await;
        drop(socket);
    });
    let request = async {
        requested_rx.await.unwrap();
        tokio::time::pause();
        tokio::time::advance(Duration::from_secs(3)).await;
    };
    let sources = [source];
    let (result, ()) = tokio::join!(harness.start(&fileid, &sources), request);
    assert!(matches!(
        result,
        Err(HathError::ProxyDownloader { status: 504, .. })
    ));
    tokio::time::resume();
    server.abort();
    assert!(server.await.unwrap_err().is_cancelled());
}
