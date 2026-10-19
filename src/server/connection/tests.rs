use super::*;
use crate::cache::CacheHandler;
use crate::config::Config;
use crate::gallery_downloader::GalleryDownloadSupervisor;
use crate::rpc_client::RpcClient;
use crate::server::SessionManager;
use crate::stats::Stats;
use crate::test_support::{FakeHttpServer, FakeResponse, FixtureDirs};
use arc_swap::{ArcSwap, ArcSwapOption};
use dashmap::DashMap;
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU32, Ordering},
};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify};
use tokio::task::JoinHandle;
use tokio::time::{advance, pause, resume, timeout};
use tokio_rustls::{TlsAcceptor, TlsConnector};

struct Fixture {
    _dirs: FixtureDirs,
    state: AppState,
    shutdown: CancellationToken,
    connector: TlsConnector,
}

impl Fixture {
    fn new() -> Self {
        let dirs = FixtureDirs::new();
        let mut cfg = dirs.config();
        cfg.override_conns = 1;
        let config = Arc::new(ArcSwap::from_pointee(cfg));
        let stats = Arc::new(Stats::new());
        let shutdown = CancellationToken::new();
        let cache =
            Arc::new(CacheHandler::new(config.clone(), stats.clone(), shutdown.clone()).unwrap());
        let rpc_client = Arc::new(RpcClient::new(config.clone()).unwrap());
        let cert = CertificateDer::from(
            include_bytes!("../../../tests/fixtures/tls/localhost.cert.der").to_vec(),
        );
        let key = PrivatePkcs8KeyDer::from(
            include_bytes!("../../../tests/fixtures/tls/localhost.key.der").to_vec(),
        );
        let server_tls = rustls::ServerConfig::builder_with_provider(crate::tls::aws_lc_provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert.clone()], key.into())
            .unwrap();
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert).unwrap();
        let client_tls = rustls::ClientConfig::builder_with_provider(crate::tls::aws_lc_provider())
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let connector = TlsConnector::from(Arc::new(client_tls));
        let acceptor = Arc::new(TlsAcceptor::from(Arc::new(server_tls)));
        let state = AppState {
            config: config.clone(),
            stats: stats.clone(),
            cache,
            gallery_downloader: GalleryDownloadSupervisor::new(
                config.clone(),
                rpc_client.clone(),
                stats.clone(),
                shutdown.clone(),
                crate::tasks::TransferTasks::new(crate::shutdown::Shutdown::default()),
            ),
            rpc_client,
            allow_normal_connections: Arc::new(AtomicBool::new(true)),
            flood_control: Arc::new(DashMap::new()),
            tls_acceptor: Arc::new(ArcSwapOption::from(Some(acceptor))),
            cert_expiry: Arc::new(Mutex::new(None)),
            bandwidth_monitor: Arc::new(ArcSwapOption::const_empty()),
            session_manager: Arc::new(SessionManager::new(stats)),
            next_conn_id: Arc::new(AtomicU32::new(0)),
            last_overload_notification: Arc::new(Mutex::new(Some(std::time::Instant::now()))),
            do_cert_refresh: Arc::new(AtomicBool::new(false)),
            cert_refresh_notify: Arc::new(Notify::new()),
            generation: Arc::new(Mutex::new(None)),
            shutdown: crate::shutdown::Shutdown::default(),
            transfers: crate::tasks::TransferTasks::new(crate::shutdown::Shutdown::default()),
            proxy_client: crate::proxy_downloader::build_proxy_client(&config.load()).unwrap(),
        };
        Self {
            _dirs: dirs,
            state,
            shutdown,
            connector,
        }
    }

    fn configure(&self, change: impl FnOnce(&mut Config)) {
        let mut config = (**self.state.config.load()).clone();
        change(&mut config);
        self.state.config.store(Arc::new(config));
    }

    async fn accept(&self) -> (TcpStream, Option<Connection>) {
        self.accept_from("203.0.113.1:12345").await
    }

    async fn accept_from(&self, remote: &str) -> (TcpStream, Option<Connection>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let (peer, accepted) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let connection = Connection::accept(
            &self.state,
            accepted.unwrap().0,
            remote.parse().unwrap(),
            &self.shutdown,
        );
        (peer.unwrap(), connection)
    }

    async fn connect_tls(
        &self,
        peer: TcpStream,
        connection: Connection,
    ) -> (tokio_rustls::client::TlsStream<TcpStream>, JoinHandle<()>) {
        let task = tokio::spawn(connection.serve());
        let peer = timeout(
            Duration::from_secs(2),
            self.connector
                .connect("localhost".try_into().unwrap(), peer),
        )
        .await
        .expect("valid TLS handshake must complete")
        .unwrap();
        (peer, task)
    }

    fn assert_active(&self, expected: u32) {
        assert_eq!(self.active(), expected);
        assert_eq!(
            self.state.stats.open_connections.load(Ordering::Relaxed),
            expected
        );
    }

    fn active(&self) -> u32 {
        self.state.session_manager.active_count()
    }
}

#[tokio::test]
async fn pending_tls_counts_toward_connection_limit_before_task_starts() {
    let fixture = Fixture::new();
    let (_peer, first) = fixture.accept().await;
    assert!(first.is_some());
    assert_eq!(
        fixture.active(),
        1,
        "TLS peers must be registered before handshaking"
    );
    assert_eq!(
        fixture.state.stats.open_connections.load(Ordering::Relaxed),
        1
    );
    let (_peer, second) = fixture.accept().await;
    assert!(second.is_some(), "Java admits when previous count == max");
    assert_eq!(fixture.active(), 2);
    let (_peer, rejected) = fixture.accept().await;
    assert!(
        rejected.is_none(),
        "pending TLS must enforce the connection limit"
    );
    drop((first, second));
    assert_eq!(fixture.active(), 0);
    assert_eq!(
        fixture.state.stats.open_connections.load(Ordering::Relaxed),
        0
    );
}

async fn assert_closed(mut peer: impl AsyncRead + Unpin) {
    // A failed TLS handshake may send an alert before closing the socket.
    let result = timeout(Duration::from_secs(1), peer.read_to_end(&mut Vec::new()))
        .await
        .expect("peer socket must be released");
    assert!(
        result.is_ok()
            || result.is_err_and(|error| matches!(
                error.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::UnexpectedEof
            ))
    );
}

#[tokio::test]
async fn silent_tls_peer_hits_read_timeout_and_releases_registration() {
    let fixture = Fixture::new();
    let (peer, connection) = fixture.accept().await;
    pause();
    let task = tokio::spawn(connection.unwrap().serve());
    tokio::task::yield_now().await;
    advance(Duration::from_secs(9)).await;
    assert!(!task.is_finished());
    advance(Duration::from_secs(2)).await;
    timeout(Duration::from_secs(1), task)
        .await
        .expect("silent TLS peer must time out after 10s")
        .unwrap();
    fixture.assert_active(0);
    resume();
    assert_closed(peer).await;
}

#[tokio::test]
async fn partial_http_header_hits_the_same_read_timeout() {
    let fixture = Fixture::new();
    let (peer, connection) = fixture.accept().await;
    let (mut peer, task) = fixture.connect_tls(peer, connection.unwrap()).await;
    peer.write_all(b"GET /robots.txt HTTP/1.1\r\nHost:")
        .await
        .unwrap();
    // Keep real time for this wire-level check; paused time can outrun TCP readiness delivery.
    timeout(Duration::from_secs(12), task)
        .await
        .expect("incomplete HTTP headers must hit the 10s read timeout")
        .unwrap();
    fixture.assert_active(0);
    assert_closed(peer).await;
}

#[tokio::test]
async fn tls_completion_does_not_restart_pending_session_age() {
    let fixture = Fixture::new();
    let (peer, connection) = fixture.accept().await;
    pause();
    advance(Duration::from_secs(24)).await;
    resume();
    let (peer, task) = fixture.connect_tls(peer, connection.unwrap()).await;
    pause();
    advance(Duration::from_secs(7)).await;
    assert_eq!(
        fixture.state.session_manager.nuke_old_connections(),
        1,
        "30s pending age must include time before TLS completed"
    );
    timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap();
    fixture.assert_active(0);
    resume();
    assert_closed(peer).await;
}

#[tokio::test]
async fn pending_tls_is_subject_to_flood_control() {
    let fixture = Fixture::new();
    fixture.configure(|cfg| cfg.override_conns = 100);
    let mut pending = Vec::new();
    let mut peers = Vec::new();
    for _ in 0..10 {
        let (peer, connection) = fixture.accept().await;
        peers.push(peer);
        pending.push(connection.expect("first ten peers should be admitted"));
    }
    let (peer, connection) = fixture.accept().await;
    assert!(
        connection.is_none(),
        "eleventh peer must be blocked even without TLS"
    );
    fixture.assert_active(10);
    assert_closed(peer).await;
    drop(pending);
    fixture.assert_active(0);
}

#[tokio::test]
async fn rpc_and_local_peers_are_exempt_but_still_counted() {
    let fixture = Fixture::new();
    let (_peer, first) = fixture.accept().await;
    let (_peer, second) = fixture.accept().await;
    fixture
        .state
        .allow_normal_connections
        .store(false, Ordering::Relaxed);
    fixture.configure(|cfg| cfg.rpc_servers.push("203.0.113.2".parse().unwrap()));
    let (peer, blocked) = fixture.accept_from("127.0.0.1:12345").await;
    assert!(
        blocked.is_none(),
        "startup permits RPC peers, not all local peers"
    );
    assert_closed(peer).await;
    let (_peer, rpc) = fixture.accept_from("203.0.113.2:12345").await;
    assert!(rpc.is_some());
    fixture
        .state
        .allow_normal_connections
        .store(true, Ordering::Relaxed);
    let (_peer, local) = fixture.accept_from("127.0.0.1:12345").await;
    assert!(local.is_some());
    fixture.assert_active(4);
    assert_eq!(
        fixture.state.flood_control.len(),
        1,
        "exempt peers bypass flood control"
    );
    drop((first, second, rpc, local));
    fixture.assert_active(0);
}

#[tokio::test]
async fn failed_and_cancelled_handshakes_release_sockets_and_counts() {
    for mode in ["invalid TLS", "generation stop", "session reaper"] {
        let fixture = Fixture::new();
        let (mut peer, connection) = fixture.accept().await;
        let task = tokio::spawn(connection.unwrap().serve());
        match mode {
            "invalid TLS" => peer.write_all(b"not TLS\r\n").await.unwrap(),
            "generation stop" => fixture.shutdown.cancel(),
            _ => {
                pause();
                advance(Duration::from_secs(31)).await;
                assert_eq!(fixture.state.session_manager.nuke_old_connections(), 1);
            }
        }
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap();
        fixture.assert_active(0);
        if mode == "session reaper" {
            resume();
        }
        assert_closed(peer).await;
    }
}

#[tokio::test]
async fn stalled_overload_notification_cannot_hold_a_pending_socket() {
    let fixture = Fixture::new();
    let rpc = FakeHttpServer::start(vec![
        FakeResponse::ok(b"OK\n").delay_before_body(Duration::from_secs(3600)),
    ])
    .await;
    fixture.configure(|cfg| {
        cfg.rpc_servers = vec!["127.0.0.1".parse().unwrap()];
        cfg.rpc_port = rpc.port();
    });
    *fixture.state.last_overload_notification.lock().await = None;
    let (_peer, first) = fixture.accept().await;
    let (peer, second) = fixture.accept().await;
    let task = tokio::spawn(second.unwrap().serve());
    timeout(Duration::from_secs(2), async {
        while rpc.requests().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("near-limit session must notify the local RPC fixture");
    pause();
    advance(Duration::from_secs(31)).await;
    timeout(Duration::from_secs(1), task)
        .await
        .expect("handshake deadline must cover overload RPC waits")
        .unwrap();
    drop(first);
    fixture.assert_active(0);
    resume();
    assert_closed(peer).await;
}

#[tokio::test]
async fn long_response_survives_read_timeout_and_generation_stop() {
    let fixture = Fixture::new();
    let (peer, connection) = fixture.accept().await;
    socket2::SockRef::from(&peer)
        .set_recv_buffer_size(4096)
        .unwrap();
    let (mut peer, task) = fixture.connect_tls(peer, connection.unwrap()).await;
    let size = 8 * 1024 * 1024;
    let config = fixture.state.config.load();
    let time = config.server_time();
    let key = crate::utils::sha1_string(&format!(
        "hentai@home-speedtest-{size}-{time}-{}-{}",
        config.client_id.0,
        config.client_key.as_str()
    ));
    drop(config);
    peer.write_all(
        format!("GET /t/{size}/{time}/{key} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut headers = Vec::new();
    timeout(Duration::from_secs(2), async {
        while !headers.ends_with(b"\r\n\r\n") {
            headers.push(peer.read_u8().await.unwrap());
        }
    })
    .await
    .unwrap();
    assert!(headers.starts_with(b"HTTP/1.1 200"));
    pause();
    advance(Duration::from_secs(11)).await;
    fixture.shutdown.cancel();
    tokio::task::yield_now().await;
    assert!(
        !task.is_finished(),
        "read timeout and generation stop must not interrupt a valid response"
    );
    assert_eq!(
        fixture.state.session_manager.nuke_old_connections(),
        0,
        "a parsed request uses the normal session lifetime"
    );
    resume();
    socket2::SockRef::from(peer.get_ref().0)
        .set_recv_buffer_size(1024 * 1024)
        .unwrap();
    let mut bytes = vec![0; size];
    timeout(Duration::from_secs(10), peer.read_exact(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    timeout(Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap();
    fixture.assert_active(0);
}

#[tokio::test]
async fn process_shutdown_drains_streaming_proxy_before_cache_snapshot() {
    use crate::server::proxy_transfer::ProxyTransfer;
    use crate::server::response::ResponsePolicy;
    use http_body_util::BodyExt;

    let fixture = Fixture::new();
    let contents = vec![42; 4096];
    let fileid = format!(
        "{}-{}-jpg",
        crate::utils::sha1_bytes(&contents),
        contents.len()
    );
    let hv = crate::hvfile::HVFile::from_fileid(&fileid).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url =
        reqwest::Url::parse(&format!("http://{}/image", listener.local_addr().unwrap())).unwrap();
    let (release, released) = tokio::sync::oneshot::channel();
    let upstream = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        while !request.ends_with(b"\r\n\r\n") {
            request.push(socket.read_u8().await.unwrap());
        }
        socket
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4096\r\nConnection: close\r\n\r\n")
            .await
            .unwrap();
        socket.write_all(&vec![42; 2048]).await.unwrap();
        released.await.unwrap();
        socket.write_all(&vec![42; 2048]).await.unwrap();
    });
    let config = fixture.state.config.load_full();
    let transfer = ProxyTransfer::start(&fileid, &[url], &fixture.state, &config)
        .await
        .unwrap();
    let response = transfer.into_response(false, fixture.state.stats.clone());
    let mut body = ResponsePolicy::new(true, None, fixture.state.stats.clone())
        .finalize(response)
        .await
        .into_body();
    let first = timeout(Duration::from_secs(2), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(
        !upstream.is_finished(),
        "first frame must be served before upstream completion"
    );
    let mut received = first.to_vec();
    fixture
        .state
        .shutdown
        .request(crate::shutdown::ShutdownReason::Signal("SIGTERM"));
    let tasks = fixture.state.transfers.clone();
    let drain = tokio::spawn(async move {
        tasks
            .finish(tokio::time::Instant::now() + Duration::from_secs(5))
            .await
    });
    tokio::task::yield_now().await;
    assert!(
        !drain.is_finished(),
        "shutdown must wait for proxy finalization"
    );
    release.send(()).unwrap();
    while let Some(frame) = body.frame().await {
        received.extend_from_slice(&frame.unwrap().into_data().unwrap());
    }
    assert_eq!(received, contents);
    assert!(drain.await.unwrap());
    upstream.await.unwrap();
    assert_eq!(
        std::fs::read(hv.cache_path(&config.cache_dir)).unwrap(),
        contents
    );
    assert!(fixture.state.cache.save_persistent_data());
    let stats = Arc::new(Stats::new());
    let _restarted = CacheHandler::new(
        fixture.state.config.clone(),
        stats.clone(),
        CancellationToken::new(),
    )
    .unwrap();
    assert_eq!(stats.cache_count.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn shutdown_during_certificate_suspend_cannot_start_another_generation() {
    let fixture = Fixture::new();
    let rpc = FakeHttpServer::start(vec![
        FakeResponse::ok("OK\n").delay_before_body(Duration::from_secs(3600)),
    ])
    .await;
    fixture.configure(|cfg| {
        cfg.rpc_servers = vec!["127.0.0.1".parse().unwrap()];
        cfg.rpc_port = rpc.port();
    });
    let watcher = crate::server::spawn_cert_refresh_watcher(
        fixture.state.clone(),
        fixture.state.rpc_client.clone(),
        fixture.state.shutdown.clone(),
    );
    fixture.state.do_cert_refresh.store(true, Ordering::Release);
    fixture.state.cert_refresh_notify.notify_one();
    timeout(Duration::from_secs(2), async {
        while rpc.requests().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    fixture
        .state
        .shutdown
        .request(crate::shutdown::ShutdownReason::Signal("SIGTERM"));
    timeout(Duration::from_secs(1), watcher)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        crate::server::start_server(fixture.state.clone()).await,
        Err(crate::error::HathError::Shutdown)
    ));
    assert!(fixture.state.generation.lock().await.is_none());
    assert_eq!(rpc.requests().len(), 1);
}

#[tokio::test]
async fn generation_stop_joins_pending_tls_before_returning() {
    let fixture = Fixture::new();
    let (peer, connection) = fixture.accept().await;
    let task = tokio::spawn(connection.unwrap().serve());
    *fixture.state.generation.lock().await = Some(crate::server::ServerGeneration {
        stop: fixture.shutdown.clone(),
        task,
    });
    assert!(
        crate::server::stop_server(
            &fixture.state,
            tokio::time::Instant::now() + Duration::from_secs(1)
        )
        .await
    );
    fixture.assert_active(0);
    assert_closed(peer).await;
    assert!(fixture.state.generation.lock().await.is_none());
}
