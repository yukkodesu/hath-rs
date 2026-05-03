use crate::config::{CliArgs, Config};
use clap::Parser;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub(crate) struct FixtureDirs {
    _dir: TempDir,
    pub(crate) cache_dir: PathBuf,
    pub(crate) temp_dir: PathBuf,
    pub(crate) data_dir: PathBuf,
    pub(crate) log_dir: PathBuf,
    pub(crate) download_dir: PathBuf,
}

impl FixtureDirs {
    pub(crate) fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        Self {
            cache_dir: root.join("cache"),
            temp_dir: root.join("tmp"),
            data_dir: root.join("data"),
            log_dir: root.join("log"),
            download_dir: root.join("download"),
            _dir: dir,
        }
    }

    pub(crate) fn config(&self) -> Config {
        let args = CliArgs::try_parse_from([
            "hath-rs",
            "--client-id",
            "12345",
            "--client-key",
            "abcde12345abcde12345",
            "--cache-dir",
            self.cache_dir.to_str().unwrap(),
            "--temp-dir",
            self.temp_dir.to_str().unwrap(),
            "--data-dir",
            self.data_dir.to_str().unwrap(),
            "--log-dir",
            self.log_dir.to_str().unwrap(),
            "--download-dir",
            self.download_dir.to_str().unwrap(),
        ])
        .unwrap();
        let config = Config::load(args).unwrap();
        config.initialize_directories().unwrap();
        config
    }
}

#[derive(Clone)]
pub(crate) struct FakeResponse {
    status: u16,
    reason: &'static str,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    close_after: Option<usize>,
    delay_before_body: Duration,
}

impl FakeResponse {
    pub(crate) fn ok(body: impl Into<Vec<u8>>) -> Self {
        let body = body.into();
        Self {
            status: 200,
            reason: "OK",
            headers: vec![("Content-Length".to_string(), body.len().to_string())],
            body,
            close_after: None,
            delay_before_body: Duration::ZERO,
        }
    }

    pub(crate) fn status(status: u16, reason: &'static str, body: impl Into<Vec<u8>>) -> Self {
        let body = body.into();
        Self {
            status,
            reason,
            headers: vec![("Content-Length".to_string(), body.len().to_string())],
            body,
            close_after: None,
            delay_before_body: Duration::ZERO,
        }
    }

    pub(crate) fn close_after(mut self, bytes: usize) -> Self {
        self.close_after = Some(bytes);
        self
    }

    pub(crate) fn delay_before_body(mut self, delay: Duration) -> Self {
        self.delay_before_body = delay;
        self
    }
}

pub(crate) struct FakeHttpServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<String>>>,
    handle: JoinHandle<()>,
}

impl FakeHttpServer {
    pub(crate) async fn start(responses: Vec<FakeResponse>) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let reqs = requests.clone();
        let handle = tokio::spawn(async move {
            for response in responses {
                let Ok((mut stream, _)) = listener.accept().await else {
                    break;
                };
                let request = read_http_request(&mut stream).await;
                reqs.lock().unwrap().push(request);
                write_http_response(&mut stream, response).await;
            }
        });
        Self {
            addr,
            requests,
            handle,
        }
    }

    pub(crate) fn port(&self) -> u16 {
        self.addr.port()
    }

    pub(crate) fn url(&self, path: &str) -> reqwest::Url {
        reqwest::Url::parse(&format!("http://{}{}", self.addr, path)).unwrap()
    }

    pub(crate) fn requests(&self) -> Vec<String> {
        self.requests.lock().unwrap().clone()
    }
}

impl Drop for FakeHttpServer {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

pub(crate) async fn wait_for_file_contents(path: &Path, expected: &[u8]) {
    for _ in 0..100 {
        if let Ok(contents) = tokio::fs::read(path).await
            && contents == expected
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!(
        "timed out waiting for {} to contain {} bytes",
        path.display(),
        expected.len()
    );
}

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
    let mut buf = Vec::new();
    let mut scratch = [0u8; 1024];
    loop {
        let Ok(n) = stream.read(&mut scratch).await else {
            break;
        };
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&scratch[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

async fn write_http_response(stream: &mut tokio::net::TcpStream, response: FakeResponse) {
    let mut head = format!("HTTP/1.1 {} {}\r\n", response.status, response.reason);
    for (name, value) in response.headers {
        head.push_str(&name);
        head.push_str(": ");
        head.push_str(&value);
        head.push_str("\r\n");
    }
    head.push_str("Connection: close\r\n\r\n");
    stream.write_all(head.as_bytes()).await.unwrap();
    if !response.delay_before_body.is_zero() {
        tokio::time::sleep(response.delay_before_body).await;
    }
    let write_len = response.close_after.unwrap_or(response.body.len());
    stream
        .write_all(&response.body[..write_len.min(response.body.len())])
        .await
        .unwrap();
    let _ = stream.shutdown().await;
}
