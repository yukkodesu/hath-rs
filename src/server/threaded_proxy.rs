use crate::error::{HathError, Result};
use reqwest::Url;
use std::net::IpAddr;
use std::time::Duration;

/// Run the threaded proxy test: spawn concurrent outbound GET requests
/// to {protocol}://{hostname}:{port}/t/{testsize}/{testtime}/{testkey}/{random_int}
/// and return (successful_tests, total_time_millis).
/// Java: HTTPResponse.processThreadedProxyTest()
pub(crate) async fn run_threaded_proxy_test(
    hostname: &str,
    protocol: &str,
    port: u16,
    testsize: u64,
    testcount: u32,
    testtime: u32,
    testkey: &str,
) -> (u32, u64) {
    use rand::RngExt;

    // Java: FileDownloader(source, 10000, 60000, true)
    // connectTimeout=5s, readTimeout=10s,
    // maxDLTime=60s is stored but not enforced, 3 retries.
    let client = crate::tls::configure_reqwest(
        reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .read_timeout(Duration::from_secs(10)),
    )
    .and_then(|builder| {
        builder
            .build()
            .map_err(|error| HathError::Network(error.to_string()))
    });

    let Ok(client) = client else {
        return (0, 0);
    };

    let mut handles = Vec::with_capacity(testcount as usize);

    for _ in 0..testcount {
        let random_int: u32 = rand::rng().random();
        let Ok(url) = build_threaded_proxy_test_url(
            protocol, hostname, port, testsize, testtime, testkey, random_int,
        ) else {
            continue;
        };
        let client = client.clone();

        handles.push(tokio::spawn(async move {
            run_threaded_proxy_test_download(&client, url, testsize).await
        }));
    }

    let mut successful = 0u32;
    let mut total_time_ms = 0u64;

    for handle in handles {
        if let Ok(Some(ms)) = handle.await {
            successful += 1;
            total_time_ms += ms;
        }
    }

    (successful, total_time_ms)
}

enum ThreadedProxyTestAttemptError {
    NotFound,
    Retryable,
}

async fn run_threaded_proxy_test_download(
    client: &reqwest::Client,
    url: Url,
    testsize: u64,
) -> Option<u64> {
    use std::time::Instant;

    // Java FileDownloader keeps timeFirstByte across retries; a failed first
    // attempt that received bytes still contributes to getDownloadTimeMillis().
    let mut first_byte_at: Option<Instant> = None;

    for retries_left in (0..3).rev() {
        let result = async {
            let mut resp = client
                .get(url.clone())
                .header("Connection", "Close")
                .header(
                    hyper::header::USER_AGENT,
                    format!("Hentai@Home {}", crate::rpc::CLIENT_VERSION),
                )
                .send()
                .await
                .map_err(|_| ThreadedProxyTestAttemptError::Retryable)?;

            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                return Err(ThreadedProxyTestAttemptError::NotFound);
            }
            if !resp.status().is_success() {
                return Err(ThreadedProxyTestAttemptError::Retryable);
            }

            let content_length = resp
                .content_length()
                .ok_or(ThreadedProxyTestAttemptError::Retryable)?;
            let mut received = 0u64;

            while let Some(chunk) = resp
                .chunk()
                .await
                .map_err(|_| ThreadedProxyTestAttemptError::Retryable)?
            {
                if !chunk.is_empty() {
                    first_byte_at.get_or_insert_with(Instant::now);
                    received += chunk.len() as u64;
                }
            }

            if received != content_length {
                return Err(ThreadedProxyTestAttemptError::Retryable);
            }

            Ok((content_length >= testsize).then(|| {
                first_byte_at
                    .map(|start| start.elapsed().as_millis() as u64)
                    .unwrap_or(0)
            }))
        }
        .await;

        match result {
            Ok(download_time_ms) => return download_time_ms,
            Err(ThreadedProxyTestAttemptError::NotFound) => return None,
            Err(ThreadedProxyTestAttemptError::Retryable) => {
                tracing::warn!(
                    "Threaded proxy test failed (retrying, {} left)",
                    retries_left
                );
            }
        }
    }

    None
}

pub(crate) fn build_threaded_proxy_test_url(
    protocol: &str,
    hostname: &str,
    port: u16,
    testsize: u64,
    testtime: u32,
    testkey: &str,
    random_int: u32,
) -> Result<Url> {
    let mut url = Url::parse("http://hath.invalid/")
        .map_err(|e| HathError::Network(format!("invalid speedtest URL base: {}", e)))?;
    url.set_scheme(protocol)
        .map_err(|_| HathError::Network(format!("invalid speedtest protocol: {}", protocol)))?;
    if let Ok(ip) = hostname.parse::<IpAddr>() {
        url.set_ip_host(ip)
            .map_err(|_| HathError::Network("invalid speedtest host".into()))?;
    } else {
        url.set_host(Some(hostname))
            .map_err(|e| HathError::Network(format!("invalid speedtest host: {}", e)))?;
    }
    url.set_port(Some(port))
        .map_err(|_| HathError::Network(format!("invalid speedtest port: {}", port)))?;
    url.set_path(&format!(
        "/t/{}/{}/{}/{}",
        testsize, testtime, testkey, random_int
    ));
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{FakeHttpServer, FakeResponse};
    use std::time::Duration;

    #[test]
    fn test_threaded_proxy_test_url_handles_ipv6_host() {
        let url = build_threaded_proxy_test_url(
            "http",
            "::ffff:192.0.2.1",
            8443,
            1024,
            30,
            "testkey",
            12345,
        )
        .unwrap();

        assert_eq!(
            url.as_str(),
            "http://[::ffff:c000:201]:8443/t/1024/30/testkey/12345"
        );
    }

    #[tokio::test]
    async fn threaded_proxy_test_times_from_first_byte_across_retries() {
        let testsize = 8u64;
        let server = FakeHttpServer::start(vec![
            FakeResponse::ok(vec![b'a'; testsize as usize]).close_after(1),
            FakeResponse::ok(vec![b'b'; testsize as usize])
                .delay_before_body(Duration::from_millis(60)),
        ])
        .await;
        let client = crate::tls::configure_reqwest(
            reqwest::Client::builder()
                .connect_timeout(Duration::from_secs(5))
                .read_timeout(Duration::from_secs(10)),
        )
        .unwrap()
        .build()
        .unwrap();

        let elapsed_ms =
            run_threaded_proxy_test_download(&client, server.url("/t/8/30/key/1"), testsize)
                .await
                .unwrap();

        assert!(
            elapsed_ms >= 40,
            "elapsed_ms={} should include retry gap from first byte",
            elapsed_ms
        );
    }
}
