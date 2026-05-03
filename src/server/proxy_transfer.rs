use super::body::StreamingBody;
use super::response;
use crate::bandwidth::BandwidthMonitor;
use crate::cache::CacheHandler;
use crate::config::Config;
use crate::error::Result;
use crate::proxy_downloader::ProxyFileDownloader;
use crate::stats::Stats;
use hyper::{Response, StatusCode};
use std::sync::Arc;

pub(crate) struct ProxyTransfer {
    fileid: String,
    content_length: usize,
    content_type: String,
    parts: crate::proxy_downloader::ProxyDownloadParts,
}

impl ProxyTransfer {
    pub(crate) async fn start(
        fileid: &str,
        sources: &[reqwest::Url],
        config: &Config,
        cache_handler: Option<Arc<CacheHandler>>,
        stats: Arc<Stats>,
        client: &Arc<reqwest::Client>,
    ) -> Result<Self> {
        let downloader =
            ProxyFileDownloader::new(fileid, sources, config, cache_handler, stats, client).await?;
        let parts = downloader.into_parts();
        let content_length = parts.content_length();
        let content_type = parts.content_type().to_string();
        Ok(Self {
            fileid: fileid.to_string(),
            content_length,
            content_type,
            parts,
        })
    }

    pub(crate) fn into_response(
        self,
        head_only: bool,
        bwm: Option<Arc<BandwidthMonitor>>,
        stats: Option<Arc<Stats>>,
    ) -> Result<Response<StreamingBody>> {
        let Self {
            fileid,
            content_length,
            content_type,
            parts,
        } = self;

        if head_only {
            return response::head_response(&content_type, content_length);
        }

        let file = match parts.open_temp_file() {
            Ok(file) => file,
            Err(e) => {
                tracing::warn!("Proxy: cannot open temp file for {}: {}", fileid, e);
                parts.abandon();
                return response::text_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "proxy temp file unavailable",
                );
            }
        };

        let body =
            StreamingBody::new_proxy(content_length, file, parts.into_body_source(), bwm, stats);
        let response = response::proxy_body_response(&content_type, content_length, body);

        if response.is_ok() {
            tracing::info!(
                "Proxy download: returning body for {} ({} bytes, {})",
                fileid,
                content_length,
                content_type
            );
        }

        response
    }
}
