use super::body::StreamingBody;
use super::response::{self, ResponseSpec};
use crate::config::Config;
use crate::error::Result;
use crate::stats::Stats;
use hyper::StatusCode;
use std::sync::Arc;

pub(crate) struct ProxyTransfer {
    fileid: String,
    content_length: usize,
    content_type: String,
    download: crate::proxy_downloader::ProxyDownload,
}

impl ProxyTransfer {
    pub(crate) async fn start(
        fileid: &str,
        sources: &[reqwest::Url],
        state: &super::AppState,
        config: &Config,
    ) -> Result<Self> {
        let download = state
            .proxy_downloader
            .start(
                fileid,
                sources,
                config,
                Some(state.cache.clone()),
                state.stats.clone(),
                &state.transfers,
            )
            .await?;
        let content_length = download.content_length();
        let content_type = download.content_type().to_string();
        Ok(Self {
            fileid: fileid.to_string(),
            content_length,
            content_type,
            download,
        })
    }

    pub(crate) fn into_response(
        self,
        head_only: bool,
        file_stats: Arc<Stats>,
    ) -> Result<ResponseSpec> {
        let Self {
            fileid,
            content_length,
            content_type,
            download,
        } = self;

        if head_only {
            return response::head_response(&content_type, content_length);
        }

        let file = match download.open_temp_file() {
            Ok(file) => file,
            Err(e) => {
                tracing::warn!("Proxy: cannot open temp file for {}: {}", fileid, e);
                download.abandon();
                return response::text_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "proxy temp file unavailable",
                );
            }
        };

        let body = StreamingBody::new_proxy(
            content_length,
            file,
            download.into_body_source(),
            Some(file_stats),
        );
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
