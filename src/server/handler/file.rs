use super::super::body::StreamingBody;
use super::super::proxy_transfer::ProxyTransfer;
use super::super::response;
use super::RequestContext;
use crate::error::Result;
use crate::hvfile::HVFile;
use crate::utils::Additional;
use hyper::{Response, StatusCode};
use reqwest::Url;

pub(super) async fn handle_file_serve(
    fileid: String,
    hv_file: Option<HVFile>,
    additional: Box<Additional>,
    keystamp_valid: bool,
    head_only: bool,
    ctx: RequestContext,
) -> Result<Response<StreamingBody>> {
    // Java validates fileindex/xres BEFORE cache hit check
    // (line 194 in HTTPResponse.processRequest). Even a cached
    // file with missing/invalid arguments returns 404.
    let fileindex_valid = additional
        .fileindex
        .as_deref()
        .is_some_and(|v| v.parse::<u32>().is_ok());
    let xres_valid = additional
        .xres
        .as_deref()
        .is_some_and(|v| v == "org" || v.parse::<u32>().is_ok());

    if !keystamp_valid {
        return response::forbidden_response();
    }
    if hv_file.is_none() || !fileindex_valid || !xres_valid {
        return response::not_found_response();
    }

    let hv = hv_file.as_ref().unwrap();
    let fileindex = additional.fileindex.as_deref().unwrap();
    let xres = additional.xres.as_deref().unwrap();
    let cache_path = hv.cache_path(&ctx.config.cache_dir);
    let cache_hit = cache_path.exists()
        && cache_path
            .metadata()
            .map(|m| m.len() == hv.size as u64)
            .unwrap_or(false);

    if cache_hit {
        // Java: if markRecentlyAccessed returns true (LRU bit was
        // not set) and verification is not disabled/on cooldown,
        // verify SHA1 inline and delete corrupt file in cleanup().
        let recently_accessed = ctx.state.cache.mark_recently_accessed(hv, false);
        let verify = recently_accessed
            && !ctx.config.disable_file_verification
            && !ctx.state.cache.is_file_verification_on_cooldown();
        let stats = if ctx.client.is_normal_hath_connection() {
            Some(ctx.state.stats.clone())
        } else {
            None
        };
        return response::file_response(
            hv,
            &ctx.config.cache_dir,
            head_only,
            ctx.state.stats.clone(),
            ctx.client.bwm,
            verify,
            Some(ctx.state.cache.clone()),
            stats,
        );
    }

    match ctx
        .state
        .rpc_client
        .static_range_fetch(fileindex, xres, &fileid)
        .await
    {
        Ok(sr) if sr.status == crate::rpc::ResponseStatus::Ok => {
            let sources: Vec<Url> = sr
                .lines
                .iter()
                .filter(|s| !s.is_empty())
                .filter_map(|s| Url::parse(s).ok())
                .collect();
            if sources.is_empty() {
                response::not_found_response()
            } else {
                // Java creates HTTPResponseProcessorProxy and calls
                // initialize() for both GET and HEAD. The init result
                // (connecting to source, checking Content-Length/size)
                // determines the status code. HEAD then skips body.
                match ProxyTransfer::start(
                    &fileid,
                    &sources,
                    &ctx.config,
                    Some(ctx.state.cache.clone()),
                    ctx.state.stats.clone(),
                    &ctx.state.proxy_client,
                )
                .await
                {
                    Ok(proxy) => {
                        // Stats for proxy are recorded in body's finish_with()
                        // after actual transmission - Java: proxyThreadCompleted().
                        let proxy_stats = if ctx.client.is_normal_hath_connection() {
                            Some(ctx.state.stats.clone())
                        } else {
                            None
                        };
                        proxy.into_response(head_only, ctx.client.bwm, proxy_stats)
                    }
                    Err(e) => {
                        tracing::warn!("Proxy download failed for {}: {}", fileid, e);
                        if let crate::error::HathError::ProxyDownloader { status, message } = e {
                            response::text_response(
                                StatusCode::from_u16(status)
                                    .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                                &message,
                            )
                        } else {
                            // Java: connection failures -> 500
                            response::text_response(
                                StatusCode::INTERNAL_SERVER_ERROR,
                                &e.to_string(),
                            )
                        }
                    }
                }
            }
        }
        _ => response::not_found_response(),
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::cache::CacheHandler;
    use crate::rpc_client::RpcClient;
    use crate::server::AppState;
    use crate::server::handler::RequestClientContext;
    use crate::stats::Stats;
    use crate::test_support::FixtureDirs;
    use arc_swap::{ArcSwap, ArcSwapOption};
    use dashmap::DashMap;
    use std::os::unix::fs::PermissionsExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
    use tokio::sync::{Mutex, Notify};

    fn test_state(config: crate::config::Config) -> AppState {
        let config = Arc::new(ArcSwap::from_pointee(config));
        let stats = Arc::new(Stats::new());
        let cache = Arc::new(
            CacheHandler::new(
                config.clone(),
                stats.clone(),
                tokio_util::sync::CancellationToken::new(),
            )
            .unwrap(),
        );
        let rpc_client = Arc::new(RpcClient::new(config.clone()).unwrap());
        let proxy_client = crate::proxy_downloader::build_proxy_client(&config.load()).unwrap();

        AppState {
            config,
            stats: stats.clone(),
            cache,
            rpc_client,
            allow_normal_connections: Arc::new(AtomicBool::new(true)),
            flood_control: Arc::new(DashMap::new()),
            tls_acceptor: Arc::new(ArcSwapOption::const_empty()),
            cert_expiry: Arc::new(Mutex::new(None)),
            bandwidth_monitor: Arc::new(ArcSwapOption::const_empty()),
            session_manager: Arc::new(crate::server::SessionManager::new(stats)),
            next_conn_id: Arc::new(AtomicU32::new(0)),
            last_overload_notification: Arc::new(Mutex::new(None)),
            do_cert_refresh: Arc::new(AtomicBool::new(false)),
            cert_refresh_notify: Arc::new(Notify::new()),
            server_shutdown_token: Arc::new(ArcSwapOption::const_empty()),
            server_terminated: Arc::new(AtomicBool::new(false)),
            proxy_client,
        }
    }

    #[tokio::test]
    async fn cached_head_opens_file_before_returning_ok() {
        let fixture = FixtureDirs::new();
        let config = fixture.config();
        let state = test_state(config);
        let config = state.config.load_full();
        let hv = HVFile::from_fileid("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-4-jpg").unwrap();
        let path = hv.cache_path(&config.cache_dir);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"data").unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o000)).unwrap();

        if std::fs::File::open(&path).is_ok() {
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
            return;
        }

        let result = handle_file_serve(
            hv.fileid().as_str().to_string(),
            Some(hv),
            Box::new(Additional {
                fileindex: Some("1".to_string()),
                xres: Some("org".to_string()),
                ..Additional::default()
            }),
            true,
            true,
            RequestContext {
                config: state.config.load_full(),
                client: RequestClientContext::new(false, false, None),
                state: state.clone(),
            },
        )
        .await;

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();

        assert!(result.is_err());
        assert_eq!(state.stats.files_sent.load(Ordering::Relaxed), 0);
    }
}
