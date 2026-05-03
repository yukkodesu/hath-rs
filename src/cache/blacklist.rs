use crate::cache::CacheHandler;
use crate::rpc::ResponseStatus;
use crate::rpc_client::RpcClient;
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;

/// Synchronous initial blacklist fetch at startup.
/// Fetches blacklist with 3-day delta and deletes matching files from cache.
pub(crate) async fn fetch_initial(rpc_client: &RpcClient, cache: &CacheHandler) {
    match rpc_client.get_blacklist(259200).await {
        Ok(resp) if resp.status == ResponseStatus::Ok => {
            delete_listed_files(cache, &resp.lines);
        }
        Ok(_) => {}
        Err(e) => {
            tracing::warn!("Initial blacklist fetch failed: {}", e);
        }
    }
}

/// Spawn periodic blacklist fetch (6h interval).
pub(crate) fn spawn_fetcher(
    rpc_client: Arc<RpcClient>,
    cache: Arc<CacheHandler>,
    shutdown: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(crate::utils::tick_every(
        shutdown,
        Duration::from_secs(21600),
        move || {
            let rpc_client = rpc_client.clone();
            let cache = cache.clone();
            async move {
                match rpc_client.get_blacklist(43200).await {
                    Ok(resp) if resp.status == ResponseStatus::Ok => {
                        delete_listed_files(&cache, &resp.lines);
                    }
                    _ => {
                        tracing::warn!(
                            "CacheHandler: Failed to retrieve file blacklist, will try again later."
                        );
                    }
                }
            }
        },
    ))
}

fn delete_listed_files(cache: &CacheHandler, fileids: &[String]) {
    for fileid in fileids {
        let _ = cache.delete_file_from_cache(fileid);
    }
}
