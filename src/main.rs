pub mod bandwidth;
pub mod cache;
pub mod client;
pub mod config;
pub mod downloader;
pub mod error;
pub mod gallery_downloader;
pub mod hvfile;
pub mod logging;
pub mod proxy_downloader;
pub mod rpc;
pub mod rpc_client;
pub mod server;
pub mod stats;
pub mod tls;
pub mod types;
pub mod utils;

#[cfg(test)]
pub(crate) mod test_support;

/// Opt-in allocator for deployment builds. The default build deliberately
/// continues to use Rust's platform allocator for compatibility.
#[cfg(feature = "mimalloc")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() {
    if let Err(e) = client::run().await {
        eprintln!("Fatal error: {}", e);
        std::process::exit(1);
    }
}
