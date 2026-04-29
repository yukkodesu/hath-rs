pub mod error;
pub mod types;
pub mod utils;
pub mod hvfile;
pub mod logging;
pub mod stats;
pub mod config;
pub mod rpc;
pub mod rpc_client;
pub mod bandwidth;
pub mod downloader;
pub mod proxy_downloader;
pub mod cache;
pub mod server;
pub mod scheduler;
pub mod client;

#[tokio::main]
async fn main() {
    if let Err(e) = client::run().await {
        eprintln!("Fatal error: {}", e);
        std::process::exit(1);
    }
}
