use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio_io_timeout::TimeoutReader;
use tokio_rustls::{TlsAcceptor, server::TlsStream};

const READ_TIMEOUT: Duration = Duration::from_secs(10);
const ACCEPT_RETRY_DELAY: Duration = Duration::from_secs(1);

pub(super) async fn accept_tcp(listener: &TcpListener) -> (TcpStream, SocketAddr) {
    loop {
        match listener.accept().await {
            Ok(connection) => return connection,
            Err(error) => {
                tracing::warn!("Accept error: {error}; retrying in {ACCEPT_RETRY_DELAY:?}");
                // Resource exhaustion must yield so other tasks can release their sockets.
                tokio::time::sleep(ACCEPT_RETRY_DELAY).await;
            }
        }
    }
}

/// Keep the Java socket read timeout active through both TLS and HTTP parsing.
pub(super) async fn accept_tls(
    acceptor: &TlsAcceptor,
    stream: TcpStream,
) -> io::Result<TlsStream<Pin<Box<TimeoutReader<TcpStream>>>>> {
    let mut stream = TimeoutReader::new(stream);
    stream.set_timeout(Some(READ_TIMEOUT));
    acceptor.accept(Box::pin(stream)).await
}
