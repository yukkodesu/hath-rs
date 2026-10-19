use super::admission::{self, AcceptedConnection};
use super::middleware::access_log::AccessLogService;
use super::peer::SessionOrigin;
use super::service::HathService;
use super::{AppState, transport};
use hyper::server::conn::http1;
use hyper_util::rt::TokioIo;
use std::io;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::time::timeout_at;
use tokio_util::sync::CancellationToken;

/// An admitted session owns its socket and registration from before TLS until HTTP ends.
pub(super) struct Connection {
    state: AppState,
    stream: TcpStream,
    remote_addr: SocketAddr,
    origin: SessionOrigin,
    admission: AcceptedConnection,
    shutdown: CancellationToken,
}

impl Connection {
    pub(super) fn accept(
        state: &AppState,
        stream: TcpStream,
        remote_addr: SocketAddr,
        shutdown: &CancellationToken,
    ) -> Option<Self> {
        let config = state.config.load_full();
        let origin = SessionOrigin::from_remote_addr(remote_addr, &config);
        let admission = match admission::admit_connection(state, remote_addr, origin, &config) {
            Ok(admission) => admission,
            Err(reason) => {
                tracing::warn!("Rejecting connection from {remote_addr}: {reason:?}");
                return None;
            }
        };
        admission::configure_send_buffer(&stream, &config);
        Some(Self {
            state: state.clone(),
            stream,
            remote_addr,
            origin,
            admission,
            shutdown: shutdown.clone(),
        })
    }

    pub(super) async fn serve(self) {
        let Self {
            state,
            stream,
            remote_addr,
            origin,
            admission,
            shutdown,
        } = self;
        let AcceptedConnection {
            conn_id,
            handle: session,
            cancel,
            guard: _guard,
            pending_deadline,
            near_limit,
        } = admission;
        let lifecycle = async {
            let handshake = async {
                if near_limit {
                    admission::notify_overload(&state).await;
                }
                let acceptor = state
                    .tls_acceptor
                    .load_full()
                    .expect("TLS acceptor not initialized");
                transport::accept_tls(&acceptor, stream).await
            };
            let stream = tokio::select! {
                biased;
                _ = shutdown.cancelled() => return,
                result = timeout_at(pending_deadline, handshake) => {
                    match result.unwrap_or_else(|_| Err(io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out"))) {
                        Ok(stream) => stream,
                        Err(error) => {
                            match error.kind() {
                                io::ErrorKind::ConnectionReset | io::ErrorKind::UnexpectedEof | io::ErrorKind::TimedOut => {
                                    tracing::debug!("TLS accept from {remote_addr} ended: {error}");
                                }
                                _ => tracing::warn!("TLS accept failed from {remote_addr}: {error:?}"),
                            }
                            return;
                        }
                    }
                }
            };
            let service = AccessLogService::new(
                HathService {
                    state,
                    origin,
                    session: session.clone(),
                },
                conn_id,
                remote_addr,
                Some(session),
            );
            // H@H serves one request per socket. Do not poll for peer EOF while writing a response.
            if let Err(error) = http1::Builder::new()
                .keep_alive(false)
                .half_close(true)
                .header_read_timeout(None)
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!("HTTP connection error: {error}");
            }
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                tracing::debug!("HTTP session {conn_id} timed out; closing connection");
            }
            _ = lifecycle => {}
        }
    }
}

#[cfg(test)]
mod tests;
