use super::AppState;
use super::middleware::session::{SessionAdmitError, SessionGuard, SessionHandle};
use super::peer::{self, SessionOrigin};
use crate::config::Config;
use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone)]
pub struct FloodControlEntry {
    pub connect_count: u32,
    pub last_connect: Instant,
    pub block_until: Option<Instant>,
}

impl FloodControlEntry {
    pub fn is_blocked(&self) -> bool {
        self.block_until.is_some_and(|b| b > Instant::now())
    }

    pub fn is_stale(&self, now: Instant) -> bool {
        now.checked_duration_since(self.last_connect)
            .is_some_and(|d| d > Duration::from_secs(60))
    }

    /// Returns true if the connection should be allowed.
    pub fn hit(&mut self) -> bool {
        let now = Instant::now();
        let elapsed_ms = now
            .checked_duration_since(self.last_connect)
            .unwrap_or_default()
            .as_millis() as u32;
        self.connect_count = self
            .connect_count
            .saturating_sub(elapsed_ms / 1000)
            .saturating_add(1);
        self.last_connect = now;

        if self.connect_count > 10 {
            self.block_until = Some(now + Duration::from_secs(60));
            false
        } else {
            true
        }
    }
}

pub(crate) struct AcceptedConnection {
    pub(crate) conn_id: u32,
    pub(crate) handle: SessionHandle,
    pub(crate) cancel: CancellationToken,
    pub(crate) guard: SessionGuard,
}

pub(crate) enum AdmissionOutcome {
    Accepted(AcceptedConnection),
    Rejected(AdmissionRejection),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdmissionRejection {
    Startup,
    FloodControl,
    MaxConnectionsExceeded { max_connections: u32 },
}

pub(crate) fn configure_send_buffer(stream: &TcpStream, config: &Config) {
    if config.throttle_bytes == 0 {
        return;
    }

    let memory_cap = if config.use_less_memory {
        131_072
    } else {
        524_288
    };
    let throttle_cap = (0.2 * config.throttle_bytes as f64).round() as usize;
    let requested = memory_cap.min(throttle_cap);
    if requested == 0 {
        return;
    }

    if let Err(e) = socket2::SockRef::from(stream).set_send_buffer_size(requested) {
        tracing::debug!(
            "Failed to set TCP send buffer to {} bytes: {}",
            requested,
            e
        );
    }
}

pub(crate) async fn admit_connection(
    state: &AppState,
    remote_addr: SocketAddr,
    origin: SessionOrigin,
    config: &Config,
) -> AdmissionOutcome {
    let rpc_authorized = peer::is_rpc_authorized(origin.peer_ip(), config);
    let admission_class = origin.admission_class(rpc_authorized);
    let host_addr = origin.peer_ip();

    let allow = state.allow_normal_connections.load(Ordering::Relaxed);
    if !allow && !rpc_authorized {
        tracing::warn!(
            "Rejecting connection from {} during startup (rpc_servers={:?})",
            host_addr,
            config.rpc_servers
        );
        return AdmissionOutcome::Rejected(AdmissionRejection::Startup);
    }

    if matches!(admission_class, peer::AdmissionClass::Limited) && !config.disable_flood_control {
        let mut entry = state
            .flood_control
            .entry(host_addr.to_string())
            .or_insert_with(|| FloodControlEntry {
                connect_count: 0,
                last_connect: Instant::now(),
                block_until: None,
            });
        if entry.is_blocked() || !entry.hit() {
            tracing::warn!("Flood control activated for {}", host_addr);
            return AdmissionOutcome::Rejected(AdmissionRejection::FloodControl);
        }
    }

    let conn_id = state.next_conn_id.fetch_add(1, Ordering::Relaxed) + 1;
    let max_conns = config.max_connections();
    let admission =
        match state
            .session_manager
            .admit(conn_id, remote_addr, admission_class, max_conns)
        {
            Ok(admission) => admission,
            Err(SessionAdmitError::MaxConnectionsExceeded { max_connections }) => {
                tracing::warn!(
                    "Exceeded the maximum allowed number of incoming connections ({}).",
                    max_connections
                );
                return AdmissionOutcome::Rejected(AdmissionRejection::MaxConnectionsExceeded {
                    max_connections,
                });
            }
        };

    if let Some(prev) = admission.active_before
        && prev > (max_conns as f64 * 0.8) as u32
        && prev > 0
    {
        tracing::warn!(
            "Near connection limit: {} / {} active connections",
            prev + 1,
            max_conns
        );
        let now = Instant::now();
        let mut last = state.last_overload_notification.lock().await;
        let should_notify = last.is_none_or(|t| now - t >= Duration::from_secs(30));
        if should_notify {
            *last = Some(now);
            drop(last);
            let _ = state.rpc_client.notify_overload().await;
        }
    }

    AdmissionOutcome::Accepted(AcceptedConnection {
        conn_id,
        handle: admission.handle,
        cancel: admission.cancel,
        guard: admission.guard,
    })
}

/// Spawn periodic flood control pruning (60s interval).
pub(crate) fn spawn_flood_control_pruner(
    state: AppState,
    shutdown: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(crate::utils::tick_every(
        shutdown,
        Duration::from_secs(60),
        move || {
            let state = state.clone();
            async move { prune_flood_control(&state).await }
        },
    ))
}

/// Prune stale flood control entries. Called periodically from main loop.
pub(crate) async fn prune_flood_control(state: &AppState) {
    let now = Instant::now();
    state.flood_control.retain(|_, entry| !entry.is_stale(now));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flood_control_blocks_eleventh_quick_connection_for_sixty_seconds() {
        let mut entry = FloodControlEntry {
            connect_count: 10,
            last_connect: Instant::now(),
            block_until: None,
        };

        assert!(!entry.hit());
        assert!(entry.is_blocked());
        assert!(
            entry
                .block_until
                .is_some_and(|until| until > Instant::now() + Duration::from_secs(50))
        );
    }

    #[test]
    fn flood_control_count_decays_by_elapsed_seconds() {
        let mut entry = FloodControlEntry {
            connect_count: 10,
            last_connect: Instant::now() - Duration::from_secs(2),
            block_until: None,
        };

        assert!(entry.hit());
        assert_eq!(entry.connect_count, 9);
        assert!(!entry.is_blocked());
    }
}
