use crate::stats::Stats;
use dashmap::DashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const PENDING_TIMEOUT: Duration = Duration::from_secs(30);
const NORMAL_TIMEOUT: Duration = Duration::from_secs(180);
const SERVERCMD_TIMEOUT: Duration = Duration::from_secs(1800);
const PACKET_SEND_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionKind {
    Pending,
    Normal,
    Servercmd,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionTimeoutReason {
    StartAge,
    LastPacketSend,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionAdmitError {
    MaxConnectionsExceeded { max_connections: u32 },
}

#[derive(Debug)]
struct SessionState {
    remote_addr: SocketAddr,
    started_at: Instant,
    kind: SessionKind,
    last_packet_send: Option<Instant>,
    cancel: CancellationToken,
}

impl SessionState {
    fn new(remote_addr: SocketAddr, cancel: CancellationToken) -> Self {
        Self {
            remote_addr,
            started_at: Instant::now(),
            kind: SessionKind::Pending,
            last_packet_send: None,
            cancel,
        }
    }

    fn timeout_reason(&self, now: Instant) -> Option<SessionTimeoutReason> {
        if self
            .last_packet_send
            .and_then(|t| now.checked_duration_since(t))
            .is_some_and(|age| age > PACKET_SEND_TIMEOUT)
        {
            return Some(SessionTimeoutReason::LastPacketSend);
        }

        let start_timeout = match self.kind {
            SessionKind::Pending => PENDING_TIMEOUT,
            SessionKind::Normal => NORMAL_TIMEOUT,
            SessionKind::Servercmd => SERVERCMD_TIMEOUT,
        };

        now.checked_duration_since(self.started_at)
            .is_some_and(|age| age > start_timeout)
            .then_some(SessionTimeoutReason::StartAge)
    }
}

#[derive(Debug)]
struct SessionManagerInner {
    stats: Arc<Stats>,
    active_connections: AtomicU32,
    sessions: DashMap<u32, SessionState>,
}

#[derive(Debug, Clone)]
pub struct SessionManager {
    inner: Arc<SessionManagerInner>,
}

impl SessionManager {
    pub fn new(stats: Arc<Stats>) -> Self {
        Self {
            inner: Arc::new(SessionManagerInner {
                stats,
                active_connections: AtomicU32::new(0),
                sessions: DashMap::new(),
            }),
        }
    }

    pub fn active_count(&self) -> u32 {
        self.inner.active_connections.load(Ordering::Relaxed)
    }

    pub fn admit(
        &self,
        conn_id: u32,
        remote_addr: SocketAddr,
        is_local: bool,
        is_rpc: bool,
        max_connections: u32,
    ) -> Result<SessionAdmission, SessionAdmitError> {
        let counted = !is_local && !is_rpc;
        let mut active_before = None;

        if counted {
            let prev = self
                .inner
                .active_connections
                .fetch_add(1, Ordering::Relaxed);
            if prev > max_connections {
                self.inner
                    .active_connections
                    .fetch_sub(1, Ordering::Relaxed);
                return Err(SessionAdmitError::MaxConnectionsExceeded { max_connections });
            }
            self.inner.stats.set_open_connections(prev + 1);
            active_before = Some(prev);
        }

        let cancel = CancellationToken::new();
        self.inner
            .sessions
            .insert(conn_id, SessionState::new(remote_addr, cancel.clone()));

        Ok(SessionAdmission {
            guard: SessionGuard {
                conn_id,
                counted,
                inner: self.inner.clone(),
            },
            handle: SessionHandle {
                conn_id,
                inner: self.inner.clone(),
            },
            cancel,
            active_before,
        })
    }

    pub fn nuke_old_connections(&self) -> usize {
        let now = Instant::now();
        let mut expired = Vec::new();

        self.inner.sessions.retain(|conn_id, state| {
            if let Some(reason) = state.timeout_reason(now) {
                tracing::debug!(
                    "Adding session {} ({}) to timeout kill queue: {:?}",
                    conn_id,
                    state.remote_addr,
                    reason
                );
                expired.push((*conn_id, state.cancel.clone()));
                false
            } else {
                true
            }
        });

        for (conn_id, cancel) in &expired {
            tracing::debug!("Closing timed-out HTTP session {}", conn_id);
            cancel.cancel();
        }

        expired.len()
    }
}

pub struct SessionAdmission {
    pub guard: SessionGuard,
    pub handle: SessionHandle,
    pub cancel: CancellationToken,
    pub active_before: Option<u32>,
}

#[derive(Debug)]
pub struct SessionGuard {
    conn_id: u32,
    counted: bool,
    inner: Arc<SessionManagerInner>,
}

impl Drop for SessionGuard {
    fn drop(&mut self) {
        self.inner.sessions.remove(&self.conn_id);
        if self.counted {
            let prev = self
                .inner
                .active_connections
                .fetch_sub(1, Ordering::Relaxed);
            self.inner
                .stats
                .set_open_connections(prev.saturating_sub(1));
        }
    }
}

#[derive(Debug, Clone)]
pub struct SessionHandle {
    conn_id: u32,
    inner: Arc<SessionManagerInner>,
}

impl SessionHandle {
    pub fn mark_normal(&self) {
        self.mark_kind(SessionKind::Normal);
    }

    pub fn mark_servercmd(&self) {
        self.mark_kind(SessionKind::Servercmd);
    }

    pub fn mark_packet_sent(&self) {
        if let Some(mut state) = self.inner.sessions.get_mut(&self.conn_id) {
            state.last_packet_send = Some(Instant::now());
        }
    }

    fn mark_kind(&self, kind: SessionKind) {
        if let Some(mut state) = self.inner.sessions.get_mut(&self.conn_id) {
            state.kind = kind;
        }
    }
}

/// Spawn Java-style session timeout cleanup.
pub fn spawn_session_reaper(
    session_manager: Arc<SessionManager>,
    shutdown: tokio_util::sync::CancellationToken,
) -> JoinHandle<()> {
    tokio::spawn(crate::utils::tick_every(
        shutdown,
        Duration::from_secs(10),
        move || {
            let session_manager = session_manager.clone();
            async move {
                session_manager.nuke_old_connections();
            }
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stats::Stats;

    fn remote_addr() -> SocketAddr {
        "127.0.0.1:12345".parse().unwrap()
    }

    fn state(kind: SessionKind, start_age: Duration) -> SessionState {
        SessionState {
            remote_addr: remote_addr(),
            started_at: Instant::now() - start_age,
            kind,
            last_packet_send: None,
            cancel: CancellationToken::new(),
        }
    }

    #[test]
    fn timeout_classifier_matches_java_start_age_rules() {
        let now = Instant::now();

        assert_eq!(
            state(SessionKind::Pending, Duration::from_secs(29)).timeout_reason(now),
            None
        );
        assert_eq!(
            state(SessionKind::Pending, Duration::from_secs(31)).timeout_reason(now),
            Some(SessionTimeoutReason::StartAge)
        );
        assert_eq!(
            state(SessionKind::Normal, Duration::from_secs(179)).timeout_reason(now),
            None
        );
        assert_eq!(
            state(SessionKind::Normal, Duration::from_secs(181)).timeout_reason(now),
            Some(SessionTimeoutReason::StartAge)
        );
        assert_eq!(
            state(SessionKind::Servercmd, Duration::from_secs(1799)).timeout_reason(now),
            None
        );
        assert_eq!(
            state(SessionKind::Servercmd, Duration::from_secs(1801)).timeout_reason(now),
            Some(SessionTimeoutReason::StartAge)
        );
    }

    #[test]
    fn timeout_classifier_matches_java_last_packet_send_rule() {
        let now = Instant::now();
        let mut session = state(SessionKind::Normal, Duration::from_secs(1));
        session.last_packet_send = Some(now - Duration::from_secs(31));

        assert_eq!(
            session.timeout_reason(now),
            Some(SessionTimeoutReason::LastPacketSend)
        );
    }

    #[test]
    fn public_admission_preserves_java_max_connection_boundary() {
        let manager = SessionManager::new(Arc::new(Stats::new()));
        let first = manager
            .admit(1, remote_addr(), false, false, 0)
            .expect("prev == max is accepted");
        assert_eq!(manager.active_count(), 1);
        assert!(manager.admit(2, remote_addr(), false, false, 0).is_err());
        assert_eq!(manager.active_count(), 1);
        drop(first);
        assert_eq!(manager.active_count(), 0);
    }

    #[test]
    fn local_and_rpc_sessions_are_not_counted() {
        let manager = SessionManager::new(Arc::new(Stats::new()));
        let local = manager.admit(1, remote_addr(), true, false, 0).unwrap();
        let rpc = manager.admit(2, remote_addr(), false, true, 0).unwrap();

        assert_eq!(manager.active_count(), 0);
        drop(local);
        drop(rpc);
        assert_eq!(manager.active_count(), 0);
    }

    #[test]
    fn reaper_cancels_timed_out_sessions() {
        let manager = SessionManager::new(Arc::new(Stats::new()));
        let admission = manager.admit(1, remote_addr(), true, false, 0).unwrap();
        manager.inner.sessions.get_mut(&1).unwrap().started_at =
            Instant::now() - Duration::from_secs(31);

        assert_eq!(manager.nuke_old_connections(), 1);
        assert!(admission.cancel.is_cancelled());
    }
}
