//! Process shutdown requests retain their cause independently of task cancellation.
use crate::error::{HathError, Result};
use std::sync::{Arc, Mutex};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
pub(crate) struct Shutdown {
    token: CancellationToken,
    reason: Arc<Mutex<Option<ShutdownReason>>>,
}

#[derive(Debug)]
pub(crate) enum ShutdownReason {
    Signal(&'static str),
    Failure(HathError),
}

impl Shutdown {
    pub(crate) fn token(&self) -> CancellationToken {
        self.token.clone()
    }

    pub(crate) fn request(&self, reason: ShutdownReason) {
        let mut current = self.reason.lock().unwrap();
        if current.is_none() {
            tracing::info!(?reason, "Shutdown requested");
            *current = Some(reason);
        }
        self.token.cancel();
    }

    pub(crate) fn fail(&self, error: HathError) {
        self.request(ShutdownReason::Failure(error));
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    pub(crate) async fn cancelled(&self) {
        self.token.cancelled().await;
    }

    pub(crate) fn result(&self) -> Result<()> {
        match self.reason.lock().unwrap().take() {
            Some(ShutdownReason::Signal(signal)) => {
                tracing::info!(signal, "Stopped after signal");
                Ok(())
            }
            Some(ShutdownReason::Failure(error)) => Err(error),
            None => Err(HathError::Fatal(
                "Shutdown requested without a reason".into(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn first_request_preserves_its_reason_and_wakes_waiters() {
        let shutdown = Shutdown::default();
        shutdown.fail(HathError::CertExpired);
        shutdown.request(ShutdownReason::Signal("SIGTERM"));
        shutdown.cancelled().await;
        assert!(matches!(shutdown.result(), Err(HathError::CertExpired)));
    }
}
