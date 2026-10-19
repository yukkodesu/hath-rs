//! Owns on-demand transfers until they finish or process shutdown drains them.
use crate::error::HathError;
use crate::shutdown::Shutdown;
use futures_util::FutureExt;
use std::future::Future;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::task::JoinSet;
use tokio::time::{Instant, timeout_at};

#[derive(Clone)]
pub(crate) struct TransferTasks {
    inner: Arc<Mutex<Option<JoinSet<()>>>>,
    clean: Arc<AtomicBool>,
    shutdown: Shutdown,
}

impl TransferTasks {
    pub(crate) fn new(shutdown: Shutdown) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Some(JoinSet::new()))),
            clean: Arc::new(AtomicBool::new(true)),
            shutdown,
        }
    }

    pub(crate) fn spawn(&self, future: impl Future<Output = ()> + Send + 'static) -> bool {
        let mut guard = self.inner.lock().unwrap();
        let Some(tasks) = guard.as_mut() else {
            return false;
        };
        while let Some(result) = tasks.try_join_next() {
            if result.is_err() {
                self.invalidate();
            }
        }
        let clean = self.clean.clone();
        let shutdown = self.shutdown.clone();
        tasks.spawn(async move {
            if std::panic::AssertUnwindSafe(future)
                .catch_unwind()
                .await
                .is_err()
            {
                clean.store(false, Ordering::Release);
                shutdown.fail(HathError::Fatal("transfer task panicked".into()));
            }
        });
        true
    }

    pub(crate) fn invalidate(&self) {
        self.clean.store(false, Ordering::Release);
    }

    pub(crate) async fn finish(&self, deadline: Instant) -> bool {
        let tasks = self.inner.lock().unwrap().take();
        let Some(mut tasks) = tasks else {
            return self.clean.load(Ordering::Acquire);
        };
        while !tasks.is_empty() {
            match timeout_at(deadline, tasks.join_next()).await {
                Ok(Some(Ok(()))) => {}
                Ok(Some(Err(error))) => {
                    tracing::warn!(%error, "Transfer task did not finish cleanly");
                    self.invalidate();
                }
                Ok(None) => break,
                Err(_) => {
                    tracing::warn!(remaining = tasks.len(), "Transfer drain timed out");
                    self.invalidate();
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    break;
                }
            }
        }
        self.clean.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test(start_paused = true)]
    async fn drain_waits_for_transfers_and_rejects_new_work() {
        let tasks = TransferTasks::new(Shutdown::default());
        let finished = Arc::new(AtomicBool::new(false));
        let flag = finished.clone();
        assert!(tasks.spawn(async move {
            tokio::time::sleep(Duration::from_secs(2)).await;
            flag.store(true, Ordering::Release);
        }));
        assert!(tasks.finish(Instant::now() + Duration::from_secs(3)).await);
        assert!(finished.load(Ordering::Acquire));
        assert!(!tasks.spawn(async {}));
    }
}
