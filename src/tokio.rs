//! Tokio-backed [`Executor`] implementation. Enabled by the `tokio`
//! feature.

use std::time::Duration;

use crate::Executor;

/// Drives sleeps and timers on a tokio runtime. Construct inside a tokio
/// runtime — it captures `Handle::current()` so it can schedule work
/// from threads that are themselves outside the runtime.
pub struct TokioExecutor {
    handle: tokio::runtime::Handle,
}

impl TokioExecutor {
    pub fn new() -> Self {
        Self {
            handle: tokio::runtime::Handle::current(),
        }
    }
}

impl Default for TokioExecutor {
    fn default() -> Self {
        Self::new()
    }
}

impl Executor for TokioExecutor {
    fn sleep_blocking(&self, dur: Duration) {
        // Bounce through a oneshot so we can block this (non-tokio) thread
        // on a tokio timer without needing a runtime on the current thread.
        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        self.handle.spawn(async move {
            tokio::time::sleep(dur).await;
            let _ = tx.send(());
        });
        let _ = rx.recv();
    }

    fn schedule_after(&self, dur: Duration, body: Box<dyn FnOnce() + Send + 'static>) {
        self.handle.spawn(async move {
            tokio::time::sleep(dur).await;
            // Hop to the blocking pool — `body` may call into lisp, which
            // can block on `sleep-for` or a timer Mutex.
            tokio::task::spawn_blocking(body);
        });
    }
}
