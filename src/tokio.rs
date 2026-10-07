//! Tokio-backed [`Executor`] implementation. Enabled by the `tokio`
//! feature.

use std::time::{Duration, Instant};

use tulisp::{Error, TulispContext};

use crate::pending::{self, Mailbox};
use crate::{Clock, Executor};

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
}

/// Async counterpart to `pending::drain_until`. Reaches each task's
/// deadline via `clock.advance_to` — awaiting `tokio::time::sleep` for the
/// residual real time instead of parking the calling thread — fires the
/// body on `ctx`, and returns when the mailbox has no live entries left
/// (`wake = None`) or when `wake` is reached (`wake = Some(t)`). Under a
/// virtual clock the residual is zero, so this fast-forwards sim-time with
/// no real waiting. Repeating timers re-push themselves, so a caller that
/// picks `None` must arrange for every timer to cancel itself eventually.
/// A stopped body ends the run with its error, as in
/// `pending::drain_until`.
pub(crate) async fn run_until(
    ctx: &mut TulispContext,
    mailbox: &Mailbox,
    clock: &dyn Clock,
    wake: Option<Instant>,
) -> Result<(), Error> {
    loop {
        let Some(deadline) = pending::next_deadline(mailbox, wake) else {
            if let Some(w) = wake {
                let wait = clock.advance_to(w);
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
            }
            return Ok(());
        };

        let wait = clock.advance_to(deadline);
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        if let Some(task) = pending::pop_due(mailbox, deadline) {
            pending::fire(ctx, mailbox, task)?;
        }
    }
}
