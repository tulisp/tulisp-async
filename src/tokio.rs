//! Tokio-backed [`Executor`] implementation. Enabled by the `tokio`
//! feature.

use std::time::{Duration, Instant};

use tulisp::TulispContext;

use crate::pending::{self, Mailbox, PendingTask};
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
pub(crate) async fn run_until(
    ctx: &mut TulispContext,
    mailbox: &Mailbox,
    clock: &dyn Clock,
    wake: Option<Instant>,
) {
    loop {
        let popped = {
            let mut tasks = mailbox.lock().unwrap();
            tasks.retain(|t| !t.cancel.is_cancelled());
            match pending::earliest_pending(&tasks) {
                Some(i) if wake.is_none_or(|w| tasks[i].deadline <= w) => Some(tasks.remove(i)),
                _ => None,
            }
        };

        let Some(task) = popped else {
            if let Some(w) = wake {
                let wait = clock.advance_to(w);
                if !wait.is_zero() {
                    tokio::time::sleep(wait).await;
                }
            }
            return;
        };

        let wait = clock.advance_to(task.deadline);
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        if task.cancel.is_cancelled() {
            continue;
        }
        if let Err(e) = ctx.apply(&task.body, &task.args) {
            eprintln!("run-with-timer: {e}");
        }
        if task.cancel.is_cancelled() {
            continue;
        }
        if let Some(repeat) = task.repeat {
            mailbox.lock().unwrap().push(PendingTask {
                deadline: task.deadline + repeat,
                repeat: Some(repeat),
                body: task.body,
                args: task.args,
                cancel: task.cancel,
            });
        }
    }
}
