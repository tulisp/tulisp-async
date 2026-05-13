//! Tokio-backed [`Executor`] implementation. Enabled by the `tokio`
//! feature.

use std::time::{Duration, Instant};

use tulisp::{TulispContext, TulispObject};

use crate::Executor;
use crate::pending::{self, Mailbox, PendingTask};

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

/// Async counterpart to `pending::drain_until`. Awaits each task's
/// deadline via `tokio::time::sleep` instead of parking the calling
/// thread, fires the body on `ctx`, and returns when the mailbox has
/// no live entries left. Repeating timers re-push themselves, so a
/// caller that doesn't want this future to run forever must arrange
/// for every timer to cancel itself eventually (or `select!` against
/// an external shutdown signal).
pub(crate) async fn run_until_idle(ctx: &mut TulispContext, mailbox: &Mailbox) {
    loop {
        let popped = {
            let mut tasks = mailbox.lock().unwrap();
            tasks.retain(|t| !t.cancel.is_cancelled());
            pending::earliest_pending(&tasks).map(|i| tasks.remove(i))
        };
        let Some(task) = popped else { return; };

        let now = Instant::now();
        if task.deadline > now {
            tokio::time::sleep(task.deadline - now).await;
        }
        if task.cancel.is_cancelled() {
            continue;
        }
        if let Err(e) = ctx.funcall(&task.body, &TulispObject::nil()) {
            eprintln!("run-with-timer: {}", e.format(ctx));
        }
        if task.cancel.is_cancelled() {
            continue;
        }
        if let Some(repeat) = task.repeat {
            mailbox.lock().unwrap().push(PendingTask {
                deadline: task.deadline + repeat,
                repeat: Some(repeat),
                body: task.body,
                cancel: task.cancel,
            });
        }
    }
}
