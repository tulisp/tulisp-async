//! In-process queue of pending timer firings. Each `(run-with-timer …)`
//! registers a [`PendingTask`]; drain helpers fire them in deadline
//! order against the calling `&mut TulispContext`.
//!
//! Single mailbox per [`register`](crate::register) call, shared by
//! closure-capture between every builtin that needs to read or write
//! it. Mutation is gated by a `Mutex` so a `(run-with-timer …)` called
//! from inside a firing body sees a consistent view.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tulisp::{TulispContext, TulispObject};

use crate::{Executor, TimerHandle};

/// A timer firing that hasn't happened yet. `deadline` is the absolute
/// `Instant` the body should be funcalled at. For one-shot timers
/// `repeat` is `None`; for repeating timers it's the interval, and the
/// driver re-pushes the task with `deadline + repeat` after each firing.
/// `args` is the list of `&rest` arguments to pass to the body — `nil`
/// if `(run-with-timer …)` was called with only the three required
/// positional args.
pub(crate) struct PendingTask {
    pub(crate) deadline: Instant,
    pub(crate) repeat: Option<Duration>,
    pub(crate) body: TulispObject,
    pub(crate) args: TulispObject,
    pub(crate) cancel: TimerHandle,
}

/// Shared queue of pending firings. `Arc<Mutex<Vec<_>>>` rather than a
/// channel because the driver wants to peek at the earliest deadline
/// without consuming, and a new `(run-with-timer …)` from inside a
/// firing body needs to be visible to the same draining loop.
pub(crate) type Mailbox = Arc<Mutex<Vec<PendingTask>>>;

pub(crate) fn new_mailbox() -> Mailbox {
    Arc::new(Mutex::new(Vec::new()))
}

/// Index of the earliest-deadline, not-cancelled task. `None` if every
/// entry is cancelled (or the queue is empty). Cancelled-but-not-yet-
/// reaped entries are skipped here; the drain loop sweeps them on
/// its own schedule rather than walking the queue per cancel.
pub(crate) fn earliest_pending(tasks: &[PendingTask]) -> Option<usize> {
    tasks
        .iter()
        .enumerate()
        .filter(|(_, t)| !t.cancel.is_cancelled())
        .min_by_key(|(_, t)| t.deadline)
        .map(|(i, _)| i)
}

/// Drive pending firings against `ctx` until `wake`. Mimics Emacs's
/// main-loop behavior under `sit-for` / `sleep-for`: the calling lisp
/// thread blocks, but any registered timer whose deadline falls before
/// `wake` fires (in deadline order) on the same `ctx` before control
/// returns to the lisp caller.
///
/// `executor` is only used for blocking sleeps between fires; the
/// timer body itself runs synchronously on the calling thread, so the
/// caller's mutable borrow on `ctx` is the only one in play.
///
/// Errors from a firing body are written to stderr — one bad timer
/// shouldn't shut down the rest of the program — and the loop
/// continues. The body itself can cancel the timer (via
/// `cancel-timer`) or register more timers; both are observed in the
/// next iteration.
pub(crate) fn drain_until(
    ctx: &mut TulispContext,
    mailbox: &Mailbox,
    executor: &dyn Executor,
    wake: Instant,
) {
    loop {
        // Lock just long enough to reap cancelled entries and pop the
        // next firing whose deadline falls before `wake`. The body
        // itself can re-enter the mailbox (e.g., by calling
        // `run-with-timer` or a nested `sleep-for`), so we never hold
        // the lock across `funcall` or `sleep_blocking`.
        let popped = {
            let mut tasks = mailbox.lock().unwrap();
            tasks.retain(|t| !t.cancel.is_cancelled());
            match earliest_pending(&tasks) {
                Some(i) if tasks[i].deadline <= wake => Some(tasks.remove(i)),
                _ => None,
            }
        };

        // No more due-before-wake firings. Sleep out any remaining
        // window (so `(sleep-for 1)` still parks for ~1s when no
        // timers fire) and return. `tick`'s `wake == now` case
        // skips this sleep because the saturating subtraction is 0.
        let Some(task) = popped else {
            let now = Instant::now();
            if now < wake {
                executor.sleep_blocking(wake - now);
            }
            return;
        };

        let now = Instant::now();
        if task.deadline > now {
            executor.sleep_blocking(task.deadline - now);
        }
        if task.cancel.is_cancelled() {
            continue;
        }
        if let Err(e) = ctx.funcall(&task.body, &task.args) {
            eprintln!("run-with-timer: {}", e.format(ctx));
        }
        // Re-check after funcall: the body can cancel its own handle.
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
