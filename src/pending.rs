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

use tulisp::TulispObject;

use crate::TimerHandle;

/// A timer firing that hasn't happened yet. `deadline` is the absolute
/// `Instant` the body should be funcalled at. For one-shot timers
/// `repeat` is `None`; for repeating timers it's the interval, and the
/// driver re-pushes the task with `deadline + repeat` after each firing.
#[allow(dead_code)]
pub(crate) struct PendingTask {
    pub(crate) deadline: Instant,
    pub(crate) repeat: Option<Duration>,
    pub(crate) body: TulispObject,
    pub(crate) cancel: TimerHandle,
}

/// Shared queue of pending firings. `Arc<Mutex<Vec<_>>>` rather than a
/// channel because the driver wants to peek at the earliest deadline
/// without consuming, and a new `(run-with-timer …)` from inside a
/// firing body needs to be visible to the same draining loop.
pub(crate) type Mailbox = Arc<Mutex<Vec<PendingTask>>>;

#[allow(dead_code)]
pub(crate) fn new_mailbox() -> Mailbox {
    Arc::new(Mutex::new(Vec::new()))
}

/// Index of the earliest-deadline, not-cancelled task. `None` if every
/// entry is cancelled (or the queue is empty). Cancelled-but-not-yet-
/// reaped entries are skipped here; the drain loop sweeps them on
/// its own schedule rather than walking the queue per cancel.
#[allow(dead_code)]
pub(crate) fn earliest_pending(tasks: &[PendingTask]) -> Option<usize> {
    tasks
        .iter()
        .enumerate()
        .filter(|(_, t)| !t.cancel.is_cancelled())
        .min_by_key(|(_, t)| t.deadline)
        .map(|(i, _)| i)
}
