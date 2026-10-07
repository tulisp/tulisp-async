//! In-process queue of pending timer firings. Each `(run-with-timer …)`
//! registers a [`PendingTask`]; drain helpers fire them in deadline
//! order against the calling `&mut TulispContext`.
//!
//! Single mailbox per [`register`](crate::register) call, shared by
//! closure-capture between every builtin that needs to read or write
//! it. The queue's `Mutex` serializes each read-modify-write of it
//! (reap-cancelled, pop-earliest, push-on-repeat), so a
//! `(run-with-timer …)` called from inside a firing body, or a
//! nested `(sleep-for …)`, sees a consistent view.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tulisp::{Error, ErrorKind, TulispContext, TulispObject};

use crate::{Clock, Executor, TimerHandle};

/// A timer firing that hasn't happened yet. `deadline` is the absolute
/// `Instant` the body should be called at. For one-shot timers
/// `repeat` is `None`; for repeating timers it's the interval, and the
/// driver re-pushes the task with `deadline + repeat` after each firing,
/// unless that is too large for an `Instant`.
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
fn earliest_pending(tasks: &[PendingTask]) -> Option<usize> {
    tasks
        .iter()
        .enumerate()
        .filter(|(_, t)| !t.cancel.is_cancelled())
        .min_by_key(|(_, t)| t.deadline)
        .map(|(i, _)| i)
}

/// Drop the cancelled tasks and return the earliest deadline, if it is at
/// or before `wake` (any deadline when `wake` is `None`).
///
/// The drains wait for that deadline before they take its task with
/// [`pop_due`], so a drain that stops during the wait (an async one whose
/// future is dropped) leaves the task queued. The lock is held only
/// inside these two calls: a body can re-enter the mailbox, by calling
/// `run-with-timer` or a nested `sleep-for`.
pub(crate) fn next_deadline(mailbox: &Mailbox, wake: Option<Instant>) -> Option<Instant> {
    let mut tasks = mailbox.lock().unwrap();
    tasks.retain(|t| !t.cancel.is_cancelled());
    let deadline = tasks[earliest_pending(&tasks)?].deadline;
    wake.is_none_or(|w| deadline <= w).then_some(deadline)
}

/// Take the earliest live task whose deadline is at or before `at`.
pub(crate) fn pop_due(mailbox: &Mailbox, at: Instant) -> Option<PendingTask> {
    let mut tasks = mailbox.lock().unwrap();
    let i = earliest_pending(&tasks)?;
    (tasks[i].deadline <= at).then(|| tasks.remove(i))
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
/// continues; a stopped body ends the loop with its error (see
/// [`fire`]). The body itself can cancel the timer (via `cancel-timer`)
/// or register more timers; both are observed in the next iteration.
pub(crate) fn drain_until(
    ctx: &mut TulispContext,
    mailbox: &Mailbox,
    executor: &dyn Executor,
    clock: &dyn Clock,
    wake: Instant,
) -> Result<(), Error> {
    loop {
        // No more due-before-wake firings. Reach `wake` and return: a
        // wall clock parks for the remaining window (so `(sleep-for 1)`
        // still waits ~1s when no timers fire); a virtual clock jumps to
        // `wake` for free. `tick`'s `wake == now` case yields a zero
        // residual either way, so it never sleeps.
        let Some(deadline) = next_deadline(mailbox, Some(wake)) else {
            let wait = clock.advance_to(wake);
            if !wait.is_zero() {
                executor.sleep_blocking(wait);
            }
            return Ok(());
        };

        // Reach this firing's deadline before running it: a wall clock
        // really sleeps the gap, a virtual clock jumps forward for free.
        let wait = clock.advance_to(deadline);
        if !wait.is_zero() {
            executor.sleep_blocking(wait);
        }
        if let Some(task) = pop_due(mailbox, deadline) {
            fire(ctx, mailbox, task)?;
        }
    }
}

/// Run `task`'s body on `ctx`, unless it was cancelled, and queue its
/// next firing if it repeats. The body can cancel its own timer, which
/// stops the next firing.
///
/// When the body ends in `quit`, which the host's interrupt check raises
/// and the body can raise itself, or in the `Interrupted` error of an
/// `Interrupt::Stop`, `fire` returns that error and the timer does not
/// fire again: a body that runs past the host's limit once would do so
/// on every firing.
pub(crate) fn fire(
    ctx: &mut TulispContext,
    mailbox: &Mailbox,
    task: PendingTask,
) -> Result<(), Error> {
    if task.cancel.is_cancelled() {
        return Ok(());
    }
    if let Err(e) = ctx.apply(&task.body, &task.args) {
        if e.is_a(ctx, "quit") || matches!(e.kind(), ErrorKind::Interrupted) {
            return Err(e);
        }
        eprintln!("run-with-timer: {e}");
    }
    if task.cancel.is_cancelled() {
        return Ok(());
    }
    // A next firing past the end of the clock never comes.
    if let Some(deadline) = task.repeat.and_then(|r| task.deadline.checked_add(r)) {
        mailbox
            .lock()
            .unwrap()
            .push(PendingTask { deadline, ..task });
    }
    Ok(())
}
