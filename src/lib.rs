#![doc = include_str!("../README.md")]

use std::borrow::Cow;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tulisp::{Error, Rest, TulispAny, TulispContext, TulispObject};

mod pending;

#[cfg(feature = "tokio")]
mod tokio;
#[cfg(feature = "tokio")]
pub use self::tokio::TokioExecutor;

/// Host-provided async driver. `tulisp-async` ships `TokioExecutor` behind
/// the `tokio` feature; other runtimes can implement this trait to reuse
/// the same lisp-visible primitives.
pub trait Executor: Send + Sync + 'static {
    /// Park the calling thread for `dur`. The drain helpers call this
    /// between firings to wait for the next deadline; an implementation
    /// is free to use `std::thread::sleep`, a runtime-driven timer, or
    /// any other mechanism that blocks the calling thread.
    fn sleep_blocking(&self, dur: Duration);
}

// -- clock --------------------------------------------------------------

/// Source of "now" for the timer queue, and the policy for *reaching* a
/// deadline. The default ([`WallClock`]) reads `Instant::now()`, so timers
/// fire on real time. A host that drives a simulation can instead supply a
/// hand-advanced clock (see [`ManualClock`]) via [`register_with_clock`]
/// and fire timers on *simulated* time — fast-forwarding or stepping
/// deterministically rather than waiting on the wall clock. The clock owns
/// the wait, so *every* driver (the lisp-visible `(sleep-for …)`, the Rust
/// [`Handle::tick`], and the async [`Handle::run_until_idle`] /
/// [`Handle::run_for`]) honors it; a virtual clock never sleeps on real
/// time on any of those paths.
///
/// Deadlines are still `Instant`s; a virtual clock expresses sim-time
/// as an offset from a base `Instant` captured at construction, so the
/// existing deadline arithmetic and ordering are unchanged.
pub trait Clock: Send + Sync + 'static {
    /// Current instant on this clock's timeline.
    fn now(&self) -> Instant;

    /// Reach `deadline`, returning how much *real* time the caller must
    /// still block to get there. A real-time clock can't be moved, so it
    /// returns `deadline - now()` and the caller really waits (which is
    /// what advances a wall clock); a virtual clock jumps its own `now()`
    /// forward to `deadline` and returns [`Duration::ZERO`], so a
    /// simulation arrives at the deadline with no real-time sleeping. The
    /// drain loops call this before firing each due body and then block —
    /// sync via the [`Executor`], async via the runtime timer — only for
    /// the returned residual. The default is correct for any clock whose
    /// `now()` tracks elapsed real time 1:1, since really sleeping the
    /// residual is what advances it. A *virtual* clock — whose `now()`
    /// only moves on command — **must** override this to jump its own
    /// time forward and return [`Duration::ZERO`]; inheriting the default
    /// would make it really sleep, or hang on a deadline its `now()`
    /// never reaches.
    fn advance_to(&self, deadline: Instant) -> Duration {
        deadline.saturating_duration_since(self.now())
    }
}

/// Wall-clock: `now()` is `Instant::now()`, and reaching a deadline is a
/// real sleep (the default [`Clock::advance_to`]). The default for
/// [`register`].
pub struct WallClock;

impl Clock for WallClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// A hand-advanced clock for driving timers on simulated time. `now()`
/// is a fixed base `Instant` plus an elapsed offset. It moves forward
/// when the host calls [`advance`](Self::advance) — step the queue
/// deterministically by advancing and calling [`Handle::tick`](Handle::tick)
/// — and also when a drain has to *wait* past the current sim-time
/// (`(sleep-for …)` or [`Handle::run_for`] / [`Handle::run_until_idle`]):
/// rather than sleep on real time, the clock jumps straight to the
/// deadline. Elapsed time is monotonic — it never runs backward.
pub struct ManualClock {
    base: Instant,
    elapsed_nanos: AtomicU64,
}

impl ManualClock {
    pub fn new() -> Self {
        Self {
            base: Instant::now(),
            elapsed_nanos: AtomicU64::new(0),
        }
    }

    /// Move simulated time forward by `dur`. Saturates at `u64::MAX`
    /// nanoseconds (~584 years) — both a single oversized `dur` and the
    /// running total clamp rather than wrapping, so elapsed time stays
    /// monotonic.
    pub fn advance(&self, dur: Duration) {
        let nanos = u64::try_from(dur.as_nanos()).unwrap_or(u64::MAX);
        let mut elapsed = self.elapsed_nanos.load(Ordering::Relaxed);
        while let Err(current) = self.elapsed_nanos.compare_exchange_weak(
            elapsed,
            elapsed.saturating_add(nanos),
            Ordering::Relaxed,
            Ordering::Relaxed,
        ) {
            elapsed = current;
        }
    }

    /// Simulated time elapsed since construction.
    pub fn elapsed(&self) -> Duration {
        Duration::from_nanos(self.elapsed_nanos.load(Ordering::Relaxed))
    }
}

impl Default for ManualClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Instant {
        self.base + self.elapsed()
    }

    /// Jump simulated time forward to `deadline` (never backward) and
    /// return [`Duration::ZERO`] — reaching a deadline on a virtual clock
    /// costs no real time.
    fn advance_to(&self, deadline: Instant) -> Duration {
        if let Some(target) = deadline.checked_duration_since(self.base) {
            let nanos = u64::try_from(target.as_nanos()).unwrap_or(u64::MAX);
            self.elapsed_nanos.fetch_max(nanos, Ordering::Relaxed);
        }
        Duration::ZERO
    }
}

// -- timer handle -------------------------------------------------------

/// Opaque handle returned by `(run-with-timer …)` and consumed by
/// `(cancel-timer …)`.
#[derive(Clone)]
pub struct TimerHandle {
    /// Process-wide-unique id assigned at construction. Surfaced via
    /// `Display` so debug prints can tell handles apart without
    /// pointer-chasing the cancel flag.
    id: u64,
    cancelled: Arc<AtomicBool>,
}

/// Next id to hand out. Plain `Relaxed` fetch_add is sufficient — we
/// only need atomicity, not happens-before ordering between threads.
static NEXT_TIMER_ID: AtomicU64 = AtomicU64::new(1);

impl TimerHandle {
    fn new() -> Self {
        Self {
            id: NEXT_TIMER_ID.fetch_add(1, Ordering::Relaxed),
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn cancel(&self) {
        // `Release` pairs with `Acquire` in `is_cancelled` so a cross-
        // thread cancel — e.g. a Rust task that holds a clone of the
        // handle and flips it from outside the lisp thread — is
        // guaranteed to be observed by the drain loop's post-fire
        // re-check.
        self.cancelled.store(true, Ordering::Release);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }
}

impl fmt::Display for TimerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#<timer-handle {}>", self.id)
    }
}

impl TulispAny for TimerHandle {
    fn lisp_type_name() -> Cow<'static, str> {
        Cow::Borrowed("timer-handle")
    }
}

// -- registration -------------------------------------------------------

/// Handle returned by [`register`] that lets non-lisp callers drive
/// the timer queue from Rust. The same mailbox the lisp builtins use
/// is captured here, so a `(run-with-timer …)` from lisp and a
/// [`tick`](Self::tick) from Rust see the same set of pending firings.
///
/// `Clone` is shallow — clones share the same mailbox and executor,
/// not the same `&mut TulispContext`. Useful when one Rust task wants
/// to tick while another monitors the same queue for diagnostics.
#[derive(Clone)]
pub struct Handle {
    mailbox: pending::Mailbox,
    executor: Arc<dyn Executor>,
    clock: Arc<dyn Clock>,
}

impl Handle {
    /// Fire every timer body whose deadline has already passed, in
    /// deadline order, against the calling `&mut ctx`. Returns as soon
    /// as no more firings are due. Repeating timers re-enter the queue
    /// with their next deadline, but `tick` does not block waiting for
    /// that next firing — call again after time has elapsed, or use
    /// `(sleep-for …)` from lisp, which drains while it waits.
    ///
    /// A body that ends in `quit`, which the context's interrupt check
    /// raises and the body can raise itself, or in the `Interrupted`
    /// error of an `Interrupt::Stop`, ends the drain: `tick` returns
    /// that error, the timer does not fire again, and the other due
    /// timers stay queued for the next call. When a body is stopped
    /// inside another body's `(sleep-for …)`, that call returns the
    /// error, and a waiting body that ends in it counts as stopped too.
    /// Any other error from a body goes to stderr, and the drain goes on.
    pub fn tick(&self, ctx: &mut TulispContext) -> Result<(), Error> {
        let now = self.clock.now();
        pending::drain_until(ctx, &self.mailbox, &*self.executor, &*self.clock, now)
    }

    /// Drive the timer queue asynchronously on the Handle's clock,
    /// reaching each task's deadline via `tokio::time::sleep` (or, on a
    /// virtual clock, jumping to it for free), until the mailbox has no
    /// live entries left. Repeating timers re-push themselves, so
    /// this future does not return until every timer has been
    /// cancelled (typically from inside a body via `cancel-timer`), or
    /// a body is stopped, which returns the error as
    /// [`tick`](Self::tick) does. Drop the returned future or `select!`
    /// against a shutdown signal to stop earlier.
    #[cfg(feature = "tokio")]
    pub async fn run_until_idle(&self, ctx: &mut TulispContext) -> Result<(), Error> {
        crate::tokio::run_until(ctx, &self.mailbox, &*self.clock, None).await
    }

    /// Drive the timer queue asynchronously for `dur` of the Handle's
    /// clock, then return. Fires every body whose deadline falls inside
    /// the window, in deadline order. On a virtual clock the window is
    /// `dur` of sim-time and is fast-forwarded with no real waiting.
    /// Repeating timers re-push themselves; firings scheduled beyond the
    /// window stay in the mailbox for a future `tick` / `run_until_idle`
    /// / `run_for`. A stopped body ends the window early and returns the
    /// error, as in [`tick`](Self::tick).
    #[cfg(feature = "tokio")]
    pub async fn run_for(&self, ctx: &mut TulispContext, dur: Duration) -> Result<(), Error> {
        let wake = self.clock.now() + dur;
        crate::tokio::run_until(ctx, &self.mailbox, &*self.clock, Some(wake)).await
    }
}

/// Wire `timerp`, `sleep-for`, `run-with-timer`, and `cancel-timer` into
/// `ctx`, all backed by `executor`. Each call sets up a fresh
/// pending-firings mailbox shared by these four builtins via closure
/// capture — `(run-with-timer …)` pushes, `(sleep-for …)` drains.
///
/// The returned [`Handle`] keeps a reference to the same mailbox so
/// Rust-side callers can `tick` the queue without going through lisp.
/// Callers that only drive timers from lisp can ignore the return.
///
/// # Re-registering
///
/// `register` should be called **at most once per context.** A second
/// call overwrites the `sleep-for` / `run-with-timer` / `cancel-timer`
/// bindings with closures that point at a fresh mailbox — any timer
/// already pending under the first mailbox stays alive but becomes
/// unreachable from lisp (since the new `sleep-for` drains a different
/// queue). The first [`Handle`] can still tick those orphans; if you
/// don't keep it around, they're effectively leaked until the `Arc`
/// chain unwinds at process exit.
pub fn register(ctx: &mut TulispContext, executor: Arc<dyn Executor>) -> Handle {
    register_with_clock(ctx, executor, Arc::new(WallClock))
}

/// Like [`register`], but timers fire on `clock` instead of the wall
/// clock. Pass a [`ManualClock`] to drive the queue on simulated time:
/// advance the clock by hand and call [`Handle::tick`] to fire whatever
/// is due (a deadline at or before the advanced `now` fires
/// immediately), or let `(sleep-for …)` / [`Handle::run_for`] /
/// [`Handle::run_until_idle`] fast-forward sim-time to the next deadline
/// — every path reaches deadlines through the clock, so none sleeps on
/// real time.
pub fn register_with_clock(
    ctx: &mut TulispContext,
    executor: Arc<dyn Executor>,
    clock: Arc<dyn Clock>,
) -> Handle {
    let mailbox = pending::new_mailbox();

    ctx.defun("timerp", |v: TulispObject| {
        v.downcast::<TimerHandle>().is_some()
    });

    let exec_sleep = executor.clone();
    let mb_sleep = mailbox.clone();
    let clk_sleep = clock.clone();
    ctx.defun("sleep-for", move |ctx: &mut TulispContext, secs: f64| {
        if !secs.is_finite() || secs < 0.0 {
            return Err(Error::out_of_range(format!(
                "sleep-for: invalid duration: {secs}"
            )));
        }
        let wake = clk_sleep.now() + Duration::from_secs_f64(secs);
        pending::drain_until(ctx, &mb_sleep, &*exec_sleep, &*clk_sleep, wake)
    });

    let mb_timer = mailbox.clone();
    let clk_timer = clock.clone();
    ctx.defun(
        "run-with-timer",
        move |secs: f64, repeat: Option<f64>, f: TulispObject, args: Rest<TulispObject>| {
            if !secs.is_finite() || secs < 0.0 {
                return Err(Error::out_of_range(format!(
                    "run-with-timer: invalid secs: {secs}"
                )));
            }
            // A repeat that rounds to zero or is too large to represent is
            // one-shot, like a repeat that is not positive.
            let repeat = repeat
                .and_then(|r| Duration::try_from_secs_f64(r).ok())
                .filter(|r| !r.is_zero());
            let handle = TimerHandle::new();
            mb_timer.lock().unwrap().push(pending::PendingTask {
                deadline: clk_timer.now() + Duration::from_secs_f64(secs),
                repeat,
                body: f,
                args: args.into(),
                cancel: handle.clone(),
            });
            Ok::<_, Error>(handle)
        },
    );

    ctx.defun("cancel-timer", |h: TimerHandle| h.cancel());

    Handle {
        mailbox,
        executor,
        clock,
    }
}
