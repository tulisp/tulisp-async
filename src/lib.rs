#![doc = include_str!("../README.md")]

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tulisp::{Error, Shared, TulispContext, TulispConvertible, TulispObject, TulispValue};

#[cfg(feature = "tokio")]
mod tokio;
#[cfg(feature = "tokio")]
pub use self::tokio::TokioExecutor;

/// Accepts either lisp `nil` or a typed `T`. Use this in defun
/// signatures where a non-trailing parameter can be nil — tulisp's
/// defun macro reserves `Option<T>` for trailing `&optional` args, so
/// "nil or value" in any other position needs this wrapper.
pub struct NilOr<T>(pub Option<T>);

impl<T: TulispConvertible> TulispConvertible for NilOr<T> {
    fn from_tulisp(value: &TulispObject) -> Result<Self, Error> {
        if value.null() {
            Ok(NilOr(None))
        } else {
            Ok(NilOr(Some(T::from_tulisp(value)?)))
        }
    }

    fn into_tulisp(self) -> TulispObject {
        match self.0 {
            None => TulispObject::nil(),
            Some(v) => v.into_tulisp(),
        }
    }
}

/// Host-provided async driver. `tulisp-async` ships `TokioExecutor` behind
/// the `tokio` feature; other runtimes can implement this trait to reuse
/// the same lisp-visible primitives.
pub trait Executor: Send + Sync + 'static {
    /// Park the calling thread for `dur`, integrating with the host's
    /// timer wheel if it has one.
    fn sleep_blocking(&self, dur: Duration);

    /// Schedule `body` to run once after `dur` on the host's thread pool.
    /// `run-with-timer` relies on this to keep thread count bounded when
    /// there are many active timers.
    fn schedule_after(&self, dur: Duration, body: Box<dyn FnOnce() + Send + 'static>);
}

// -- timer handle -------------------------------------------------------

/// Opaque handle returned by `(run-with-timer …)` and consumed by
/// `(cancel-timer …)`.
#[derive(Clone)]
pub struct TimerHandle {
    cancelled: Arc<AtomicBool>,
}

impl TimerHandle {
    fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
        }
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }
}

impl fmt::Display for TimerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#<timer-handle>")
    }
}

impl TulispConvertible for TimerHandle {
    fn from_tulisp(value: &TulispObject) -> Result<Self, Error> {
        let any = value.as_any().map_err(|e| e.with_trace(value.clone()))?;
        any.downcast_ref::<TimerHandle>()
            .cloned()
            .ok_or_else(|| {
                Error::type_mismatch(format!("Expected a timer handle, got: {value}"))
                    .with_trace(value.clone())
            })
    }

    fn into_tulisp(self) -> TulispObject {
        TulispValue::from(Shared::new(self)).into_ref(None)
    }
}

/// Recursive re-scheduling for repeating timers. Each firing fires `f`
/// and, if `repeat` is set and we haven't been cancelled, schedules the
/// next firing. One context per timer, reused across firings under a
/// `Mutex` so we don't pay `TulispContext::new()` on every tick.
fn schedule_timer(
    executor: Arc<dyn Executor>,
    handle: TimerHandle,
    task_ctx: Arc<Mutex<TulispContext>>,
    delay: Duration,
    repeat: Option<Duration>,
    f: TulispObject,
) {
    let executor_inner = executor.clone();
    executor.schedule_after(
        delay,
        Box::new(move || {
            if handle.is_cancelled() {
                return;
            }
            let fire_result = {
                let mut ctx = task_ctx.lock().unwrap();
                ctx.funcall(&f, &TulispObject::nil())
            };
            if let Err(e) = fire_result {
                log_timer_error(&task_ctx, &e);
            }
            if handle.is_cancelled() {
                return;
            }
            if let Some(interval) = repeat {
                schedule_timer(executor_inner, handle, task_ctx, interval, Some(interval), f);
            }
        }),
    );
}

fn log_timer_error(task_ctx: &Mutex<TulispContext>, e: &Error) {
    // `Error::format` needs the ctx for backtrace symbol resolution.
    // If a previous firing panicked we can't lock — fall back to the
    // raw error so we still surface something rather than wedging.
    match task_ctx.lock() {
        Ok(ctx) => eprintln!("run-with-timer: {}", e.format(&ctx)),
        Err(_) => eprintln!("run-with-timer: {e} (ctx mutex poisoned)"),
    }
}

// -- registration -------------------------------------------------------

fn is_timer_handle(v: &TulispObject) -> bool {
    v.as_any()
        .ok()
        .map(|any| any.downcast_ref::<TimerHandle>().is_some())
        .unwrap_or(false)
}

/// Wire `timerp`, `sleep-for`, `run-with-timer`, and `cancel-timer` into
/// `ctx`, all backed by `executor`.
pub fn register(ctx: &mut TulispContext, executor: Arc<dyn Executor>) {
    ctx.defun("timerp", |v: TulispObject| is_timer_handle(&v));

    let exec_sleep = executor.clone();
    ctx.defun("sleep-for", move |secs: f64| {
        if !secs.is_finite() || secs < 0.0 {
            return Err(Error::out_of_range(format!(
                "sleep-for: invalid duration: {secs}"
            )));
        }
        exec_sleep.sleep_blocking(Duration::from_secs_f64(secs));
        Ok::<_, Error>(TulispObject::nil())
    });

    let exec_timer = executor.clone();
    ctx.defun(
        "run-with-timer",
        move |secs: f64, repeat: NilOr<f64>, f: TulispObject| {
            if !secs.is_finite() || secs < 0.0 {
                return Err(Error::out_of_range(format!(
                    "run-with-timer: invalid secs: {secs}"
                )));
            }
            let repeat = match repeat.0 {
                None => None,
                Some(r) if !r.is_finite() || r <= 0.0 => None,
                Some(r) => Some(Duration::from_secs_f64(r)),
            };
            let handle = TimerHandle::new();
            let mut task_ctx = TulispContext::new();
            register(&mut task_ctx, exec_timer.clone());
            schedule_timer(
                exec_timer.clone(),
                handle.clone(),
                Arc::new(Mutex::new(task_ctx)),
                Duration::from_secs_f64(secs),
                repeat,
                f,
            );
            Ok::<_, Error>(handle)
        },
    );

    ctx.defun("cancel-timer", |h: TimerHandle| {
        h.cancel();
        Ok::<_, Error>(TulispObject::nil())
    });
}
