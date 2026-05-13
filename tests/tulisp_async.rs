//! Integration tests for `tulisp-async`.
//!
//! Uses the tokio feature (default) so we get `TokioExecutor`. Each test
//! uses `flavor = "multi_thread"` because `sleep-for` blocks a thread on
//! a tokio-driven timer — on current-thread, blocking the worker while
//! its own timer needs to run deadlocks.
//!
//! With `--no-default-features` the file compiles to nothing — there's
//! no executor to drive the tests against.

#![cfg(feature = "tokio")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tulisp::{Error, TulispContext, TulispObject};
use tulisp_async::{Handle, NilOr, TokioExecutor};

// Compile-time guarantee: Handle is freely movable/shareable across
// tokio tasks. Catches a future regression where someone adds a
// non-Send field.
const _: () = {
    const fn assert_send<T: Send>() {}
    const fn assert_sync<T: Sync>() {}
    assert_send::<Handle>();
    assert_sync::<Handle>();
};

fn setup() -> TulispContext {
    let mut ctx = TulispContext::new();
    tulisp_async::register(&mut ctx, Arc::new(TokioExecutor::new()));
    ctx
}

fn eval(ctx: &mut TulispContext, src: &str) -> Result<TulispObject, String> {
    ctx.eval_string(src).map_err(|e| e.format(ctx))
}

fn eval_ok(ctx: &mut TulispContext, src: &str) -> TulispObject {
    eval(ctx, src).unwrap_or_else(|e| panic!("eval `{src}` failed:\n{e}"))
}

fn eval_i64(ctx: &mut TulispContext, src: &str) -> i64 {
    format!("{}", eval_ok(ctx, src))
        .parse()
        .expect("result is an integer")
}

// -- sleep-for ----------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn sleep_for_blocks_for_duration() {
    let mut ctx = setup();
    let start = Instant::now();
    eval_ok(&mut ctx, "(sleep-for 0.1)");
    let elapsed = start.elapsed();
    assert!(elapsed >= Duration::from_millis(95), "elapsed = {elapsed:?}");
    assert!(elapsed < Duration::from_millis(500), "elapsed = {elapsed:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn sleep_for_accepts_integer_via_coercion() {
    let mut ctx = setup();
    // tulisp's f64 conversion coerces from int.
    eval_ok(&mut ctx, "(sleep-for 0)");
}

#[tokio::test(flavor = "multi_thread")]
async fn sleep_for_negative_errors() {
    let mut ctx = setup();
    let err = eval(&mut ctx, "(sleep-for -1.0)").unwrap_err();
    assert!(err.contains("invalid duration"), "err = {err}");
}

// -- run-with-timer / cancel-timer --------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn timer_one_shot_fires_once() {
    let mut ctx = setup();
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0.05 nil (lambda () (setq counter (1+ counter))))",
    );
    eval_ok(&mut ctx, "(sleep-for 0.2)");
    assert_eq!(eval_i64(&mut ctx, "counter"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_repeats_until_cancelled() {
    let mut ctx = setup();
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(setq h (run-with-timer 0.05 0.05 \
                   (lambda () (setq counter (1+ counter)))))",
    );
    eval_ok(&mut ctx, "(sleep-for 0.28)");
    eval_ok(&mut ctx, "(cancel-timer h)");
    let fired = eval_i64(&mut ctx, "counter");
    assert!((3..=7).contains(&fired), "counter = {fired}");
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_cancel_before_firing() {
    let mut ctx = setup();
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(cancel-timer
           (run-with-timer 0.1 nil (lambda () (setq counter (1+ counter)))))",
    );
    eval_ok(&mut ctx, "(sleep-for 0.25)");
    assert_eq!(eval_i64(&mut ctx, "counter"), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_cancel_stops_repeats() {
    let mut ctx = setup();
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(setq h (run-with-timer 0.05 0.05 \
                   (lambda () (setq counter (1+ counter)))))",
    );
    eval_ok(&mut ctx, "(sleep-for 0.12)");
    eval_ok(&mut ctx, "(cancel-timer h)");
    let at_cancel = eval_i64(&mut ctx, "counter");
    eval_ok(&mut ctx, "(sleep-for 0.3)");
    let later = eval_i64(&mut ctx, "counter");
    // Same-thread firing: cancel can't race a body. The drain loop
    // pops one task at a time, the cancel happens between firings,
    // and the post-fire check stops the re-push.
    assert_eq!(later, at_cancel, "at_cancel = {at_cancel}, later = {later}");
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_nil_repeat_does_not_repeat() {
    let mut ctx = setup();
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0.05 nil (lambda () (setq counter (1+ counter))))",
    );
    eval_ok(&mut ctx, "(sleep-for 0.3)");
    assert_eq!(eval_i64(&mut ctx, "counter"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_zero_repeat_does_not_repeat() {
    let mut ctx = setup();
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0.05 0 (lambda () (setq counter (1+ counter))))",
    );
    eval_ok(&mut ctx, "(sleep-for 0.3)");
    assert_eq!(eval_i64(&mut ctx, "counter"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn timer_many_one_shot() {
    // 50 one-shot timers all due at ~the same instant. The drain loop
    // fires them sequentially on the lisp thread; checks via a Rust
    // atomic just to keep the assertion thread-safe — same-thread
    // increments would be fine too.
    let mut ctx = setup();
    let counter = Arc::new(AtomicUsize::new(0));
    let c = counter.clone();
    ctx.defun("bump", move || {
        c.fetch_add(1, Ordering::SeqCst);
        Ok::<_, Error>(TulispObject::nil())
    });
    eval_ok(
        &mut ctx,
        "(dotimes (_ 50) (run-with-timer 0.05 nil (lambda () (bump))))",
    );
    eval_ok(&mut ctx, "(sleep-for 0.3)");
    assert_eq!(counter.load(Ordering::SeqCst), 50);
}

#[tokio::test(flavor = "multi_thread")]
async fn run_with_timer_invalid_secs_errors() {
    let mut ctx = setup();
    let err = eval(&mut ctx, "(run-with-timer -1 nil (lambda () nil))").unwrap_err();
    assert!(err.contains("invalid secs"), "err = {err}");
}

// -- predicates ---------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn timerp_detects_timer_handle() {
    let mut ctx = setup();
    let res = eval_ok(
        &mut ctx,
        "(let* ((tm (run-with-timer 10 nil (lambda () nil)))
                (p  (if (timerp tm) 'yes 'no)))
           (cancel-timer tm)
           p)",
    );
    assert_eq!(format!("{res}"), "yes");
}

#[tokio::test(flavor = "multi_thread")]
async fn timerp_rejects_other_values() {
    let mut ctx = setup();
    for expr in ["nil", "42", "\"hi\"", "'sym"] {
        let src = format!("(if (timerp {expr}) 'yes 'no)");
        let res = eval_ok(&mut ctx, &src);
        assert_eq!(format!("{res}"), "no", "timerp on {expr}");
    }
}

// -- Handle::tick -------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn tick_fires_due_timers_without_sleep_for() {
    let mut ctx = TulispContext::new();
    let handle = tulisp_async::register(&mut ctx, Arc::new(TokioExecutor::new()));
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0 nil (lambda () (setq counter (1+ counter))))",
    );
    // No (sleep-for …) — drive from Rust instead.
    handle.tick(&mut ctx);
    assert_eq!(eval_i64(&mut ctx, "counter"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn run_until_idle_drains_self_cancelling_timer() {
    let mut ctx = TulispContext::new();
    let handle = tulisp_async::register(&mut ctx, Arc::new(TokioExecutor::new()));
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(setq h (run-with-timer 0.02 0.02 \
                   (lambda () \
                     (setq counter (1+ counter)) \
                     (when (>= counter 3) (cancel-timer h)))))",
    );
    let start = Instant::now();
    handle.run_until_idle(&mut ctx).await;
    let elapsed = start.elapsed();
    assert_eq!(eval_i64(&mut ctx, "counter"), 3);
    // Each fire awaits ~20ms; three fires + bookkeeping ≈ 60ms.
    assert!(elapsed < Duration::from_millis(500), "elapsed = {elapsed:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_for_fires_window_and_returns() {
    let mut ctx = TulispContext::new();
    let handle = tulisp_async::register(&mut ctx, Arc::new(TokioExecutor::new()));
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0.02 0.02 (lambda () (setq counter (1+ counter))))",
    );
    let start = Instant::now();
    handle.run_for(&mut ctx, Duration::from_millis(150)).await;
    let elapsed = start.elapsed();
    let fired = eval_i64(&mut ctx, "counter");
    // 0.02 .. 0.14 in 0.02 steps → expect ~7 firings, allow slop.
    assert!((4..=8).contains(&fired), "counter = {fired}");
    // Must respect the window — within ~50ms of the requested 150.
    assert!(
        elapsed >= Duration::from_millis(145) && elapsed < Duration::from_millis(300),
        "elapsed = {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn run_until_idle_returns_on_empty_mailbox() {
    let mut ctx = TulispContext::new();
    let handle = tulisp_async::register(&mut ctx, Arc::new(TokioExecutor::new()));
    // No timers registered — should return immediately.
    let start = Instant::now();
    handle.run_until_idle(&mut ctx).await;
    assert!(start.elapsed() < Duration::from_millis(50));
}

#[tokio::test(flavor = "multi_thread")]
async fn tick_leaves_future_timers_pending() {
    let mut ctx = TulispContext::new();
    let handle = tulisp_async::register(&mut ctx, Arc::new(TokioExecutor::new()));
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 10 nil (lambda () (setq counter (1+ counter))))",
    );
    handle.tick(&mut ctx);
    // Timer is 10s out — tick must return immediately without firing.
    assert_eq!(eval_i64(&mut ctx, "counter"), 0);
}

// -- NilOr<T> -----------------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn nil_or_accepts_nil_and_value() {
    let mut ctx = setup();
    // Register a Rust fn that mirrors its NilOr<i64> arg back to a
    // convention: -1 for nil, n for Some(n).
    ctx.defun("nil-or-i64-probe", |v: NilOr<i64>| {
        Ok::<_, Error>(v.0.unwrap_or(-1))
    });
    assert_eq!(eval_i64(&mut ctx, "(nil-or-i64-probe nil)"), -1);
    assert_eq!(eval_i64(&mut ctx, "(nil-or-i64-probe 7)"), 7);
}

#[tokio::test(flavor = "multi_thread")]
async fn nil_or_rejects_wrong_type() {
    let mut ctx = setup();
    ctx.defun("nil-or-i64-probe", |v: NilOr<i64>| {
        Ok::<_, Error>(v.0.unwrap_or(-1))
    });
    let err = eval(&mut ctx, r#"(nil-or-i64-probe "hello")"#).unwrap_err();
    // tulisp's i64 conversion error mentions the expected type.
    assert!(
        err.to_lowercase().contains("int") || err.to_lowercase().contains("number"),
        "err = {err}"
    );
}
