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
use tulisp_async::{NilOr, TokioExecutor};

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
    // Allow one in-flight firing to win the cancellation race.
    assert!(later <= at_cancel + 1, "at_cancel = {at_cancel}, later = {later}");
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
async fn timer_many_pool_backed() {
    // 50 one-shot timers firing concurrently on the blocking pool. Uses a
    // Rust atomic because a lisp-level `(setq counter (1+ counter))`
    // isn't atomic across threads — the point here is that the plumbing
    // survives the fan-out, not that tulisp linearizes sets.
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
