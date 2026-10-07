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

use tulisp::{TulispContext, TulispObject};
use tulisp_async::{Handle, TokioExecutor};

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
    ctx.eval_string(src).map_err(|e| e.to_string())
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
    assert!(
        elapsed >= Duration::from_millis(95),
        "elapsed = {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_millis(500),
        "elapsed = {elapsed:?}"
    );
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
async fn cancel_timer_rejects_a_non_handle() {
    let mut ctx = setup();
    let err = eval(&mut ctx, "(cancel-timer 42)").unwrap_err();
    assert!(err.contains("Expected timer-handle"), "err = {err}");
}

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
    });
    eval_ok(
        &mut ctx,
        "(dotimes (_ 50) (run-with-timer 0.05 nil (lambda () (bump))))",
    );
    eval_ok(&mut ctx, "(sleep-for 0.3)");
    assert_eq!(counter.load(Ordering::SeqCst), 50);
}

#[tokio::test(flavor = "multi_thread")]
async fn run_with_timer_passes_rest_args() {
    let mut ctx = setup();
    eval_ok(&mut ctx, "(setq result nil)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0.02 nil \
           (lambda (a b) (setq result (list a b))) \
           'hello 42)",
    );
    eval_ok(&mut ctx, "(sleep-for 0.1)");
    let res = eval_ok(&mut ctx, "result");
    assert_eq!(format!("{res}"), "(hello 42)");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_with_timer_no_args_calls_body_with_nil() {
    // Pre-existing behavior: no &rest args → body called with no
    // arguments. Sanity check that adding the args field didn't
    // regress the zero-args path.
    let mut ctx = setup();
    eval_ok(&mut ctx, "(setq fired nil)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0.02 nil (lambda () (setq fired t)))",
    );
    eval_ok(&mut ctx, "(sleep-for 0.1)");
    assert_eq!(format!("{}", eval_ok(&mut ctx, "fired")), "t");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_with_timer_invalid_secs_errors() {
    let mut ctx = setup();
    let err = eval(&mut ctx, "(run-with-timer -1 nil (lambda () nil))").unwrap_err();
    assert!(err.contains("invalid secs"), "err = {err}");
}

// -- body re-entrancy ---------------------------------------------------

#[tokio::test(flavor = "multi_thread")]
async fn timer_body_error_does_not_stop_repeats() {
    let mut ctx = setup();
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0.02 0.02 \
           (lambda () \
             (setq counter (1+ counter)) \
             (when (= counter 1) (error \"oops\"))))",
    );
    eval_ok(&mut ctx, "(sleep-for 0.15)");
    let fired = eval_i64(&mut ctx, "counter");
    // First firing errors after incrementing; the repeating timer
    // must keep firing on subsequent deadlines (drain catches and
    // logs, doesn't unwind).
    assert!(fired >= 3, "counter = {fired}");
}

#[tokio::test(flavor = "multi_thread")]
async fn nested_sleep_for_drains_outer_timers() {
    // Outer schedules a, b, c at 0.05 / 0.10 / 0.12. b's body
    // (sleep-for 0.05) runs at 0.10–0.15; during that nested wait,
    // c (deadline 0.12) must fire on the same ctx — Emacs's
    // sit-for-while-sit-for behavior.
    let mut ctx = setup();
    eval_ok(&mut ctx, "(setq trace '())");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0.05 nil (lambda () (setq trace (cons 'a trace))))",
    );
    eval_ok(
        &mut ctx,
        "(run-with-timer 0.10 nil \
           (lambda () \
             (setq trace (cons 'b-start trace)) \
             (sleep-for 0.05) \
             (setq trace (cons 'b-end trace))))",
    );
    eval_ok(
        &mut ctx,
        "(run-with-timer 0.12 nil (lambda () (setq trace (cons 'c trace))))",
    );
    eval_ok(&mut ctx, "(sleep-for 0.25)");
    let trace = eval_ok(&mut ctx, "(reverse trace)");
    assert_eq!(format!("{trace}"), "(a b-start c b-end)");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_with_timer_from_body_schedules_more() {
    // Body of parent schedules a child timer; the outer drain must
    // see the new task on its next iteration and fire it.
    let mut ctx = setup();
    eval_ok(&mut ctx, "(setq trace '())");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0.05 nil \
           (lambda () \
             (setq trace (cons 'parent trace)) \
             (run-with-timer 0.05 nil \
               (lambda () (setq trace (cons 'child trace))))))",
    );
    eval_ok(&mut ctx, "(sleep-for 0.2)");
    let trace = eval_ok(&mut ctx, "(reverse trace)");
    assert_eq!(format!("{trace}"), "(parent child)");
}

#[tokio::test(flavor = "multi_thread")]
async fn sleep_for_zero_drains_due_tasks() {
    // wake = now case via lisp's (sleep-for 0) — companion to
    // tick_fires_due_timers_without_sleep_for which exercises the
    // same path through Handle::tick.
    let mut ctx = setup();
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0 nil (lambda () (setq counter (1+ counter))))",
    );
    eval_ok(&mut ctx, "(sleep-for 0)");
    assert_eq!(eval_i64(&mut ctx, "counter"), 1);
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
async fn timer_handles_display_distinct_ids() {
    let mut ctx = setup();
    let a = eval_ok(&mut ctx, "(run-with-timer 10 nil (lambda () nil))");
    let b = eval_ok(&mut ctx, "(run-with-timer 10 nil (lambda () nil))");
    let sa = format!("{a}");
    let sb = format!("{b}");
    assert!(
        sa.starts_with("#<timer-handle ") && sa.ends_with('>'),
        "sa = {sa}"
    );
    assert_ne!(sa, sb, "expected distinct ids: {sa} vs {sb}");
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
    handle.tick(&mut ctx).unwrap();
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
    handle.run_until_idle(&mut ctx).await.unwrap();
    let elapsed = start.elapsed();
    assert_eq!(eval_i64(&mut ctx, "counter"), 3);
    // Each fire awaits ~20ms; three fires + bookkeeping ≈ 60ms.
    assert!(
        elapsed < Duration::from_millis(500),
        "elapsed = {elapsed:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_run_until_idle_keeps_the_timer_it_waits_for() {
    // On the wall clock, so the first poll of the drain stops in the wait,
    // and the select drops it there.
    let mut ctx = TulispContext::new();
    let handle = tulisp_async::register(&mut ctx, Arc::new(TokioExecutor::new()));
    eval_ok(&mut ctx, "(setq ran nil)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0.2 nil (lambda () (setq ran t)))",
    );
    let waited = tokio::select! {
        biased;
        r = handle.run_until_idle(&mut ctx) => Some(r),
        _ = std::future::ready(()) => None,
    };
    assert!(waited.is_none(), "the select drops the future");
    handle.run_until_idle(&mut ctx).await.unwrap();
    assert_eq!(format!("{}", eval_ok(&mut ctx, "ran")), "t");
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
    handle
        .run_for(&mut ctx, Duration::from_millis(150))
        .await
        .unwrap();
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
    handle.run_until_idle(&mut ctx).await.unwrap();
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
    handle.tick(&mut ctx).unwrap();
    // Timer is 10s out — tick must return immediately without firing.
    assert_eq!(eval_i64(&mut ctx, "counter"), 0);
}

// -- manual (sim-time) clock --------------------------------------------

#[test]
fn manual_clock_advance_saturates() {
    let clock = tulisp_async::ManualClock::new();
    clock.advance(Duration::MAX);
    clock.advance(Duration::from_secs(1));
    assert_eq!(clock.elapsed(), Duration::from_nanos(u64::MAX));
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_clock_fires_one_shot_on_advanced_time() {
    use tulisp_async::ManualClock;
    let mut ctx = TulispContext::new();
    let clock = Arc::new(ManualClock::new());
    let handle =
        tulisp_async::register_with_clock(&mut ctx, Arc::new(TokioExecutor::new()), clock.clone());
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 5.0 nil (lambda () (setq counter (1+ counter))))",
    );
    let start = Instant::now();
    // Clock still at 0 — nothing due.
    handle.tick(&mut ctx).unwrap();
    assert_eq!(eval_i64(&mut ctx, "counter"), 0);
    // Advance past the deadline; the timer fires with no real sleeping.
    clock.advance(Duration::from_secs(5));
    handle.tick(&mut ctx).unwrap();
    assert_eq!(eval_i64(&mut ctx, "counter"), 1);
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "sim-time tick must not wall-sleep, elapsed = {:?}",
        start.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_clock_repeats_per_advanced_interval() {
    use tulisp_async::ManualClock;
    let mut ctx = TulispContext::new();
    let clock = Arc::new(ManualClock::new());
    let handle =
        tulisp_async::register_with_clock(&mut ctx, Arc::new(TokioExecutor::new()), clock.clone());
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 5.0 5.0 (lambda () (setq counter (1+ counter))))",
    );
    // Jump 15 s and drain once: the 5 s repeater is due at 5/10/15.
    clock.advance(Duration::from_secs(15));
    handle.tick(&mut ctx).unwrap();
    assert_eq!(eval_i64(&mut ctx, "counter"), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_clock_sleep_for_fast_forwards_without_real_sleep() {
    use tulisp_async::ManualClock;
    let mut ctx = TulispContext::new();
    let clock = Arc::new(ManualClock::new());
    // Driven entirely from lisp via `(sleep-for …)`, so the Handle is
    // unused — the defuns hold their own clones of the mailbox and clock.
    let _handle =
        tulisp_async::register_with_clock(&mut ctx, Arc::new(TokioExecutor::new()), clock.clone());
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 5.0 nil (lambda () (setq counter (1+ counter))))",
    );
    let start = Instant::now();
    // `(sleep-for 10)` jumps sim-time past the 5 s deadline — the timer
    // fires and the call returns at once, with no real 10 s wait.
    eval_ok(&mut ctx, "(sleep-for 10.0)");
    assert_eq!(eval_i64(&mut ctx, "counter"), 1);
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "sleep-for on a manual clock must not wall-sleep, elapsed = {:?}",
        start.elapsed()
    );
    // Sim-time really advanced to the full sleep window.
    assert!(
        clock.elapsed() >= Duration::from_secs(10),
        "elapsed sim time"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_clock_run_until_idle_fast_forwards_without_real_sleep() {
    use tulisp_async::ManualClock;
    let mut ctx = TulispContext::new();
    let clock = Arc::new(ManualClock::new());
    let handle =
        tulisp_async::register_with_clock(&mut ctx, Arc::new(TokioExecutor::new()), clock.clone());
    eval_ok(&mut ctx, "(setq counter 0)");
    // 5 s repeater that cancels itself after three fires (at sim 5/10/15).
    eval_ok(
        &mut ctx,
        "(setq h (run-with-timer 5.0 5.0 (lambda () (setq counter (1+ counter)) (when (>= counter 3) (cancel-timer h)))))",
    );
    let start = Instant::now();
    handle.run_until_idle(&mut ctx).await.unwrap();
    assert_eq!(eval_i64(&mut ctx, "counter"), 3);
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "async fast-forward must not wall-sleep, elapsed = {:?}",
        start.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_clock_run_for_uses_sim_window() {
    use tulisp_async::ManualClock;
    let mut ctx = TulispContext::new();
    let clock = Arc::new(ManualClock::new());
    let handle =
        tulisp_async::register_with_clock(&mut ctx, Arc::new(TokioExecutor::new()), clock.clone());
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 5.0 5.0 (lambda () (setq counter (1+ counter))))",
    );
    let start = Instant::now();
    // 12 sim-seconds covers the firings at 5 and 10, not the one at 15.
    handle
        .run_for(&mut ctx, Duration::from_secs(12))
        .await
        .unwrap();
    assert_eq!(eval_i64(&mut ctx, "counter"), 2);
    // A further 5 sim-seconds (window now reaches 17) fires the one at 15.
    handle
        .run_for(&mut ctx, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(eval_i64(&mut ctx, "counter"), 3);
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "run_for on a manual clock must not wall-sleep, elapsed = {:?}",
        start.elapsed()
    );
}

// -- quit ---------------------------------------------------------------

/// A context on a `ManualClock`, so timers due at 0 fire on `tick` in
/// the order they were added.
fn setup_manual() -> (TulispContext, Handle, Arc<tulisp_async::ManualClock>) {
    let mut ctx = TulispContext::new();
    let clock = Arc::new(tulisp_async::ManualClock::new());
    let handle =
        tulisp_async::register_with_clock(&mut ctx, Arc::new(TokioExecutor::new()), clock.clone());
    (ctx, handle, clock)
}

#[tokio::test(flavor = "multi_thread")]
async fn a_repeating_timer_stopped_by_quit_does_not_fire_again() {
    let (mut ctx, handle, clock) = setup_manual();
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0 1 (lambda () (setq counter (1+ counter)) (while t)))",
    );
    ctx.set_interrupt_check(|| true);
    let err = handle.tick(&mut ctx).expect_err("quit");
    assert!(err.is_a(&ctx, "quit"), "{err}");
    clock.advance(Duration::from_secs(1));
    handle.tick(&mut ctx).unwrap();
    ctx.clear_interrupt_check();
    assert_eq!(eval_i64(&mut ctx, "counter"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_quit_leaves_the_other_due_timers_queued() {
    let (mut ctx, handle, _clock) = setup_manual();
    eval_ok(&mut ctx, "(setq ran nil)");
    eval_ok(&mut ctx, "(run-with-timer 0 nil (lambda () (while t)))");
    eval_ok(&mut ctx, "(run-with-timer 0 nil (lambda () (setq ran t)))");
    ctx.set_interrupt_check(|| true);
    handle.tick(&mut ctx).expect_err("quit");
    ctx.clear_interrupt_check();
    assert_eq!(format!("{}", eval_ok(&mut ctx, "ran")), "nil");
    handle.tick(&mut ctx).unwrap();
    assert_eq!(format!("{}", eval_ok(&mut ctx, "ran")), "t");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_body_that_signals_quit_itself_ends_the_drain() {
    let (mut ctx, handle, clock) = setup_manual();
    eval_ok(&mut ctx, "(setq c 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0 1 (lambda () (setq c (1+ c)) (signal 'quit nil)))",
    );
    let err = handle.tick(&mut ctx).expect_err("quit");
    assert!(err.is_a(&ctx, "quit"), "{err}");
    clock.advance(Duration::from_secs(1));
    handle.tick(&mut ctx).unwrap();
    assert_eq!(eval_i64(&mut ctx, "c"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn sleep_for_signals_a_quit_from_a_timer() {
    let mut ctx = setup();
    eval_ok(&mut ctx, "(run-with-timer 0 nil (lambda () (while t)))");
    ctx.set_interrupt_check(|| true);
    let err = ctx.eval_string("(sleep-for 0.05)").expect_err("quit");
    assert!(err.is_a(&ctx, "quit"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn run_until_idle_ends_on_quit() {
    // On the wall clock, so the future yields between firings and the
    // timeout can end a run that goes on.
    let mut ctx = TulispContext::new();
    let handle = tulisp_async::register(&mut ctx, Arc::new(TokioExecutor::new()));
    eval_ok(&mut ctx, "(run-with-timer 0 0.1 (lambda () (while t)))");
    ctx.set_interrupt_check(|| true);
    let err = tokio::time::timeout(Duration::from_secs(2), handle.run_until_idle(&mut ctx))
        .await
        .expect("run_until_idle returns")
        .expect_err("quit");
    assert!(err.is_a(&ctx, "quit"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_repeating_timer_stopped_by_the_host_does_not_fire_again() {
    let (mut ctx, handle, clock) = setup_manual();
    eval_ok(&mut ctx, "(setq counter 0)");
    eval_ok(
        &mut ctx,
        "(run-with-timer 0 1 (lambda () (setq counter (1+ counter)) (while t)))",
    );
    ctx.set_interrupt_check(|| tulisp::Interrupt::Stop("over time".to_string()));
    let err = handle.tick(&mut ctx).expect_err("stop");
    assert!(
        matches!(err.kind(), tulisp::ErrorKind::Interrupted),
        "{err}"
    );
    clock.advance(Duration::from_secs(1));
    handle.tick(&mut ctx).unwrap();
    ctx.clear_interrupt_check();
    assert_eq!(eval_i64(&mut ctx, "counter"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn sleep_for_passes_on_a_stop_from_a_timer() {
    let mut ctx = setup();
    eval_ok(&mut ctx, "(run-with-timer 0 nil (lambda () (while t)))");
    ctx.set_interrupt_check(|| tulisp::Interrupt::Stop("over time".to_string()));
    let err = ctx.eval_string("(sleep-for 0.05)").expect_err("stop");
    assert!(
        matches!(err.kind(), tulisp::ErrorKind::Interrupted),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn run_for_ends_the_window_on_a_stop() {
    let (mut ctx, handle, clock) = setup_manual();
    eval_ok(&mut ctx, "(setq ran nil)");
    eval_ok(&mut ctx, "(run-with-timer 1 1 (lambda () (while t)))");
    eval_ok(&mut ctx, "(run-with-timer 5 nil (lambda () (setq ran t)))");
    ctx.set_interrupt_check(|| tulisp::Interrupt::Stop("over time".to_string()));
    let err = handle
        .run_for(&mut ctx, Duration::from_secs(10))
        .await
        .expect_err("stop");
    ctx.clear_interrupt_check();
    assert!(
        matches!(err.kind(), tulisp::ErrorKind::Interrupted),
        "{err}"
    );
    assert_eq!(clock.elapsed(), Duration::from_secs(1));
    assert_eq!(format!("{}", eval_ok(&mut ctx, "ran")), "nil");
}
