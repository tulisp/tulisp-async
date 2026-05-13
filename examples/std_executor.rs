//! Run: `cargo run --no-default-features --example std_executor`
//!
//! Demonstrates that the host can plug in any runtime. This example
//! implements [`Executor`] using only `std::thread`, so it builds with
//! `--no-default-features` (no tokio).
//!
//! Timer firings drive off the main thread via `(sleep-for …)`, so the
//! executor only needs to know how to park the calling thread —
//! `schedule_after` is unused by the timer builtins but still on the
//! trait for non-lisp embedders.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use tulisp::TulispContext;
use tulisp_async::Executor;

struct StdExecutor;

impl Executor for StdExecutor {
    fn sleep_blocking(&self, dur: Duration) {
        thread::sleep(dur);
    }

    fn schedule_after(&self, dur: Duration, body: Box<dyn FnOnce() + Send + 'static>) {
        thread::spawn(move || {
            thread::sleep(dur);
            body();
        });
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut ctx = TulispContext::new();
    tulisp_async::register(&mut ctx, Arc::new(StdExecutor));

    ctx.eval_string(
        r#"
(setq fired 0)
(run-with-timer 0.3 nil (lambda () (setq fired (1+ fired))))
(run-with-timer 0.3 nil (lambda () (setq fired (1+ fired))))
(sleep-for 0.5)
(princ (format "fired: %d\n" fired))
"#,
    )
    .map_err(|e| format!("lisp error:\n{}", e.format(&ctx)))?;

    Ok(())
}
