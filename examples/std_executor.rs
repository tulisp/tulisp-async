//! Run: `cargo run --no-default-features --example std_executor`
//!
//! Demonstrates that the host can plug in any runtime. This example
//! implements [`Executor`] using only `std::thread`, so it builds with
//! `--no-default-features` (no tokio). Firings drive off the lisp
//! thread via `(sleep-for …)`, so the executor only needs to know how
//! to park the calling thread.

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
    .map_err(|e| format!("lisp error:\n{e}"))?;

    Ok(())
}
