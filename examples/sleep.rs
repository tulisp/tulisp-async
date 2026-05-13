//! Run: `cargo run --example sleep` (tokio feature is on by default)
//!
//! Schedules two one-shot timers and lets them fire while the main
//! thread sleeps inside `(sleep-for …)`. The drain loop wakes up at
//! each deadline, funcalls the body on the calling ctx, and goes back
//! to sleep — single-threaded firing in deadline order.

use std::sync::Arc;
use std::time::Instant;

use tulisp::TulispContext;
use tulisp_async::TokioExecutor;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut ctx = TulispContext::new();
    tulisp_async::register(&mut ctx, Arc::new(TokioExecutor::new()));

    let start = Instant::now();
    ctx.eval_string(
        r#"
(setq fired '())
(run-with-timer 0.5 nil (lambda () (setq fired (cons 'a fired))))
(run-with-timer 0.7 nil (lambda () (setq fired (cons 'b fired))))
(sleep-for 0.8)
(princ (format "fired: %S\n" fired))
"#,
    )
    .map_err(|e| format!("lisp error:\n{}", e.format(&ctx)))?;

    println!("elapsed: {:?}", start.elapsed());
    Ok(())
}
