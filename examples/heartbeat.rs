//! Run: `cargo run --example heartbeat`
//!
//! Demonstrates a repeating `run-with-timer` that cancels itself from
//! its own body once a target firing count is reached. Timer firings
//! run on the host's thread pool, so a thousand idle timers wouldn't
//! cost a thousand threads — only a few are scheduled at a time.

use std::sync::Arc;

use tulisp::TulispContext;
use tulisp_async::TokioExecutor;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut ctx = TulispContext::new();
    tulisp_async::register(&mut ctx, Arc::new(TokioExecutor::new()));

    ctx.eval_string(
        r#"
(setq counter 0)
(setq h (run-with-timer 0.1 0.1
          (lambda ()
            (setq counter (1+ counter))
            (princ (format "tick %d\n" counter))
            (when (>= counter 5)
              (cancel-timer h)))))

;; Wait long enough for 5 firings, plus a small margin.
(sleep-for 0.7)
(princ (format "final counter: %d\n" counter))
"#,
    )
    .map_err(|e| format!("lisp error:\n{}", e.format(&ctx)))?;

    Ok(())
}
