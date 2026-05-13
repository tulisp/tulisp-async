//! Run: `cargo run --example rust_driver` (tokio feature is on by default)
//!
//! The mirror of `channel.rs`: lisp produces, Rust consumes. A lisp
//! timer fires once a second and pushes a message into an mpsc
//! channel via a Rust-backed `(send-msg X)` defun; the body cancels
//! itself after four firings. A separate tokio task reads and prints.
//! The main task drives the timer queue with one `await` — no
//! `(sleep-for …)` on the lisp side, no manual sleep loop on the
//! Rust side.

use std::sync::Arc;

use tokio::sync::mpsc;
use tulisp::{Error, TulispContext, TulispObject};
use tulisp_async::TokioExecutor;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();

    // Consumer task: print whatever arrives until the channel closes.
    let reader = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            println!("rust got: {msg}");
        }
    });

    let mut ctx = TulispContext::new();
    let handle = tulisp_async::register(&mut ctx, Arc::new(TokioExecutor::new()));

    // Bridge: (send-msg S) pushes onto the channel. UnboundedSender
    // is Send + Sync + Clone — Mutex-free on the defun side.
    ctx.defun("send-msg", move |s: String| -> Result<TulispObject, Error> {
        let _ = tx.send(s);
        Ok(TulispObject::nil())
    });

    ctx.eval_string(
        r#"
(setq tick 0)
(setq h (run-with-timer 1 1
          (lambda ()
            (setq tick (1+ tick))
            (send-msg (format "tick %d" tick))
            (when (>= tick 4) (cancel-timer h)))))
"#,
    )
    .map_err(|e| format!("lisp error:\n{}", e.format(&ctx)))?;

    // One await drives the queue. Returns when the timer body cancels
    // itself and the mailbox empties.
    handle.run_until_idle(&mut ctx).await;

    // Close the channel so the reader exits: the sender lives inside
    // the `send-msg` defun closure (and in the lambda's compiled
    // `RustCallTyped`, still referenced from `h`). Drop the ctx and
    // the handle so every closure is released.
    drop(handle);
    drop(ctx);
    let _ = reader.await;
    Ok(())
}
