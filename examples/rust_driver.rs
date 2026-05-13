//! Run: `cargo run --example rust_driver` (tokio feature is on by default)
//!
//! The mirror of `channel.rs`: lisp produces, Rust consumes. A lisp
//! timer fires once a second and pushes a message into an mpsc
//! channel via a Rust-backed `(send-msg X)` defun. A separate tokio
//! task reads and prints. The main task drives the timer queue from
//! Rust via `Handle::tick` while awaiting on its own loop, so neither
//! `(sleep-for …)` on the lisp side nor a tokio block on the main
//! task is needed.

use std::sync::Arc;
use std::time::Duration;

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
(run-with-timer 1 1
  (lambda ()
    (setq tick (1+ tick))
    (send-msg (format "tick %d" tick))))
"#,
    )
    .map_err(|e| format!("lisp error:\n{}", e.format(&ctx)))?;

    // Drive the lisp timer queue from Rust async. Between ticks we
    // yield to the runtime — that's what lets the reader task above
    // make progress. ~4.5s, so we capture four firings.
    for _ in 0..45 {
        handle.tick(&mut ctx);
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Close the channel so the reader exits: the sender lives inside
    // `send-msg`'s closure (held by the ctx) AND inside the lambda
    // body's compiled `RustCallTyped` instruction (held by the timer
    // task, which the mailbox in `handle` keeps alive). Drop both
    // before awaiting.
    drop(handle);
    drop(ctx);
    let _ = reader.await;
    Ok(())
}
