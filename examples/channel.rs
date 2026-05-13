//! Run: `cargo run --example channel` (tokio feature is on by default)
//!
//! Bridges a Rust async producer into a lisp consumer via a tokio
//! mpsc channel and a small `(try-recv)` defun. The lisp side polls
//! the channel from a recurring timer body — Emacs's process-filter
//! pattern, with tokio doing the producing.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::mpsc;
use tulisp::{Error, TulispContext, TulispConvertible, TulispObject};
use tulisp_async::TokioExecutor;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (tx, rx) = mpsc::unbounded_channel::<String>();

    // Producer: five messages, 50ms apart, then drop the sender so the
    // channel closes. The lisp-side poller sees each one in order.
    tokio::spawn(async move {
        for i in 0..5 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let _ = tx.send(format!("msg-{i}"));
        }
    });

    let mut ctx = TulispContext::new();
    tulisp_async::register(&mut ctx, Arc::new(TokioExecutor::new()));

    // Bridge: (try-recv) returns the next message as a string, or nil
    // when nothing is ready. `UnboundedReceiver` is `!Sync`, so we
    // serialize through a Mutex to satisfy the defun closure's
    // Send + Sync bounds under tulisp's `sync` feature.
    let rx = Mutex::new(rx);
    ctx.defun("try-recv", move || -> Result<TulispObject, Error> {
        match rx.lock().unwrap().try_recv() {
            Ok(msg) => Ok(msg.into_tulisp()),
            Err(_) => Ok(TulispObject::nil()),
        }
    });

    ctx.eval_string(
        r#"
(setq received '())
(setq poller
      (run-with-timer 0.02 0.02
        (lambda ()
          (let ((msg (try-recv)))
            (when msg
              (setq received (cons msg received))
              (princ (format "got %s\n" msg)))))))
(sleep-for 0.4)
(cancel-timer poller)
(princ (format "received %d in order: %S\n"
        (length received)
        (reverse received)))
"#,
    )
    .map_err(|e| format!("lisp error:\n{}", e.format(&ctx)))?;

    Ok(())
}
