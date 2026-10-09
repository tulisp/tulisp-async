# tulisp-async

Runtime-agnostic timer primitives for [tulisp](https://github.com/shsms/tulisp).

Wires Elisp-shaped timer builtins (`sleep-for`, `run-with-timer`,
`cancel-timer`) onto a `TulispContext`, backed by a host-supplied
`Executor`. A tokio implementation ships behind the default `tokio`
feature.

## Quick start

```toml
[dependencies]
tulisp = "0.32"
tulisp-async = "0.4"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust,ignore
use std::sync::Arc;
use tulisp::TulispContext;
use tulisp_async::TokioExecutor;

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut ctx = TulispContext::new();
    tulisp_async::register(&mut ctx, Arc::new(TokioExecutor::new()));

    let result = ctx.eval_string(r#"
        (setq fired 0)
        (run-with-timer 0.5 nil (lambda () (setq fired (1+ fired))))
        (sleep-for 0.7)
        fired
    "#).map_err(|e| format!("{e}"))?;
    println!("fired = {result}");  // 1
    Ok(())
}
```

Run the bundled example:

```bash
cargo run --example sleep
```

## Builtins

| Form | Description |
| --- | --- |
| `(timerp X)` | Predicate: `t` if `X` is a timer handle, else `nil`. |
| `(sleep-for SECS)` | Park the current lisp thread for `SECS` seconds, draining due timers along the way. |
| `(run-with-timer SECS REPEAT FN &rest ARGS)` | Fire `(FN ARGS…)` after `SECS`; if `REPEAT` is a positive number, re-fire every `REPEAT` seconds. `nil`, `0`, or any non-positive `REPEAT` means one-shot, and so does a `REPEAT` under half a nanosecond or too large to represent. Returns a timer handle. |
| `(cancel-timer H)` | Stop further firings of timer `H`. Returns `nil`. |

Timer bodies run on the calling `TulispContext` — the same one
the parent program runs on, so defuns, defvars, load state, and
error-trace filenames all carry through. `(sleep-for …)` drains
pending firings in deadline order while it waits, matching Emacs's
main-loop behavior.

## Driving from Rust

`register` returns a `Handle` that lets non-lisp callers drive the
timer queue without going through `(sleep-for …)`. Three methods, each
returning `Result<(), tulisp::Error>`, the error of a stopped body (see
the design notes below):

- `handle.tick(&mut ctx)?` — sync. Fires every body whose deadline has
  already passed; returns immediately when none remain.
- `handle.run_until_idle(&mut ctx).await?` — async (tokio feature).
  Awaits each task's deadline via `tokio::time::sleep`, fires, repeats
  until the mailbox is empty. Repeating timers re-push themselves, so
  the future runs until every timer self-cancels.
- `handle.run_for(&mut ctx, dur).await?` — async (tokio feature). Same
  drain loop but bounded — returns after `dur` regardless of pending
  firings beyond that window. A `dur` too large for an `Instant`, such
  as `Duration::MAX`, means no bound, as in `run_until_idle`.

`handle.set_body_error_handler(|ctx, err| …)` takes the error of each
body that fails without stopping the drain, which otherwise goes to
stderr.

`Handle` is `Clone` (shallow — clones share the same mailbox) and
`Send + Sync`, so it can travel into a spawned tokio task that
wants to tick the queue from elsewhere.

## Features

- `tokio` *(default)* — pulls in `tokio` and exposes `TokioExecutor`.

Disable defaults to depend only on the `Executor` trait and handle
types:

```toml
tulisp-async = { version = "0.4", default-features = false }
```

Add a new runtime by implementing `Executor` in your own crate (or
behind a feature here, mirroring `src/tokio.rs`).

## Design notes

- **Same-context firings.** `run-with-timer` pushes a pending firing
  onto a per-`register` mailbox; the drain helpers call the body on
  the calling `&mut ctx`. No fork, no `Arc<Mutex<TulispContext>>`.
- **Drained from `(sleep-for …)` or `Handle::tick`.** A program that
  registers a timer and immediately returns to Rust without ticking
  will not fire it. Either call `(sleep-for …)` (drains while waiting)
  or `Handle::tick(&mut ctx)` from Rust.
- **Runtime-agnostic core.** No tokio types appear in the public API
  outside the `tokio` module.
- **A stopped body ends the drain.** A timer body that ends in
  `quit`, which the context's interrupt check
  (`TulispContext::set_interrupt_check`) raises and the body can raise
  itself, or in the `Interrupted` error of an `Interrupt::Stop`, stops
  the drain, and its timer does not fire again, even if it repeats.
  `(sleep-for …)` passes the error on to its caller, and the `Handle`
  methods return it. So a body stopped inside another body's
  `(sleep-for …)` stops that body too, when the error ends it: a body
  can catch a `quit`, but no handler catches the `Interrupted` error.
  The other due timers stay queued. Other errors from a body go to the
  handler set with `set_body_error_handler`, or to stderr, and the
  drain goes on.

## Footguns

- **Long-running bodies stall the rest of the queue.** Firings are
  serialized on the calling lisp thread, so a body that takes longer
  than the next deadline shifts subsequent firings later. Same shape
  as Emacs's main loop blocking on a slow command.

## Testing

```bash
cargo build                            # default features (tokio on)
cargo build --no-default-features      # trait + types only
cargo test --test tulisp_async
cargo clippy --all-targets
```

Tests use `#[tokio::test(flavor = "multi_thread")]` — `sleep-for` blocks
the calling thread on a tokio-driven timer, which would deadlock a
single-worker runtime.

## License

GPL-3.0
