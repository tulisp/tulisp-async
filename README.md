# tulisp-async

Runtime-agnostic timer primitives for [tulisp](https://github.com/shsms/tulisp).

Wires Elisp-shaped timer builtins (`sleep-for`, `run-with-timer`,
`cancel-timer`) onto a `TulispContext`, backed by a host-supplied
`Executor`. A tokio implementation ships behind the default `tokio`
feature.

## Quick start

```toml
[dependencies]
tulisp-async = "0.1"
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
| `(run-with-timer SECS REPEAT FN &rest ARGS)` | Fire `(FN ARGS…)` after `SECS`; if `REPEAT` is a positive number, re-fire every `REPEAT` seconds. `nil`, `0`, or any non-positive `REPEAT` means one-shot. Returns a timer handle. |
| `(cancel-timer H)` | Stop further firings of timer `H`. Returns `nil`. |

Timer bodies run on the calling `TulispContext` — the same one
the parent program runs on, so defuns, defvars, load state, and
error-trace filenames all carry through. `(sleep-for …)` drains
pending firings in deadline order while it waits, matching Emacs's
main-loop behavior.

## Driving from Rust

`register` returns a `Handle` that lets non-lisp callers drive the
timer queue without going through `(sleep-for …)`. Three methods, each
returning `Result<(), tulisp::Error>`:

- `handle.tick(&mut ctx)?` — sync. Fires every body whose deadline has
  already passed; returns immediately when none remain.
- `handle.run_until_idle(&mut ctx).await?` — async (tokio feature).
  Awaits each task's deadline via `tokio::time::sleep`, fires, repeats
  until the mailbox is empty. Repeating timers re-push themselves, so
  the future runs until every timer self-cancels.
- `handle.run_for(&mut ctx, dur).await?` — async (tokio feature). Same
  drain loop but bounded — returns after `dur` regardless of pending
  firings beyond that window.

`Handle` is `Clone` (shallow — clones share the same mailbox) and
`Send + Sync`, so it can travel into a spawned tokio task that
wants to tick the queue from elsewhere.

## Features

- `tokio` *(default)* — pulls in `tokio` and exposes `TokioExecutor`.

Disable defaults to depend only on the `Executor` trait and handle
types:

```toml
tulisp-async = { version = "0.1", default-features = false }
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
