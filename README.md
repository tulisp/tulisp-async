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
    "#).map_err(|e| format!("{}", e.format(&ctx)))?;
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
| `(sleep-for SECS)` | Park the current lisp thread for `SECS` seconds. |
| `(run-with-timer SECS REPEAT FN)` | Fire `FN` after `SECS`; if `REPEAT` is a number, re-fire every `REPEAT` seconds. Returns a timer handle. |
| `(cancel-timer H)` | Stop further firings of timer `H`. |

Parent-defined `defun`s and `setq`'d globals are visible inside timer
bodies — each timer runs in a fresh `TulispContext`, but tulisp
symbols carry their global bindings independently of the context.

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

- **Timers are pool-backed.** `run-with-timer` schedules through
  `Executor::schedule_after`, so a thousand idle timers don't cost a
  thousand threads.
- **Runtime-agnostic core.** No tokio types appear in the public API
  outside the `tokio` module.

## Footguns

- `(setq x (1+ x))` from concurrent timer bodies is **not atomic**.
  Tulisp's per-symbol read and write are thread-safe under `sync`, but
  read-modify-write is racy. For shared counters, use a Rust-backed
  defun over an atomic, or serialize updates through a single dedicated
  timer.

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
