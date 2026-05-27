# rotor

[![Crates.io](https://img.shields.io/crates/v/rotor)](https://crates.io/crates/rotor)
[![License](https://img.shields.io/crates/l/rotor)](LICENSE)

General-purpose hierarchical timing wheel for Rust async runtimes.

Inspired by Netty's `HashedWheelTimer` — single-threaded core, lazy deletion
with generation counters, clock compensation, and concurrent batch processing.

## When to use

- **Heartbeat / keep-alive management**: `reset()` on every ping — O(1), old
  slot entries are lazily discarded.
- **One-shot delays**: `insert()` a task once, callback fires after timeout,
  no cleanup needed.
- **Request timeouts**: wrap a request ID, cancel with `remove()` on success.

For simple `tokio::time::sleep` + `tokio::spawn` patterns, this library is
overkill. It shines when you have **thousands of concurrent timers** that
need O(1) refresh.

## Features

- **O(1) reset** — lazy deletion with generation counters keeps the hot path fast.
- **Per-task timeout** — every `insert` / `reset` takes an explicit `Duration`.
- **Clock compensation** — catches up after GC pauses or system load spikes.
- **Batch processing** — limits callback spawns per tick to avoid runtime overload.
- **Shutdown drain** — fires pending callbacks on graceful shutdown.
- **Generic** — works with any `T: Eq + Hash + Clone + Send + Debug + 'static`.

## Installation

```toml
[dependencies]
rotor = "0.1"
```

## Quick start

```rust
use std::time::Duration;
use rotor::{TimingWheel, WheelConfig};

let (wheel, _guard) = TimingWheel::start(
    WheelConfig::default(),               // 64 slots × 1s = 64s window
    |id: String| async move { println!("{id} expired") },
);

// One-shot delay
wheel.insert("req-001".into(), Duration::from_secs(10));

// Heartbeat — call on every ping
wheel.reset("conn-002".into(), Duration::from_secs(60));

// Cancel
wheel.remove(&"req-001".to_string());

// Graceful shutdown (fires pending callbacks)
wheel.shutdown();
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `wheel_size` | 64 | Number of slots |
| `tick_interval` | 1 s | Duration per slot |
| `batch_size` | 200 | Max callback spawns per tick |
| `channel_capacity` | 10 000 | Command channel buffer size |

Timeout window = `wheel_size × tick_interval` (default: 64 s).
Maximum delay = `wheel_size × 2` (default: 128 s).

## How it works

```
Commands (insert / reset / remove)
    │
    ▼  mpsc channel (single producer per clone)
┌──────────────────┐
│  Worker thread   │
│                  │
│  ┌───┬───┬───┐   │  interval.tick() fires every tick_interval
│  │ 0 │ 1 │… │63│   │  ← 64 slots, each holds Vec<Scheduled<T>>
│  └───┴───┴───┘   │
│    ↑              │
│  current_tick     │  advance() → sweep slot, drain expired
│                  │
│  HashMap<T, Info> │  task_info: expire_tick + generation
└──────────────────┘
    │
    ▼  tokio::spawn per callback (bounded by batch_size)
  expired tasks
```

- **insert / reset**: pushes a `Scheduled { id, generation }` into the
  target slot, bumps the generation in `task_info`.
- **advance**: drains the slot at `current_tick`.  If `task_info` still
  holds the matching generation → task has expired → callback fires.
  Old copies from earlier `reset`s have a stale generation and are
  silently discarded.
- **clock compensation**: `elapsed / tick_interval` gives the target
  tick; the worker catches up if it falls behind.

## Performance

Benchmarks run on Apple M1, 50 000 one-shot tasks (256 slots, 20ms tick):

| Metric | Value |
|--------|-------|
| Insert throughput | ~3 000 000 tasks/s |
| Expiry accuracy | >95% within timeout window |
| 10k heartbeat refresh | 0 false expirations over 10s |
| Memory (50k active) | stable over 15s continuous churn |

Run the stress tests yourself:

```bash
cargo test stress -- --test-threads=1 --nocapture
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
