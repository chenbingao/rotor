# rotor

[![Crates.io](https://img.shields.io/crates/v/rotor)](https://crates.io/crates/rotor)
[![License](https://img.shields.io/crates/l/rotor)](LICENSE)

General-purpose hierarchical timing wheel for Rust async runtimes.

Inspired by Netty's `HashedWheelTimer` — single-threaded core, lazy deletion
with generation counters, clock compensation, and concurrent batch processing.

## When to use

- **Extend deadlines**: `reset()` to push the expiry further — heartbeat, request
  progress, lease renewal.  O(1), old slot copies lazily discarded.
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
rotor-wheel = "0.1"
```

## Quick start

```rust
use std::time::Duration;
use rotor_wheel::{TimingWheel, WheelConfig};

let (wheel, _guard) = TimingWheel::start(
    WheelConfig::default(),               // 64 slots × 1s = 64s window
    |id: String| async move { println!("{id} expired") },
);

// One-shot delay
wheel.insert("req-001".into(), Duration::from_secs(10));

// Extend deadline — reset countdown from now
wheel.reset("conn-002".into(), Duration::from_secs(60));

// Cancel
wheel.remove(&"req-001".to_string());

// Graceful shutdown (fires pending callbacks)
wheel.shutdown();
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `tick_interval` | 1 s | Duration of one Level-0 tick |
| `batch_size` | 500 | Max callback spawns per tick |
| `channel_capacity` | 65536 | Command channel buffer size |

3-level wheel, 64 slots per level.  Timeout window: 64 s (L0), ~68 min (L1), ~73 h (L2).

## How it works

```
Commands (insert / reset / remove)
    |
    v   mpsc channel
+----------------------------+
| Worker thread              |
|                            |
|  +---+---+-------+---+     |  interval.tick()
|  | 0 | 1 | ...   |63 |     |  fires every tick_interval
|  +---+---+-------+---+     |
|    ^                       |
|  current_tick              |  advance() -> sweep
|                            |  cascade L2->L1->L0
|  task_info: HashMap        |  expire_tick + generation
|  arena: Vec<TaskEntry>     |
+----------------------------+
    |
    v   tokio::spawn per callback
  expired tasks (batch_size limit)
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
