# rotor-wheel

[![Crates.io](https://img.shields.io/crates/v/rotor-wheel)](https://crates.io/crates/rotor-wheel)
[![License](https://img.shields.io/crates/l/rotor-wheel)](LICENSE)

General-purpose hierarchical timing wheel for Rust async runtimes.

Inspired by Netty's `HashedWheelTimer` — single-threaded core, synchronous
cancellation, clock compensation, and concurrent batch processing.

## When to use

- **Request timeouts**: wrap a request ID, cancel with `remove()` on success.
  The callback is blocked up to the point where the worker checks the cancelled
  set; once the check has passed it cannot be intercepted.
- **Extend deadlines**: `reset()` pushes the expiry further — heartbeat, request
  progress, lease renewal.  O(1); old callbacks are blocked up to the cancelled-set
  check.

For simple `tokio::time::sleep` + `tokio::spawn` patterns, this library is
overkill. It shines when you have **thousands of concurrent timers** that
need O(1) refresh.

## Features

- **Guaranteed cancellation** — `remove()` and `reset()` synchronously register
  a cancellation marker; callbacks are blocked up to the point the worker checks
  the cancelled set.
- **Per-task timeout** — every `insert` / `reset` takes an explicit `Duration`.
- **Clock compensation** — catches up after GC pauses or system load spikes.
- **Batch processing** — limits callback spawns per tick to avoid runtime overload.
- **Shutdown drain** — fires pending callbacks on graceful shutdown.
- **Generic** — works with any `T: Eq + Hash + Clone + Send + Debug + 'static`.

## Installation

```toml
[dependencies]
rotor-wheel = "0.4"
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
  |  id_map: HashMap           |  id → arena index
  |  arena: Vec<TaskEntry>     |
+----------------------------+
    |
    v   tokio::spawn per callback
  expired tasks (batch_size limit)
```

- **insert / reset**: allocates an arena slot and pushes its index into the
  target bucket.  `reset` bumps the generation and assigns a new arena slot;
  the old slot becomes unreachable from `id_map`.
- **advance**: drains the bucket at `current_tick`.  Looks up the task ID in
  `id_map`: if missing (removed) or the arena index has changed (stale copy
  from an earlier `reset`), the slot is reclaimed and no callback fires.
  Otherwise the task has expired → callback fires.
- **drain**: before spawning each callback, checks a shared `cancelled` set.
  `remove()` and `reset()` synchronously insert into this set; callbacks are
  blocked up to the point where `drain()` performs this check.
- **clock compensation**: `elapsed / tick_interval` gives the target
  tick; the worker catches up if it falls behind.

## Performance

Benchmarks run on Apple M1, 50 000 one-shot tasks (64 slots, 10 ms tick):

| Metric | v0.3.0 | v0.2.0 |
|--------|--------|--------|
| Insert 50k (usize) | ~3.0 ms | ~3.0 ms |
| Reset 10k (usize) | ~2.3 ms | ~0.7 ms |
| Expire 10k (usize, 100ms delay) | ~501 ms | ~501 ms |
| 10k heartbeat refresh | 0 false expirations | 0 false expirations |
| Memory (50k active) | stable under churn | stable under churn |

Note: `reset` latency increased from ~70 ns to ~230 ns per call in v0.3.0
due to the synchronous cancellation set (lock + HashMap insertion).  The
`drain` hot path is unaffected and actually faster (read-only map check
instead of write-and-remove).

Run the stress tests yourself:

```bash
cargo test stress -- --test-threads=1 --nocapture
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
