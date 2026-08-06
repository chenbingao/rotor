# rotor-wheel

[![Crates.io](https://img.shields.io/crates/v/rotor-wheel)](https://crates.io/crates/rotor-wheel)
[![License](https://img.shields.io/crates/l/rotor-wheel)](LICENSE)

General-purpose hierarchical timing wheel for Rust async runtimes.

Inspired by Netty's `HashedWheelTimer` — single-threaded core, synchronous
callback execution, guaranteed cancellation, clock compensation.

## When to use

- **Heartbeat / keep-alive**: `reset()` on every ping — O(1), lazy deletion.
- **Request timeouts**: wrap a request ID, cancel with `remove()` on success.
- **Lease renewal**: `reset()` pushes the expiry, O(1) per renewal.

For simple `tokio::time::sleep` + `tokio::spawn` patterns, this library is
overkill.  It shines when you have **thousands of concurrent timers** that
need O(1) refresh.

## Features

- **Netty-style synchronous callbacks** — callbacks run directly in the worker
  thread.  Keep them fast (single-digit microseconds).  For I/O work, send the
  ID through a channel to a dedicated async task.
- **Guaranteed cancellation** — `remove()` and `reset()` synchronously register
  a cancellation marker; callbacks are blocked up to the point the worker checks
  the cancelled set.
- **Per-task timeout** — every `insert` / `reset` takes an explicit `Duration`.
- **Clock compensation** — catches up after GC pauses or system load spikes.
- **Batch-limited drain** — limits callbacks per tick to keep the event loop
  responsive.
- **Shutdown drain** — fires pending callbacks on graceful shutdown.
- **Generic** — works with any `T: Eq + Hash + Clone + Send + 'static`.

## Installation

```toml
[dependencies]
rotor-wheel = "0.5"
```

## Quick start

> **Migration from 0.4.x**: callbacks are now synchronous.  Replace
> `|id| async move { ... }` with `|id| { ... }`.  For async work, use a channel.
> See [Callbacks must be fast](#callbacks-must-be-fast) below.

```rust
use std::time::Duration;
use rotor_wheel::{TimingWheel, WheelConfig};

let (wheel, _guard) = TimingWheel::start(
    WheelConfig::default(),
    |id: String| println!("{id} timed out"),
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

### Callbacks must be fast

The callback runs synchronously in the worker thread.  Keep it light — log,
increment a counter, push to a Vec.  For async I/O (network close, DB write),
use a channel:

```rust
use tokio::sync::mpsc;

let (tx, mut rx) = mpsc::unbounded_channel();

let (wheel, _guard) = TimingWheel::start(
    WheelConfig::default(),
    move |conn_id: u64| {
        let _ = tx.send(conn_id); // ~100 ns
    },
);

// Dedicated task handles the real work
tokio::spawn(async move {
    while let Some(conn_id) = rx.recv().await {
        conns.remove(&conn_id).unwrap().close().await;
    }
});
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `tick_interval` | 1 s | Duration of one Level-0 tick |
| `batch_size` | 500 | Max callbacks executed per tick drain |
| `channel_capacity` | 65536 | Command channel buffer size |

3-level wheel, 64 slots per level.  Timeout window: 64 s (L0), ~68 min (L1), ~73 h (L2).

## How it works

```
Commands (insert / reset / remove)
    |
    v   mpsc channel
+----------------------------+
| Worker                     |
|                            |
|  +---+---+-------+---+     |  interval.tick()
|  | 0 | 1 | ...   |63 |     |  fires every tick_interval
|  +---+---+-------+---+     |
|    ^                       |
|  current_tick              |  advance() -> sweep
|                            |  cascade L2->L1->L0
|  id_map: HashMap            |  id -> arena index
|  arena: Vec<TaskEntry>     |
+----------------------------+
    |
    v   synchronous callback(id)
  expired tasks (batch_size limit)
```

- **insert / reset**: allocates an arena slot and pushes its index into the
  target bucket.  A new slot is allocated for every schedule; old slots are
  lazily reclaimed when their containing bucket drains.
- **advance**: drains the bucket at `current_tick`.  Looks up each task's ID
  in `id_map`: if missing (removed) or the arena index has changed (stale
  copy from a later `reset`), the slot is freed.  Otherwise the task has
  expired → callback fires.
- **drain**: pops expired IDs from the pending queue and calls the user's
  callback synchronously.  Before each call, checks a shared `cancelled`
  set — `remove()` and `reset()` insert into this set to block callbacks.
  The guarantee holds up to the point of this check.
- **shutdown**: calls `drain_all()` to expire every remaining task (O(192)
  bucket iterations, not O(262k) tick iterations), then drains any commands
  still in the channel.
- **clock compensation**: `elapsed / tick_interval` gives the target tick;
  the worker catches up if it falls behind, breaking every 10 ticks to stay
  responsive to commands.

## Metrics

```rust
let (wheel, _guard) = TimingWheel::start(config, callback);

wheel.active_tasks();      // tasks currently tracked
wheel.inserted_total();    // cumulative insert + reset calls
wheel.dropped_total();     // commands rejected (channel full)
wheel.expirations_total(); // callbacks that succeeded
wheel.abnormal_total();    // callbacks that panicked
wheel.pending_len();       // expired callbacks waiting to execute
```

## Performance

Benchmarks run on Apple M1, 50 000 one-shot tasks (64 slots, 10 ms tick):

| Metric | v0.5.0 | v0.4.2 |
|--------|--------|--------|
| Insert 50k (usize, 30s delay) | ~8.5 ms | ~11.9 ms |
| Expire 10k (usize, 100ms delay) | ~500 ms | ~500 ms |
| 10k heartbeat refresh | 0 false expirations | 0 false expirations |
| Memory (50k active) | stable under churn | stable under churn |

Run benchmarks yourself:

```bash
cargo bench --bench throughput
```

Run stress tests:

```bash
cargo test stress -- --test-threads=1 --nocapture
```

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option.
