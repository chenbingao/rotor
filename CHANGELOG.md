# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] — 2026-07-29

### Fixed

- **`remove()` arena slot premature reuse.** `remove()` used to immediately push the
  freed arena index back to the free-list, but stale bucket entries in levels 0–2
  still held that index. A subsequent `insert` could reuse the slot, causing the old
  bucket to incorrectly fire the new task's callback when it drained. The fix defers
  freeing: `remove()` now only deletes the `id_map` entry; `process_slot` reclaims
  the arena slot naturally when the old bucket drains and the `id_map` lookup misses.

- **Broken generation staleness check in `process_slot`.** The comparison
  `entry.generation != self.arena[idx].generation` was a self-comparison (both read
  the same arena element) and could never evaluate to `true`. The check is now
  simplified to `*current_info != idx`, which correctly detects stale entries from
  `reset`-before-drain scenarios using the `id_map` indirection.

### Added

- 4 new regression tests for the `remove`-then-`insert` arena-reuse scenario:
  different-ID reinsert, same-ID reinsert, level-1 task removal, and churn stress.

- **Synchronous cancellation guarantee for `remove()` and `reset()`.** A shared
  `Arc<Mutex<HashSet<T>>>` cancellation set is now checked in `drain()` before
  spawning any callback. `remove()` and `reset()` insert into this set
  synchronously, so once the call returns the old callback is guaranteed not to
  fire — even if it was already queued in the pending batch.

### Changed

- **`remove()` no longer returns `bool`.** The previous `bool` only indicated
  whether the async command was enqueued, which was misleading now that the
  cancellation itself is synchronous. The method signature changed from
  `fn remove(&self, &T) -> bool` to `fn remove(&self, &T)`.

- **`reset()` now guarantees the previous callback will not fire**, matching
  `remove()` in strength. Internally, `reset` inserts into the cancellation set
  and the worker purges any matching entry from the pending batch.

## [0.1.0] — 2026-05-27

### Added

- 3-level hierarchical timing wheel (L2 hours / L1 minutes / L0 seconds) with 64 slots per level.
- Arena-based task storage with free-list reuse — slots store indices, not ID copies.
- Lazy deletion via generation counters — O(1) `reset()` at high frequency.
- Clock compensation — catches up after GC pauses or system load spikes.
- Batch-limited callback spawning — prevents runtime overload on tick bursts.
- Graceful shutdown with full drain of pending callbacks.
- `WheelConfig` for customising tick interval, batch size, and channel capacity.
- `Metrics` counters: `active_tasks`, `inserted_total`, `dropped_total`, `expirations_total`.
- Callback panic isolation — panics are logged and swallowed, never crash the worker.
- Public API: `TimingWheel::insert`, `reset`, `remove`, `shutdown`.
- 6 unit tests covering basic operations and edge cases.
- 11 stress tests covering 50k one-shot, 10k heartbeat, concurrent mixed workload,
  channel throughput, callback panic resilience, shutdown drain, arena reuse,
  sustained heartbeat, and 3-hour delay cascading.
- Criterion benchmarks for insert, reset, and expire throughput.
- Full rustdoc on all public items with `# Examples` sections.
