# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.3] — 2026-07-29

### Fixed

- **batch drain 分配公式修正。** `batch_size=1` 时用 `(batch+1)/2` 导致两次
  drain 仍各取 1，总计 2/每 tick。改用 `div_ceil(2)` / `batch/2` 分配，
  总回调数严格 ≤ batch_size。

### Added

- 3 个 0.3.2 修复的回归测试：failed remove 计数回滚、dropped_total 递增、
  failed insert 回滚。

## [0.3.2] — 2026-07-29

### Fixed

- **`remove(false)` 泄漏 cancelled 计数 (#1)。** `try_send` 失败时计数已递增
  但未回滚，导致永久残留一个 block。现在失败时回滚计数并递增 `dropped_total`。

- **`insert()` 无同步取消保护 (#3)。** `insert()` 原来不写入 cancelled 集合，
  同 ID 旧回调可能在 `Cmd::Insert` 被处理之前就触发。现在与 `remove()`/`reset()`
  一致，`insert()` 同步递增 cancelled 计数，`try_send` 失败时回滚。

- **`remove(false)` 不递增 `dropped_total` (#7)。** 修复后 `try_send` 失败时
  显式 `dropped.fetch_add(1)`。

- **`batch_size=1` 每 tick 可能触发 2 个回调 (#5)。** drain 上限从
  `max(batch/2, 1)` 改为 `half + extra` 分配（首段 ceiling 除，尾段 floor 除），
  确保每 tick 总回调数 ≤ batch_size。

### Changed

- **文档缩小取消保证范围 (#8)。** `remove()`/`reset()`/`insert()` 的保证描述
  加上 "up to the point the callback is spawned" 限定。

- **移除 `dispatch()` 方法。** `insert()` 不再复用 `dispatch()`，与
  `reset()`/`remove()` 采用统一的 try_send + rollback 模式。

## [0.3.1] — 2026-07-29

### Fixed

- **`test_remove_level1_task_insert_new` 时序敏感导致偶发失败 (#10)。** 测试中
  `advance` 同时驱动 tick 和命令处理，`select!` 交错顺序不确定。修复方案：
  `insert` 后用 `sleep` 排空命令队列，确保任务已调度后再推进时间。

## [0.3.0] — 2026-07-29

### Fixed

- **`insert()` replacement of same-ID could fire old callback (#3).** The
  `Cmd::Insert` handler now purges matching entries from the `pending` batch
  (via `pending.retain`) in the same way `Cmd::Reset` does.

- **Concurrent `remove`/`reset` of the same ID were unsafe (#1).** The
  cancellation set was a `HashSet` (boolean flag) that multiple concurrent
  calls could stomp. Replaced with `HashMap<T, usize>` reference counting —
  each call increments the count, each worker command decrements, and
  `drain()` performs a read-only check.

- **Missing `inserted_total` increment for `reset()` (#6).** `reset()` now
  explicitly increments the counter on a successful channel send.

- **`reset()` doc claimed marker retained on failure (#7).** Documentation
  updated to reflect the actual 0.2.1 rollback behaviour.

- **`batch_size` of 1 deadlocked callback execution (#8).** `drain` limit
  is now `max(batch_size / 2, 1)`; `start()` asserts `batch_size >= 1`.

- **Benchmarks did not compile (#9).** Fixed `use rotor::` → `use rotor_wheel::`.

### Changed

- **`remove()` returns `bool` again.** `false` means the async arena cleanup
  could not be enqueued; the cancellation guarantee still holds regardless.

- **Cancellation guarantee scope tightened (#4).** The guarantee covers
  callbacks up to the point they are spawned. Once inside `tokio::spawn`
  the callback cannot be intercepted. Documented in `drain()` and API docs.

- **Slow callback blocking documented (#5).** `drain()` awaits each batch
  in order. Long-running callbacks should spawn internally.

### Added

- 5 new regression tests: insert-replaces-pending, concurrent-remove-same-id,
  remove-returns-false-on-full, reset-increments-inserted, batch-size-one.

## [0.2.1] — 2026-07-29

### Fixed

- **Cancellation set leak for `remove()`.** `Cmd::Remove` handler did not clear
  the synchronous cancellation marker, causing every removed ID to permanently
  occupy an entry in the shared set. Now cleaned up in both the main loop and
  the shutdown drain path.

- **`reset()` cancelled-set rollback on channel full.** When `try_send` fails,
  the cancellation marker is now removed so that the lost command does not leave
  a stale entry that could incorrectly block a future retry.

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
