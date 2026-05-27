# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
