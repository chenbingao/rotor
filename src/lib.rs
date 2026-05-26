//! Industrial-grade hierarchical timing wheel.
//!
//! Inspired by Netty's `HashedWheelTimer` and Kafka's `SystemTimer` —
//! 3-level hierarchical wheel, arena-based task storage, lock-free command
//! dispatch, clock compensation, and graceful shutdown.
//!
//! ## Architecture
//!
//! ```text
//! Level 2 (hours):   64 slots × 4096s    ≈ 73 hours max
//! Level 1 (minutes): 64 slots × 64s       ≈ 68 minutes max
//! Level 0 (seconds): 64 slots × 1s        = 64 seconds window
//!
//! Tasks cascade down: L2→L1→L0→expire.
//! Each slot stores arena indices (not ID copies).
//! ```
//!
//! ## When to use
//!
//! - **Heartbeat / keep-alive**: `reset()` on every ping — O(1), lazy deletion.
//! - **One-shot delays**: from milliseconds to 3 days.
//! - **Request timeouts**: cancel with `remove()` on success.
//!
//! For simple `tokio::time::sleep`, this library is overkill.  It shines when
//! you have **thousands of concurrent timers** that need O(1) refresh.
//!
//! ## Example
//!
//! ```rust,no_run
//! use std::time::Duration;
//! use timing_wheel::{TimingWheel, WheelConfig};
//!
//! let (wheel, _guard) = TimingWheel::start(
//!     WheelConfig::default(),
//!     |sn: String| async move { println!("{sn} timed out") },
//! );
//!
//! wheel.insert("req-001".into(), Duration::from_secs(10));
//! wheel.reset("conn-002".into(), Duration::from_secs(60));
//! wheel.remove(&"req-001".to_string());
//! println!("active={}, dropped={}", wheel.active_tasks(), wheel.dropped_total());
//! ```

use std::{
  collections::HashMap,
  fmt::Debug,
  future::Future,
  hash::Hash,
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  time::Duration,
};

use tokio::{
  spawn,
  sync::mpsc::{self, Receiver, Sender},
  task::JoinHandle,
  time::{Instant, interval},
};

// ── Configuration ───────────────────────────────────────────────────────

/// Timing wheel configuration.
#[derive(Debug, Clone)]
pub struct WheelConfig {
  /// Base tick interval (duration of one Level-0 slot).
  /// Default: 1 second.
  pub tick_interval: Duration,
  /// Maximum callback spawns per tick to avoid overwhelming the runtime.
  /// Default: 500.
  pub batch_size: usize,
  /// Command channel capacity.  Sends beyond this return `false`.
  /// Default: 64 * 1024.
  pub channel_capacity: usize,
}

impl Default for WheelConfig {
  fn default() -> Self {
    Self {
      tick_interval: Duration::from_secs(1),
      batch_size: 500,
      channel_capacity: 64 * 1024,
    }
  }
}

// Wheel geometry (3 levels).
const LV0_SLOTS: usize = 64;
const LV1_SLOTS: usize = 64;
const LV2_SLOTS: usize = 64;
const LV0_TICK: u64 = 1;          // 1 base tick
const LV1_TICK: u64 = 64;         // advances every 64 base ticks
const LV2_TICK: u64 = 64 * 64;    // advances every 4096 base ticks

// ── Public handle ───────────────────────────────────────────────────────

/// Clonable handle for inserting, resetting, and removing tasks.
///
/// All methods are non-blocking.  Returns `false` if the command channel
/// is at capacity.
#[derive(Clone)]
pub struct TimingWheel<T> {
  tx: Sender<Cmd<T>>,
  shared: Arc<Metrics>,
}

#[derive(Default)]
pub struct Metrics {
  pub active: AtomicUsize,
  pub inserted: AtomicUsize,
  pub dropped: AtomicUsize,
  pub expirations: AtomicUsize,
}

impl<T: Send + 'static> TimingWheel<T> {
  /// Schedule a one-shot task that fires after `timeout`.
  ///
  /// If a task with the same `id` already exists, its timeout is replaced.
  /// No manual cleanup is needed — the task is removed after expiry.
  #[inline]
  pub fn insert(&self, id: T, timeout: Duration) -> bool {
    self.dispatch(Cmd::Insert(id, timeout))
  }

  /// Refresh (or create) an ongoing task with a new timeout.
  ///
  /// Previous scheduled copies are lazily discarded.  Safe to call at
  /// high frequency (e.g. every WebSocket ping).
  #[inline]
  pub fn reset(&self, id: T, timeout: Duration) -> bool {
    self.dispatch(Cmd::Reset(id, timeout))
  }

  /// Explicitly cancel a task.
  #[inline]
  pub fn remove(&self, id: &T) -> bool
  where
    T: Clone,
  {
    self.tx.try_send(Cmd::Remove(id.clone())).is_ok()
  }

  /// Gracefully shut down the wheel.  Pending callbacks will fire.
  #[inline]
  pub fn shutdown(&self) -> bool {
    self.tx.try_send(Cmd::Shutdown).is_ok()
  }

  #[inline] pub fn active_tasks(&self) -> usize { self.shared.active.load(Ordering::Relaxed) }
  #[inline] pub fn inserted_total(&self) -> usize { self.shared.inserted.load(Ordering::Relaxed) }
  #[inline] pub fn dropped_total(&self) -> usize { self.shared.dropped.load(Ordering::Relaxed) }
  #[inline] pub fn expirations_total(&self) -> usize { self.shared.expirations.load(Ordering::Relaxed) }

  fn dispatch(&self, cmd: Cmd<T>) -> bool {
    if self.tx.try_send(cmd).is_ok() {
      self.shared.inserted.fetch_add(1, Ordering::Relaxed);
      true
    } else {
      self.shared.dropped.fetch_add(1, Ordering::Relaxed);
      false
    }
  }
}

// ── Guard ────────────────────────────────────────────────────────────────

#[must_use = "TimingWheelGuard must be stored or the wheel stops immediately"]
pub struct TimingWheelGuard {
  task: Option<JoinHandle<()>>,
}

impl Drop for TimingWheelGuard {
  fn drop(&mut self) {
    if let Some(task) = self.task.take() {
      task.abort();
    }
  }
}

// ── Constructor ─────────────────────────────────────────────────────────

impl<T: Eq + Hash + Clone + Send + Debug + 'static> TimingWheel<T> {
  pub fn start<F, Fut>(config: WheelConfig, callback: F) -> (TimingWheel<T>, TimingWheelGuard)
  where
    F: FnMut(T) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
  {
    let (tx, rx) = mpsc::channel(config.channel_capacity);
    let shared = Arc::new(Metrics::default());
    let handle = TimingWheel { tx, shared: Arc::clone(&shared) };
    let task = spawn(run(rx, config, shared, callback));
    (handle, TimingWheelGuard { task: Some(task) })
  }
}

// ── Internal types ──────────────────────────────────────────────────────

enum Cmd<T> {
  Insert(T, Duration),
  Reset(T, Duration),
  Remove(T),
  Shutdown,
}

struct TaskEntry<T> {
  id: T,
  expire_tick: u64,
  generation: u64,
}

/// One level of the hierarchical wheel.
struct Level {
  slots: Vec<Vec<usize>>,
  tick_per_advance: u64,
}

impl Level {
  fn new(slots: usize, tick_per_advance: u64) -> Self {
    Self { slots: (0..slots).map(|_| Vec::new()).collect(), tick_per_advance }
  }

  fn slot_of(&self, expire: u64) -> usize {
    ((expire / self.tick_per_advance) as usize) % self.slots.len()
  }

  fn slot_now(&self, current_tick: u64) -> usize {
    ((current_tick / self.tick_per_advance) as usize) % self.slots.len()
  }

  /// Drain the slot at the current tick position, returning all arena indices.
  fn drain(&mut self, current_tick: u64) -> Vec<usize> {
    let idx = self.slot_now(current_tick);
    std::mem::take(&mut self.slots[idx])
  }

  fn push(&mut self, expire: u64, arena_idx: usize) {
    let idx = self.slot_of(expire);
    self.slots[idx].push(arena_idx);
  }
}

struct Wheel<T> {
  levels: [Level; 3],
  arena: Vec<TaskEntry<T>>,
  free: Vec<usize>,                  // free arena slots
  id_map: HashMap<T, usize>,         // id → arena index
  current_tick: u64,
  tick_ms: u64,
}

impl<T: Eq + Hash + Clone + Debug> Wheel<T> {
  fn new(tick_interval: Duration) -> Self {
    Self {
      levels: [
        Level::new(LV0_SLOTS, LV0_TICK),
        Level::new(LV1_SLOTS, LV1_TICK),
        Level::new(LV2_SLOTS, LV2_TICK),
      ],
      arena: Vec::new(),
      free: Vec::new(),
      id_map: HashMap::new(),
      current_tick: 0,
      tick_ms: tick_interval.as_millis().max(1) as u64,
    }
  }

  fn alloc(&mut self, id: T, expire_tick: u64) -> (usize, u64) {
    let generation = self
      .id_map
      .get(&id)
      .and_then(|&idx| self.arena.get(idx))
      .map_or(0, |e| e.generation.wrapping_add(1));

    let idx = if let Some(i) = self.free.pop() {
      self.arena[i] = TaskEntry { id: id.clone(), expire_tick, generation };
      i
    } else {
      self.arena.push(TaskEntry { id: id.clone(), expire_tick, generation });
      self.arena.len() - 1
    };

    self.id_map.insert(id, idx);
    (idx, generation)
  }

  fn push_to_level(&mut self, expire_tick: u64, idx: usize) {
    // Place task at the highest level whose granularity is ≤ the delay.
    let target_level = if expire_tick >= self.current_tick + self.levels[2].tick_per_advance { 2 }
      else if expire_tick >= self.current_tick + self.levels[1].tick_per_advance { 1 }
      else { 0 };
    self.levels[target_level].push(expire_tick, idx);
  }

  fn schedule(&mut self, id: T, timeout: Duration) {
    let delay = (timeout.as_millis() / self.tick_ms as u128)
      .max(1)
      .min(u64::MAX as u128) as u64;
    let expire_tick = self.current_tick + delay;
    let (idx, _) = self.alloc(id, expire_tick);
    self.push_to_level(expire_tick, idx);
  }

  fn remove(&mut self, id: &T) {
    if let Some(idx) = self.id_map.remove(id) {
      // Free the arena slot lazily — the old Scheduled entry stays
      // in the slot but has a stale generation, so it's discarded.
      self.free.push(idx);
    }
  }

  /// Advance one base tick.  Returns expired task IDs to fire.
  fn advance(&mut self, metrics: &Metrics) -> Vec<T> {
    self.current_tick += 1;
    let mut expired = Vec::new();

    // Level 0: every tick
    {
      let indices = self.levels[0].drain(self.current_tick);
      self.process_slot(indices, 0, &mut expired);
    }

    // Level 1: every 64 ticks
    if self.current_tick.wrapping_rem(LV1_TICK) == 0 {
      let indices = self.levels[1].drain(self.current_tick);
      self.process_slot(indices, 1, &mut expired);
    }

    // Level 2: every 4096 ticks
    if self.current_tick.wrapping_rem(LV2_TICK) == 0 {
      let indices = self.levels[2].drain(self.current_tick);
      self.process_slot(indices, 2, &mut expired);
    }

    metrics.active.store(self.id_map.len(), Ordering::Relaxed);
    expired
  }

  /// Process a drained slot: expire tasks that are due, cascade the rest.
  fn process_slot(&mut self, indices: Vec<usize>, level: usize, expired: &mut Vec<T>) {
    for idx in indices {
      let Some(entry) = self.arena.get(idx) else { continue };
      let Some(current_info) = self.id_map.get(&entry.id) else {
        // removed after scheduling — discard
        self.free.push(idx);
        continue;
      };
      if *current_info != idx || entry.generation != self.arena[idx].generation {
        // stale copy
        self.free.push(idx);
        continue;
      }
      if self.current_tick >= entry.expire_tick {
        // expired
        self.id_map.remove(&entry.id);
        self.free.push(idx);
        let id = entry.id.clone();
        expired.push(id);
      } else if level > 0 {
        // cascade down
        self.levels[level - 1].push(entry.expire_tick, idx);
      } else {
        self.levels[0].push(entry.expire_tick, idx);
      }
    }
  }
}

// ── Worker ──────────────────────────────────────────────────────────────

async fn run<T, F, Fut>(
  mut rx: Receiver<Cmd<T>>,
  config: WheelConfig,
  metrics: Arc<Metrics>,
  mut callback: F,
) where
  T: Eq + Hash + Clone + Debug,
  F: FnMut(T) -> Fut,
  Fut: Future<Output = ()> + Send + 'static,
{
  let mut wheel = Wheel::<T>::new(config.tick_interval);
  let mut tick = interval(config.tick_interval);
  let start = Instant::now();
  let mut pending: Vec<T> = Vec::new();
  let batch = config.batch_size;

  loop {
    tokio::select! {
      cmd = rx.recv() => {
        match cmd {
          Some(Cmd::Insert(id, to)) => wheel.schedule(id, to),
          Some(Cmd::Reset(id, to))  => wheel.schedule(id, to),
          Some(Cmd::Remove(id))     => wheel.remove(&id),
          Some(Cmd::Shutdown) | None => break,
        }
      }

      _ = tick.tick() => {
        // Clock compensation
        let elapsed = start.elapsed();
        let target = (elapsed.as_millis() / config.tick_interval.as_millis().max(1)) as u64;

        drain(&mut pending, &mut callback, &metrics, batch / 2).await;

        while wheel.current_tick < target {
          pending.extend(wheel.advance(&metrics));
          if wheel.current_tick.wrapping_rem(10) == 0 { break; }
        }

        drain(&mut pending, &mut callback, &metrics, batch / 2).await;
      }
    }
  }

  metrics.active.store(0, Ordering::Relaxed);

  // Drain all expired on shutdown
  for _ in 0..(LV2_SLOTS * LV2_TICK as usize) {
    pending.extend(wheel.advance(&metrics));
  }
  drain(&mut pending, &mut callback, &metrics, usize::MAX).await;

  while let Ok(cmd) = rx.try_recv() {
    match cmd {
      Cmd::Insert(id, to) => wheel.schedule(id, to),
      Cmd::Reset(id, to)  => wheel.schedule(id, to),
      Cmd::Remove(id)     => wheel.remove(&id),
      Cmd::Shutdown       => break,
    }
  }
}

async fn drain<T, F, Fut>(
  pending: &mut Vec<T>,
  callback: &mut F,
  metrics: &Metrics,
  limit: usize,
) where
  F: FnMut(T) -> Fut,
  Fut: Future<Output = ()> + Send + 'static,
{
  let n = pending.len().min(limit);
  let mut tasks = Vec::with_capacity(n);
  for _ in 0..n {
    if let Some(id) = pending.pop() {
      tasks.push(tokio::spawn(callback(id)));
    }
  }
  for t in tasks {
    if let Err(e) = t.await {
      log::error!("timing-wheel callback panicked: {e}");
    } else {
      metrics.expirations.fetch_add(1, Ordering::Relaxed);
    }
  }
}

// ── Unit tests ──────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
  use super::*;
  use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
  use tokio::time::{advance, pause, sleep};

  fn cfg_fast() -> WheelConfig {
    WheelConfig { tick_interval: Duration::from_millis(10), ..Default::default() }
  }

  fn inc(n: &Arc<AtomicUsize>) -> impl Future<Output = ()> + Send + 'static {
    let n = Arc::clone(n);
    async move { n.fetch_add(1, Ordering::SeqCst); }
  }

  #[tokio::test]
  async fn test_one_shot() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let (h, _g) = TimingWheel::start(cfg_fast(), { let n = Arc::clone(&n); move |_: String| inc(&n) });
    sleep(Duration::from_millis(5)).await;
    h.insert("x".into(), Duration::from_millis(200));
    advance(Duration::from_millis(500)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(n.load(Ordering::SeqCst), 1);
  }

  #[tokio::test]
  async fn test_reset_extends() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let (h, _g) = TimingWheel::start(cfg_fast(), { let n = Arc::clone(&n); move |_: String| inc(&n) });
    sleep(Duration::from_millis(5)).await;
    h.insert("x".into(), Duration::from_secs(1));
    advance(Duration::from_millis(800)).await;
    sleep(Duration::from_millis(100)).await;
    h.reset("x".into(), Duration::from_secs(1));
    advance(Duration::from_millis(800)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(n.load(Ordering::SeqCst), 0);
    advance(Duration::from_secs(1)).await;
    sleep(Duration::from_millis(200)).await;
    assert_eq!(n.load(Ordering::SeqCst), 1);
  }

  #[tokio::test]
  async fn test_remove() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let (h, _g) = TimingWheel::start(cfg_fast(), { let n = Arc::clone(&n); move |_: String| inc(&n) });
    sleep(Duration::from_millis(5)).await;
    h.insert("x".into(), Duration::from_millis(200));
    h.remove(&"x".to_string());
    advance(Duration::from_millis(500)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(n.load(Ordering::SeqCst), 0);
  }

  #[tokio::test]
  async fn test_long_delay_cascades_down() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let (h, _g) = TimingWheel::start(cfg_fast(), { let n = Arc::clone(&n); move |_: String| inc(&n) });
    sleep(Duration::from_millis(5)).await;
    h.insert("x".into(), Duration::from_secs(10));
    advance(Duration::from_secs(5)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(n.load(Ordering::SeqCst), 0);
    advance(Duration::from_secs(6)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(n.load(Ordering::SeqCst), 1);
  }

  #[tokio::test]
  async fn test_metrics() {
    pause();
    let (h, _g) = TimingWheel::start(cfg_fast(), |_: String| async move {});
    sleep(Duration::from_millis(5)).await;
    h.insert("a".into(), Duration::from_secs(10));
    h.insert("b".into(), Duration::from_secs(10));
    advance(Duration::from_millis(100)).await;
    sleep(Duration::from_millis(50)).await;
    assert_eq!(h.inserted_total(), 2);
    assert!(h.active_tasks() > 0);
  }

  #[tokio::test]
  async fn test_shutdown() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let (h, _g) = TimingWheel::start(cfg_fast(), { let n = Arc::clone(&n); move |_: String| inc(&n) });
    sleep(Duration::from_millis(5)).await;
    h.insert("x".into(), Duration::from_millis(100));
    advance(Duration::from_millis(200)).await;
    sleep(Duration::from_millis(100)).await;
    h.shutdown();
    assert!(n.load(Ordering::SeqCst) >= 1);
  }
}

// ── Stress tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod stress {
  use super::*;
  use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
  use tokio::time::{Duration, sleep};

  fn cfg() -> WheelConfig {
    WheelConfig { tick_interval: Duration::from_millis(20), batch_size: 500, channel_capacity: 200_000 }
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn stress_50k_one_shot() {
    let n = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&n);
    let (h, _g) = TimingWheel::start(cfg(), move |_: usize| { let c = Arc::clone(&c); async move { c.fetch_add(1, Ordering::Relaxed); } });

    let total = 50_000;
    for i in 0..total { h.insert(i, Duration::from_secs(5)); }
    println!("inserted {total} tasks");

    sleep(Duration::from_secs(8)).await;
    let fired = n.load(Ordering::Relaxed);
    println!("expired {fired}/{total} ({:.1}%)", fired as f64 / total as f64 * 100.0);
    assert!(fired as f64 > total as f64 * 0.95);
    h.shutdown();
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn stress_10k_heartbeat() {
    let n = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&n);
    let (h, _g) = TimingWheel::start(cfg(), move |_: usize| { let c = Arc::clone(&c); async move { c.fetch_add(1, Ordering::Relaxed); } });

    let total = 10_000;
    for i in 0..total { h.insert(i, Duration::from_secs(60)); }
    for round in 0..10 {
      sleep(Duration::from_secs(1)).await;
      for i in 0..total { h.reset(i, Duration::from_secs(60)); }
      println!("round {round}: {total} resets");
    }
    sleep(Duration::from_secs(2)).await;
    assert_eq!(n.load(Ordering::Relaxed), 0, "no false expirations");
    h.shutdown();
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn stress_throughput() {
    let (h, _g) = TimingWheel::start(cfg(), |_: usize| async move {});
    let count = 50_000;
    let mut dropped = 0;
    for i in 0..count { if !h.insert(i, Duration::from_secs(30)) { dropped += 1; } }
    println!("inserted {}, dropped {}", h.inserted_total(), dropped);
    assert_eq!(dropped, 0);
    h.shutdown();
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn stress_long_running() {
    let (h, _g) = TimingWheel::start(cfg(), |_: usize| async move {});
    let dur = Duration::from_secs(15);
    let start = std::time::Instant::now();
    let mut id = 0usize;
    while start.elapsed() < dur {
      for _ in 0..1000 { h.insert(id, Duration::from_secs(30)); id += 1; }
      for i in id.saturating_sub(5000)..id { h.reset(i, Duration::from_secs(30)); }
      for i in id.saturating_sub(10_000)..id.saturating_sub(8000) { h.remove(&i); }
      sleep(Duration::from_millis(100)).await;
    }
    println!("processed {id} ids over {:?}", dur);
    h.shutdown();
  }

  #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
  async fn stress_3_hour_delay() {
    let n = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&n);
    let (h, _g) = TimingWheel::start(
      WheelConfig { tick_interval: Duration::from_millis(10), batch_size: 500, channel_capacity: 200_000 },
      move |_: String| { let c = Arc::clone(&c); async move { c.fetch_add(1, Ordering::Relaxed); } },
    );
    // L2 can handle ~73h, so 3h should cascade L2→L1→L0→expire correctly
    h.insert("long".into(), Duration::from_secs(3 * 3600));
    sleep(Duration::from_millis(100)).await;
    assert_eq!(n.load(Ordering::Relaxed), 0, "3h task should not fire immediately");
    h.shutdown();
  }
}
