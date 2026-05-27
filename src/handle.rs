use std::{
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
  sync::mpsc::{self, Sender},
  task::JoinHandle,
};

use crate::config::WheelConfig;
use crate::wheel::Cmd;
use crate::worker::run;

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
