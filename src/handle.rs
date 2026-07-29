use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    hash::Hash,
    sync::{
        Arc, Mutex,
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

fn cancel_increment<T>(map: &Mutex<HashMap<T, usize>>, id: &T)
where
    T: Eq + Hash + Clone,
{
    map.lock()
        .unwrap()
        .entry(id.clone())
        .and_modify(|c| *c += 1)
        .or_insert(1);
}

fn cancel_decrement<T: Eq + Hash>(map: &Mutex<HashMap<T, usize>>, id: &T) {
    let mut guard = map.lock().unwrap();
    if let Some(c) = guard.get_mut(id) {
        *c = c.saturating_sub(1);
        if *c == 0 {
            guard.remove(id);
        }
    }
}

// ── Public handle ───────────────────────────────────────────────────────

/// Clonable handle for inserting, resetting, and removing tasks.
///
/// `remove()` and `reset()` register a synchronous cancellation marker
/// that prevents the callback from firing even if the task was already
/// queued for execution when the call was made.  The guarantee holds up
/// to the point where the callback is spawned; once inside `tokio::spawn`
/// the callback cannot be intercepted.
#[derive(Clone)]
pub struct TimingWheel<T> {
    tx: Sender<Cmd<T>>,
    shared: Arc<Metrics>,
    cancelled: Arc<Mutex<HashMap<T, usize>>>,
}

/// Runtime statistics exposed by the timing wheel.
///
/// All counters are relaxed-ordered atomics and are **not** intended for
/// strict synchronisation — use them for observability and diagnostics.
#[derive(Default)]
pub struct Metrics {
    /// Number of tasks currently tracked by the wheel.
    pub active: AtomicUsize,
    /// Cumulative `insert` + `reset` calls.
    pub inserted: AtomicUsize,
    /// Commands dropped because the channel was at capacity.
    pub dropped: AtomicUsize,
    /// Callbacks that fired successfully.
    pub expirations: AtomicUsize,
    /// Callbacks that panicked or otherwise terminated abnormally.
    pub abnormal: AtomicUsize,
}

impl<T: Send + 'static> TimingWheel<T> {
    /// Schedule a one-shot task that fires after `timeout`.
    ///
    /// If a task with the same `id` already exists, its timeout is replaced
    /// and any pending callback for that id is cancelled.
    ///
    /// Returns `false` if the command channel is at capacity.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use std::time::Duration;
    /// # use rotor_wheel::{TimingWheel, WheelConfig};
    /// # let (wheel, _guard) = TimingWheel::start(WheelConfig::default(), |_: String| async {});
    /// wheel.insert("my-task".into(), Duration::from_secs(10));
    /// ```
    #[inline]
    pub fn insert(&self, id: T, timeout: Duration) -> bool
    where
        T: Clone,
    {
        self.dispatch(Cmd::Insert(id, timeout))
    }

    /// Refresh (or create) an ongoing task with a new timeout.
    ///
    /// The previous callback (if already queued for execution) is guaranteed
    /// not to fire — a synchronous cancellation marker is registered before
    /// the new timeout is scheduled.
    ///
    /// Returns `false` if the command channel is at capacity; in that case
    /// the cancellation marker is rolled back and the caller should retry.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use std::time::Duration;
    /// # use rotor_wheel::{TimingWheel, WheelConfig};
    /// # let (wheel, _guard) = TimingWheel::start(WheelConfig::default(), |_: String| async {});
    /// wheel.reset("conn-1".into(), Duration::from_secs(60));
    /// ```
    #[inline]
    pub fn reset(&self, id: T, timeout: Duration) -> bool
    where
        T: Clone + Eq + Hash,
    {
        cancel_increment(&self.cancelled, &id);
        if self.tx.try_send(Cmd::Reset(id.clone(), timeout)).is_ok() {
            self.shared.inserted.fetch_add(1, Ordering::Relaxed);
            true
        } else {
            cancel_decrement(&self.cancelled, &id);
            self.shared.dropped.fetch_add(1, Ordering::Relaxed);
            false
        }
    }

    /// Cancel a task.  Once this method returns the callback is guaranteed
    /// not to fire — the cancellation is registered synchronously.
    ///
    /// Safe to call for IDs that do not exist (no-op).
    ///
    /// Returns `false` when the command channel is at capacity and the
    /// asynchronous arena cleanup could not be enqueued.  The cancellation
    /// guarantee still holds regardless.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use std::time::Duration;
    /// # use rotor_wheel::{TimingWheel, WheelConfig};
    /// # let (wheel, _guard) = TimingWheel::start(WheelConfig::default(), |_: String| async {});
    /// wheel.insert("req-1".into(), Duration::from_secs(30));
    /// wheel.remove(&"req-1".to_string());
    /// ```
    #[inline]
    pub fn remove(&self, id: &T) -> bool
    where
        T: Clone + Eq + Hash,
    {
        cancel_increment(&self.cancelled, id);
        self.tx.try_send(Cmd::Remove(id.clone())).is_ok()
    }

    /// Gracefully shut down the wheel.  Pending callbacks will still fire
    /// unless they were previously cancelled via [`remove`](Self::remove)
    /// or [`reset`](Self::reset).
    ///
    /// Returns `false` if the command channel is at capacity.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use rotor_wheel::{TimingWheel, WheelConfig};
    /// # let (wheel, _guard) = TimingWheel::start(WheelConfig::default(), |_: String| async {});
    /// wheel.shutdown();
    /// ```
    #[inline]
    pub fn shutdown(&self) -> bool {
        self.tx.try_send(Cmd::Shutdown).is_ok()
    }

    /// Number of tasks currently active (not yet expired or removed).
    #[inline]
    pub fn active_tasks(&self) -> usize {
        self.shared.active.load(Ordering::Relaxed)
    }

    /// Total `insert` + `reset` calls since the wheel started.
    #[inline]
    pub fn inserted_total(&self) -> usize {
        self.shared.inserted.load(Ordering::Relaxed)
    }

    /// Total commands dropped because the channel was at capacity.
    #[inline]
    pub fn dropped_total(&self) -> usize {
        self.shared.dropped.load(Ordering::Relaxed)
    }

    /// Total callbacks that have fired successfully.
    #[inline]
    pub fn expirations_total(&self) -> usize {
        self.shared.expirations.load(Ordering::Relaxed)
    }

    /// Total callbacks that panicked or otherwise terminated abnormally.
    #[inline]
    pub fn abnormal_total(&self) -> usize {
        self.shared.abnormal.load(Ordering::Relaxed)
    }

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

/// RAII guard that keeps the timing wheel worker task alive.
///
/// Dropping this guard **aborts** the worker.  Store it in a variable
/// for the lifetime of the wheel, or use `Box::leak` / `forget` if you
/// need the wheel to outlive the current scope.
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
    /// Start a new timing wheel on a background tokio task.
    ///
    /// The returned [`TimingWheelGuard`] **must** be held for the wheel's
    /// lifetime — dropping it aborts the worker immediately.
    ///
    /// The `callback` closure is invoked (via `tokio::spawn`) for each
    /// expired task.  Returning a future lets you perform async work such
    /// as network I/O or database writes inside the timeout handler.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::time::Duration;
    /// use rotor_wheel::{TimingWheel, WheelConfig};
    ///
    /// let (wheel, _guard) = TimingWheel::start(
    ///     WheelConfig::default(),
    ///     |id: String| async move { println!("{id} timed out") },
    /// );
    ///
    /// wheel.insert("req-001".into(), Duration::from_secs(10));
    /// ```
    pub fn start<F, Fut>(config: WheelConfig, callback: F) -> (TimingWheel<T>, TimingWheelGuard)
    where
        F: FnMut(T) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        assert!(config.batch_size >= 1, "batch_size must be >= 1");

        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let shared = Arc::new(Metrics::default());
        let cancelled = Arc::new(Mutex::new(HashMap::new()));
        let handle = TimingWheel {
            tx,
            shared: Arc::clone(&shared),
            cancelled: Arc::clone(&cancelled),
        };
        let task = spawn(run(rx, config, shared, cancelled, callback));
        (handle, TimingWheelGuard { task: Some(task) })
    }
}
