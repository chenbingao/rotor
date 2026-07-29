use std::{
    collections::HashMap,
    fmt::Debug,
    future::Future,
    hash::Hash,
    sync::{Arc, Mutex, atomic::Ordering},
};

use tokio::{
    sync::mpsc::Receiver,
    time::{Instant, interval},
};

use crate::{
    config::*,
    handle::Metrics,
    wheel::{Cmd, Wheel},
};

// ── Worker ──────────────────────────────────────────────────────────────

/// Worker event-loop: dispatch commands and advance the wheel on each tick.
///
/// On `Cmd::Shutdown` (or channel closure) the loop drains all remaining
/// expired tasks before returning, and resets the active counter to zero.
pub(crate) async fn run<T, F, Fut>(
    mut rx: Receiver<Cmd<T>>,
    config: WheelConfig,
    metrics: Arc<Metrics>,
    cancelled: Arc<Mutex<HashMap<T, usize>>>,
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
    let half1 = batch.div_ceil(2);
    let half2 = batch / 2;

    loop {
        tokio::select! {
          cmd = rx.recv() => {
            match cmd {
              Some(Cmd::Insert(id, to)) => {
                cancel_decrement(&cancelled, &id);
                pending.retain(|x| x != &id);
                wheel.schedule(id, to);
              }
              Some(Cmd::Reset(id, to))  => {
                cancel_decrement(&cancelled, &id);
                pending.retain(|x| x != &id);
                wheel.schedule(id, to);
              }
              Some(Cmd::Remove(id))     => {
                cancel_decrement(&cancelled, &id);
                pending.retain(|x| x != &id);
                wheel.remove(&id);
              }
              Some(Cmd::Shutdown) | None => break,
            }
          }

          _ = tick.tick() => {
            // Clock compensation
            let elapsed = start.elapsed();
            let target = (elapsed.as_millis() / config.tick_interval.as_millis().max(1)) as u64;

            drain(&mut pending, &mut callback, &metrics, &cancelled, half1).await;

            while wheel.current_tick < target {
              pending.extend(wheel.advance(&metrics));
              if wheel.current_tick.wrapping_rem(10) == 0 { break; }
            }

            drain(&mut pending, &mut callback, &metrics, &cancelled, half2).await;
          }
        }
    }

    metrics.active.store(0, Ordering::Relaxed);

    // Drain all expired on shutdown
    for _ in 0..(LV2_SLOTS * LV2_TICK as usize) {
        pending.extend(wheel.advance(&metrics));
    }
    drain(
        &mut pending,
        &mut callback,
        &metrics,
        &cancelled,
        usize::MAX,
    )
    .await;

    while let Ok(cmd) = rx.try_recv() {
        match cmd {
            Cmd::Insert(id, to) => {
                cancel_decrement(&cancelled, &id);
                pending.retain(|x| x != &id);
                wheel.schedule(id, to);
            }
            Cmd::Reset(id, to) => {
                cancel_decrement(&cancelled, &id);
                pending.retain(|x| x != &id);
                wheel.schedule(id, to);
            }
            Cmd::Remove(id) => {
                cancel_decrement(&cancelled, &id);
                pending.retain(|x| x != &id);
                wheel.remove(&id);
            }
            Cmd::Shutdown => break,
        }
    }
}

/// Decrement the cancellation count for `id`.  If it reaches zero the
/// entry is removed so the map does not grow without bound.
fn cancel_decrement<T: Eq + Hash>(cancelled: &Mutex<HashMap<T, usize>>, id: &T) {
    let mut map = cancelled.lock().unwrap();
    if let Some(c) = map.get_mut(id) {
        *c = c.saturating_sub(1);
        if *c == 0 {
            map.remove(id);
        }
    }
}

/// Fire up to `limit` callbacks from the `pending` batch.
/// Each callback is spawned via `tokio::spawn` and awaited.
///
/// Before spawning, each ID is checked against the shared `cancelled` map.
/// If the reference count is > 0 the callback is skipped; the count is
/// not consumed here — it will be decremented when the corresponding
/// worker command is processed.
///
/// The cancellation guarantee holds up to the point where `drain()` checks
/// the cancelled set.  Once the check has passed, subsequent calls to
/// `remove`/`reset`/`insert` cannot intercept the callback.
///
/// Long-running callbacks block the worker.  Spawn inside the callback
/// for I/O-heavy work.
///
/// Panicked callbacks are logged and counted in [`Metrics::abnormal`].
async fn drain<T, F, Fut>(
    pending: &mut Vec<T>,
    callback: &mut F,
    metrics: &Metrics,
    cancelled: &Mutex<HashMap<T, usize>>,
    limit: usize,
) where
    T: Eq + Hash,
    F: FnMut(T) -> Fut,
    Fut: Future<Output = ()> + Send + 'static,
{
    let n = pending.len().min(limit);
    let mut tasks = Vec::with_capacity(n);
    for _ in 0..n {
        if let Some(id) = pending.pop() {
            let blocked = {
                let map = cancelled.lock().unwrap();
                map.get(&id).copied().unwrap_or(0) > 0
            };
            if blocked {
                continue;
            }
            tasks.push(tokio::spawn(callback(id)));
        }
    }
    for t in tasks {
        if let Err(e) = t.await {
            log::error!("rotor callback panicked: {e}");
            metrics.abnormal.fetch_add(1, Ordering::Relaxed);
        } else {
            metrics.expirations.fetch_add(1, Ordering::Relaxed);
        }
    }
}
