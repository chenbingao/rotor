use std::{
  collections::HashMap,
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

pub(crate) async fn run<T, F>(
  mut rx: Receiver<Cmd<T>>,
  config: WheelConfig,
  metrics: Arc<Metrics>,
  cancelled: Arc<Mutex<HashMap<T, usize>>>,
  mut callback: F,
) where
  T: Eq + Hash + Clone,
  F: FnMut(T) + Send + 'static,
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
        let elapsed = start.elapsed();
        let target = (elapsed.as_millis() / config.tick_interval.as_millis().max(1)) as u64;

        while wheel.current_tick < target {
          pending.extend(wheel.advance(&metrics));
          if wheel.current_tick.wrapping_rem(10) == 0 { break; }
        }

        drain(&mut pending, &mut callback, &metrics, &cancelled, batch);
      }
    }
  }

  metrics.active.store(0, Ordering::Relaxed);

  pending.extend(wheel.drain_all());

  drain(
    &mut pending,
    &mut callback,
    &metrics,
    &cancelled,
    usize::MAX,
  );

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
      Cmd::Shutdown => {} // already shutting down
    }
  }

  pending.extend(wheel.drain_all());
  drain(
    &mut pending,
    &mut callback,
    &metrics,
    &cancelled,
    usize::MAX,
  );
}

// ── Drain ────────────────────────────────────────────────────────────────

fn drain<T, F>(
  pending: &mut Vec<T>,
  callback: &mut F,
  metrics: &Metrics,
  cancelled: &Mutex<HashMap<T, usize>>,
  limit: usize,
) where
  T: Eq + Hash,
  F: FnMut(T),
{
  let n = pending.len().min(limit);
  for _ in 0..n {
    let Some(id) = pending.pop() else {
      break;
    };

    let blocked = {
      let map = cancelled.lock().unwrap();
      map.get(&id).copied().unwrap_or(0) > 0
    };
    if blocked {
      continue;
    }

    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| callback(id))) {
      Ok(()) => {
        metrics.expirations.fetch_add(1, Ordering::Relaxed);
      }
      Err(_) => {
        log::error!("rotor callback panicked");
        metrics.abnormal.fetch_add(1, Ordering::Relaxed);
      }
    }
  }
  metrics.pending.store(pending.len(), Ordering::Relaxed);
}

// ── Cancelled-set helpers ────────────────────────────────────────────────

pub(crate) fn cancel_decrement<T: Eq + Hash>(cancelled: &Mutex<HashMap<T, usize>>, id: &T) {
  let mut map = cancelled.lock().unwrap();
  if let Some(c) = map.get_mut(id) {
    *c = c.saturating_sub(1);
    if *c == 0 {
      map.remove(id);
    }
  }
}
