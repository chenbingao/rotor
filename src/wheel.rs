use std::{
  collections::HashMap,
  fmt::Debug,
  hash::Hash,
  sync::atomic::Ordering,
  time::Duration,
};

use crate::config::{LV0_SLOTS, LV0_TICK, LV1_SLOTS, LV1_TICK, LV2_SLOTS, LV2_TICK};
use crate::handle::Metrics;

// ── Internal types ──────────────────────────────────────────────────────

pub(crate) enum Cmd<T> {
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

pub(crate) struct Wheel<T> {
  levels: [Level; 3],
  arena: Vec<TaskEntry<T>>,
  free: Vec<usize>,                  // free arena slots
  id_map: HashMap<T, usize>,         // id → arena index
  pub(crate) current_tick: u64,
  tick_ms: u64,
}

impl<T: Eq + Hash + Clone + Debug> Wheel<T> {
  pub(crate) fn new(tick_interval: Duration) -> Self {
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

  pub(crate) fn alloc(&mut self, id: T, expire_tick: u64) -> (usize, u64) {
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

  pub(crate) fn push_to_level(&mut self, expire_tick: u64, idx: usize) {
    // Place task at the highest level whose granularity is ≤ the delay.
    let target_level = if expire_tick >= self.current_tick + self.levels[2].tick_per_advance { 2 }
      else if expire_tick >= self.current_tick + self.levels[1].tick_per_advance { 1 }
      else { 0 };
    self.levels[target_level].push(expire_tick, idx);
  }

  pub(crate) fn schedule(&mut self, id: T, timeout: Duration) {
    let delay = (timeout.as_millis() / self.tick_ms as u128)
      .max(1)
      .min(u64::MAX as u128) as u64;
    let expire_tick = self.current_tick + delay;
    let (idx, _) = self.alloc(id, expire_tick);
    self.push_to_level(expire_tick, idx);
  }

  pub(crate) fn remove(&mut self, id: &T) {
    if let Some(idx) = self.id_map.remove(id) {
      // Free the arena slot lazily — the old Scheduled entry stays
      // in the slot but has a stale generation, so it's discarded.
      self.free.push(idx);
    }
  }

  /// Advance one base tick.  Returns expired task IDs to fire.
  pub(crate) fn advance(&mut self, metrics: &Metrics) -> Vec<T> {
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
