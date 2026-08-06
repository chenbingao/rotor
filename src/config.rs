// ── Configuration ───────────────────────────────────────────────────────

use std::time::Duration;

/// Timing wheel configuration.
#[derive(Debug, Clone)]
pub struct WheelConfig {
  /// Base tick interval (duration of one Level-0 slot).
  /// Default: 1 second.
  pub tick_interval: Duration,
  /// Maximum callbacks executed per tick drain to avoid blocking the
  /// event loop for too long.  Must be >= 1.
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
pub(crate) const LV0_SLOTS: usize = 64;
pub(crate) const LV1_SLOTS: usize = 64;
pub(crate) const LV2_SLOTS: usize = 64;
pub(crate) const LV0_TICK: u64 = 1; // 1 base tick
pub(crate) const LV1_TICK: u64 = 64; // advances every 64 base ticks
pub(crate) const LV2_TICK: u64 = 64 * 64; // advances every 4096 base ticks
