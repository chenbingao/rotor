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
//! use rotor::{TimingWheel, WheelConfig};
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

mod config;
mod wheel;
mod handle;
mod worker;

pub use config::WheelConfig;
pub use handle::{TimingWheel, TimingWheelGuard, Metrics};

#[cfg(test)]
mod tests;
#[cfg(test)]
mod stress;
