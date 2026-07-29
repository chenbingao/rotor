#![allow(unused_imports)]

use super::*;
use crate::*;
use std::future::Future;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::time::{advance, pause, sleep};

fn cfg_fast() -> WheelConfig {
    WheelConfig {
        tick_interval: Duration::from_millis(10),
        ..Default::default()
    }
}

macro_rules! cb {
    ($n:ident) => {{
        let n = Arc::clone(&$n);
        move |_: String| {
            let n = Arc::clone(&n);
            async move {
                n.fetch_add(1, Ordering::SeqCst);
            }
        }
    }};
}

#[tokio::test]
async fn test_one_shot() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let (h, _g) = TimingWheel::start(cfg_fast(), cb!(n));
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
    let (h, _g) = TimingWheel::start(cfg_fast(), cb!(n));
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
    let (h, _g) = TimingWheel::start(cfg_fast(), cb!(n));
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
    let (h, _g) = TimingWheel::start(cfg_fast(), cb!(n));
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
    let (h, _g) = TimingWheel::start(cfg_fast(), cb!(n));
    sleep(Duration::from_millis(5)).await;
    h.insert("x".into(), Duration::from_millis(100));
    advance(Duration::from_millis(200)).await;
    sleep(Duration::from_millis(100)).await;
    h.shutdown();
    assert!(n.load(Ordering::SeqCst) >= 1);
}

#[tokio::test]
async fn test_panicked_callback_increments_abnormal() {
    pause();
    let (h, _g) = TimingWheel::start(
        cfg_fast(),
        move |_: String| async move { panic!("intentional") },
    );
    sleep(Duration::from_millis(5)).await;
    h.insert("x".into(), Duration::from_millis(100));
    advance(Duration::from_millis(300)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(h.abnormal_total(), 1);
    assert_eq!(h.expirations_total(), 0);
    h.shutdown();
}

// ── remove-then-insert regression tests ──────────────────────────────────

/// remove → insert (different ID): the new task must NOT fire when the
/// old task's bucket drains.  This is the core arena-reuse correctness bug.
#[tokio::test]
async fn test_remove_then_insert_different_id_no_false_fire() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let (h, _g) = TimingWheel::start(cfg_fast(), cb!(n));

    // Let the wheel start ticking (roughly 10 ticks ahead)
    advance(Duration::from_millis(100)).await;
    sleep(Duration::from_millis(30)).await;

    // A expires at ~300ms from start (200ms delay from tick ~100ms)
    h.insert("A".into(), Duration::from_millis(200));

    // Move forward a bit then remove A
    advance(Duration::from_millis(80)).await;
    h.remove(&"A".to_string());

    // B expires later: ~500ms from start (400ms delay from now)
    h.insert("B".into(), Duration::from_millis(400));

    // Advance past A's original expiry but before B's
    advance(Duration::from_millis(80)).await;
    sleep(Duration::from_millis(50)).await;
    assert_eq!(
        n.load(Ordering::SeqCst),
        0,
        "B must not fire when A's old bucket drains"
    );

    // Advance past B's expiry
    advance(Duration::from_millis(350)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(n.load(Ordering::SeqCst), 1, "B must fire at its own expiry");

    h.shutdown();
}

/// remove → insert (same ID): the new schedule must fire exactly once
/// at the new expiry, not at the old one.
#[tokio::test]
async fn test_remove_then_insert_same_id() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let (h, _g) = TimingWheel::start(cfg_fast(), cb!(n));

    advance(Duration::from_millis(100)).await;
    sleep(Duration::from_millis(30)).await;

    // A expires at ~300ms
    h.insert("A".into(), Duration::from_millis(200));

    advance(Duration::from_millis(80)).await;
    h.remove(&"A".to_string());

    // Reinsert same ID with a much later expiry
    h.insert("A".into(), Duration::from_millis(600));

    // Advance past the original 200ms expiry
    advance(Duration::from_millis(200)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(
        n.load(Ordering::SeqCst),
        0,
        "A must not fire at the old expiry"
    );

    // Advance past the new 600ms expiry
    advance(Duration::from_millis(400)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(
        n.load(Ordering::SeqCst),
        1,
        "A must fire once at the new expiry"
    );

    h.shutdown();
}

/// remove on a level-1 (longer-delay) task, then insert a new task.
/// Verifies arena-reuse safety when the stale bucket is one level up.
#[tokio::test]
async fn test_remove_level1_task_insert_new() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let (h, _g) = TimingWheel::start(cfg_fast(), cb!(n));

    advance(Duration::from_millis(100)).await;
    sleep(Duration::from_millis(30)).await;

    // Level 1 threshold: 64 ticks × 10ms = 640ms
    // A expires at ~900ms (800ms delay)
    h.insert("A".into(), Duration::from_millis(800));

    advance(Duration::from_millis(200)).await;
    h.remove(&"A".to_string());

    // B expires at ~400ms from here; sleep first to drain the command queue
    // so B is guaranteed scheduled before the next advance.
    h.insert("B".into(), Duration::from_millis(400));
    sleep(Duration::from_millis(100)).await;

    // Advance past B's expiry but before A's original expiry
    advance(Duration::from_millis(400)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(n.load(Ordering::SeqCst), 1, "B must fire at its own time");

    // Advance past A's original expiry — must not fire again
    advance(Duration::from_millis(600)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(
        n.load(Ordering::SeqCst),
        1,
        "no additional fire from A's stale bucket"
    );

    h.shutdown();
}

/// Insert and remove many tasks, then verify later tasks fire correctly.
/// Exercises the free-list and arena slot lifecycle under churn.
#[tokio::test]
async fn test_churn_then_insert() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let (h, _g) = TimingWheel::start(cfg_fast(), cb!(n));

    advance(Duration::from_millis(100)).await;
    sleep(Duration::from_millis(30)).await;

    // Insert and remove many tasks — these never fire
    for i in 0..100 {
        let id = format!("churn-{i}");
        h.insert(id.clone(), Duration::from_millis(300));
        h.remove(&id);
    }

    // Fresh tasks that SHOULD fire
    for i in 0..50 {
        h.insert(format!("final-{i}"), Duration::from_millis(400));
    }

    // Advance past the churned tasks' expiry
    advance(Duration::from_millis(300)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(n.load(Ordering::SeqCst), 0, "churned tasks must not fire");

    // Advance past the final tasks' expiry
    advance(Duration::from_millis(200)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(n.load(Ordering::SeqCst), 50, "final tasks must all fire");

    h.shutdown();
}

// ── 0.3.0 regression tests ──────────────────────────────────────────────

/// insert with same ID must cancel any old pending callback (#3).
#[tokio::test]
async fn test_insert_replaces_pending() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let (h, _g) = TimingWheel::start(cfg_fast(), cb!(n));

    advance(Duration::from_millis(100)).await;
    sleep(Duration::from_millis(30)).await;

    // Schedule A to expire soon
    h.insert("A".into(), Duration::from_millis(200));

    // Advance part-way, then reinsert with a longer timeout before expiry
    advance(Duration::from_millis(100)).await;
    h.insert("A".into(), Duration::from_millis(600));

    // Advance past the original 200ms expiry
    advance(Duration::from_millis(200)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(
        n.load(Ordering::SeqCst),
        0,
        "old pending callback must not fire after reinsert"
    );

    // Advance past the new 600ms expiry
    advance(Duration::from_millis(500)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(
        n.load(Ordering::SeqCst),
        1,
        "new task must fire at its own expiry"
    );

    h.shutdown();
}

/// Concurrent remove of the same ID must not cause a callback to fire (#1).
#[tokio::test]
async fn test_concurrent_remove_same_id() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let (h, _g) = TimingWheel::start(cfg_fast(), cb!(n));

    advance(Duration::from_millis(100)).await;
    sleep(Duration::from_millis(30)).await;

    h.insert("A".into(), Duration::from_millis(200));

    // Two concurrent removes on the same id
    let h1 = h.clone();
    let h2 = h.clone();
    let (r1, r2) = tokio::join!(
        tokio::task::spawn_blocking(move || h1.remove(&"A".to_string())),
        tokio::task::spawn_blocking(move || h2.remove(&"A".to_string())),
    );
    assert!(r1.unwrap(), "first remove must succeed");
    assert!(r2.unwrap(), "second remove must succeed");

    advance(Duration::from_millis(500)).await;
    sleep(Duration::from_millis(100)).await;

    assert_eq!(
        n.load(Ordering::SeqCst),
        0,
        "concurrent removes must prevent the callback"
    );
    h.shutdown();
}

/// remove() returns false when the channel is at capacity (#2).
#[tokio::test]
async fn test_remove_returns_false_on_full_channel() {
    let n = Arc::new(AtomicUsize::new(0));
    let config = WheelConfig {
        tick_interval: Duration::from_millis(10),
        channel_capacity: 1,
        batch_size: 500,
    };
    let (h, _g) = TimingWheel::start(config, cb!(n));
    let mut ok = true;
    while ok {
        ok = h.insert("filler".into(), Duration::from_secs(99));
    }
    assert!(
        !h.remove(&"x".to_string()),
        "remove on full channel must return false"
    );
    h.shutdown();
}

/// reset() must increment inserted_total after a successful call (#6).
#[tokio::test]
async fn test_reset_increments_inserted() {
    pause();
    let (h, _g) = TimingWheel::start(cfg_fast(), |_: String| async move {});

    advance(Duration::from_millis(100)).await;
    sleep(Duration::from_millis(30)).await;

    h.insert("A".into(), Duration::from_millis(200));
    let before = h.inserted_total();

    assert!(h.reset("A".into(), Duration::from_millis(600)));
    assert_eq!(
        h.inserted_total(),
        before + 1,
        "reset must increment inserted_total"
    );

    h.shutdown();
}

/// batch_size of 1 must not deadlock callback execution (#8).
#[tokio::test]
async fn test_batch_size_one_works() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let config = WheelConfig {
        tick_interval: Duration::from_millis(10),
        batch_size: 1,
        channel_capacity: 64 * 1024,
    };
    let (h, _g) = TimingWheel::start(config, cb!(n));

    sleep(Duration::from_millis(5)).await;
    h.insert("x".into(), Duration::from_millis(100));
    advance(Duration::from_millis(200)).await;
    sleep(Duration::from_millis(100)).await;

    assert_eq!(
        n.load(Ordering::SeqCst),
        1,
        "task must fire with batch_size=1"
    );

    h.shutdown();
}

// ── 0.3.2 regression tests ──────────────────────────────────────────────

/// remove() 失败后同 ID insert 不应被残留计数拦截 (#1)。
#[tokio::test]
async fn test_failed_remove_does_not_leak_count() {
    pause();
    let n = Arc::new(AtomicUsize::new(0));
    let config = WheelConfig {
        tick_interval: Duration::from_millis(10),
        channel_capacity: 1,
        batch_size: 500,
    };
    let (h, _g) = TimingWheel::start(config, cb!(n));

    // Fill channel so remove fails
    let mut ok = true;
    while ok {
        ok = h.insert("filler".into(), Duration::from_secs(99));
    }

    // remove fails → count should be rolled back
    assert!(
        !h.remove(&"a".to_string()),
        "remove on full channel must return false"
    );

    // New wheel with room for commands: a fresh insert of "a" must not be blocked
    let (h2, _g2) = TimingWheel::start(cfg_fast(), cb!(n));
    advance(Duration::from_millis(100)).await;
    sleep(Duration::from_millis(30)).await;

    h2.insert("a".into(), Duration::from_millis(200));
    advance(Duration::from_millis(300)).await;
    sleep(Duration::from_millis(100)).await;
    assert_eq!(
        n.load(Ordering::SeqCst),
        1,
        "a must fire after failed remove on another wheel"
    );

    h.shutdown();
    h2.shutdown();
}

/// remove() 失败时应递增 dropped_total (#7)。
#[tokio::test]
async fn test_failed_remove_increments_dropped() {
    let config = WheelConfig {
        tick_interval: Duration::from_millis(10),
        channel_capacity: 1,
        batch_size: 500,
    };
    let (h, _g) = TimingWheel::start(config, |_: String| async move {});

    let mut ok = true;
    while ok {
        ok = h.insert("filler".into(), Duration::from_secs(99));
    }

    let before = h.dropped_total();
    h.remove(&"x".to_string());
    assert_eq!(
        h.dropped_total(),
        before + 1,
        "failed remove must increment dropped_total"
    );

    h.shutdown();
}

/// insert() 失败时应回滚 cancelled 计数 (#3)。
#[tokio::test]
async fn test_failed_insert_rolls_back() {
    let n = Arc::new(AtomicUsize::new(0));
    let config = WheelConfig {
        tick_interval: Duration::from_millis(10),
        channel_capacity: 1,
        batch_size: 500,
    };
    let (h, _g) = TimingWheel::start(config, cb!(n));

    let mut ok = true;
    while ok {
        ok = h.insert("filler".into(), Duration::from_secs(99));
    }

    let before_dropped = h.dropped_total();
    let before_inserted = h.inserted_total();
    assert!(!h.insert("a".into(), Duration::from_secs(99)));

    // dropped_total 应递增，inserted_total 应不变
    assert_eq!(h.dropped_total(), before_dropped + 1);
    assert_eq!(h.inserted_total(), before_inserted);

    h.shutdown();
}
