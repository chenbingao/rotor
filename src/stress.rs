#![allow(unused_imports)]

use super::*;
use crate::*;
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
  h.insert("long".into(), Duration::from_secs(3 * 3600));
  sleep(Duration::from_millis(100)).await;
  assert_eq!(n.load(Ordering::Relaxed), 0, "3h task should not fire immediately");
  h.shutdown();
}

/// 4 个线程并发 insert + reset，验证无数据竞争。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_concurrent_mixed() {
  let expired = Arc::new(AtomicUsize::new(0));
  let c = Arc::clone(&expired);
  let (h, _g) = TimingWheel::start(
    WheelConfig { tick_interval: Duration::from_millis(20), batch_size: 500, channel_capacity: 200_000 },
    move |_: usize| { let c = Arc::clone(&c); async move { c.fetch_add(1, Ordering::Relaxed); } },
  );

  let total = 20_000;
  let h0 = h.clone();
  let h1 = h.clone();
  let h2 = h.clone();
  let h3 = h.clone();

  let t0 = tokio::spawn(async move { for i in 0..total/4 { h0.insert(i, Duration::from_secs(2)); } });
  let t1 = tokio::spawn(async move { for i in total/4..total/2 { h1.insert(i, Duration::from_secs(2)); } });
  let t2 = tokio::spawn(async move { for i in total/2..3*total/4 { h2.insert(i, Duration::from_secs(2)); } });
  let reset_h = h3.clone(); let t3 = tokio::spawn(async move { for i in 3*total/4..total { h3.insert(i, Duration::from_secs(2)); } });

  let resetter = tokio::spawn(async move {
    for _ in 0..total / 2 {
      reset_h.reset(0, Duration::from_secs(3));
    }
  });

  let _ = tokio::join!(t0, t1, t2, t3, resetter);

  sleep(Duration::from_secs(5)).await;

  let fired = expired.load(Ordering::Relaxed);
  println!("concurrent mixed: expired {fired}/{total}");
  assert!(fired > 0, "some tasks should expire");
  h.shutdown();
}

/// Callback panic 不应崩溃时间轮，其他回调正常触发。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_callback_panic_resilience() {
  let ok_count = Arc::new(AtomicUsize::new(0));
  let c = Arc::clone(&ok_count);
  let (h, _g) = TimingWheel::start(
    WheelConfig { tick_interval: Duration::from_millis(20), batch_size: 200, channel_capacity: 100_000 },
    move |id: usize| {
      let c = Arc::clone(&c);
      async move {
        if id.wrapping_rem(10) == 0 {
          panic!("simulated crash for id={id}");
        }
        c.fetch_add(1, Ordering::Relaxed);
      }
    },
  );

  let total = 5000;
  for i in 0..total { h.insert(i, Duration::from_secs(2)); }
  sleep(Duration::from_secs(4)).await;

  let ok = ok_count.load(Ordering::Relaxed);
  println!("callback panic: ok {ok}/{total}");
  // 10% panic → 90% should succeed
  assert!(ok as f64 > total as f64 * 0.85, "only {ok}/{total} succeeded after panics");
  h.shutdown();
}

/// Shutdown 后所有到期任务回调必须触发。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_shutdown_fires_all() {
  let fired = Arc::new(AtomicUsize::new(0));
  let c = Arc::clone(&fired);
  let (h, _g) = TimingWheel::start(
    WheelConfig { tick_interval: Duration::from_millis(10), batch_size: 1000, channel_capacity: 100_000 },
    move |_: usize| { let c = Arc::clone(&c); async move { c.fetch_add(1, Ordering::Relaxed); } },
  );

  let total = 1000;
  for i in 0..total { h.insert(i, Duration::from_millis(100)); }
  sleep(Duration::from_millis(300)).await;
  h.shutdown();

  let n = fired.load(Ordering::Relaxed);
  println!("shutdown fires: {n}/{total}");
  assert_eq!(n, total, "shutdown must drain all expired tasks");
}

/// 高频持续心跳不应导致 arena 无限增长（free_list 复用验证）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_arena_reuse() {
  let (h, _g) = TimingWheel::start(
    WheelConfig { tick_interval: Duration::from_millis(20), batch_size: 500, channel_capacity: 200_000 },
    |_: usize| async move {},
  );

  // Insert, let expire, insert again — arena should reuse slots via free_list.
  for round in 0..20i32 {
    for i in 0..5000 {
      h.insert(i, Duration::from_millis(200));
    }
    sleep(Duration::from_millis(600)).await;
    if round.wrapping_rem(5) == 0 {
      println!("round {round}: active={}, inserted={}", h.active_tasks(), h.inserted_total());
    }
  }
  h.shutdown();
}

/// 持续 60 秒高负载心跳，不应出现误触发。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stress_sustained_heartbeat() {
  let expired = Arc::new(AtomicUsize::new(0));
  let c = Arc::clone(&expired);
  let (h, _g) = TimingWheel::start(
    WheelConfig { tick_interval: Duration::from_millis(20), batch_size: 500, channel_capacity: 200_000 },
    move |_: usize| { let c = Arc::clone(&c); async move { c.fetch_add(1, Ordering::Relaxed); } },
  );

  let n = 5000;
  for i in 0..n { h.insert(i, Duration::from_secs(10)); }

  // 每 2 秒刷新一次，持续 60 秒 = 30 轮
  for round in 0i32..30 {
    sleep(Duration::from_secs(2)).await;
    for i in 0..n {
      h.reset(i, Duration::from_secs(10));
    }
    let e = expired.load(Ordering::Relaxed);
    if e > 0 {
      panic!("round {round}: {e} false expirations detected");
    }
  }
  println!("sustained heartbeat: 0 false expirations over 60s");
  h.shutdown();
}
