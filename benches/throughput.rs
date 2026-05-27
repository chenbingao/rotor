use criterion::{Criterion, black_box, criterion_group, criterion_main};
use std::time::Duration;
use rotor::{TimingWheel, WheelConfig};

fn config() -> WheelConfig {
  WheelConfig {
    tick_interval: Duration::from_millis(10),
    batch_size: 2000,
    channel_capacity: 1_000_000,
  }
}

fn bench_insert(c: &mut Criterion) {
  c.bench_function("insert_50k", |b| {
    let rt = tokio::runtime::Builder::new_multi_thread()
      .worker_threads(2)
      .build()
      .unwrap();
    let total = 50_000;

    b.to_async(&rt).iter(|| async {
      let (h, _g) = TimingWheel::start(config(), |_: usize| async move {});
      for i in 0..total {
        black_box(h.insert(i, Duration::from_secs(30)));
      }
      h.shutdown();
    });
  });
}

fn bench_reset(c: &mut Criterion) {
  c.bench_function("reset_10k", |b| {
    let rt = tokio::runtime::Builder::new_multi_thread()
      .worker_threads(2)
      .build()
      .unwrap();
    let total = 10_000;

    b.to_async(&rt).iter(|| async {
      let (h, _g) = TimingWheel::start(config(), |_: usize| async move {});
      for i in 0..total {
        h.insert(i, Duration::from_secs(60));
      }
      for i in 0..total {
        black_box(h.reset(i, Duration::from_secs(60)));
      }
      h.shutdown();
    });
  });
}

fn bench_expire(c: &mut Criterion) {
  c.bench_function("expire_10k", |b| {
    let rt = tokio::runtime::Builder::new_multi_thread()
      .worker_threads(2)
      .build()
      .unwrap();

    b.to_async(&rt).iter(|| async {
      let (h, _g) = TimingWheel::start(
        WheelConfig { tick_interval: Duration::from_millis(1), batch_size: 2000, channel_capacity: 200_000 },
        |_: usize| async move {},
      );
      for i in 0..10_000 {
        h.insert(i, Duration::from_millis(100));
      }
      tokio::time::sleep(Duration::from_millis(500)).await;
      h.shutdown();
    });
  });
}

criterion_group!(benches, bench_insert, bench_reset, bench_expire);
criterion_main!(benches);
