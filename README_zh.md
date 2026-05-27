# rotor

[![Crates.io](https://img.shields.io/crates/v/rotor)](https://crates.io/crates/rotor)
[![License](https://img.shields.io/crates/l/rotor)](LICENSE)

基于分层时间轮的通用异步定时任务调度器。

灵感来自 Netty 的 `HashedWheelTimer` — 单线程核心、生成计数器懒删除、时钟补偿、批量并发处理。

## 适用场景

- **心跳 / 长连接保活**：每次 ping 调用 `reset()` — O(1)，旧槽位副本懒删除。
- **一次性延时**：`insert()` 到期后自动触发回调并清除，无需手动清理。
- **请求超时**：包裹请求 ID，成功后 `remove()` 取消。

简单的 `tokio::time::sleep` + `tokio::spawn` 场景不需要这个库。当你有**数千个并发定时器**需要 O(1) 刷新时才适用。

## 特性

- **O(1) reset** — 生成计数器懒删除，高频刷新路径极快。
- **显式超时** — 每次 `insert` / `reset` 接受 `Duration`，无隐式默认值。
- **时钟补偿** — GC 暂停或系统负载尖峰后自动追格，不会丢失时间。
- **批量处理** — 每 tick 限制 spawn 数量，防止瞬间压爆 runtime。
- **优雅关闭** — shutdown 前排空所有到期任务，不丢回调。
- **panic 隔离** — 回调 panic 被捕获并记入 `abnormal_total` 计数器，不传播。
- **泛型** — 支持任意 `T: Eq + Hash + Clone + Send + Debug + 'static`。

## 安装

```toml
[dependencies]
rotor = "0.1"
```

## 快速开始

```rust
use std::time::Duration;
use rotor::{TimingWheel, WheelConfig};

let (wheel, _guard) = TimingWheel::start(
    WheelConfig::default(),
    |id: String| async move { println!("{id} 到期") },
);

// 一次性延时 — 10 秒后触发，自动清除
wheel.insert("req-001".into(), Duration::from_secs(10));

// 重新计时 — 到期时间从当前时刻再推迟 60 秒
wheel.reset("conn-002".into(), Duration::from_secs(60));

// 主动取消
wheel.remove(&"req-001".to_string());

// 优雅关闭 — 排空所有待触发回调
wheel.shutdown();

// 运行时指标
println!("活跃={} 插入={} 丢弃={} 到期={}",
    wheel.active_tasks(),
    wheel.inserted_total(),
    wheel.dropped_total(),
    wheel.expirations_total(),
);
```

## 配置

| 参数 | 默认值 | 说明 |
|-----------|---------|-------------|
| `tick_interval` | 1 s | Level-0 每格时长 |
| `batch_size` | 500 | 每 tick 最多 spawn 回调数 |
| `channel_capacity` | 65536 | mpsc 命令通道容量 |

3 层轮，每层 64 槽。超时窗口：L0 = 64 秒，L1 ≈ 68 分钟，L2 ≈ 73 小时。

## 原理

```
insert / reset / remove 命令
    |
    v   mpsc 通道
+----------------------------+
| Worker（单线程核心）       |
|                            |
| L2 小时层: 64 槽 x 4096t   |
| L1 分钟层: 64 槽 x 64t     |
| L0 秒层:   64 槽 x 1t      |
|    ^                       |
|  current_tick              |
|                            |
|  arena: Vec                |
|  task_info: HashMap        |
+----------------------------+
    |
    v   tokio::spawn（batch 限流）
  到期回调
```

三层轮到期向下级联：L2 → L1 → L0 → 触发回调。槽内只存 arena 索引，不复制 ID。旧 reset 副本 generation 不匹配时自动丢弃。

- **insert / reset**：将 `{ generation }` 推入目标槽位，bump `task_info` 中的 generation。
- **advance**：清空 `current_tick` 对应槽位。如果 `task_info` 中 generation 匹配 → 到期 → 触发回调。旧 `reset` 副本 generation 不匹配 → 丢弃。
- **时钟补偿**：`已流逝时长 / tick_interval` 得出目标 tick，落后则连续追格。

## 性能

Apple M1，release build，50k 一次性任务，criterion 基准：

| 指标 | 数据 |
|--------|-------|
| insert 吞吐 (usize) | ~290k ops/s |
| reset 吞吐 (usize) | ~100k ops/s |
| 到期准确性 | >95% 在超时窗口内触发 |
| 10k 心跳持续刷新 | 10 秒内 0 误触发 |
| 内存（50k 活跃） | 15 秒持续 churn 稳定 |

```bash
# 运行压力测试
cargo test stress -- --test-threads=1 --nocapture

# 运行基准
cargo bench
```

## License

MIT OR Apache-2.0
