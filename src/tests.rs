#![allow(unused_imports)]

use super::*;
use crate::*;
use std::future::Future;
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::Duration;
use tokio::time::{advance, pause, sleep};

fn cfg_fast() -> WheelConfig {
  WheelConfig { tick_interval: Duration::from_millis(10), ..Default::default() }
}

macro_rules! cb {
  ($n:ident) => {{
    let n = Arc::clone(&$n);
    move |_: String| { let n = Arc::clone(&n); async move { n.fetch_add(1, Ordering::SeqCst); } }
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
  let (h, _g) = TimingWheel::start(cfg_fast(), move |_: String| {
    async move { panic!("intentional") }
  });
  sleep(Duration::from_millis(5)).await;
  h.insert("x".into(), Duration::from_millis(100));
  advance(Duration::from_millis(300)).await;
  sleep(Duration::from_millis(100)).await;
  assert_eq!(h.abnormal_total(), 1);
  assert_eq!(h.expirations_total(), 0);
  h.shutdown();
}
