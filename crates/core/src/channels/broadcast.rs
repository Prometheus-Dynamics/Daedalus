use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};

use super::{Backpressure, ChannelRecv, ChannelSend, ChannelStats, CloseBehavior, RecvOutcome};

#[cfg(feature = "metrics")]
use crate::metrics::MetricsSink;

struct Subscriber<T> {
    buffer: Mutex<VecDeque<Arc<T>>>,
}

struct BroadcastInner<T> {
    subscribers: Mutex<Vec<Weak<Subscriber<T>>>>,
    closed: AtomicBool,
    senders: AtomicUsize,
    receivers: AtomicUsize,
    capacity: usize,
    enqueued: AtomicU64,
    dropped: AtomicU64,
    drained: AtomicU64,
    close_behavior: CloseBehavior,
    #[cfg(feature = "metrics")]
    metrics: Option<Arc<dyn MetricsSink>>,
}

impl<T> BroadcastInner<T> {
    fn new(capacity: usize, close_behavior: CloseBehavior) -> Self {
        Self {
            subscribers: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            senders: AtomicUsize::new(1),
            receivers: AtomicUsize::new(1),
            capacity,
            enqueued: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            drained: AtomicU64::new(0),
            close_behavior,
            #[cfg(feature = "metrics")]
            metrics: None,
        }
    }

    #[cfg(feature = "metrics")]
    fn new_with_metrics(
        capacity: usize,
        close_behavior: CloseBehavior,
        metrics: Arc<dyn MetricsSink>,
    ) -> Self {
        Self {
            subscribers: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
            senders: AtomicUsize::new(1),
            receivers: AtomicUsize::new(1),
            capacity,
            enqueued: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            drained: AtomicU64::new(0),
            close_behavior,
            metrics: Some(metrics),
        }
    }

    fn mark_closed(&self) {
        self.closed.store(true, Ordering::Release);
    }

    fn try_close(&self) {
        match self.close_behavior {
            CloseBehavior::FailFast => {
                if self.senders.load(Ordering::Acquire) == 0
                    || self.receivers.load(Ordering::Acquire) == 0
                {
                    self.mark_closed();
                }
            }
            CloseBehavior::DrainUntilSendersDone => {
                if self.senders.load(Ordering::Acquire) == 0 {
                    self.mark_closed();
                }
            }
        }
    }

    #[cfg(feature = "metrics")]
    fn inc(&self, key: &'static str) {
        if let Some(metrics) = &self.metrics {
            metrics.increment(key, 1);
        }
    }
}

pub struct BroadcastSender<T> {
    inner: Arc<BroadcastInner<T>>,
}

impl<T> Clone for BroadcastSender<T> {
    fn clone(&self) -> Self {
        self.inner.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for BroadcastSender<T> {
    fn drop(&mut self) {
        self.inner.senders.fetch_sub(1, Ordering::Relaxed);
        self.inner.try_close();
    }
}

impl<T> std::fmt::Debug for BroadcastSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BroadcastSender").finish_non_exhaustive()
    }
}

pub struct BroadcastReceiver<T> {
    inner: Arc<BroadcastInner<T>>,
    subscriber: Arc<Subscriber<T>>,
}

impl<T> Drop for BroadcastReceiver<T> {
    fn drop(&mut self) {
        self.inner.receivers.fetch_sub(1, Ordering::Relaxed);
        self.inner.try_close();
    }
}

pub fn broadcast<T: Send + Sync>(capacity: usize) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    assert!(capacity > 0, "capacity must be greater than zero");
    let inner = Arc::new(BroadcastInner::new(capacity, CloseBehavior::FailFast));
    let recv = subscribe_inner(&inner);
    (
        BroadcastSender {
            inner: Arc::clone(&inner),
        },
        recv,
    )
}

pub fn broadcast_with_behavior<T: Send + Sync>(
    capacity: usize,
    close_behavior: CloseBehavior,
) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    assert!(capacity > 0, "capacity must be greater than zero");
    let inner = Arc::new(BroadcastInner::new(capacity, close_behavior));
    let recv = subscribe_inner(&inner);
    (
        BroadcastSender {
            inner: Arc::clone(&inner),
        },
        recv,
    )
}

#[cfg(feature = "metrics")]
pub fn broadcast_with_metrics<T: Send + Sync>(
    capacity: usize,
    metrics: Arc<dyn MetricsSink>,
) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    assert!(capacity > 0, "capacity must be greater than zero");
    let inner = Arc::new(BroadcastInner::new_with_metrics(
        capacity,
        CloseBehavior::FailFast,
        metrics,
    ));
    let recv = subscribe_inner(&inner);
    (
        BroadcastSender {
            inner: Arc::clone(&inner),
        },
        recv,
    )
}

#[cfg(feature = "metrics")]
pub fn broadcast_with_metrics_and_behavior<T: Send + Sync>(
    capacity: usize,
    close_behavior: CloseBehavior,
    metrics: Arc<dyn MetricsSink>,
) -> (BroadcastSender<T>, BroadcastReceiver<T>) {
    assert!(capacity > 0, "capacity must be greater than zero");
    let inner = Arc::new(BroadcastInner::new_with_metrics(
        capacity,
        close_behavior,
        metrics,
    ));
    let recv = subscribe_inner(&inner);
    (
        BroadcastSender {
            inner: Arc::clone(&inner),
        },
        recv,
    )
}

fn subscribe_inner<T: Send + Sync>(inner: &Arc<BroadcastInner<T>>) -> BroadcastReceiver<T> {
    let subscriber = Arc::new(Subscriber {
        buffer: Mutex::new(VecDeque::with_capacity(inner.capacity)),
    });
    {
        let mut subs = inner
            .subscribers
            .lock()
            .expect("broadcast subscriber list poisoned");
        subs.push(Arc::downgrade(&subscriber));
    }
    BroadcastReceiver {
        inner: Arc::clone(inner),
        subscriber,
    }
}

impl<T: Send + Sync> BroadcastSender<T> {
    pub fn subscribe(&self) -> BroadcastReceiver<T> {
        self.inner.receivers.fetch_add(1, Ordering::Relaxed);
        subscribe_inner(&self.inner)
    }
}

impl<T: Send + Sync> ChannelSend<Arc<T>> for BroadcastSender<T> {
    fn send(&self, value: Arc<T>) -> Backpressure {
        if self.inner.closed.load(Ordering::Acquire) {
            #[cfg(feature = "metrics")]
            self.inner.inc("channel.broadcast.closed");
            return Backpressure::Closed;
        }

        let mut live = 0usize;
        let mut upgraded = Vec::new();
        {
            let mut subs = self
                .inner
                .subscribers
                .lock()
                .expect("broadcast subscriber list poisoned");
            subs.retain(|weak_sub| {
                if let Some(sub) = weak_sub.upgrade() {
                    upgraded.push(sub);
                    true
                } else {
                    false
                }
            });
        }

        for sub in upgraded {
            live += 1;
            let mut buf = sub.buffer.lock().expect("broadcast buffer poisoned");
            if buf.len() >= self.inner.capacity {
                buf.pop_front();
                #[cfg(feature = "metrics")]
                self.inner.inc("channel.broadcast.dropped");
                self.inner.dropped.fetch_add(1, Ordering::Relaxed);
            }
            buf.push_back(Arc::clone(&value));
            self.inner.enqueued.fetch_add(1, Ordering::Relaxed);
        }

        if live == 0 {
            self.inner.mark_closed();
            #[cfg(feature = "metrics")]
            self.inner.inc("channel.broadcast.closed");
            Backpressure::Closed
        } else {
            Backpressure::Ok
        }
    }
}

impl<T: Send + Sync> ChannelRecv<Arc<T>> for BroadcastReceiver<T> {
    fn try_recv(&self) -> RecvOutcome<Arc<T>> {
        let mut buf = self
            .subscriber
            .buffer
            .lock()
            .expect("broadcast buffer poisoned");
        match buf.pop_front() {
            Some(v) => {
                self.inner.drained.fetch_add(1, Ordering::Relaxed);
                RecvOutcome::Data(v)
            }
            None if self.inner.closed.load(Ordering::Acquire) => RecvOutcome::Closed,
            None => RecvOutcome::Empty,
        }
    }
}

impl<T: Send + Sync> BroadcastReceiver<T> {
    pub fn stats(&self) -> ChannelStats {
        ChannelStats {
            enqueued: self.inner.enqueued.load(Ordering::Relaxed),
            dropped: self.inner.dropped.load(Ordering::Relaxed),
            drained: self.inner.drained.load(Ordering::Relaxed),
            depth: self.subscriber.buffer.lock().map(|b| b.len()).unwrap_or(0),
            closed: self.inner.closed.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;

    #[test]
    fn broadcast_to_multiple_subscribers() {
        let (tx, rx1) = broadcast::<u32>(2);
        let rx2 = tx.subscribe();

        let payload = Arc::new(5);
        tx.send(Arc::clone(&payload));

        assert_eq!(rx1.try_recv(), RecvOutcome::Data(Arc::new(5)));
        assert_eq!(rx2.try_recv(), RecvOutcome::Data(Arc::new(5)));
    }

    #[test]
    fn broadcast_drops_oldest() {
        let (tx, rx) = broadcast::<u32>(1);
        tx.send(Arc::new(1));
        tx.send(Arc::new(2));
        assert_eq!(rx.try_recv(), RecvOutcome::Data(Arc::new(2)));
    }

    proptest! {
        #[test]
        fn broadcast_respects_per_subscriber_capacity(values in proptest::collection::vec(any::<u8>(), 2..20)) {
            let (tx, rx1) = broadcast::<u8>(2);
            let rx2 = tx.subscribe();
            for v in &values {
                let _ = tx.send(Arc::new(*v));
            }
            let mut seen1 = Vec::new();
            while let RecvOutcome::Data(v) = rx1.try_recv() {
                seen1.push(*v);
            }
            let mut seen2 = Vec::new();
            while let RecvOutcome::Data(v) = rx2.try_recv() {
                seen2.push(*v);
            }
            // Each subscriber keeps only the last 2 items due to capacity=2
            let expected: Vec<u8> = values.into_iter().rev().take(2).collect::<Vec<_>>().into_iter().rev().collect();
            prop_assert_eq!(seen1, expected.clone());
            prop_assert_eq!(seen2, expected);
        }
    }

    #[test]
    fn broadcast_mpmc_smoke() {
        let (tx, rx1) = broadcast::<u32>(4);
        let rx2 = tx.subscribe();
        let tx = Arc::new(tx);
        let produced = 4u32 * 50u32;
        let seen1 = Arc::new(AtomicUsize::new(0));
        let seen2 = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for offset in 0..4u32 {
            let txc = tx.clone();
            handles.push(thread::spawn(move || {
                for i in 0..50u32 {
                    let _ = txc.send(Arc::new(i + offset * 1_000));
                }
            }));
        }

        let recv1 = rx1;
        let recv2 = rx2;
        let h1 = {
            let seen1 = seen1.clone();
            thread::spawn(move || {
                loop {
                    match recv1.try_recv() {
                        RecvOutcome::Data(_) => {
                            seen1.fetch_add(1, Ordering::Relaxed);
                        }
                        RecvOutcome::Empty => {
                            if seen1.load(Ordering::Relaxed) >= produced as usize {
                                break;
                            }
                            std::thread::yield_now();
                        }
                        RecvOutcome::Closed => break,
                    }
                }
            })
        };
        let h2 = {
            let seen2 = seen2.clone();
            thread::spawn(move || {
                loop {
                    match recv2.try_recv() {
                        RecvOutcome::Data(_) => {
                            seen2.fetch_add(1, Ordering::Relaxed);
                        }
                        RecvOutcome::Empty => {
                            if seen2.load(Ordering::Relaxed) >= produced as usize {
                                break;
                            }
                            std::thread::yield_now();
                        }
                        RecvOutcome::Closed => break,
                    }
                }
            })
        };

        for h in handles {
            h.join().unwrap();
        }
        drop(tx);
        h1.join().unwrap();
        h2.join().unwrap();

        assert!(seen1.load(Ordering::Relaxed) <= produced as usize);
        assert!(seen2.load(Ordering::Relaxed) <= produced as usize);
    }
}

#[cfg(all(test, feature = "metrics"))]
mod metric_tests {
    use super::*;
    use crate::metrics::InMemoryMetrics;
    use std::sync::Arc;

    #[test]
    fn metrics_record_drops_and_closed() {
        let metrics = Arc::new(InMemoryMetrics::default());
        let collector: Arc<dyn crate::metrics::MetricsSink> = metrics.clone();
        let (tx, rx) = broadcast_with_metrics::<u32>(1, collector);
        tx.send(Arc::new(1));
        tx.send(Arc::new(2));
        assert_eq!(metrics.counter("channel.broadcast.dropped"), 1);
        drop(rx);
        assert_eq!(tx.send(Arc::new(3)), Backpressure::Closed);
        assert_eq!(metrics.counter("channel.broadcast.closed"), 1);
    }
}
