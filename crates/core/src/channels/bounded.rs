use crossbeam_queue::ArrayQueue;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use super::{Backpressure, ChannelRecv, ChannelSend, ChannelStats, CloseBehavior, RecvOutcome};

#[cfg(feature = "metrics")]
use crate::metrics::MetricsSink;
#[cfg(feature = "async-channels")]
use tokio::sync::Notify;

struct BoundedInner<T> {
    queue: ArrayQueue<T>,
    closed: AtomicBool,
    senders: AtomicUsize,
    receivers: AtomicUsize,
    enqueued: AtomicU64,
    dropped: AtomicU64,
    drained: AtomicU64,
    depth: AtomicUsize,
    close_behavior: CloseBehavior,
    #[cfg(feature = "async-channels")]
    notify: Arc<Notify>,
    #[cfg(feature = "metrics")]
    metrics: Option<Arc<dyn MetricsSink>>,
}

impl<T> BoundedInner<T> {
    fn new(capacity: usize, close_behavior: CloseBehavior) -> Self {
        Self {
            queue: ArrayQueue::new(capacity),
            closed: AtomicBool::new(false),
            senders: AtomicUsize::new(1),
            receivers: AtomicUsize::new(1),
            enqueued: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            drained: AtomicU64::new(0),
            depth: AtomicUsize::new(0),
            close_behavior,
            #[cfg(feature = "async-channels")]
            notify: Arc::new(Notify::new()),
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
            queue: ArrayQueue::new(capacity),
            closed: AtomicBool::new(false),
            senders: AtomicUsize::new(1),
            receivers: AtomicUsize::new(1),
            enqueued: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            drained: AtomicU64::new(0),
            depth: AtomicUsize::new(0),
            close_behavior,
            #[cfg(feature = "async-channels")]
            notify: Arc::new(Notify::new()),
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

pub struct BoundedSender<T> {
    inner: Arc<BoundedInner<T>>,
}

impl<T> Clone for BoundedSender<T> {
    fn clone(&self) -> Self {
        self.inner.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for BoundedSender<T> {
    fn drop(&mut self) {
        self.inner.senders.fetch_sub(1, Ordering::Relaxed);
        self.inner.try_close();
    }
}

impl<T> std::fmt::Debug for BoundedSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedSender").finish_non_exhaustive()
    }
}

pub struct BoundedReceiver<T> {
    inner: Arc<BoundedInner<T>>,
}

impl<T> Clone for BoundedReceiver<T> {
    fn clone(&self) -> Self {
        self.inner.receivers.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for BoundedReceiver<T> {
    fn drop(&mut self) {
        self.inner.receivers.fetch_sub(1, Ordering::Relaxed);
        self.inner.try_close();
    }
}

pub fn bounded<T>(capacity: usize) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(capacity > 0, "capacity must be greater than zero");
    let inner = Arc::new(BoundedInner::new(capacity, CloseBehavior::FailFast));
    (
        BoundedSender {
            inner: Arc::clone(&inner),
        },
        BoundedReceiver { inner },
    )
}

pub fn bounded_with_behavior<T>(
    capacity: usize,
    close_behavior: CloseBehavior,
) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(capacity > 0, "capacity must be greater than zero");
    let inner = Arc::new(BoundedInner::new(capacity, close_behavior));
    (
        BoundedSender {
            inner: Arc::clone(&inner),
        },
        BoundedReceiver { inner },
    )
}

#[cfg(feature = "metrics")]
pub fn bounded_with_metrics<T>(
    capacity: usize,
    metrics: Arc<dyn MetricsSink>,
) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(capacity > 0, "capacity must be greater than zero");
    let inner = Arc::new(BoundedInner::new_with_metrics(
        capacity,
        CloseBehavior::FailFast,
        metrics,
    ));
    (
        BoundedSender {
            inner: Arc::clone(&inner),
        },
        BoundedReceiver { inner },
    )
}

#[cfg(feature = "metrics")]
pub fn bounded_with_metrics_and_behavior<T>(
    capacity: usize,
    close_behavior: CloseBehavior,
    metrics: Arc<dyn MetricsSink>,
) -> (BoundedSender<T>, BoundedReceiver<T>) {
    assert!(capacity > 0, "capacity must be greater than zero");
    let inner = Arc::new(BoundedInner::new_with_metrics(
        capacity,
        close_behavior,
        metrics,
    ));
    (
        BoundedSender {
            inner: Arc::clone(&inner),
        },
        BoundedReceiver { inner },
    )
}

impl<T: Send> ChannelSend<T> for BoundedSender<T> {
    fn send(&self, value: T) -> Backpressure {
        match self.try_send_owned(value) {
            Ok(()) => Backpressure::Ok,
            Err((Backpressure::Closed, _)) => {
                #[cfg(feature = "metrics")]
                self.inner.inc("channel.bounded.closed");
                Backpressure::Closed
            }
            Err((Backpressure::Full, _)) => {
                // Sync bounded send is best-effort: it drops when full.
                self.inner.dropped.fetch_add(1, Ordering::Relaxed);
                #[cfg(feature = "metrics")]
                self.inner.inc("channel.bounded.full");
                Backpressure::Full
            }
            Err((Backpressure::Ok, _)) => Backpressure::Ok,
        }
    }
}

impl<T> BoundedSender<T> {
    /// Try to send without dropping on `Full` by returning the value back to the caller.
    pub fn try_send_owned(&self, value: T) -> Result<(), (Backpressure, T)> {
        if self.inner.closed.load(Ordering::Acquire) {
            return Err((Backpressure::Closed, value));
        }

        match self.inner.queue.push(value) {
            Ok(()) => {
                self.inner.enqueued.fetch_add(1, Ordering::Relaxed);
                self.inner.depth.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            Err(v) => Err((Backpressure::Full, v)),
        }
    }
}

impl<T: Send> ChannelRecv<T> for BoundedReceiver<T> {
    fn try_recv(&self) -> RecvOutcome<T> {
        match self.inner.queue.pop() {
            Some(v) => {
                self.inner.drained.fetch_add(1, Ordering::Relaxed);
                self.inner.depth.fetch_sub(1, Ordering::Relaxed);
                #[cfg(feature = "async-channels")]
                self.inner.notify.notify_one();
                RecvOutcome::Data(v)
            }
            None if self.inner.closed.load(Ordering::Acquire) => RecvOutcome::Closed,
            None => RecvOutcome::Empty,
        }
    }
}

impl<T> BoundedReceiver<T> {
    pub fn stats(&self) -> ChannelStats {
        ChannelStats {
            enqueued: self.inner.enqueued.load(Ordering::Relaxed),
            dropped: self.inner.dropped.load(Ordering::Relaxed),
            drained: self.inner.drained.load(Ordering::Relaxed),
            depth: self.inner.depth.load(Ordering::Relaxed),
            closed: self.inner.closed.load(Ordering::Relaxed),
        }
    }
}

#[cfg(feature = "async-channels")]
impl<T> BoundedSender<T> {
    pub(crate) fn async_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.inner.notify)
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
    fn bounded_capacity_respected() {
        let (tx, rx) = bounded(1);
        assert_eq!(tx.send(1), Backpressure::Ok);
        assert_eq!(tx.send(2), Backpressure::Full);
        assert_eq!(rx.try_recv(), RecvOutcome::Data(1));
        assert_eq!(rx.try_recv(), RecvOutcome::Empty);
    }

    #[test]
    fn bounded_closed_when_senders_drop() {
        let (tx, rx) = bounded::<u8>(1);
        drop(tx);
        assert_eq!(rx.try_recv(), RecvOutcome::Closed);
    }

    #[test]
    fn bounded_preserves_order_until_capacity() {
        let (tx, rx) = bounded(3);
        for i in 0..5 {
            let _ = tx.send(i);
        }
        assert_eq!(rx.try_recv(), RecvOutcome::Data(0));
        assert_eq!(rx.try_recv(), RecvOutcome::Data(1));
        assert_eq!(rx.try_recv(), RecvOutcome::Data(2));
        assert_eq!(rx.try_recv(), RecvOutcome::Empty);
    }

    #[test]
    fn bounded_multi_consumer_does_not_drop_when_space() {
        let (tx, rx1) = bounded(4);
        let rx2 = rx1.clone();
        for i in 0..2 {
            assert_eq!(tx.send(i), Backpressure::Ok);
        }
        // Each receiver drains independently; capacity covers both
        assert_eq!(rx1.try_recv(), RecvOutcome::Data(0));
        assert_eq!(rx2.try_recv(), RecvOutcome::Data(1));
    }

    proptest! {
        #[test]
        fn bounded_keeps_first_cap_messages_in_order(input in proptest::collection::vec(any::<u8>(), 1..20)) {
            let capacity = 5usize;
            let (tx, rx) = bounded(capacity);
            for v in &input {
                let _ = tx.send(*v);
            }
            let mut drained = Vec::new();
            while let RecvOutcome::Data(v) = rx.try_recv() {
                drained.push(v);
            }
            let expected: Vec<u8> = input.into_iter().take(capacity).collect();
            prop_assert_eq!(drained, expected);
        }
    }

    #[test]
    fn bounded_mpmc_stress() {
        let (tx, rx) = bounded(8);
        let tx = Arc::new(tx);
        let rx = Arc::new(rx);
        let produced = 4usize * 50usize;
        let received = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let txc = tx.clone();
            handles.push(thread::spawn(move || {
                for i in 0..50u32 {
                    loop {
                        if txc.send(i) == Backpressure::Ok {
                            break;
                        }
                        std::thread::yield_now();
                    }
                }
            }));
        }

        let mut recv_handles = Vec::new();
        for _ in 0..2 {
            let rxc = rx.clone();
            let recv_count = received.clone();
            recv_handles.push(thread::spawn(move || {
                loop {
                    match rxc.try_recv() {
                        RecvOutcome::Data(_) => {
                            recv_count.fetch_add(1, Ordering::Relaxed);
                        }
                        RecvOutcome::Empty => {
                            if recv_count.load(Ordering::Relaxed) >= produced {
                                break;
                            }
                            std::thread::yield_now();
                        }
                        RecvOutcome::Closed => break,
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }
        drop(tx);

        for h in recv_handles {
            h.join().unwrap();
        }
        assert_eq!(received.load(Ordering::Relaxed), produced);
    }
}

#[cfg(all(test, feature = "metrics"))]
mod metric_tests {
    use super::*;
    use crate::metrics::InMemoryMetrics;
    use std::sync::Arc;

    #[test]
    fn metrics_record_full_and_closed() {
        let metrics = Arc::new(InMemoryMetrics::default());
        let collector: Arc<dyn crate::metrics::MetricsSink> = metrics.clone();
        let (tx, rx) = bounded_with_metrics(1, collector);
        assert_eq!(tx.send(1), Backpressure::Ok);
        assert_eq!(tx.send(2), Backpressure::Full);
        assert_eq!(metrics.counter("channel.bounded.full"), 1);
        drop(rx);
        assert_eq!(tx.send(3), Backpressure::Closed);
        assert_eq!(metrics.counter("channel.bounded.closed"), 1);
    }
}
