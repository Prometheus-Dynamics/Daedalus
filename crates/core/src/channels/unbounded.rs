use crossbeam_queue::SegQueue;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

use super::{Backpressure, ChannelRecv, ChannelSend, ChannelStats, CloseBehavior, RecvOutcome};

#[cfg(feature = "metrics")]
use crate::metrics::MetricsSink;
#[cfg(feature = "async-channels")]
use tokio::sync::Notify;

struct UnboundedInner<T> {
    queue: SegQueue<T>,
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

impl<T> UnboundedInner<T> {
    fn new(close_behavior: CloseBehavior) -> Self {
        Self {
            queue: SegQueue::new(),
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
    fn new_with_metrics(close_behavior: CloseBehavior, metrics: Arc<dyn MetricsSink>) -> Self {
        Self {
            queue: SegQueue::new(),
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
        #[cfg(feature = "async-channels")]
        self.notify.notify_waiters();
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

pub struct UnboundedSender<T> {
    inner: Arc<UnboundedInner<T>>,
}

impl<T> Clone for UnboundedSender<T> {
    fn clone(&self) -> Self {
        self.inner.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for UnboundedSender<T> {
    fn drop(&mut self) {
        self.inner.senders.fetch_sub(1, Ordering::Relaxed);
        self.inner.try_close();
    }
}

impl<T> std::fmt::Debug for UnboundedSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnboundedSender").finish_non_exhaustive()
    }
}

pub struct UnboundedReceiver<T> {
    inner: Arc<UnboundedInner<T>>,
}

impl<T> Clone for UnboundedReceiver<T> {
    fn clone(&self) -> Self {
        self.inner.receivers.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for UnboundedReceiver<T> {
    fn drop(&mut self) {
        self.inner.receivers.fetch_sub(1, Ordering::Relaxed);
        self.inner.try_close();
    }
}

pub fn unbounded<T>() -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let inner = Arc::new(UnboundedInner::new(CloseBehavior::FailFast));
    (
        UnboundedSender {
            inner: Arc::clone(&inner),
        },
        UnboundedReceiver { inner },
    )
}

pub fn unbounded_with_behavior<T>(
    close_behavior: CloseBehavior,
) -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let inner = Arc::new(UnboundedInner::new(close_behavior));
    (
        UnboundedSender {
            inner: Arc::clone(&inner),
        },
        UnboundedReceiver { inner },
    )
}

#[cfg(feature = "metrics")]
pub fn unbounded_with_metrics<T>(
    metrics: Arc<dyn MetricsSink>,
) -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let inner = Arc::new(UnboundedInner::new_with_metrics(
        CloseBehavior::FailFast,
        metrics,
    ));
    (
        UnboundedSender {
            inner: Arc::clone(&inner),
        },
        UnboundedReceiver { inner },
    )
}

#[cfg(feature = "metrics")]
pub fn unbounded_with_metrics_and_behavior<T>(
    close_behavior: CloseBehavior,
    metrics: Arc<dyn MetricsSink>,
) -> (UnboundedSender<T>, UnboundedReceiver<T>) {
    let inner = Arc::new(UnboundedInner::new_with_metrics(close_behavior, metrics));
    (
        UnboundedSender {
            inner: Arc::clone(&inner),
        },
        UnboundedReceiver { inner },
    )
}

impl<T: Send> ChannelSend<T> for UnboundedSender<T> {
    fn send(&self, value: T) -> Backpressure {
        if self.inner.closed.load(Ordering::Acquire) {
            #[cfg(feature = "metrics")]
            self.inner.inc("channel.unbounded.closed");
            return Backpressure::Closed;
        }
        self.inner.queue.push(value);
        self.inner.enqueued.fetch_add(1, Ordering::Relaxed);
        self.inner.depth.fetch_add(1, Ordering::Relaxed);
        #[cfg(feature = "async-channels")]
        self.inner.notify.notify_one();
        Backpressure::Ok
    }
}

impl<T: Send> ChannelRecv<T> for UnboundedReceiver<T> {
    fn try_recv(&self) -> RecvOutcome<T> {
        match self.inner.queue.pop() {
            Some(v) => {
                self.inner.drained.fetch_add(1, Ordering::Relaxed);
                self.inner.depth.fetch_sub(1, Ordering::Relaxed);
                RecvOutcome::Data(v)
            }
            None if self.inner.closed.load(Ordering::Acquire) => RecvOutcome::Closed,
            None => RecvOutcome::Empty,
        }
    }
}

impl<T> UnboundedReceiver<T> {
    pub fn stats(&self) -> ChannelStats {
        ChannelStats {
            enqueued: self.inner.enqueued.load(Ordering::Relaxed),
            dropped: self.inner.dropped.load(Ordering::Relaxed),
            drained: self.inner.drained.load(Ordering::Relaxed),
            depth: self.inner.depth.load(Ordering::Relaxed),
            closed: self.inner.closed.load(Ordering::Relaxed),
        }
    }

    #[cfg(feature = "async-channels")]
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
    fn unbounded_send_recv() {
        let (tx, rx) = unbounded();
        assert_eq!(tx.send(7), Backpressure::Ok);
        assert_eq!(rx.try_recv(), RecvOutcome::Data(7));
        assert_eq!(rx.try_recv(), RecvOutcome::Empty);
    }

    #[test]
    fn unbounded_closed_when_senders_drop() {
        let (tx, rx) = unbounded::<u64>();
        drop(tx);
        assert_eq!(rx.try_recv(), RecvOutcome::Closed);
    }

    proptest! {
        #[test]
        fn unbounded_preserves_order(input in proptest::collection::vec(any::<u32>(), 1..50)) {
            let (tx, rx) = unbounded();
            for v in &input {
                let _ = tx.send(*v);
            }
            let mut drained = Vec::new();
            while let RecvOutcome::Data(v) = rx.try_recv() {
                drained.push(v);
            }
            prop_assert_eq!(drained, input);
        }
    }

    #[test]
    fn unbounded_mpmc_stress() {
        let (tx, rx) = unbounded();
        let tx = Arc::new(tx);
        let rx = Arc::new(rx);
        let produced = 4usize * 50usize;
        let received = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..4 {
            let txc = tx.clone();
            handles.push(thread::spawn(move || {
                for i in 0..50u32 {
                    let _ = txc.send(i);
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
    fn metrics_record_closed() {
        let metrics = Arc::new(InMemoryMetrics::default());
        let collector: Arc<dyn crate::metrics::MetricsSink> = metrics.clone();
        let (tx, rx) = unbounded_with_metrics(collector);
        drop(rx);
        assert_eq!(tx.send(1), Backpressure::Closed);
        assert_eq!(metrics.counter("channel.unbounded.closed"), 1);
    }
}
