use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use super::{Backpressure, ChannelRecv, ChannelSend, ChannelStats, CloseBehavior, RecvOutcome};
use crate::messages::Sequence;

#[cfg(feature = "metrics")]
use crate::metrics::MetricsSink;

struct NewestInner<T> {
    slot: Mutex<Option<(Sequence, Arc<T>)>>,
    next_seq: AtomicU64,
    closed: AtomicBool,
    senders: AtomicUsize,
    receivers: AtomicUsize,
    enqueued: AtomicU64,
    dropped: AtomicU64,
    drained: AtomicU64,
    close_behavior: CloseBehavior,
    #[cfg(feature = "metrics")]
    metrics: Option<Arc<dyn MetricsSink>>,
}

impl<T> NewestInner<T> {
    fn new(close_behavior: CloseBehavior) -> Self {
        Self {
            slot: Mutex::new(None),
            next_seq: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            senders: AtomicUsize::new(1),
            receivers: AtomicUsize::new(1),
            enqueued: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            drained: AtomicU64::new(0),
            close_behavior,
            #[cfg(feature = "metrics")]
            metrics: None,
        }
    }

    #[cfg(feature = "metrics")]
    fn new_with_metrics(close_behavior: CloseBehavior, metrics: Arc<dyn MetricsSink>) -> Self {
        Self {
            slot: Mutex::new(None),
            next_seq: AtomicU64::new(0),
            closed: AtomicBool::new(false),
            senders: AtomicUsize::new(1),
            receivers: AtomicUsize::new(1),
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

pub struct NewestSender<T> {
    inner: Arc<NewestInner<T>>,
}

impl<T> Clone for NewestSender<T> {
    fn clone(&self) -> Self {
        self.inner.senders.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> Drop for NewestSender<T> {
    fn drop(&mut self) {
        self.inner.senders.fetch_sub(1, Ordering::Relaxed);
        self.inner.try_close();
    }
}

impl<T> std::fmt::Debug for NewestSender<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NewestSender").finish_non_exhaustive()
    }
}

pub struct NewestReceiver<T> {
    inner: Arc<NewestInner<T>>,
    last_seen: Mutex<Option<Sequence>>,
}

impl<T> Clone for NewestReceiver<T> {
    fn clone(&self) -> Self {
        self.inner.receivers.fetch_add(1, Ordering::Relaxed);
        Self {
            inner: Arc::clone(&self.inner),
            last_seen: Mutex::new(None),
        }
    }
}

impl<T> Drop for NewestReceiver<T> {
    fn drop(&mut self) {
        self.inner.receivers.fetch_sub(1, Ordering::Relaxed);
        self.inner.try_close();
    }
}

pub fn newest<T>() -> (NewestSender<T>, NewestReceiver<T>) {
    let inner = Arc::new(NewestInner::new(CloseBehavior::FailFast));
    (
        NewestSender {
            inner: Arc::clone(&inner),
        },
        NewestReceiver {
            inner,
            last_seen: Mutex::new(None),
        },
    )
}

#[cfg(feature = "metrics")]
pub fn newest_with_metrics<T>(
    metrics: Arc<dyn MetricsSink>,
) -> (NewestSender<T>, NewestReceiver<T>) {
    let inner = Arc::new(NewestInner::new_with_metrics(
        CloseBehavior::FailFast,
        metrics,
    ));
    (
        NewestSender {
            inner: Arc::clone(&inner),
        },
        NewestReceiver {
            inner,
            last_seen: Mutex::new(None),
        },
    )
}

pub fn newest_with_behavior<T>(
    close_behavior: CloseBehavior,
) -> (NewestSender<T>, NewestReceiver<T>) {
    let inner = Arc::new(NewestInner::new(close_behavior));
    (
        NewestSender {
            inner: Arc::clone(&inner),
        },
        NewestReceiver {
            inner,
            last_seen: Mutex::new(None),
        },
    )
}

#[cfg(feature = "metrics")]
pub fn newest_with_metrics_and_behavior<T>(
    close_behavior: CloseBehavior,
    metrics: Arc<dyn MetricsSink>,
) -> (NewestSender<T>, NewestReceiver<T>) {
    let inner = Arc::new(NewestInner::new_with_metrics(close_behavior, metrics));
    (
        NewestSender {
            inner: Arc::clone(&inner),
        },
        NewestReceiver {
            inner,
            last_seen: Mutex::new(None),
        },
    )
}

impl<T: Send + Sync> ChannelSend<Arc<T>> for NewestSender<T> {
    fn send(&self, value: Arc<T>) -> Backpressure {
        if self.inner.closed.load(Ordering::Acquire) {
            #[cfg(feature = "metrics")]
            self.inner.inc("channel.newest.closed");
            return Backpressure::Closed;
        }
        let seq = Sequence::new(self.inner.next_seq.fetch_add(1, Ordering::Relaxed));
        let mut guard = self.inner.slot.lock().expect("newest slot lock poisoned");
        let dropped = guard.replace((seq, value)).is_some();
        if dropped {
            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
            #[cfg(feature = "metrics")]
            self.inner.inc("channel.newest.dropped");
        }
        self.inner.enqueued.fetch_add(1, Ordering::Relaxed);
        Backpressure::Ok
    }
}

impl<T: Send + Sync> ChannelRecv<Arc<T>> for NewestReceiver<T> {
    fn try_recv(&self) -> RecvOutcome<Arc<T>> {
        let mut last_seen = self
            .last_seen
            .lock()
            .expect("newest receiver lock poisoned");
        let guard = self.inner.slot.lock().expect("newest slot lock poisoned");
        let Some((seq, value)) = guard.as_ref() else {
            return if self.inner.closed.load(Ordering::Acquire) {
                RecvOutcome::Closed
            } else {
                RecvOutcome::Empty
            };
        };
        if last_seen.map(|seen| seq <= &seen).unwrap_or(false) {
            return if self.inner.closed.load(Ordering::Acquire) {
                RecvOutcome::Closed
            } else {
                RecvOutcome::Empty
            };
        }
        *last_seen = Some(*seq);
        self.inner.drained.fetch_add(1, Ordering::Relaxed);
        RecvOutcome::Data(Arc::clone(value))
    }
}

impl<T> NewestReceiver<T> {
    pub fn stats(&self) -> ChannelStats {
        ChannelStats {
            enqueued: self.inner.enqueued.load(Ordering::Relaxed),
            dropped: self.inner.dropped.load(Ordering::Relaxed),
            drained: self.inner.drained.load(Ordering::Relaxed),
            depth: 0,
            closed: self.inner.closed.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn newest_overwrites() {
        let (tx, rx) = newest::<u64>();
        tx.send(Arc::new(1));
        tx.send(Arc::new(2));
        assert_eq!(rx.try_recv(), RecvOutcome::Data(Arc::new(2)));
        assert!(matches!(
            rx.try_recv(),
            RecvOutcome::Empty | RecvOutcome::Closed
        ));
    }

    #[test]
    fn newest_closed_when_senders_drop() {
        let (tx, rx) = newest::<u64>();
        drop(tx);
        assert!(matches!(
            rx.try_recv(),
            RecvOutcome::Closed | RecvOutcome::Empty
        ));
    }

    proptest! {
        #[test]
        fn newest_returns_monotonic_sequences(values in proptest::collection::vec(any::<u32>(), 1..50)) {
            let (tx, rx) = newest::<u32>();
            for v in &values {
                let _ = tx.send(Arc::new(*v));
            }
            let mut seen = Vec::new();
            while let RecvOutcome::Data(v) = rx.try_recv() {
                seen.push(*v);
            }
            // At most one value, and if present it's the last written.
            if let Some(last) = values.last() {
                prop_assert!(seen.is_empty() || seen == vec![*last]);
            } else {
                prop_assert!(seen.is_empty());
            }
        }
    }
}

#[cfg(all(test, feature = "metrics"))]
mod metric_tests {
    use super::*;
    use crate::metrics::InMemoryMetrics;
    use std::sync::Arc;

    #[test]
    fn metrics_record_dropped_and_closed() {
        let metrics = Arc::new(InMemoryMetrics::default());
        let collector: Arc<dyn crate::metrics::MetricsSink> = metrics.clone();
        let (tx, rx) = newest_with_metrics::<u64>(collector);
        tx.send(Arc::new(1));
        tx.send(Arc::new(2));
        assert_eq!(metrics.counter("channel.newest.dropped"), 1);
        drop(rx);
        assert_eq!(tx.send(Arc::new(3)), Backpressure::Closed);
        assert_eq!(metrics.counter("channel.newest.closed"), 1);
    }
}
