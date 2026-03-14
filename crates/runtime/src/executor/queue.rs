use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

#[cfg(feature = "lockfree-queues")]
use crossbeam_queue::ArrayQueue;

use crate::plan::{BackpressureStrategy, EdgePolicyKind};

use super::payload_size_bytes;
use super::{CorrelatedPayload, ExecutionTelemetry};

/// Simple ring buffer for bounded queues.
pub struct RingBuf {
    buf: Vec<Option<CorrelatedPayload>>,
    head: usize,
    len: usize,
}

impl RingBuf {
    pub fn new(cap: usize) -> Self {
        Self {
            buf: vec![None; cap.max(1)],
            head: 0,
            len: 0,
        }
    }

    pub fn cap(&self) -> usize {
        self.buf.len()
    }

    pub fn pop_front(&mut self) -> Option<CorrelatedPayload> {
        if self.len == 0 {
            return None;
        }
        let idx = self.head;
        let out = self.buf[idx].take();
        self.head = (self.head + 1) % self.cap();
        self.len -= 1;
        out
    }

    pub fn push_back(&mut self, payload: CorrelatedPayload) -> bool {
        let mut dropped = false;
        if self.len == self.cap() {
            // drop oldest
            self.pop_front();
            dropped = true;
        }
        let idx = (self.head + self.len) % self.cap();
        self.buf[idx] = Some(payload);
        self.len = (self.len + 1).min(self.cap());
        dropped
    }

    pub fn is_full(&self) -> bool {
        self.len == self.cap()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn payload_bytes(&self) -> u64 {
        let mut total = 0u64;
        for offset in 0..self.len {
            let idx = (self.head + offset) % self.cap();
            if let Some(payload) = self.buf[idx].as_ref() {
                total = total.saturating_add(payload_size_bytes(&payload.inner).unwrap_or(0));
            }
        }
        total
    }
}

pub enum EdgeQueue {
    Deque(std::collections::VecDeque<CorrelatedPayload>),
    Bounded { ring: RingBuf },
}

impl Default for EdgeQueue {
    fn default() -> Self {
        EdgeQueue::Deque(std::collections::VecDeque::new())
    }
}

impl EdgeQueue {
    pub(crate) fn pop_front(&mut self) -> Option<CorrelatedPayload> {
        match self {
            EdgeQueue::Deque(d) => d.pop_front(),
            EdgeQueue::Bounded { ring } => ring.pop_front(),
        }
    }

    pub fn ensure_policy(&mut self, policy: &EdgePolicyKind) {
        match policy {
            EdgePolicyKind::Bounded { cap } => match self {
                EdgeQueue::Bounded { ring } => {
                    if ring.cap() != *cap {
                        *ring = RingBuf::new(*cap);
                    }
                }
                _ => {
                    *self = EdgeQueue::Bounded {
                        ring: RingBuf::new(*cap),
                    }
                }
            },
            EdgePolicyKind::Fifo | EdgePolicyKind::Broadcast | EdgePolicyKind::NewestWins => {
                if let EdgeQueue::Bounded { .. } = self {
                    *self = EdgeQueue::Deque(std::collections::VecDeque::new());
                }
            }
        }
    }

    pub fn is_full(&self) -> bool {
        match self {
            EdgeQueue::Deque(_) => false,
            EdgeQueue::Bounded { ring } => ring.is_full(),
        }
    }

    pub fn len(&self) -> usize {
        match self {
            EdgeQueue::Deque(d) => d.len(),
            EdgeQueue::Bounded { ring } => ring.len(),
        }
    }

    pub fn capacity(&self) -> Option<usize> {
        match self {
            EdgeQueue::Deque(_) => None,
            EdgeQueue::Bounded { ring } => Some(ring.cap()),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            EdgeQueue::Deque(d) => d.is_empty(),
            EdgeQueue::Bounded { ring } => ring.is_empty(),
        }
    }

    pub fn payload_bytes(&self) -> u64 {
        match self {
            EdgeQueue::Deque(d) => d
                .iter()
                .map(|payload| payload_size_bytes(&payload.inner).unwrap_or(0))
                .fold(0u64, u64::saturating_add),
            EdgeQueue::Bounded { ring } => ring.payload_bytes(),
        }
    }

    pub fn push(&mut self, policy: &EdgePolicyKind, payload: CorrelatedPayload) -> bool {
        match policy {
            EdgePolicyKind::NewestWins => {
                match self {
                    EdgeQueue::Deque(d) => {
                        d.clear();
                        d.push_back(payload);
                    }
                    EdgeQueue::Bounded { .. } => {
                        *self = EdgeQueue::Deque(std::collections::VecDeque::from([payload]));
                    }
                }
                false
            }
            EdgePolicyKind::Broadcast | EdgePolicyKind::Fifo => {
                match self {
                    EdgeQueue::Deque(d) => d.push_back(payload),
                    EdgeQueue::Bounded { .. } => {
                        *self = EdgeQueue::Deque(std::collections::VecDeque::from([payload]));
                    }
                }
                false
            }
            EdgePolicyKind::Bounded { cap } => match self {
                EdgeQueue::Bounded { ring } => ring.push_back(payload),
                EdgeQueue::Deque(d) => {
                    let mut ring = RingBuf::new(*cap);
                    for p in d.drain(..) {
                        ring.push_back(p);
                    }
                    let dropped = ring.push_back(payload);
                    *self = EdgeQueue::Bounded { ring };
                    dropped
                }
            },
        }
    }
}

#[derive(Default)]
pub struct EdgeStorageMetrics {
    current_queue_bytes: AtomicU64,
    peak_queue_bytes: AtomicU64,
}

impl EdgeStorageMetrics {
    pub(crate) fn set_current_bytes(&self, current_bytes: u64) {
        self.current_queue_bytes
            .store(current_bytes, Ordering::Relaxed);
        self.peak_queue_bytes
            .fetch_max(current_bytes, Ordering::Relaxed);
    }

    pub(crate) fn adjust_bytes(&self, added_bytes: u64, removed_bytes: u64) {
        let mut current = self.current_queue_bytes.load(Ordering::Relaxed);
        loop {
            let next = current
                .saturating_add(added_bytes)
                .saturating_sub(removed_bytes);
            match self.current_queue_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.peak_queue_bytes.fetch_max(next, Ordering::Relaxed);
                    break;
                }
                Err(observed) => current = observed,
            }
        }
    }

    pub(crate) fn snapshot(&self) -> (u64, u64) {
        (
            self.current_queue_bytes.load(Ordering::Relaxed),
            self.peak_queue_bytes.load(Ordering::Relaxed),
        )
    }
}

/// Storage wrapper per edge; allows swapping queue implementations.
pub enum EdgeStorage {
    Locked {
        queue: Arc<Mutex<EdgeQueue>>,
        metrics: Arc<EdgeStorageMetrics>,
    },
    #[cfg(feature = "lockfree-queues")]
    BoundedLf {
        queue: Arc<ArrayQueue<CorrelatedPayload>>,
        metrics: Arc<EdgeStorageMetrics>,
    },
}

pub fn build_queues(plan: &crate::plan::RuntimePlan) -> Vec<EdgeStorage> {
    #[cfg(feature = "lockfree-queues")]
    let use_lockfree = plan.lockfree_queues;
    plan.edges
        .iter()
        .map(|(_, _, _, _, policy)| match policy {
            EdgePolicyKind::Bounded { cap } => {
                let metrics = Arc::new(EdgeStorageMetrics::default());
                #[cfg(feature = "lockfree-queues")]
                {
                    if use_lockfree {
                        EdgeStorage::BoundedLf {
                            queue: Arc::new(ArrayQueue::new(*cap)),
                            metrics,
                        }
                    } else {
                        EdgeStorage::Locked {
                            queue: Arc::new(Mutex::new(EdgeQueue::Bounded {
                                ring: RingBuf::new(*cap),
                            })),
                            metrics,
                        }
                    }
                }
                #[cfg(not(feature = "lockfree-queues"))]
                {
                    EdgeStorage::Locked {
                        queue: Arc::new(Mutex::new(EdgeQueue::Bounded {
                            ring: RingBuf::new(*cap),
                        })),
                        metrics,
                    }
                }
            }
            _ => EdgeStorage::Locked {
                queue: Arc::new(Mutex::new(EdgeQueue::default())),
                metrics: Arc::new(EdgeStorageMetrics::default()),
            },
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub fn apply_policy(
    edge_idx: usize,
    policy: &EdgePolicyKind,
    payload: &CorrelatedPayload,
    queues: &Arc<Vec<EdgeStorage>>,
    warnings_seen: &Arc<Mutex<std::collections::HashSet<String>>>,
    telem: &mut ExecutionTelemetry,
    warning_label: Option<String>,
    backpressure: BackpressureStrategy,
) {
    if let Some(storage) = queues.get(edge_idx) {
        let payload_bytes = if cfg!(feature = "metrics") && telem.metrics_level.is_detailed() {
            payload_size_bytes(&payload.inner)
        } else {
            None
        };
        telem.record_edge_payload(edge_idx, payload_bytes);
        match storage {
            EdgeStorage::Locked { queue, metrics } => {
                if let Ok(mut q) = queue.lock() {
                    q.ensure_policy(policy);
                    telem.record_edge_capacity(edge_idx, q.capacity());
                    let dropped = match (policy, backpressure) {
                        (EdgePolicyKind::Bounded { .. }, BackpressureStrategy::BoundedQueues)
                            if q.is_full() =>
                        {
                            true
                        }
                        (EdgePolicyKind::Bounded { .. }, BackpressureStrategy::ErrorOnOverflow)
                            if q.is_full() =>
                        {
                            telem.backpressure_events += 1;
                            let label = warning_label
                                .clone()
                                .unwrap_or_else(|| format!("bounded_error_edge_{edge_idx}"));
                            record_warning(&label, warnings_seen, telem);
                            return;
                        }
                        _ => {
                            let mut payload = payload.clone();
                            payload.enqueued_at = Instant::now();
                            q.push(policy, payload)
                        }
                    };
                    if dropped {
                        telem.backpressure_events += 1;
                        telem.record_edge_drop(edge_idx, 1);
                        let label = warning_label
                            .clone()
                            .unwrap_or_else(|| format!("bounded_drop_edge_{edge_idx}"));
                        record_warning(&label, warnings_seen, telem);
                    }
                    telem.record_edge_depth(edge_idx, q.len());
                    let current_queue_bytes = q.payload_bytes();
                    metrics.set_current_bytes(current_queue_bytes);
                    telem.record_edge_queue_bytes(edge_idx, current_queue_bytes);
                }
            }
            #[cfg(feature = "lockfree-queues")]
            EdgeStorage::BoundedLf { queue, metrics } => {
                let mut dropped = false;
                telem.record_edge_capacity(edge_idx, Some(queue.capacity()));
                let added_bytes = payload_bytes.unwrap_or(0);
                let mut removed_bytes = 0u64;
                match backpressure {
                    BackpressureStrategy::BoundedQueues => {
                        if queue.is_full() {
                            dropped = true;
                        } else {
                            let mut payload = payload.clone();
                            payload.enqueued_at = Instant::now();
                            queue.push(payload).unwrap();
                            metrics.adjust_bytes(added_bytes, 0);
                        }
                    }
                    BackpressureStrategy::ErrorOnOverflow => {
                        if queue.is_full() {
                            telem.backpressure_events += 1;
                            let label = warning_label
                                .clone()
                                .unwrap_or_else(|| format!("bounded_error_edge_{edge_idx}"));
                            record_warning(&label, warnings_seen, telem);
                            return;
                        } else {
                            let mut payload = payload.clone();
                            payload.enqueued_at = Instant::now();
                            queue.push(payload).unwrap();
                            metrics.adjust_bytes(added_bytes, 0);
                        }
                    }
                    BackpressureStrategy::None => {
                        let mut payload = payload.clone();
                        payload.enqueued_at = Instant::now();
                        if queue.push(payload.clone()).is_err() {
                            removed_bytes = queue
                                .pop()
                                .and_then(|removed| payload_size_bytes(&removed.inner))
                                .unwrap_or(0);
                            let _ = queue.push(payload.clone());
                            dropped = true;
                            metrics.adjust_bytes(added_bytes, removed_bytes);
                        } else {
                            metrics.adjust_bytes(added_bytes, 0);
                        }
                    }
                }
                if dropped {
                    telem.backpressure_events += 1;
                    telem.record_edge_drop(edge_idx, 1);
                    let label = warning_label
                        .clone()
                        .unwrap_or_else(|| format!("bounded_drop_edge_{edge_idx}"));
                    record_warning(&label, warnings_seen, telem);
                }
                telem.record_edge_depth(edge_idx, queue.len());
                let (current_queue_bytes, _) = metrics.snapshot();
                telem.record_edge_queue_bytes(edge_idx, current_queue_bytes);
            }
        }
    }
}

pub struct ApplyPolicyOwnedArgs<'a> {
    pub edge_idx: usize,
    pub policy: &'a EdgePolicyKind,
    pub payload: CorrelatedPayload,
    pub queues: &'a Arc<Vec<EdgeStorage>>,
    pub warnings_seen: &'a Arc<Mutex<std::collections::HashSet<String>>>,
    pub telem: &'a mut ExecutionTelemetry,
    pub warning_label: Option<String>,
    pub backpressure: BackpressureStrategy,
}

pub fn apply_policy_owned(args: ApplyPolicyOwnedArgs<'_>) {
    let ApplyPolicyOwnedArgs {
        edge_idx,
        policy,
        mut payload,
        queues,
        warnings_seen,
        telem,
        warning_label,
        backpressure,
    } = args;
    if let Some(storage) = queues.get(edge_idx) {
        let payload_bytes = if cfg!(feature = "metrics") && telem.metrics_level.is_detailed() {
            payload_size_bytes(&payload.inner)
        } else {
            None
        };
        telem.record_edge_payload(edge_idx, payload_bytes);
        match storage {
            EdgeStorage::Locked { queue, metrics } => {
                if let Ok(mut q) = queue.lock() {
                    q.ensure_policy(policy);
                    telem.record_edge_capacity(edge_idx, q.capacity());
                    let dropped = match (policy, backpressure) {
                        (EdgePolicyKind::Bounded { .. }, BackpressureStrategy::BoundedQueues)
                            if q.is_full() =>
                        {
                            true
                        }
                        (EdgePolicyKind::Bounded { .. }, BackpressureStrategy::ErrorOnOverflow)
                            if q.is_full() =>
                        {
                            telem.backpressure_events += 1;
                            let label = warning_label
                                .clone()
                                .unwrap_or_else(|| format!("bounded_error_edge_{edge_idx}"));
                            record_warning(&label, warnings_seen, telem);
                            return;
                        }
                        _ => {
                            payload.enqueued_at = Instant::now();
                            q.push(policy, payload)
                        }
                    };
                    if dropped {
                        telem.backpressure_events += 1;
                        telem.record_edge_drop(edge_idx, 1);
                        let label = warning_label
                            .clone()
                            .unwrap_or_else(|| format!("bounded_drop_edge_{edge_idx}"));
                        record_warning(&label, warnings_seen, telem);
                    }
                    telem.record_edge_depth(edge_idx, q.len());
                    let current_queue_bytes = q.payload_bytes();
                    metrics.set_current_bytes(current_queue_bytes);
                    telem.record_edge_queue_bytes(edge_idx, current_queue_bytes);
                }
            }
            #[cfg(feature = "lockfree-queues")]
            EdgeStorage::BoundedLf { queue, metrics } => {
                let mut dropped = false;
                telem.record_edge_capacity(edge_idx, Some(queue.capacity()));
                let added_bytes = payload_bytes.unwrap_or(0);
                let mut removed_bytes = 0u64;
                match backpressure {
                    BackpressureStrategy::BoundedQueues => {
                        if queue.is_full() {
                            dropped = true;
                        } else {
                            payload.enqueued_at = Instant::now();
                            queue.push(payload).unwrap();
                            metrics.adjust_bytes(added_bytes, 0);
                        }
                    }
                    BackpressureStrategy::ErrorOnOverflow => {
                        if queue.is_full() {
                            telem.backpressure_events += 1;
                            let label = warning_label
                                .clone()
                                .unwrap_or_else(|| format!("bounded_error_edge_{edge_idx}"));
                            record_warning(&label, warnings_seen, telem);
                            return;
                        } else {
                            payload.enqueued_at = Instant::now();
                            queue.push(payload).unwrap();
                            metrics.adjust_bytes(added_bytes, 0);
                        }
                    }
                    BackpressureStrategy::None => {
                        payload.enqueued_at = Instant::now();
                        if let Err(payload) = queue.push(payload) {
                            removed_bytes = queue
                                .pop()
                                .and_then(|removed| payload_size_bytes(&removed.inner))
                                .unwrap_or(0);
                            let _ = queue.push(payload);
                            dropped = true;
                        } else {
                            metrics.adjust_bytes(added_bytes, 0);
                        }
                        if dropped {
                            metrics.adjust_bytes(added_bytes, removed_bytes);
                        }
                    }
                }
                if dropped {
                    telem.backpressure_events += 1;
                    telem.record_edge_drop(edge_idx, 1);
                    let label = warning_label
                        .clone()
                        .unwrap_or_else(|| format!("bounded_drop_edge_{edge_idx}"));
                    record_warning(&label, warnings_seen, telem);
                }
                telem.record_edge_depth(edge_idx, queue.len());
                let (current_queue_bytes, _) = metrics.snapshot();
                telem.record_edge_queue_bytes(edge_idx, current_queue_bytes);
            }
        }
    }
}

fn record_warning(
    label: &str,
    seen: &Arc<Mutex<std::collections::HashSet<String>>>,
    telem: &mut ExecutionTelemetry,
) {
    if let Ok(mut s) = seen.lock()
        && s.insert(label.to_string())
    {
        telem.warnings.push(label.to_string());
    }
}
