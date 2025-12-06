use std::sync::{Arc, Mutex};
use std::time::Instant;

#[cfg(feature = "lockfree-queues")]
use crossbeam_queue::ArrayQueue;

use crate::plan::{BackpressureStrategy, EdgePolicyKind};

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

/// Storage wrapper per edge; allows swapping queue implementations.
pub enum EdgeStorage {
    Locked(Arc<Mutex<EdgeQueue>>),
    #[cfg(feature = "lockfree-queues")]
    BoundedLf(Arc<ArrayQueue<CorrelatedPayload>>),
}

pub fn build_queues(plan: &crate::plan::RuntimePlan) -> Vec<EdgeStorage> {
    #[cfg(feature = "lockfree-queues")]
    let use_lockfree = plan.lockfree_queues;
    plan.edges
        .iter()
        .map(|(_, _, _, _, policy)| match policy {
            EdgePolicyKind::Bounded { cap } => {
                #[cfg(feature = "lockfree-queues")]
                {
                    if use_lockfree {
                        EdgeStorage::BoundedLf(Arc::new(ArrayQueue::new(*cap)))
                    } else {
                        EdgeStorage::Locked(Arc::new(Mutex::new(EdgeQueue::Bounded {
                            ring: RingBuf::new(*cap),
                        })))
                    }
                }
                #[cfg(not(feature = "lockfree-queues"))]
                {
                    EdgeStorage::Locked(Arc::new(Mutex::new(EdgeQueue::Bounded {
                        ring: RingBuf::new(*cap),
                    })))
                }
            }
            _ => EdgeStorage::Locked(Arc::new(Mutex::new(EdgeQueue::default()))),
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
        match storage {
            EdgeStorage::Locked(q_arc) => {
                if let Ok(mut q) = q_arc.lock() {
                    q.ensure_policy(policy);
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
                        let label = warning_label
                            .clone()
                            .unwrap_or_else(|| format!("bounded_drop_edge_{edge_idx}"));
                        record_warning(&label, warnings_seen, telem);
                    }
                }
            }
            #[cfg(feature = "lockfree-queues")]
            EdgeStorage::BoundedLf(q) => {
                let mut dropped = false;
                match backpressure {
                    BackpressureStrategy::BoundedQueues => {
                        if q.is_full() {
                            dropped = true;
                        } else {
                            let mut payload = payload.clone();
                            payload.enqueued_at = Instant::now();
                            q.push(payload).unwrap();
                        }
                    }
                    BackpressureStrategy::ErrorOnOverflow => {
                        if q.is_full() {
                            telem.backpressure_events += 1;
                            let label = warning_label
                                .clone()
                                .unwrap_or_else(|| format!("bounded_error_edge_{edge_idx}"));
                            record_warning(&label, warnings_seen, telem);
                            return;
                        } else {
                            let mut payload = payload.clone();
                            payload.enqueued_at = Instant::now();
                            q.push(payload).unwrap();
                        }
                    }
                    BackpressureStrategy::None => {
                        let mut payload = payload.clone();
                        payload.enqueued_at = Instant::now();
                        if q.push(payload.clone()).is_err() {
                            let _ = q.pop();
                            let _ = q.push(payload.clone());
                            dropped = true;
                        }
                    }
                }
                if dropped {
                    telem.backpressure_events += 1;
                    let label = warning_label
                        .clone()
                        .unwrap_or_else(|| format!("bounded_drop_edge_{edge_idx}"));
                    record_warning(&label, warnings_seen, telem);
                }
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
