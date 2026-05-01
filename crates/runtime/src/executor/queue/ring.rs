use super::payload_size_bytes;
use crate::executor::{CorrelatedPayload, RuntimeDataSizeInspectors};

/// Simple ring buffer for bounded queues.
pub struct RingBuf {
    buf: Vec<Option<CorrelatedPayload>>,
    head: usize,
    len: usize,
}

impl RingBuf {
    pub(super) fn new(cap: usize) -> Self {
        Self {
            buf: vec![None; cap.max(1)],
            head: 0,
            len: 0,
        }
    }

    pub(super) fn cap(&self) -> usize {
        self.buf.len()
    }

    pub(super) fn pop_front(&mut self) -> Option<CorrelatedPayload> {
        if self.len == 0 {
            return None;
        }
        let idx = self.head;
        let out = self.buf[idx].take();
        self.head = (self.head + 1) % self.cap();
        self.len -= 1;
        out
    }

    pub(super) fn push_back(&mut self, payload: CorrelatedPayload) -> bool {
        let mut dropped = false;
        if self.len == self.cap() {
            self.pop_front();
            dropped = true;
        }
        let idx = (self.head + self.len) % self.cap();
        self.buf[idx] = Some(payload);
        self.len = (self.len + 1).min(self.cap());
        dropped
    }

    pub(super) fn is_full(&self) -> bool {
        self.len == self.cap()
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(super) fn clear(&mut self) {
        for offset in 0..self.len {
            let idx = (self.head + offset) % self.cap();
            let _ = self.buf[idx].take();
        }
        self.head = 0;
        self.len = 0;
    }

    pub(super) fn transport_bytes(&self, inspectors: &RuntimeDataSizeInspectors) -> u64 {
        let mut total = 0u64;
        for offset in 0..self.len {
            let idx = (self.head + offset) % self.cap();
            if let Some(payload) = self.buf[idx].as_ref() {
                total = total
                    .saturating_add(payload_size_bytes(inspectors, &payload.inner).unwrap_or(0));
            }
        }
        total
    }
}
