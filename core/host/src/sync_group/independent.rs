//! Inputs that tick alone. Each holds its latest value, and a value too old is dropped.

#![cfg(feature = "std")]

use std::vec;
use std::vec::Vec;

use super::{GroupSink, InputSet, SyncStats};

/// How independent inputs behave.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct IndependentConfig {
    /// Drop a value once it is this old, so a stalled input's readers see nothing rather than an
    /// ever older value. `None` keeps it.
    pub max_age_ns: Option<u64>,
}

/// Inputs with nothing to wait for.
pub struct Independent {
    arrived: Vec<Option<u64>>,
    config: IndependentConfig,
    stats: SyncStats,
}

impl Independent {
    pub fn new(count: usize, config: IndependentConfig) -> Independent {
        Independent {
            arrived: vec![None; count.min(super::MAX_INPUTS)],
            config,
            stats: SyncStats::default(),
        }
    }

    pub fn stats(&self) -> SyncStats {
        self.stats
    }

    /// Replaces what `input` has waiting. Returns false for an unknown input.
    pub fn push<V>(
        &mut self,
        input: usize,
        value: V,
        now: u64,
        sink: &mut impl GroupSink<V>,
    ) -> bool {
        let Some(arrived) = self.arrived.get_mut(input) else {
            return false;
        };
        *arrived = Some(now);
        self.stats.values += 1;
        sink.clear(input);
        sink.deliver(input, value);
        true
    }

    /// Inputs holding a value now.
    pub fn present(&self) -> InputSet {
        self.arrived
            .iter()
            .enumerate()
            .filter(|(_, at)| at.is_some())
            .fold(InputSet::EMPTY, |set, (input, _)| set.with(input))
    }

    /// When `input`'s value arrived.
    pub fn arrived(&self, input: usize) -> Option<u64> {
        self.arrived.get(input).copied().flatten()
    }

    /// Gives up `input`'s value.
    pub fn clear<V>(&mut self, input: usize, sink: &mut impl GroupSink<V>) {
        if let Some(arrived) = self.arrived.get_mut(input) {
            *arrived = None;
            sink.clear(input);
        }
    }

    /// When the oldest value expires, if any can.
    pub fn next_deadline(&self) -> Option<u64> {
        let max_age = self.config.max_age_ns?;
        let oldest = self.arrived.iter().flatten().min()?;
        Some(oldest.saturating_add(max_age))
    }

    /// Drops every value past its age. Returns true if any was.
    pub fn expire<V>(&mut self, now: u64, sink: &mut impl GroupSink<V>) -> bool {
        let Some(max_age) = self.config.max_age_ns else {
            return false;
        };
        let mut expired = false;
        for (input, arrived) in self.arrived.iter_mut().enumerate() {
            if arrived.is_some_and(|at| at.saturating_add(max_age) <= now) {
                *arrived = None;
                sink.clear(input);
                self.stats.expired_values += 1;
                expired = true;
            }
        }
        expired
    }
}
