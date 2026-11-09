//! Fusing several timestamped inputs. [`Synchronized`] groups values by stamp into whole ticks, and
//! [`Independent`] lets each input tick alone and ages its value out.
//!
//! These hold no clock and run no timers. A caller passes the time in, and the drive loop checks
//! [`Synchronized::next_deadline`] and calls `expire` when it comes. Nothing here runs in a tick.

#![cfg(feature = "std")]

mod independent;
mod synchronized;

use std::sync::Arc;

use daedalus_value::Value;

use crate::Host;

pub use independent::{Independent, IndependentConfig};
pub use synchronized::{PartialPolicy, Push, SyncConfig, Synchronized};

/// The most inputs one group can have.
pub const MAX_INPUTS: usize = 64;

/// A set of input numbers.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InputSet(u64);

impl InputSet {
    pub const EMPTY: InputSet = InputSet(0);

    /// Inputs `0..count`.
    pub const fn all(count: usize) -> InputSet {
        if count >= MAX_INPUTS {
            InputSet(u64::MAX)
        } else {
            InputSet((1 << count) - 1)
        }
    }

    pub const fn only(input: usize) -> InputSet {
        InputSet(1 << input)
    }

    pub const fn with(self, input: usize) -> InputSet {
        InputSet(self.0 | (1 << input))
    }

    pub const fn contains(self, input: usize) -> bool {
        input < MAX_INPUTS && self.0 & (1 << input) != 0
    }

    pub const fn len(self) -> usize {
        self.0.count_ones() as usize
    }

    pub const fn is_empty(self) -> bool {
        self.0 == 0
    }

    pub fn iter(self) -> impl Iterator<Item = usize> {
        (0..MAX_INPUTS).filter(move |&input| self.contains(input))
    }
}

/// One resolved group.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Group {
    /// Inputs that contributed a value of their own.
    pub inputs: InputSet,
    /// Inputs given their last value again.
    pub reused: InputSet,
    /// The oldest stamp in the group.
    pub stamp: u64,
    /// The largest distance from `stamp`.
    pub skew: u64,
    pub complete: bool,
}

/// What a group has done so far.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SyncStats {
    pub values: u64,
    pub complete_groups: u64,
    pub partial_groups: u64,
    pub dropped_groups: u64,
    pub dropped_values: u64,
    pub timeouts: u64,
    /// Groups resolved early because a buffer was full.
    pub overflows: u64,
    /// Groups replaced before any tick took them.
    pub superseded_groups: u64,
    pub late_values: u64,
    pub expired_values: u64,
    pub max_skew: u64,
    pub last_group: Option<Group>,
}

/// Where resolved values go.
pub trait GroupSink<V> {
    fn deliver(&mut self, input: usize, value: V);
    /// Gives up whatever `input` still has waiting. Returns true if there was something.
    fn clear(&mut self, input: usize) -> bool;
}

/// Delivers to a host's input ports, numbered by position in `ports`.
pub struct HostSink<'a, 'h> {
    pub host: &'a mut Host<'h>,
    pub ports: &'a [&'a str],
    /// Values a port refused.
    pub refused: u32,
}

impl<'a, 'h> HostSink<'a, 'h> {
    pub fn new(host: &'a mut Host<'h>, ports: &'a [&'a str]) -> HostSink<'a, 'h> {
        HostSink {
            host,
            ports,
            refused: 0,
        }
    }
}

impl GroupSink<Value> for HostSink<'_, '_> {
    fn deliver(&mut self, input: usize, value: Value) {
        if self.host.feed(self.ports[input], value).is_err() {
            self.refused += 1;
        }
    }

    fn clear(&mut self, input: usize) -> bool {
        self.host.discard_queued(self.ports[input]).unwrap_or(0) > 0
    }
}

impl<T: Send + Sync + 'static> GroupSink<Arc<T>> for HostSink<'_, '_> {
    fn deliver(&mut self, input: usize, value: Arc<T>) {
        if self.host.feed_typed(self.ports[input], value).is_err() {
            self.refused += 1;
        }
    }

    fn clear(&mut self, input: usize) -> bool {
        self.host.discard_queued(self.ports[input]).unwrap_or(0) > 0
    }
}
