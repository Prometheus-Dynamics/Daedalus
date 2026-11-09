//! A bounded log of recent ticks. It is host side only, and it is off until a host turns it on.

use std::collections::VecDeque;

/// One logged tick.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Event {
    pub tick: u32,
    pub step_calls: u32,
    pub faults: u32,
    pub skipped: u32,
}

/// The most recent events, up to `limit`. Older ones are dropped first.
#[derive(Debug)]
pub struct EventLog {
    on: bool,
    limit: usize,
    events: VecDeque<Event>,
}

impl EventLog {
    /// An off log that keeps at most `limit` events once it is on.
    pub fn new(limit: usize) -> EventLog {
        EventLog {
            on: false,
            limit,
            events: VecDeque::with_capacity(limit),
        }
    }

    pub fn set_on(&mut self, on: bool) {
        self.on = on;
        if !on {
            self.events.clear();
        }
    }

    pub fn is_on(&self) -> bool {
        self.on
    }

    pub(crate) fn push(&mut self, event: Event) {
        if !self.on || self.limit == 0 {
            return;
        }
        if self.events.len() == self.limit {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }

    /// The logged events, oldest first.
    pub fn recent(&self) -> impl Iterator<Item = &Event> + '_ {
        self.events.iter()
    }
}
