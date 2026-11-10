//! How many submissions a device has unfinished at once.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::fence::TicketState;

/// Submissions in flight on one device unless the host sets another. More than this and the
/// device's queue only grows: the next run is told to wait for the oldest instead.
pub(crate) const DEFAULT_IN_FLIGHT: usize = 2;

pub(crate) struct InFlight {
    limit: usize,
    unfinished: VecDeque<Arc<TicketState>>,
}

impl InFlight {
    pub(crate) fn new(limit: usize) -> Self {
        let limit = limit.max(1);
        InFlight {
            limit,
            unfinished: VecDeque::with_capacity(limit),
        }
    }

    /// Forgets what has finished. `Err` with the oldest unfinished submission when there is no
    /// room for another.
    pub(crate) fn admit(&mut self) -> Result<(), Arc<TicketState>> {
        self.unfinished.retain(|state| !state.finished());
        match self.unfinished.front() {
            Some(oldest) if self.unfinished.len() >= self.limit => Err(Arc::clone(oldest)),
            _ => Ok(()),
        }
    }

    pub(crate) fn started(&mut self, state: Arc<TicketState>) {
        self.unfinished.push_back(state);
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit
    }
}
