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

    /// How many submissions haven't been seen to finish.
    #[cfg(test)]
    pub(crate) fn unfinished(&self) -> usize {
        self.unfinished.len()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{DEFAULT_IN_FLIGHT, InFlight};
    use crate::fence::TicketState;

    #[test]
    fn the_third_submission_is_told_to_wait_for_the_oldest() {
        let mut inflight = InFlight::new(DEFAULT_IN_FLIGHT);
        let (first, second) = (TicketState::new(), TicketState::new());
        assert!(inflight.admit().is_ok());
        inflight.started(Arc::clone(&first));
        assert!(inflight.admit().is_ok());
        inflight.started(Arc::clone(&second));
        assert_eq!(inflight.unfinished(), 2);

        // Two are in flight. A third isn't let in, and is handed the oldest, however often it asks.
        for _ in 0..3 {
            let oldest = inflight.admit().expect_err("the device is full");
            assert!(Arc::ptr_eq(&oldest, &first));
            assert_eq!(inflight.unfinished(), 2);
        }
        // The newer one finishing first makes room too, and the oldest is still the first.
        second.finish();
        assert!(inflight.admit().is_ok());
        assert_eq!(inflight.unfinished(), 1);
        let third = TicketState::new();
        inflight.started(Arc::clone(&third));
        assert!(Arc::ptr_eq(
            &inflight.admit().expect_err("full again"),
            &first
        ));
        first.finish();
        third.finish();
        assert!(inflight.admit().is_ok());
        assert_eq!(inflight.unfinished(), 0);
        assert_eq!(InFlight::new(0).limit(), 1, "a limit of zero acts as one");
    }
}
