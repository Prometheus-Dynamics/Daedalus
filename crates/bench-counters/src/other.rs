//! Stub for platforms without `perf_event_open`: every call reports `Unsupported`.

use crate::{Error, Event, Sample};

const WHY: &str = "perf_event_open exists only on Linux";

/// A counter group. Off Linux it cannot be built: [`Counters::new`] always fails.
#[derive(Debug)]
pub struct Counters {
    _unbuildable: (),
}

impl Counters {
    /// Always `Err(Error::Unsupported)` off Linux.
    pub fn new(_events: &[Event]) -> Result<Counters, Error> {
        Err(Error::Unsupported(WHY))
    }

    /// No events are ever opened off Linux, so every requested event is missing.
    pub fn missing(&self) -> &[Event] {
        &[]
    }

    /// Always `Err(Error::Unsupported)` off Linux.
    pub fn measure<F: FnMut()>(&mut self, _iterations: u64, _f: F) -> Result<Sample, Error> {
        Err(Error::Unsupported(WHY))
    }
}
