//! A watch: the events of one subscription, as a stream.

use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;

use daedalus_wire::service::Event;

use crate::connect::Client;

/// What a timed wait for the next event found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WatchPoll {
    /// The next event, in order.
    Event(Event),
    /// Nothing came in time. The watch is still open.
    Timeout,
    /// The stream has ended, and no event will come.
    Ended,
}

/// The events of one watch, in order. The stream ends when the run it follows ends, when the
/// watch is closed, or when the connection drops and the client was not asked to reconnect. A
/// client that reconnects subscribes the watch again, and the stream goes on.
pub struct Watch {
    pub(crate) id: u64,
    pub(crate) events: Receiver<Event>,
    pub(crate) client: Client,
}

impl Watch {
    /// The watch's number on this client. Every event of the stream carries it as its `sub`,
    /// whatever number the server gave the subscription, so it stays the same across reconnects.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Waits for the next event. `None` when the stream has ended.
    pub fn next_event(&self) -> Option<Event> {
        self.events.recv().ok()
    }

    /// Waits up to `timeout` for the next event.
    pub fn next_event_timeout(&self, timeout: Duration) -> WatchPoll {
        match self.events.recv_timeout(timeout) {
            Ok(event) => WatchPoll::Event(event),
            Err(RecvTimeoutError::Timeout) => WatchPoll::Timeout,
            Err(RecvTimeoutError::Disconnected) => WatchPoll::Ended,
        }
    }

    /// Ends the watch on the server. Events already queued can still be read.
    pub fn close(&self) {
        self.client.unwatch(self.id);
    }
}

impl Iterator for Watch {
    type Item = Event;

    fn next(&mut self) -> Option<Event> {
        self.next_event()
    }
}

impl Drop for Watch {
    fn drop(&mut self) {
        self.client.unwatch(self.id);
    }
}
