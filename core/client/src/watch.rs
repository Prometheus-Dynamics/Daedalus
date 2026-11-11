//! A watch: the events of one subscription, as a stream.

use std::sync::mpsc::Receiver;

use daedalus_wire::service::Event;

use crate::connect::Client;

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
