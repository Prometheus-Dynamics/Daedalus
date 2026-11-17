//! A watch: the events of one subscription, as a stream.
//!
//! The stream holds at most [`WATCH_QUEUE`] events, as the daemon's queue for a watcher does. An
//! event that comes to a full stream drops the oldest one, and the next read reports how many were
//! dropped, as a `lagged` event, before anything else. A watch nobody reads never holds up the
//! client's reader, and never grows without bound.

use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

#[cfg(doc)]
use daedalus_wire::service::WATCH_QUEUE;
use daedalus_wire::service::{Event, EventBody, WatchQueue};

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

struct State {
    queue: WatchQueue,
    /// No event will be added: the client forgot the watch.
    ended: bool,
}

struct Shared {
    state: Mutex<State>,
    ready: Condvar,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// The client's end of a stream: it adds events, and ends the stream when it is dropped.
pub(crate) struct Feed {
    shared: Arc<Shared>,
}

/// The two ends of a new stream, whose events carry `watch` as their `sub`.
pub(crate) fn stream(watch: u64) -> (Feed, Stream) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            queue: WatchQueue::new(watch),
            ended: false,
        }),
        ready: Condvar::new(),
    });
    (
        Feed {
            shared: Arc::clone(&shared),
        },
        Stream { shared },
    )
}

impl Feed {
    /// Adds an event, dropping the oldest when the stream is full. False when nobody holds the
    /// stream any more.
    pub(crate) fn push(&self, body: EventBody) -> bool {
        if Arc::strong_count(&self.shared) == 1 {
            return false;
        }
        self.shared.lock().queue.push(body);
        self.shared.ready.notify_all();
        true
    }
}

impl Drop for Feed {
    fn drop(&mut self) {
        self.shared.lock().ended = true;
        self.shared.ready.notify_all();
    }
}

/// The reading end of a stream.
pub(crate) struct Stream {
    shared: Arc<Shared>,
}

impl Stream {
    fn next(&self, deadline: Option<Instant>) -> WatchPoll {
        let mut state = self.shared.lock();
        loop {
            if let Some(event) = state.queue.pop() {
                return WatchPoll::Event(event);
            }
            if state.ended {
                return WatchPoll::Ended;
            }
            state = match deadline {
                None => self
                    .shared
                    .ready
                    .wait(state)
                    .unwrap_or_else(PoisonError::into_inner),
                Some(deadline) => {
                    let Some(left) = deadline.checked_duration_since(Instant::now()) else {
                        return WatchPoll::Timeout;
                    };
                    self.shared
                        .ready
                        .wait_timeout(state, left)
                        .unwrap_or_else(PoisonError::into_inner)
                        .0
                }
            };
        }
    }
}

/// The events of one watch, in order. The stream ends when the run it follows ends, when the
/// watch is closed, or when the connection drops and the client was not asked to reconnect. A
/// client that reconnects subscribes the watch again, and the stream goes on.
///
/// It holds at most [`WATCH_QUEUE`] unread events. Past that the oldest are dropped, and the next
/// read is a `lagged` event with their count.
pub struct Watch {
    pub(crate) id: u64,
    pub(crate) events: Stream,
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
        match self.events.next(None) {
            WatchPoll::Event(event) => Some(event),
            WatchPoll::Timeout | WatchPoll::Ended => None,
        }
    }

    /// Waits up to `timeout` for the next event.
    pub fn next_event_timeout(&self, timeout: Duration) -> WatchPoll {
        // A timeout too long to add to now waits as long as it takes.
        self.events.next(Instant::now().checked_add(timeout))
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

#[cfg(test)]
mod tests {
    use daedalus_wire::service::{TelemetryEvent, WATCH_QUEUE};

    use super::*;

    fn started(n: u64) -> EventBody {
        EventBody::Telemetry(TelemetryEvent {
            run: 1,
            tick: n,
            step_calls: 0,
            faults: 0,
            skipped: 0,
        })
    }

    #[test]
    fn a_full_stream_drops_the_oldest_and_reports_how_many_first() {
        let (feed, stream) = stream(7);
        let extra = 5;
        for n in 0..(WATCH_QUEUE as u64 + extra) {
            assert!(feed.push(started(n)));
        }
        let first = stream.next(None);
        assert_eq!(
            first,
            WatchPoll::Event(Event {
                sub: 7,
                body: EventBody::Lagged { dropped: extra }
            })
        );
        // The newest WATCH_QUEUE stay, in order.
        for n in extra..(WATCH_QUEUE as u64 + extra) {
            assert_eq!(
                stream.next(None),
                WatchPoll::Event(Event {
                    sub: 7,
                    body: started(n)
                })
            );
        }
        assert_eq!(
            stream.next(Some(Instant::now() + Duration::from_millis(10))),
            WatchPoll::Timeout
        );
        drop(feed);
        assert_eq!(stream.next(None), WatchPoll::Ended);
    }

    #[test]
    fn events_queued_before_the_end_are_still_read() {
        let (feed, stream) = stream(1);
        feed.push(started(1));
        drop(feed);
        assert!(matches!(stream.next(None), WatchPoll::Event(_)));
        assert_eq!(stream.next(None), WatchPoll::Ended);
    }

    #[test]
    fn a_stream_nobody_holds_is_gone() {
        let (feed, stream) = stream(1);
        drop(stream);
        assert!(!feed.push(started(1)));
    }
}
