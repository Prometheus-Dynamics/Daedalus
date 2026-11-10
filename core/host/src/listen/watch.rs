//! Who is watching what. An event goes into the queue of each watch it matches and never waits:
//! a queue that is full drops its oldest event and counts it.

use std::collections::BTreeMap;
use std::sync::{Arc, Weak};

use daedalus_wire::service::{EndReason, EventBody, EventKinds, RunId, SubId, WatchTarget};

use super::session::Session;

struct Watcher {
    session: Weak<Session>,
    session_id: u64,
    target: WatchTarget,
    kinds: EventKinds,
}

/// Every watch on a host.
#[derive(Default)]
pub(crate) struct Watchers {
    next: SubId,
    subs: BTreeMap<SubId, Watcher>,
}

impl Watchers {
    /// Subscribes `session`. Its queue exists before this returns, so no event is lost between
    /// the answer and the first event.
    pub(crate) fn add(
        &mut self,
        session: &Arc<Session>,
        target: WatchTarget,
        kinds: EventKinds,
    ) -> SubId {
        self.next += 1;
        let sub = self.next;
        session.add_watch(sub);
        self.subs.insert(
            sub,
            Watcher {
                session: Arc::downgrade(session),
                session_id: session.id,
                target,
                kinds,
            },
        );
        sub
    }

    /// Ends one watch of `session`, by request. False when the session has no such watch.
    pub(crate) fn remove(&mut self, session: &Session, sub: SubId) -> bool {
        match self.subs.get(&sub) {
            Some(watcher) if watcher.session_id == session.id => {}
            _ => return false,
        }
        self.subs.remove(&sub);
        let reason = EndReason::Unwatched;
        session.end_watch(sub, EventBody::Ended { run: 0, reason });
        true
    }

    /// Forgets the watches of a session that is gone.
    pub(crate) fn drop_session(&mut self, session_id: u64) {
        self.subs
            .retain(|_, watcher| watcher.session_id != session_id);
    }

    pub(crate) fn len(&self) -> usize {
        self.subs.len()
    }

    /// Queues `body` for every watch on `run` that asked for `kind`.
    pub(crate) fn emit(&mut self, run: RunId, kind: EventKinds, body: &EventBody) {
        self.subs.retain(|sub, watcher| {
            let follows = match watcher.target {
                WatchTarget::All => true,
                WatchTarget::Run(target) => target == run,
            };
            if !follows || !watcher.kinds.has(kind) {
                return true;
            }
            match watcher.session.upgrade() {
                Some(session) => {
                    session.push_event(*sub, body.clone());
                    true
                }
                None => false,
            }
        });
    }

    /// Tells every watch on `run` that it ended. A watch on that one run is over.
    pub(crate) fn end_run(&mut self, run: RunId, reason: EndReason) {
        let body = EventBody::Ended { run, reason };
        self.subs.retain(|sub, watcher| {
            let Some(session) = watcher.session.upgrade() else {
                return false;
            };
            match watcher.target {
                WatchTarget::All => {
                    session.push_event(*sub, body.clone());
                    true
                }
                WatchTarget::Run(target) if target == run => {
                    session.end_watch(*sub, body.clone());
                    false
                }
                WatchTarget::Run(_) => true,
            }
        });
    }

    /// Ends every watch: the host is shutting down.
    pub(crate) fn end_all(&mut self, reason: EndReason) {
        for (sub, watcher) in std::mem::take(&mut self.subs) {
            if let Some(session) = watcher.session.upgrade() {
                session.end_watch(sub, EventBody::Ended { run: 0, reason });
            }
        }
    }
}
