//! Held (sticky) host inputs.
//!
//! A held inbound port keeps its last pushed value instead of queueing: every tick re-delivers a
//! clone of it (an `Arc` clone, no copy or allocation) until a push replaces it or
//! [`HostBridgeHandle::clear_input`] drops it. Held values are context, not work: they are not
//! pending input, so they never wake inbound waiters or make `tick_if_ready` / `drive_blocking`
//! tick on their own.

use daedalus_transport::{FeedOutcome, Payload};

use crate::handles::PortId;

use super::ports::PortDirection;
use super::{HostBridgeHandle, wait};

/// Store `payload` as a held port's value; the replaced value, if any, is reported.
pub(super) fn replace(held: &mut Option<Payload>, payload: Payload) -> FeedOutcome {
    let new = payload.correlation_id();
    match held.replace(payload) {
        Some(old) => FeedOutcome::Replaced {
            old: old.correlation_id(),
            new,
        },
        None => FeedOutcome::Accepted {
            correlation_id: new,
        },
    }
}

impl HostBridgeHandle {
    /// Make inbound `port` held: its last pushed value persists across ticks until a push
    /// replaces it or [`Self::clear_input`] drops it, and every tick delivers it to the port's
    /// consumers without the host re-pushing.
    ///
    /// The newest value already queued on the port becomes the held value. Freshness policy and
    /// the type check still apply to pushes; pressure policy does not (a held port keeps one
    /// value). Pushes to a held port never wake inbound waiters, so a held input alone never
    /// triggers a tick; it rides along with the ticks other inputs trigger.
    pub fn set_held_input(&self, port: impl Into<PortId>) {
        let mut guard = self.shared.buffers.lock();
        let state = guard.inbound.port(port.into());
        if state.held.is_none() {
            let mut newest = None;
            state.queue.drain_into(|entry| newest = Some(entry.payload));
            state.held = Some(newest);
        }
    }

    /// Make inbound `port` held (see [`Self::set_held_input`]) and also a tick trigger: a push
    /// replaces the value and leaves the port pending (waking waiters and the inbound fd) until a
    /// tick takes it, so a burst of pushes triggers one tick that sees the newest value. The value
    /// stays held for later ticks other inputs trigger. Used for independent-latest cameras
    /// ([`MultiCamera::independent`](super::multicam::MultiCamera::independent)).
    pub fn set_triggering_held_input(&self, port: impl Into<PortId>) {
        let port = port.into();
        self.set_held_input(port.clone());
        let mut guard = self.shared.buffers.lock();
        let state = guard.inbound.port(port);
        state.held_trigger = true;
        state.held_fresh = matches!(state.held, Some(Some(_)));
        if state.held_fresh {
            wait::wake_inbound(&self.shared, guard);
        }
    }

    /// Whether inbound `port` is held ([`Self::set_held_input`]).
    pub fn is_input_held(&self, port: impl AsRef<str>) -> bool {
        let guard = self.shared.buffers.lock();
        guard
            .inbound
            .get(port.as_ref())
            .is_some_and(|state| state.held.is_some())
    }

    /// Drop inbound `port`'s held value (or its queued payloads) without closing it: later pushes
    /// are accepted as usual. Consumers of a held port see no value from the next tick on.
    pub fn clear_input(&self, port: impl AsRef<str>) {
        let mut guard = self.shared.buffers.lock();
        if let Some(state) = guard.inbound.get_mut(port.as_ref()) {
            state.discard_input();
        }
    }

    /// Run `f` with the inbound ports under one bridge lock, so a tick takes every port's input
    /// from one consistent snapshot (a [`Self::batch`] commit is seen whole or not at all).
    /// `f` must not call back into this bridge.
    pub(crate) fn with_inbound<R>(&self, f: impl FnOnce(&mut InboundPorts<'_>) -> R) -> R {
        let mut guard = self.shared.buffers.lock();
        f(&mut InboundPorts {
            ports: &mut guard.inbound,
        })
    }
}

/// Inbound ports of one bridge, borrowed under its lock ([`HostBridgeHandle::with_inbound`]).
pub(crate) struct InboundPorts<'a> {
    ports: &'a mut PortDirection,
}

/// What [`InboundPorts::take`] found on a port.
pub(crate) enum InboundTake {
    /// A queued port: its payloads went to the sink.
    Drained,
    /// A held port: a clone of its current value, `None` when it has none.
    Held(Option<Payload>),
}

impl InboundPorts<'_> {
    /// Hand every queued payload of `port` (oldest first) to `sink`, or for a held port return a
    /// clone of its value without calling `sink`.
    pub(crate) fn take(&mut self, port: &str, mut sink: impl FnMut(Payload)) -> InboundTake {
        let Some(state) = self.ports.get_mut(port) else {
            return InboundTake::Drained;
        };
        if let Some(held) = state.take_held() {
            return InboundTake::Held(held);
        }
        state.drain_into(|entry| sink(entry.payload));
        InboundTake::Drained
    }
}

#[cfg(test)]
mod tests {
    use crate::prelude::*;

    use daedalus_transport::FeedOutcome;

    use crate::host_bridge::{HostBridgeManager, InboundWait};

    fn held_values(handle: &super::HostBridgeHandle) -> Vec<i64> {
        let mut out = Vec::new();
        handle.take_inbound_into(&mut out);
        out.iter()
            .filter_map(|entry| entry.payload.get_ref::<i64>().copied())
            .collect()
    }

    #[test]
    fn held_port_adopts_the_newest_queued_value_and_never_wakes_waiters() {
        let handle = HostBridgeManager::new().ensure_handle("host");
        handle.push("imu", 1_i64);
        handle.set_held_input("imu");
        assert!(handle.is_input_held("imu"));
        let waiter = handle.inbound_waiter();
        assert!(matches!(
            handle.push("imu", 2_i64),
            FeedOutcome::Replaced { .. }
        ));
        assert_eq!(waiter.poll_now(), None, "held pushes are not pending input");
        assert_eq!(held_values(&handle), [2]);
        assert_eq!(held_values(&handle), [2], "taking leaves the value held");
        handle.clear_input("imu");
        assert!(held_values(&handle).is_empty());
        handle.push("frame", 3_i64);
        assert_eq!(waiter.poll_now(), Some(InboundWait::Ready));
    }

    #[test]
    fn closing_a_held_port_drops_its_value() {
        let handle = HostBridgeManager::new().ensure_handle("host");
        handle.set_held_input("imu");
        handle.push("imu", 1_i64);
        handle.close_input("imu");
        assert!(held_values(&handle).is_empty());
        assert!(matches!(
            handle.push("imu", 2_i64),
            FeedOutcome::Dropped { .. }
        ));
    }

    #[test]
    fn a_batch_is_all_or_nothing_on_the_type_check() {
        struct Unkeyed;
        let handle = HostBridgeManager::new().ensure_handle("host");
        let rejected = handle
            .batch()
            .push("frame", 1_i64)
            .push("imu", Unkeyed)
            .commit()
            .expect_err("Unkeyed has no key");
        assert_eq!((rejected.index, rejected.port.as_str()), (1, "imu"));
        assert_eq!(handle.pending_inbound(), 0);
        let outcomes = handle
            .batch()
            .push("frame", 1_i64)
            .push("imu", 2_i64)
            .commit()
            .expect("batch");
        assert_eq!(outcomes.len(), 2);
        assert_eq!(handle.pending_inbound(), 2);
    }
}
