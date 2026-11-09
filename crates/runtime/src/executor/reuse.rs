//! Output reuse for pure nodes (`#[node(reusable)]`, metadata [`NODE_REUSABLE_META_KEY`]).
//!
//! A reusable node keeps the outputs of its last successful run together with a stamp of the
//! inputs and constants it ran on. When it is about to run again and every input carries the same
//! stamp (same ports, same order, same absences), the handler is skipped: the stored outputs are
//! pushed again as an ordinary emission, the telemetry counts a reuse, and the node counts as
//! having succeeded (so backoff recovers as it would for a run).
//!
//! Stamps. An input's stamp is the host's version for its port when the host declared one
//! (`HostBridgeHandle::set_input_version`), else the payload's identity (its lineage correlation
//! id). Clones keep the identity, so a value passed along unchanged keeps its stamp, and a reused
//! node replays the same payloads: a chain of reusable nodes reuses all the way down. Constants
//! are stamped the same way, and a patched constant is a new payload, so it invalidates. No
//! input is hashed.
//!
//! Retention. Only the last result of each node is kept, so memory is bounded by one result per
//! reusable node. A failed run (its handler, flush or publish) drops the stored result and stores
//! nothing; the failure then takes the normal path. Manual and auto-off keep the result, since the
//! stamps still decide whether it applies. Reload builds a new executor, so it starts empty, and
//! [`Executor::clear_reuse`](super::Executor::clear_reuse) drops the results on demand.
//!
//! Zero cost without opt-in: [`ReuseTable::new`] returns `None` when no node of the plan is
//! reusable, and the serial loop then skips every reuse step on a `None` it reads once per node.

use crate::io::{NodeIo, NodePort};
use crate::portable::Arc;
use crate::prelude::*;
use crate::sync::Mutex;
use daedalus_data::model::Value;
use daedalus_planner::ComputeAffinity;

use super::ExecutorBuildError;
use crate::handles::PortId;
use crate::plan::{NODE_REUSABLE_META_KEY, RuntimeNode};

/// What an input is compared by: the host's version for its port, else the payload's identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Key {
    Identity(u64),
    Version(u64),
}

/// One input's stamp: its port and what it is compared by.
pub(crate) type Stamp = Box<[(PortId, Key)]>;

/// A reusable node's last successful result and the stamp of the inputs it ran on.
struct Entry {
    stamp: Stamp,
    outputs: Vec<NodePort>,
}

/// One reusable node's slot: empty until its first success, and cleared by its first failure.
#[derive(Default)]
pub(crate) struct Slot {
    last: Mutex<Option<Entry>>,
}

/// Every reusable node's slot, indexed by node. Built once per plan (see [`ReuseTable::new`]).
pub(crate) struct ReuseTable {
    /// `None` for a node that does not opt in: it never locks or stamps anything.
    slots: Box<[Option<Slot>]>,
}

impl ReuseTable {
    /// The table for `nodes`, or `None` when no node opts in (the zero-cost case). Fails when a
    /// node opts in but cannot be reused: a GPU node (its device state is not an output of its
    /// inputs), a host-bridge node (it is fed by the host, not run), or a non-boolean marker.
    pub(crate) fn new(
        nodes: &[RuntimeNode],
        host_bridges: &[bool],
    ) -> Result<Option<Arc<Self>>, ExecutorBuildError> {
        let mut any = false;
        let mut flags = Vec::with_capacity(nodes.len());
        for (idx, node) in nodes.iter().enumerate() {
            let reusable = match node.metadata.get(NODE_REUSABLE_META_KEY) {
                None | Some(Value::Bool(false)) => false,
                Some(Value::Bool(true)) => true,
                Some(_) => {
                    return Err(ExecutorBuildError::ReusableNode {
                        node: node.id.clone(),
                        reason: "its reusable marker is not a boolean",
                    });
                }
            };
            if reusable {
                let reason = if host_bridges.get(idx).copied().unwrap_or(false) {
                    Some("it is a host bridge node, which the host feeds")
                } else if node.compute != ComputeAffinity::CpuOnly {
                    Some("it asks for a GPU, and its device state is not a function of its inputs")
                } else {
                    None
                };
                if let Some(reason) = reason {
                    return Err(ExecutorBuildError::ReusableNode {
                        node: node.id.clone(),
                        reason,
                    });
                }
                any = true;
            }
            flags.push(reusable);
        }
        if !any {
            return Ok(None);
        }
        let slots = flags
            .into_iter()
            .map(|reusable| reusable.then(Slot::default))
            .collect();
        Ok(Some(Arc::new(Self { slots })))
    }

    /// The slot of node `idx`, or `None` when the node does not opt in.
    #[inline]
    pub(crate) fn slot(&self, idx: usize) -> Option<&Slot> {
        self.slots.get(idx).and_then(Option::as_ref)
    }

    /// Drop every stored result.
    pub(crate) fn clear(&self) {
        for slot in self.slots.iter().flatten() {
            *slot.last.lock() = None;
        }
    }
}

/// Every input and constant of a call, in call order, with the key it is stamped by.
fn input_keys(io: &NodeIo) -> impl Iterator<Item = (&PortId, Key)> {
    let consts = io
        .shared_consts()
        .into_iter()
        .flat_map(|consts| consts.iter());
    io.edge_inputs()
        .iter()
        .chain(consts)
        .map(|(port, payload)| {
            let key = match payload.version {
                Some(version) => Key::Version(version),
                None => Key::Identity(payload.inner.correlation_id()),
            };
            (port, key)
        })
}

/// The stamp of the call `io` is about to make.
fn stamp_of(io: &NodeIo) -> Stamp {
    input_keys(io)
        .map(|(port, key)| (port.clone(), key))
        .collect()
}

impl Slot {
    /// Whether the call `io` makes has the stamp `stamp` (the same inputs and constants).
    fn same_inputs(stamp: &[(PortId, Key)], io: &NodeIo) -> bool {
        let mut keys = input_keys(io);
        for (port, key) in stamp {
            match keys.next() {
                Some((seen, seen_key)) if seen == port && seen_key == *key => {}
                _ => return false,
            }
        }
        keys.next().is_none()
    }

    /// If the stored result has the call's inputs, push its outputs onto `io` and return `true`:
    /// the handler must not run. Otherwise `false`, and the caller runs the node.
    pub(crate) fn replay(&self, io: &mut NodeIo) -> bool {
        let last = self.last.lock();
        let Some(entry) = last.as_ref() else {
            return false;
        };
        if !Self::same_inputs(&entry.stamp, io) {
            return false;
        }
        for (port, payload) in &entry.outputs {
            io.push_correlated_payload(port.clone(), payload.clone());
        }
        true
    }

    /// The stamp of the call `io` is about to make, for [`Self::store`] once it succeeds.
    pub(crate) fn stamp(&self, io: &NodeIo) -> Stamp {
        stamp_of(io)
    }

    /// Keep `outputs` as the result of a successful run on the inputs `stamp` describes.
    pub(crate) fn store(&self, stamp: Stamp, outputs: Vec<NodePort>) {
        *self.last.lock() = Some(Entry { stamp, outputs });
    }

    /// Drop the stored result: a run failed, so none is kept.
    pub(crate) fn forget(&self) {
        *self.last.lock() = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::CorrelatedPayload;
    use daedalus_transport::Payload;

    fn node(id: &str, reusable: Option<Value>) -> RuntimeNode {
        let mut metadata = std::collections::BTreeMap::new();
        if let Some(value) = reusable {
            metadata.insert(NODE_REUSABLE_META_KEY.to_string(), value);
        }
        RuntimeNode {
            id: id.to_string(),
            stable_id: 0,
            bundle: None,
            label: None,
            compute: ComputeAffinity::CpuOnly,
            const_inputs: Vec::new(),
            sync_groups: Vec::new(),
            metadata: Arc::new(metadata),
        }
    }

    #[test]
    fn no_opt_in_builds_no_table() {
        let nodes = [node("a", None), node("b", Some(Value::Bool(false)))];
        let table = ReuseTable::new(&nodes, &[false, false]).expect("builds");
        assert!(table.is_none(), "zero cost without an opt-in");
    }

    #[test]
    fn a_gpu_or_bridge_or_non_boolean_opt_in_is_rejected() {
        let mut gpu = node("gpu", Some(Value::Bool(true)));
        gpu.compute = ComputeAffinity::GpuRequired;
        let err = ReuseTable::new(&[gpu], &[false]).err().expect("rejected");
        assert!(matches!(err, ExecutorBuildError::ReusableNode { ref node, .. } if node == "gpu"));
        let bridge = node("bridge", Some(Value::Bool(true)));
        assert!(ReuseTable::new(&[bridge], &[true]).is_err());
        let text = node("text", Some(Value::String("yes".into())));
        assert!(ReuseTable::new(&[text], &[false]).is_err());
    }

    fn io_with(payload: CorrelatedPayload) -> NodeIo {
        NodeIo::from_inputs([(PortId::new("in"), payload)])
    }

    #[test]
    fn stamps_compare_identity_and_version_per_input() {
        let payload = CorrelatedPayload::from_edge(Payload::owned("test:i32", 1_i32));
        let stamp = stamp_of(&io_with(payload.clone()));
        assert!(
            Slot::same_inputs(&stamp, &io_with(payload.clone())),
            "a clone keeps identity"
        );
        let fresh = CorrelatedPayload::from_edge(Payload::owned("test:i32", 1_i32));
        assert!(
            !Slot::same_inputs(&stamp, &io_with(fresh)),
            "a new payload is a new identity"
        );
        assert!(
            !Slot::same_inputs(&stamp, &NodeIo::empty()),
            "a missing input differs"
        );

        let versioned = stamp_of(&io_with(payload.clone().with_version(Some(7))));
        let other = CorrelatedPayload::from_edge(Payload::owned("test:i32", 2_i32));
        assert!(
            Slot::same_inputs(&versioned, &io_with(other.with_version(Some(7)))),
            "the same version is the same stamp, whatever the payload"
        );
        assert!(
            !Slot::same_inputs(&versioned, &io_with(payload)),
            "a version never equals an identity"
        );
    }
}
