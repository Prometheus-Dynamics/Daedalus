//! What an edge is called: the node and port at each end, how many values its queue holds, and
//! its policy. A tick that fails because a queue is full says which edge with these, where the
//! runner knows only a slot number.

use std::collections::BTreeMap;
use std::format;
use std::string::{String, ToString};
use std::vec::Vec;

use daedalus_abi::NO_SLOT;
use daedalus_backend::PortRef;
use daedalus_graph::EdgePolicyDoc;
use daedalus_made::Bound;
use daedalus_runner::SlotSpec;

use crate::io::EdgeName;
use crate::label::Label;
use crate::lay_out::Draft;
use crate::load::NodeEntry;

/// The output `port`, as `node.port`. A plan stored before output names were kept gives the
/// port's number.
pub(crate) fn output(bound: &Bound, entries: &[NodeEntry], port: PortRef) -> String {
    let node = port.node.0 as usize;
    let key = entries.get(node).map_or("?", |entry| entry.key.as_str());
    match bound.facts.nodes[node]
        .outputs
        .get(usize::from(port.port.0))
    {
        Some(name) => format!("{key}.{name}"),
        None => format!("{key}.output {}", port.port.0),
    }
}

/// A policy in the words a document uses.
fn policy_text(policy: &EdgePolicyDoc) -> String {
    match policy {
        EdgePolicyDoc::Fifo => "fifo".to_string(),
        EdgePolicyDoc::LatestOnly => "latest_only".to_string(),
        EdgePolicyDoc::PerTick => "per_tick".to_string(),
        EdgePolicyDoc::Bounded { capacity, overflow } => format!("bounded {capacity} {overflow}"),
    }
}

/// The name of every slot an edge lands on, sorted by slot: the edges into each step's inputs and
/// the edges out to the host.
pub(crate) fn edge_names(
    bound: &Bound,
    entries: &[NodeEntry],
    drafts: &[Draft],
    slots: &[SlotSpec],
    extra: &[u32],
) -> Vec<(u32, EdgeName)> {
    let facts = &bound.facts;
    let host = |node: u32, port: &str| {
        let key = entries
            .get(node as usize)
            .map_or("host", |entry| &entry.key);
        format!("{key}.{port}")
    };
    // Who writes each slot: a step's output, by the name of the output it carries, or the host.
    let mut writers: BTreeMap<u32, String> = BTreeMap::new();
    for port in &facts.stitched.host_inputs {
        writers.insert(port.slot, host(port.node, &port.port));
    }
    for draft in drafts {
        for (at, slot) in draft.writes.iter().enumerate() {
            if let Some(name) = draft.write_names.get(at) {
                writers.insert(*slot, name.clone());
            }
        }
    }
    let mut named = BTreeMap::new();
    let mut name = |slot: u32, to: String, policy: &EdgePolicyDoc| {
        if slot == NO_SLOT {
            return;
        }
        // A fan-out's added slot carries the values of the plan slot it was made for.
        let source = (slot as usize)
            .checked_sub(facts.stitched.slots.len())
            .and_then(|at| extra.get(at).copied())
            .unwrap_or(slot);
        let from = writers.get(&source).map_or("?", String::as_str);
        let edge = EdgeName {
            from: Label::new(from),
            to: Label::new(&to),
            depth: slots.get(slot as usize).map_or(0, |spec| spec.cap),
            policy: Label::new(&policy_text(policy)),
        };
        named.insert(slot, edge);
    };
    for draft in drafts {
        for &(at, edge) in &draft.connected {
            let to = &facts.edges[edge];
            let reader = entries
                .get(to.to_node as usize)
                .map_or("?", |entry| entry.key.as_str());
            name(
                draft.reads[at],
                format!("{reader}.{}", to.to_port),
                &to.policy,
            );
        }
    }
    for port in &facts.stitched.host_outputs {
        let policy = facts
            .edges
            .iter()
            .find(|edge| edge.to_node == port.node && edge.to_port == port.port)
            .map_or(EdgePolicyDoc::Fifo, |edge| edge.policy.clone());
        name(port.slot, host(port.node, &port.port), &policy);
    }
    named.into_iter().collect()
}
