//! Brings a stored graph's port lists up to what the registry declares.

use daedalus_graph::{GraphBody, Lit, NodeDoc};
use daedalus_registry::{Entry, PortDecl, Registry};
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RefreshPolicy {
    /// Also drop undeclared ports only a const uses, and report the values.
    pub drop_stale_consts: bool,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct NodeRefresh {
    pub node: u32,
    pub id: String,
    pub added_inputs: Vec<String>,
    pub added_outputs: Vec<String>,
    pub removed_inputs: Vec<String>,
    pub removed_outputs: Vec<String>,
    /// Undeclared, but still used by an edge or a const.
    pub kept_inputs: Vec<String>,
    pub kept_outputs: Vec<String>,
    pub dropped_consts: Vec<(String, Lit)>,
}

impl NodeRefresh {
    fn changed(&self) -> bool {
        !(self.added_inputs.is_empty()
            && self.added_outputs.is_empty()
            && self.removed_inputs.is_empty()
            && self.removed_outputs.is_empty()
            && self.dropped_consts.is_empty())
    }

    fn reportable(&self) -> bool {
        self.changed() || !self.kept_inputs.is_empty() || !self.kept_outputs.is_empty()
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RefreshReport {
    pub nodes: Vec<NodeRefresh>,
}

impl RefreshReport {
    pub fn changed(&self) -> bool {
        self.nodes.iter().any(NodeRefresh::changed)
    }
}

fn is_fanin(entry: &Entry, port: &str) -> bool {
    entry.decl.fanin.iter().any(|fanin| {
        port.strip_prefix(fanin.prefix)
            .and_then(|digits| digits.parse::<u32>().ok())
            .is_some_and(|number| number >= fanin.start)
    })
}

/// Updates one list. Returns (added, removed, kept).
fn reconcile(
    listed: &mut Vec<String>,
    declared: &[PortDecl],
    keep: impl Fn(&str) -> bool,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let (mut removed, mut kept) = (Vec::new(), Vec::new());
    listed.retain(|name| {
        if declared.iter().any(|port| port.name == name) {
            true
        } else if keep(name) {
            kept.push(name.clone());
            true
        } else {
            removed.push(name.clone());
            false
        }
    });
    let mut added = Vec::new();
    for port in declared {
        if !listed.iter().any(|name| name == port.name) {
            listed.push(port.name.to_string());
            added.push(port.name.to_string());
        }
    }
    (added, removed, kept)
}

fn refresh_node(
    index: u32,
    node: &mut NodeDoc,
    entry: &Entry,
    wired_in: &[&str],
    wired_out: &[&str],
    policy: RefreshPolicy,
) -> NodeRefresh {
    let mut result = NodeRefresh {
        node: index,
        id: node.id.clone(),
        ..NodeRefresh::default()
    };
    let consts = node.consts.clone();
    let stale_const_kept = |name: &str| !policy.drop_stale_consts && consts.contains_key(name);
    let (added, removed, kept) = reconcile(&mut node.inputs, entry.decl.inputs, |name| {
        is_fanin(entry, name) || wired_in.contains(&name) || stale_const_kept(name)
    });
    result.added_inputs = added;
    result.kept_inputs = kept
        .into_iter()
        .filter(|name| !is_fanin(entry, name))
        .collect();
    for name in &removed {
        if let Some(value) = node.consts.remove(name) {
            result.dropped_consts.push((name.clone(), value));
        }
    }
    result.removed_inputs = removed;
    let (added, removed, kept) = reconcile(&mut node.outputs, entry.decl.outputs, |name| {
        wired_out.contains(&name)
    });
    result.added_outputs = added;
    result.removed_outputs = removed;
    result.kept_outputs = kept;
    result
}

/// Refreshes with the default policy: stale consts are kept.
pub fn refresh_ports(graph: &mut GraphBody, registry: &Registry) -> RefreshReport {
    refresh_ports_with(graph, registry, RefreshPolicy::default())
}

/// A node with no port lists, or one nothing declares, is left alone. Running it twice changes
/// nothing the second time.
pub fn refresh_ports_with(
    graph: &mut GraphBody,
    registry: &Registry,
    policy: RefreshPolicy,
) -> RefreshReport {
    let edges = graph.edges.clone();
    let mut report = RefreshReport::default();
    for (index, node) in graph.nodes.iter_mut().enumerate() {
        let at = index as u32;
        let Some(entry) = registry.entry(&node.id) else {
            continue;
        };
        if node.inputs.is_empty() && node.outputs.is_empty() {
            continue;
        }
        let wired_in: Vec<&str> = edges
            .iter()
            .filter(|e| e.to.node == at)
            .map(|e| e.to.port.as_str())
            .collect();
        let wired_out: Vec<&str> = edges
            .iter()
            .filter(|e| e.from.node == at)
            .map(|e| e.from.port.as_str())
            .collect();
        let result = refresh_node(at, node, entry, &wired_in, &wired_out, policy);
        if result.reportable() {
            report.nodes.push(result);
        }
    }
    report
}
