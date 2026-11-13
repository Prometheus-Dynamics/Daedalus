//! Reload: match nodes by key, carry the state of those whose body didn't change, and report the
//! rest. A plan that doesn't fit the running one is refused and nothing changes.

#![cfg(feature = "std")]

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::string::String;
use std::vec::Vec;

use daedalus_planner::Planned;
use daedalus_types::NodeId;

use crate::intern::intern;
use crate::load::{LoadError, LoadedHost, bridge_keys, build_host};
use crate::report::ReloadReport;

/// Why a reload was refused. The running graph is unchanged.
#[derive(Debug)]
pub enum ReloadError {
    /// The new plan is built for other host bridges.
    OtherBridges {
        running: Vec<String>,
        offered: Vec<String>,
    },
    Load(LoadError),
}

impl fmt::Display for ReloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReloadError::OtherBridges { running, offered } => {
                write!(
                    f,
                    "the plan is for bridges {offered:?}, the graph runs {running:?}"
                )
            }
            ReloadError::Load(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for ReloadError {}

impl LoadedHost {
    /// Replaces the graph with `planned`. A node keeps its state when its step is the same in both
    /// plans: the same nodes at the same places, bodies, constants and ports. Its step moves
    /// across as it is. Any other node starts fresh and is reported, and a removed node's state
    /// is dropped exactly once. Host
    /// ports the new plan lacks lose their queued values, and the report says how many.
    pub fn reload(&mut self, mut planned: Planned) -> Result<ReloadReport, ReloadError> {
        let offered = bridge_keys(&planned);
        let shares_one = offered.iter().any(|key| self.bridges.contains(key));
        if !self.bridges.is_empty() && !offered.is_empty() && !shares_one {
            return Err(ReloadError::OtherBridges {
                running: self.bridges.clone(),
                offered,
            });
        }
        let parts = build_host(&mut planned, self.options).map_err(ReloadError::Load)?;
        self.collect_faults();
        let nodes = parts.nodes;
        let mut host = parts.host;
        // A step holds its nodes' state. One that is the same in both plans moves into the new
        // runner as it is, and the step compiled for the new plan goes back in its place, to be
        // released with the old graph. Every other node starts fresh.
        let mut taken = vec![false; self.same_as.len()];
        let mut moved: HashSet<&str> = HashSet::new();
        for (at, same) in parts.same_as.iter().enumerate() {
            let Some(same) = same else { continue };
            let from = (0..self.same_as.len())
                .find(|&old| !taken[old] && self.same_as[old].as_ref() == Some(same));
            let Some(from) = from else { continue };
            if host
                .runner
                .swap_step(at as u32, &mut self.host.runner, from as u32)
            {
                taken[from] = true;
                moved.extend(parts.covers[at].iter().copied());
            }
        }
        let stateless = |key: &str| !parts.covers.iter().any(|covers| covers.contains(&key));
        let mut report = ReloadReport::default();
        let before: HashMap<&str, &str> = self
            .nodes
            .iter()
            .map(|node| (node.key.as_str(), node.body.as_str()))
            .collect();
        for node in &nodes {
            match before.get(node.key.as_str()) {
                None => report.added.push(&node.key),
                // The same body, and its step came across, or it has no step at all, as a host
                // bridge has none.
                Some(body)
                    if *body == node.body
                        && (moved.contains(node.key.as_str()) || stateless(&node.key)) =>
                {
                    report.carried.push(&node.key);
                }
                Some(_) => {
                    report.fresh.push(&node.key);
                    self.state.remove_node(node_id(&node.key));
                }
            }
        }
        let after: HashSet<&str> = nodes.iter().map(|node| node.key.as_str()).collect();
        for node in &self.nodes {
            if !after.contains(node.key.as_str()) {
                report.removed.push(&node.key);
                self.state.remove_node(node_id(&node.key));
            }
        }
        for name in host_port_names(&self.host) {
            if host.input(name).is_err() && host.output(name).is_err() {
                report.removed_ports.push(name);
                let spec = self.host.ports.find(name);
                let queued = spec
                    .and_then(|spec| self.host.runner.queue(spec.slot))
                    .map_or(0, |queue| queue.len);
                report.dropped_values += queued;
            }
        }
        self.host = host;
        self.nodes = nodes;
        self.bridges = parts.bridges;
        self.fault_seen.clear();
        self.fault_seen.resize(parts.covers.len(), 0);
        self.covers = parts.covers;
        self.same_as = parts.same_as;
        self.moves = parts.moves;
        self.node_names = parts.node_names;
        self.attach_detail();
        self.consts.seed(&planned);
        self.reapply_off_nodes();
        Ok(report)
    }
}

fn node_id(key: &str) -> NodeId {
    NodeId::new(intern(key))
}

fn host_port_names<'a>(host: &'a crate::Host<'static>) -> impl Iterator<Item = &'a str> {
    (0..host.ports.len()).filter_map(|at| host.ports.at(at).map(|spec| spec.name))
}
