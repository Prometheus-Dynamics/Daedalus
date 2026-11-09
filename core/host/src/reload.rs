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
    /// Replaces the graph with `planned`. A node with the same body keeps its state, a changed one
    /// starts fresh and is reported, and a removed node's state is dropped exactly once. Host
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
        let host = parts.host;
        let mut report = ReloadReport::default();
        let before: HashMap<&str, &str> = self
            .nodes
            .iter()
            .map(|node| (node.key.as_str(), node.body.as_str()))
            .collect();
        for node in &nodes {
            match before.get(node.key.as_str()) {
                None => report.added.push(&node.key),
                Some(body) if *body == node.body => report.carried.push(&node.key),
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
        self.node_names = parts.node_names;
        Ok(report)
    }
}

fn node_id(key: &str) -> NodeId {
    NodeId::new(intern(key))
}

fn host_port_names<'a>(host: &'a crate::Host<'static>) -> impl Iterator<Item = &'a str> {
    (0..host.ports.len()).filter_map(|at| host.ports.at(at).map(|spec| spec.name))
}
