//! Hot reload for [`HostGraph`]: swap in a recompiled graph at a tick boundary, carrying over the
//! state of the nodes and host ports that survive the change.

use crate::prelude::*;
use daedalus_planner::{Graph, GraphDocument};
use daedalus_runtime::executor::NodeHandler;
use daedalus_runtime::host_bridge::DroppedHostPorts;
use daedalus_runtime::node_instance_keys;
use daedalus_runtime::state::StateStore;

use super::HostGraph;
use crate::engine::Engine;
use crate::error::EngineError;

/// What a reload carried over and what it dropped.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HostGraphReload {
    /// Node instances (`node_instance_keys`) whose typed state and resources carried over.
    pub nodes_kept: Vec<String>,
    /// Node instances of the new graph without carried state: new, or of a changed node type.
    pub nodes_added: Vec<String>,
    /// Node instances of the old graph that are gone or changed type; their state is dropped and
    /// their managed resources stopped.
    pub nodes_removed: Vec<String>,
    /// Host ports of the old graph the new graph lacks; their queued and held values are dropped.
    pub host_ports_dropped: DroppedHostPorts,
    /// Whether the frame-overhead window carried over.
    pub frame_overhead_kept: bool,
}

impl<H: NodeHandler + Send + Sync + 'static> HostGraph<H> {
    /// Recompile `graph` with `handler` onto this graph's host bridges and swap it in.
    ///
    /// For graphs compiled with [`Engine::compile_host_graph`]. A compile error leaves this graph
    /// running unchanged. See [`Self::reload_with`] for what carries over.
    pub fn reload(
        &mut self,
        engine: &Engine,
        graph: Graph,
        handler: H,
    ) -> Result<HostGraphReload, EngineError> {
        let next = engine.compile_host_graph_with_bridges(
            graph,
            handler,
            self.bridges.clone(),
            self.host.alias().to_string(),
        )?;
        self.reload_with(next)
    }

    /// Recompile a graph document with `plugins` onto this graph's host bridges and swap it in.
    ///
    /// For graphs compiled from a plugin registry (`Engine::compile_document_host_graph`). A
    /// compile error leaves this graph running unchanged.
    #[cfg(feature = "plugins")]
    pub fn reload_document(
        &mut self,
        engine: &Engine,
        plugins: &daedalus_runtime::plugins::PluginRegistry,
        document: GraphDocument,
        handler: H,
    ) -> Result<HostGraphReload, EngineError> {
        let next = engine.compile_document_host_graph(
            plugins,
            document,
            handler,
            self.bridges.clone(),
            self.host.alias().to_string(),
        )?;
        self.reload_with(next)
    }

    /// Swap `next` in for this graph at a tick boundary and report what carried over.
    ///
    /// `next` must be compiled onto this graph's bridges (`bridge_manager().clone()`) and host
    /// alias (`host().alias()`), so host handles stay valid; otherwise nothing changes. Node state
    /// carries over for every node instance whose key and node type are unchanged. Host ports the
    /// new graph lacks drop their queued and held values; the rest keep them, policies included.
    /// The frame-overhead window carries over.
    pub fn reload_with(&mut self, mut next: HostGraph<H>) -> Result<HostGraphReload, EngineError> {
        if !self.bridges.is_same(&next.bridges) || self.host.alias() != next.host.alias() {
            return Err(EngineError::Config(
                "reload needs a graph compiled onto this graph's host bridges and alias \
                 (`bridge_manager().clone()`, `host().alias()`)"
                    .to_string(),
            ));
        }
        let mut report = HostGraphReload::default();
        let kept: HashSet<String> = {
            let (old_plan, new_plan) = (self.runner.runtime_plan(), next.runner.runtime_plan());
            let old_keys = node_instance_keys(&old_plan.nodes);
            let new_keys = node_instance_keys(&new_plan.nodes);
            let old_types: HashMap<&str, &str> = old_keys
                .iter()
                .map(String::as_str)
                .zip(old_plan.nodes.iter().map(|node| node.id.as_str()))
                .collect();
            let mut kept = HashSet::new();
            for (key, node) in new_keys.iter().zip(&new_plan.nodes) {
                if old_types.get(key.as_str()) == Some(&node.id.as_str()) {
                    kept.insert(key.clone());
                    report.nodes_kept.push(key.clone());
                } else {
                    report.nodes_added.push(key.clone());
                }
            }
            report.nodes_removed = old_keys
                .iter()
                .filter(|key| !kept.contains(key.as_str()))
                .cloned()
                .collect();
            kept
        };
        let inputs: HashSet<String> = next
            .host_inputs()
            .iter()
            .map(|port| port.name().to_string())
            .collect();
        let outputs: HashSet<String> = next
            .host_outputs()
            .iter()
            .map(|port| port.name().to_string())
            .collect();

        // Everything below is infallible: the old graph is only replaced once it is all done.
        let state = StateStore::successor_for_reload(self.runner.executor.state_store(), |key| {
            kept.contains(key)
        });
        next.runner.executor.set_state_store(state);
        report.host_ports_dropped = self
            .host
            .retain_ports(|port| inputs.contains(port), |port| outputs.contains(port));
        if let Some(overhead) = self.frame_overhead.take() {
            next.frame_overhead = Some(overhead);
            report.frame_overhead_kept = true;
        }
        *self = next;
        Ok(report)
    }
}
