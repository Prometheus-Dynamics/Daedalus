use super::{GraphBuildError, GraphBuilder};
use crate::handles::PortHandle;
use daedalus_data::model::Value;
use daedalus_planner::Graph;
use daedalus_registry::capability::CapabilityRegistry;
use daedalus_registry::ids::NodeId;

/// Graph definition context for graph-backed nodes.
pub struct GraphCtx {
    builder: GraphBuilder,
    host_alias: String,
    expected_inputs: Vec<String>,
    expected_outputs: Vec<String>,
}

impl GraphCtx {
    fn take_builder(&mut self) -> GraphBuilder {
        let capabilities = self.builder.capabilities.clone();
        std::mem::replace(&mut self.builder, GraphBuilder::new(capabilities))
    }

    pub fn new(capabilities: CapabilityRegistry, inputs: &[&str], outputs: &[&str]) -> Self {
        Self {
            builder: GraphBuilder::new(capabilities),
            host_alias: "host".to_string(),
            expected_inputs: inputs.iter().map(|v| v.to_string()).collect(),
            expected_outputs: outputs.iter().map(|v| v.to_string()).collect(),
        }
    }

    pub fn new_with_capabilities(
        capabilities: CapabilityRegistry,
        inputs: &[&str],
        outputs: &[&str],
    ) -> Self {
        Self::new(capabilities, inputs, outputs)
    }

    pub fn node(&mut self, id: &str) -> crate::handles::NodeHandle {
        self.node_as(id, id)
    }

    pub fn try_node(&mut self, id: &str) -> Result<crate::handles::NodeHandle, GraphBuildError> {
        self.try_node_as(id, id)
    }

    pub fn node_as(&mut self, id: &str, alias: &str) -> crate::handles::NodeHandle {
        self.try_node_as(id, alias)
            .unwrap_or_else(|err| panic!("{err}"))
    }

    pub fn try_node_as(
        &mut self,
        id: &str,
        alias: &str,
    ) -> Result<crate::handles::NodeHandle, GraphBuildError> {
        if !self
            .builder
            .capabilities
            .nodes()
            .contains_key(&NodeId::new(id))
        {
            return Err(GraphBuildError::MissingNodeId { id: id.to_string() });
        }
        let builder = self.take_builder();
        self.builder = builder.node_id(id, alias);
        Ok(crate::handles::NodeHandle::new(id).alias(alias))
    }

    pub fn connect(&mut self, from: &PortHandle, to: &PortHandle) {
        self.try_connect(from, to)
            .unwrap_or_else(|err| panic!("{err}"));
    }

    pub fn try_connect(
        &mut self,
        from: &PortHandle,
        to: &PortHandle,
    ) -> Result<(), GraphBuildError> {
        self.builder.try_connect_ports_in_place(from, to)?;
        Ok(())
    }

    pub fn const_input(&mut self, port: &PortHandle, value: Value) {
        self.try_const_input(port, value)
            .unwrap_or_else(|err| panic!("{err}"));
    }

    pub fn try_const_input(
        &mut self,
        port: &PortHandle,
        value: Value,
    ) -> Result<(), GraphBuildError> {
        self.validate_const_input_port(port)?;
        let builder = self.take_builder();
        self.builder = builder.const_input(port, Some(value));
        Ok(())
    }

    pub fn input(&self, name: &str) -> PortHandle {
        PortHandle::new(self.host_alias.clone(), name)
    }

    pub fn output(&self, name: &str) -> PortHandle {
        PortHandle::new(self.host_alias.clone(), name)
    }

    pub fn bind_output(&mut self, name: &str, from: &PortHandle) {
        self.try_bind_output(name, from)
            .unwrap_or_else(|err| panic!("{err}"));
    }

    pub fn try_bind_output(
        &mut self,
        name: &str,
        from: &PortHandle,
    ) -> Result<(), GraphBuildError> {
        let host = self.output(name);
        self.builder.try_connect_ports_in_place(from, &host)?;
        Ok(())
    }

    pub fn build(self) -> Graph {
        self.try_build().unwrap_or_else(|err| panic!("{err}"))
    }

    pub fn try_build(mut self) -> Result<Graph, GraphBuildError> {
        self.builder = self.builder.host_bridge(self.host_alias.clone());
        // GraphCtx models a graph-backed node as a subgraph wired through a host-bridge node.
        //
        // Internally, "graph inputs" are emitted *from* the host bridge into the subgraph
        // (host is the edge source), so they must be represented as host-bridge *outputs*.
        // Conversely, "graph outputs" are delivered *to* the host bridge (host is the edge sink),
        // so they must be represented as host-bridge *inputs*.
        //
        // If these are flipped, the UI and port-map builders can end up unioning inputs/outputs
        // for embedded graphs, making node-group ports appear on both sides.
        for name in &self.expected_inputs {
            self.builder = self.builder.ensure_host_bridge_port(true, name);
        }
        for name in &self.expected_outputs {
            self.builder = self.builder.ensure_host_bridge_port(false, name);
        }
        Ok(self.builder.build())
    }

    fn validate_const_input_port(&self, port: &PortHandle) -> Result<(), GraphBuildError> {
        let alias = port.node_alias();
        let Some(node) = self
            .builder
            .nodes
            .iter()
            .find(|node| node.label.as_deref() == Some(alias))
        else {
            return Err(GraphBuildError::MissingNodeAlias {
                alias: alias.to_string(),
            });
        };
        let Some(decl) = self.builder.capabilities.nodes().get(&node.id) else {
            return Ok(());
        };
        if decl
            .inputs
            .iter()
            .any(|candidate| candidate.name == port.port())
        {
            return Ok(());
        }
        let available = decl
            .inputs
            .iter()
            .map(|candidate| candidate.name.clone())
            .collect();
        Err(GraphBuildError::MissingNodePort {
            alias: alias.to_string(),
            node_id: node.id.0.clone(),
            direction: "input".to_string(),
            port: port.port().to_string(),
            available,
        })
    }
}
