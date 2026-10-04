//! Host I/O port declarations and single-node graph shortcuts.

use crate::handles::PortHandle;
use crate::prelude::*;

use super::{GraphBuildError, GraphBuilder};

impl GraphBuilder {
    pub fn input_ports(mut self, ports: &[PortHandle]) -> Self {
        for port in ports {
            self = self.ensure_host_bridge_port(true, port.port());
        }
        self
    }

    pub fn output_ports(mut self, ports: &[PortHandle]) -> Self {
        for port in ports {
            self = self.ensure_host_bridge_port(false, port.port());
        }
        self
    }

    /// Add one node and wire `host_input -> node_input` and `node_output -> host_output`.
    ///
    /// # Panics
    ///
    /// Panics when either generated edge is invalid. Use [`Self::try_single_node_io`]
    /// for production-facing graph construction.
    pub fn single_node_io<H>(
        self,
        node: H,
        host_input: impl Into<String>,
        node_input: impl Into<String>,
        node_output: impl Into<String>,
        host_output: impl Into<String>,
    ) -> Self
    where
        H: crate::handles::NodeHandleLike,
    {
        let id = node.id().to_string();
        let alias = node.alias().to_string();
        let host_input = host_input.into();
        let node_input = node_input.into();
        let node_output = node_output.into();
        let host_output = host_output.into();
        let host_alias = self
            .host_bridge_alias
            .clone()
            .unwrap_or_else(|| "host".to_string());
        self.input_ports(&[PortHandle::new(host_alias.clone(), host_input.clone())])
            .output_ports(&[PortHandle::new(host_alias.clone(), host_output.clone())])
            .node_id(&id, &alias)
            .connect(
                (host_alias.clone(), host_input),
                (alias.clone(), node_input),
            )
            .connect((alias, node_output), (host_alias, host_output))
    }

    /// Fallible variant of [`Self::single_node_io`].
    pub fn try_single_node_io<H>(
        self,
        node: H,
        host_input: impl Into<String>,
        node_input: impl Into<String>,
        node_output: impl Into<String>,
        host_output: impl Into<String>,
    ) -> Result<Self, GraphBuildError>
    where
        H: crate::handles::NodeHandleLike,
    {
        let id = node.id().to_string();
        let alias = node.alias().to_string();
        let host_input = host_input.into();
        let node_input = node_input.into();
        let node_output = node_output.into();
        let host_output = host_output.into();
        let host_alias = self
            .host_bridge_alias
            .clone()
            .unwrap_or_else(|| "host".to_string());
        self.input_ports(&[PortHandle::new(host_alias.clone(), host_input.clone())])
            .output_ports(&[PortHandle::new(host_alias.clone(), host_output.clone())])
            .try_node_id(&id, &alias)?
            .try_connect_ports(
                (host_alias.clone(), host_input),
                (alias.clone(), node_input),
            )?
            .try_connect_ports((alias, node_output), (host_alias, host_output))
    }

    /// Add one node and wire host ports to typed node port handles.
    ///
    /// This keeps quickstart-sized graphs compact while avoiding stringly typed
    /// node port names.
    ///
    /// # Panics
    ///
    /// Panics when either generated edge is invalid. Use [`Self::try_single_node_ports`]
    /// for production-facing graph construction.
    pub fn single_node_ports<H>(
        self,
        node: H,
        host_input: impl Into<String>,
        node_input: &PortHandle,
        node_output: &PortHandle,
        host_output: impl Into<String>,
    ) -> Self
    where
        H: crate::handles::NodeHandleLike,
    {
        self.try_single_node_ports(node, host_input, node_input, node_output, host_output)
            .unwrap_or_else(|err| panic!("{err}"))
    }

    /// Fallible variant of [`Self::single_node_ports`].
    pub fn try_single_node_ports<H>(
        self,
        node: H,
        host_input: impl Into<String>,
        node_input: &PortHandle,
        node_output: &PortHandle,
        host_output: impl Into<String>,
    ) -> Result<Self, GraphBuildError>
    where
        H: crate::handles::NodeHandleLike,
    {
        let id = node.id().to_string();
        let alias = node.alias().to_string();
        let host_input = host_input.into();
        let host_output = host_output.into();
        let host_alias = self
            .host_bridge_alias
            .clone()
            .unwrap_or_else(|| "host".to_string());
        self.input_ports(&[PortHandle::new(host_alias.clone(), host_input.clone())])
            .output_ports(&[PortHandle::new(host_alias.clone(), host_output.clone())])
            .node_id(&id, &alias)
            .try_connect_ports(&PortHandle::new(host_alias.clone(), host_input), node_input)?
            .try_connect_ports(node_output, &PortHandle::new(host_alias, host_output))
    }

    /// Add one node and wire a host input through typed node ports to a host output.
    ///
    /// This is the release-facing compact helper for the common
    /// `host input -> node input -> node output -> host output` graph shape.
    pub fn try_single_node_roundtrip<H>(
        self,
        node: H,
        host_input: impl Into<String>,
        node_input: &PortHandle,
        node_output: &PortHandle,
        host_output: impl Into<String>,
    ) -> Result<Self, GraphBuildError>
    where
        H: crate::handles::NodeHandleLike,
    {
        self.try_single_node_ports(node, host_input, node_input, node_output, host_output)
    }
}
