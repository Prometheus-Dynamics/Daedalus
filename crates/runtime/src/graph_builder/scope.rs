use super::{
    GraphBuildError, GraphBuilder, IntoPortSpec, NestedGraph, NestedGraphHandle, RuntimeEdgePolicy,
};
use crate::handles::{NodeHandle, NodeHandleLike, PortHandle};
use daedalus_planner::{ComputeAffinity, Graph};
use daedalus_registry::ids::NodeId;
use std::collections::HashMap;

/// Scoped graph definition helper used by `GraphBuilder::{inputs, outputs, nodes, edges}`.
///
/// Scope methods keep common graph setup compact. Prefer the `try_*` methods in release-facing
/// examples and tools that should report graph construction errors. The shorter methods mirror the
/// convenience `GraphBuilder` API and may panic on invalid wiring.
pub struct GraphScope {
    builder: Option<GraphBuilder>,
    node_handles: HashMap<String, NodeHandle>,
    nested_handles: HashMap<String, NestedGraphHandle>,
}

impl GraphScope {
    pub(super) fn new(builder: GraphBuilder) -> Self {
        Self {
            builder: Some(builder),
            node_handles: HashMap::new(),
            nested_handles: HashMap::new(),
        }
    }

    fn take_builder(&mut self) -> GraphBuilder {
        // `GraphScope` owns exactly one builder while a scope closure is running.
        // Public `try_*` methods clone the builder before fallible consuming calls,
        // so user-handled errors leave this slot populated.
        self.builder
            .take()
            .expect("graph scope builder should be present")
    }

    fn put_builder(&mut self, builder: GraphBuilder) {
        self.builder = Some(builder);
    }

    pub(super) fn into_builder(mut self) -> GraphBuilder {
        // `GraphScope` is created and consumed inside `GraphBuilder::{scoped, try_scoped}`;
        // no public API can call this twice or observe the intermediate `None` state.
        self.builder
            .take()
            .expect("graph scope builder should be present")
    }

    pub fn input(&mut self, name: impl Into<String>) -> PortHandle {
        let name = name.into();
        let builder = self.take_builder();
        let port = builder.input(name.clone());
        self.put_builder(builder.ensure_host_bridge_port(true, &name));
        port
    }

    pub fn output(&mut self, name: impl Into<String>) -> PortHandle {
        let name = name.into();
        let builder = self.take_builder();
        let port = builder.output(name.clone());
        self.put_builder(builder.ensure_host_bridge_port(false, &name));
        port
    }

    pub fn node(&self, alias: impl AsRef<str>) -> NodeHandle {
        let alias = alias.as_ref();
        self.node_handles
            .get(alias)
            .cloned()
            .unwrap_or_else(|| NodeHandle::new(alias).alias(alias))
    }

    /// Add a node to the scoped graph and return its typed handle.
    ///
    /// Prefer [`Self::try_add_node`] outside examples and tests.
    ///
    /// # Panics
    ///
    /// May panic through downstream builder validation when the node declaration or wiring is
    /// invalid.
    pub fn add_node(&mut self, alias: impl Into<String>, id: impl Into<String>) -> NodeHandle {
        let alias = alias.into();
        let id = id.into();
        let handle = NodeHandle::new(id.clone()).alias(alias.clone());
        let builder = self.take_builder();
        self.put_builder(builder.node_id(&id, &alias));
        self.node_handles.insert(alias, handle.clone());
        handle
    }

    /// Fallible variant of [`Self::add_node`].
    pub fn try_add_node(
        &mut self,
        alias: impl Into<String>,
        id: impl Into<String>,
    ) -> Result<NodeHandle, GraphBuildError> {
        let alias = alias.into();
        let id = id.into();
        let handle = NodeHandle::new(id.clone()).alias(alias.clone());
        self.builder = Some(
            self.builder
                .as_ref()
                .expect("graph scope builder should be present")
                .clone()
                .try_node_id(&id, &alias)?,
        );
        self.node_handles.insert(alias, handle.clone());
        Ok(handle)
    }

    /// Add a generated node handle to the scoped graph.
    ///
    /// Prefer [`Self::try_add_handle`] outside examples and tests.
    ///
    /// # Panics
    ///
    /// May panic through downstream builder validation when the node declaration or wiring is
    /// invalid.
    pub fn add_handle<H>(&mut self, handle: H) -> NodeHandle
    where
        H: NodeHandleLike,
    {
        let id = handle.id().to_string();
        let alias = handle.alias().to_string();
        let out = NodeHandle::new(id.clone()).alias(alias.clone());
        let builder = self.take_builder();
        self.put_builder(builder.node_id(&id, &alias));
        self.node_handles.insert(alias, out.clone());
        out
    }

    /// Fallible variant of [`Self::add_handle`].
    pub fn try_add_handle<H>(&mut self, handle: H) -> Result<NodeHandle, GraphBuildError>
    where
        H: NodeHandleLike,
    {
        let id = handle.id().to_string();
        let alias = handle.alias().to_string();
        let out = NodeHandle::new(id.clone()).alias(alias.clone());
        self.builder = Some(
            self.builder
                .as_ref()
                .expect("graph scope builder should be present")
                .clone()
                .try_node_id(&id, &alias)?,
        );
        self.node_handles.insert(alias, out.clone());
        Ok(out)
    }

    /// Add a generated node handle with a requested compute affinity.
    ///
    /// Prefer [`Self::try_add_handle_with_compute`] outside examples and tests.
    ///
    /// # Panics
    ///
    /// May panic through downstream builder validation when the node declaration or wiring is
    /// invalid.
    pub fn add_handle_with_compute<H>(&mut self, handle: H, compute: ComputeAffinity) -> NodeHandle
    where
        H: NodeHandleLike,
    {
        let id = handle.id().to_string();
        let alias = handle.alias().to_string();
        let out = NodeHandle::new(id.clone()).alias(alias.clone());
        let builder = self.take_builder();
        self.put_builder(builder.node_with_compute(&id, &alias, compute));
        self.node_handles.insert(alias, out.clone());
        out
    }

    /// Fallible variant of [`Self::add_handle_with_compute`].
    pub fn try_add_handle_with_compute<H>(
        &mut self,
        handle: H,
        compute: ComputeAffinity,
    ) -> Result<NodeHandle, GraphBuildError>
    where
        H: NodeHandleLike,
    {
        let id = handle.id().to_string();
        let alias = handle.alias().to_string();
        if !self
            .builder
            .as_ref()
            .expect("graph scope builder should be present")
            .capabilities
            .nodes()
            .contains_key(&NodeId::new(&id))
        {
            return Err(GraphBuildError::MissingNodeId { id });
        }
        let out = NodeHandle::new(id.clone()).alias(alias.clone());
        let builder = self.take_builder();
        self.put_builder(builder.node_with_compute(&id, &alias, compute));
        self.node_handles.insert(alias, out.clone());
        Ok(out)
    }

    /// Connect two ports within the scope.
    ///
    /// # Panics
    ///
    /// Panics when either endpoint references an unknown node alias, when nested
    /// graphs are connected directly to each other, or when a nested endpoint
    /// names a missing exposed port.
    pub fn connect<F, T>(&mut self, from: F, to: T)
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        let builder = self.take_builder();
        self.put_builder(builder.connect(from, to));
    }

    /// Try to connect two ports within the scope.
    pub fn try_connect<F, T>(&mut self, from: F, to: T) -> Result<(), GraphBuildError>
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        self.builder = Some(
            self.builder
                .as_ref()
                .expect("graph scope builder should be present")
                .clone()
                .try_connect(from, to)?,
        );
        Ok(())
    }

    /// Connect two ports and apply a runtime edge policy to the new edge.
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`GraphScope::connect`].
    pub fn connect_policy<F, T>(&mut self, from: F, to: T, policy: RuntimeEdgePolicy)
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        let builder = self.take_builder();
        self.put_builder(builder.connect(from, to).policy(policy));
    }

    /// Try to connect two ports and apply a runtime edge policy to the new edge.
    pub fn try_connect_policy<F, T>(
        &mut self,
        from: F,
        to: T,
        policy: RuntimeEdgePolicy,
    ) -> Result<(), GraphBuildError>
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        self.builder = Some(
            self.builder
                .as_ref()
                .expect("graph scope builder should be present")
                .clone()
                .try_connect(from, to)?
                .policy(policy),
        );
        Ok(())
    }

    /// Inline a nested graph and return a handle for its exposed host ports.
    ///
    /// # Errors
    ///
    /// Returns an error when the nested graph does not contain a host bridge.
    ///
    /// # Panics
    ///
    /// Panics when the requested nested alias is already used by another nested
    /// graph or node.
    pub fn subgraph(
        &mut self,
        alias: impl Into<String>,
        graph: Graph,
    ) -> Result<NestedGraphHandle, GraphBuildError> {
        let nested = NestedGraph::first_host(graph)?;
        let builder = self.take_builder();
        let (builder, handle) = builder.try_nest(&nested, alias.into())?;
        self.nested_handles
            .insert(handle.alias.clone(), handle.clone());
        self.put_builder(builder);
        Ok(handle)
    }

    pub fn try_subgraph(
        &mut self,
        alias: impl Into<String>,
        graph: Graph,
    ) -> Result<NestedGraphHandle, GraphBuildError> {
        let nested = NestedGraph::first_host(graph)?;
        let (builder, handle) = self
            .builder
            .as_ref()
            .expect("graph scope builder should be present")
            .clone()
            .try_nest(&nested, alias.into())?;
        self.nested_handles
            .insert(handle.alias.clone(), handle.clone());
        self.builder = Some(builder);
        Ok(handle)
    }

    pub fn nested(&self, alias: impl AsRef<str>) -> Option<NestedGraphHandle> {
        self.nested_handles.get(alias.as_ref()).cloned()
    }
}
