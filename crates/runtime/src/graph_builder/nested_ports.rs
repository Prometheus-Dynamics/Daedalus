//! Wiring between outer graph endpoints and nested graph ports.

use daedalus_planner::{Edge, NodeRef, PortRef};

use super::{GraphBuildError, GraphBuilder, IntoPortSpec, NestedGraphHandle, PortSpec};

impl GraphBuilder {
    /// Connect an outer node/port to a nested graph input port.
    ///
    /// # Panics
    ///
    /// Panics when the outer endpoint references an unknown node alias or `port`
    /// is not exposed as a nested graph input.
    pub fn connect_to_nested<F>(
        self,
        from: F,
        nested: &NestedGraphHandle,
        port: impl AsRef<str>,
    ) -> Self
    where
        F: IntoPortSpec,
    {
        self.try_connect_to_nested(from, nested, port)
            .unwrap_or_else(|err| panic!("{err}"))
    }

    /// Connect an outer node/port (a bare port name is a host input) to a nested graph input
    /// port. An undeclared host input takes the type the nested graph declared for `port`.
    pub fn try_connect_to_nested<F>(
        self,
        from: F,
        nested: &NestedGraphHandle,
        port: impl AsRef<str>,
    ) -> Result<Self, GraphBuildError>
    where
        F: IntoPortSpec,
    {
        self.try_connect_to_nested_spec(from.into_spec(), nested, port.as_ref())
    }

    /// Connect a nested graph output port to a node/port in the outer graph.
    ///
    /// # Panics
    ///
    /// Panics when the outer endpoint references an unknown node alias or `port`
    /// is not exposed as a nested graph output.
    pub fn connect_from_nested<T>(
        self,
        nested: &NestedGraphHandle,
        port: impl AsRef<str>,
        to: T,
    ) -> Self
    where
        T: IntoPortSpec,
    {
        self.try_connect_from_nested(nested, port, to)
            .unwrap_or_else(|err| panic!("{err}"))
    }

    /// Connect a nested graph output port to an outer node/port (a bare port name is a host
    /// output). An undeclared host output takes the type the nested graph declared for `port`.
    pub fn try_connect_from_nested<T>(
        self,
        nested: &NestedGraphHandle,
        port: impl AsRef<str>,
        to: T,
    ) -> Result<Self, GraphBuildError>
    where
        T: IntoPortSpec,
    {
        self.try_connect_from_nested_spec(nested, port.as_ref(), to.into_spec())
    }

    pub(super) fn try_connect_from_nested_spec(
        self,
        nested: &NestedGraphHandle,
        port: &str,
        to: PortSpec,
    ) -> Result<Self, GraphBuildError> {
        let sources =
            nested
                .outputs
                .get(port)
                .ok_or_else(|| GraphBuildError::MissingNestedOutput {
                    alias: nested.alias.clone(),
                    port: port.to_string(),
                })?;
        let ty = nested.host_types.outputs.get(&port.to_ascii_lowercase());
        let (mut builder, to) = self.resolve_outer_endpoint(to, false, ty)?;
        builder.edges.extend(sources.iter().map(|source| Edge {
            from: source.clone(),
            to: to.clone(),
            metadata: Default::default(),
        }));
        Ok(builder)
    }

    pub(super) fn try_connect_to_nested_spec(
        self,
        from: PortSpec,
        nested: &NestedGraphHandle,
        port: &str,
    ) -> Result<Self, GraphBuildError> {
        let targets =
            nested
                .inputs
                .get(port)
                .ok_or_else(|| GraphBuildError::MissingNestedInput {
                    alias: nested.alias.clone(),
                    port: port.to_string(),
                })?;
        let ty = nested.host_types.inputs.get(&port.to_ascii_lowercase());
        let (mut builder, from) = self.resolve_outer_endpoint(from, true, ty)?;
        builder.edges.extend(targets.iter().map(|target| Edge {
            from: from.clone(),
            to: target.clone(),
            metadata: Default::default(),
        }));
        Ok(builder)
    }

    /// Resolve the outer end of a nested connection; a bare port or the host alias is a host
    /// port (`is_host_input` when it feeds the nested graph), created on demand and, when the
    /// outer graph has not declared its type, given the nested graph's declared type `ty`.
    fn resolve_outer_endpoint(
        self,
        spec: PortSpec,
        is_host_input: bool,
        ty: Option<&daedalus_data::model::TypeExpr>,
    ) -> Result<(Self, PortRef), GraphBuildError> {
        let host_alias = self
            .host_bridge_alias
            .clone()
            .unwrap_or_else(|| "host".to_string());
        let mut builder = self;
        if spec.node.is_empty() || spec.node == host_alias {
            let declared = builder.host_bridge_node_mut().is_some_and(|host| {
                let types = daedalus_planner::HostPortTypes::from_node_metadata(&host.metadata);
                let types = if is_host_input {
                    &types.inputs
                } else {
                    &types.outputs
                };
                types.contains_key(&spec.port.to_ascii_lowercase())
            });
            builder = match ty {
                Some(ty) if !declared => {
                    builder.declare_host_port(is_host_input, &spec.port, ty.clone())
                }
                _ => builder
                    .ensure_host_bridge(Some(host_alias.clone()))
                    .ensure_host_bridge_port(is_host_input, &spec.port),
            };
            let node = NodeRef(builder.try_find_index(&host_alias)?);
            return Ok((
                builder,
                PortRef {
                    node,
                    port: spec.port,
                },
            ));
        }
        let node = NodeRef(builder.try_find_index(&spec.node)?);
        Ok((
            builder,
            PortRef {
                node,
                port: spec.port,
            },
        ))
    }
}
