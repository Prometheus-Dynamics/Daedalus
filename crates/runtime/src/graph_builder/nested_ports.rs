//! Wiring between outer graph endpoints and nested graph ports.

use std::collections::BTreeMap;

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

    pub fn try_connect_to_nested<F>(
        mut self,
        from: F,
        nested: &NestedGraphHandle,
        port: impl AsRef<str>,
    ) -> Result<Self, GraphBuildError>
    where
        F: IntoPortSpec,
    {
        let from_spec = from.into_spec();
        let host_alias = self
            .host_bridge_alias
            .clone()
            .unwrap_or_else(|| "host".to_string());
        if from_spec.node == host_alias {
            self = self.ensure_host_bridge(Some(host_alias));
            self = self.ensure_host_bridge_port(true, &from_spec.port);
        }
        let f_idx = self.try_find_index(&from_spec.node)?;
        let port = port.as_ref();
        let targets =
            nested
                .inputs
                .get(port)
                .ok_or_else(|| GraphBuildError::MissingNestedInput {
                    alias: nested.alias.clone(),
                    port: port.to_string(),
                })?;

        for target in targets {
            self.edges.push(Edge {
                from: PortRef {
                    node: NodeRef(f_idx),
                    port: from_spec.port.clone(),
                },
                to: target.clone(),
                metadata: BTreeMap::new(),
            });
        }
        Ok(self)
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

    pub fn try_connect_from_nested<T>(
        mut self,
        nested: &NestedGraphHandle,
        port: impl AsRef<str>,
        to: T,
    ) -> Result<Self, GraphBuildError>
    where
        T: IntoPortSpec,
    {
        let to_spec = to.into_spec();
        let host_alias = self
            .host_bridge_alias
            .clone()
            .unwrap_or_else(|| "host".to_string());
        if to_spec.node == host_alias {
            self = self.ensure_host_bridge(Some(host_alias));
            self = self.ensure_host_bridge_port(false, &to_spec.port);
        }
        let t_idx = self.try_find_index(&to_spec.node)?;
        let port = port.as_ref();
        let sources =
            nested
                .outputs
                .get(port)
                .ok_or_else(|| GraphBuildError::MissingNestedOutput {
                    alias: nested.alias.clone(),
                    port: port.to_string(),
                })?;

        for source in sources {
            self.edges.push(Edge {
                from: source.clone(),
                to: PortRef {
                    node: NodeRef(t_idx),
                    port: to_spec.port.clone(),
                },
                metadata: BTreeMap::new(),
            });
        }
        Ok(self)
    }

    pub(super) fn try_connect_from_nested_spec(
        mut self,
        nested: &NestedGraphHandle,
        port: &str,
        to: PortSpec,
    ) -> Result<Self, GraphBuildError> {
        let t_idx = self.try_find_index(&to.node)?;
        let sources =
            nested
                .outputs
                .get(port)
                .ok_or_else(|| GraphBuildError::MissingNestedOutput {
                    alias: nested.alias.clone(),
                    port: port.to_string(),
                })?;

        for source in sources {
            self.edges.push(Edge {
                from: source.clone(),
                to: PortRef {
                    node: NodeRef(t_idx),
                    port: to.port.clone(),
                },
                metadata: BTreeMap::new(),
            });
        }
        Ok(self)
    }

    pub(super) fn try_connect_to_nested_spec(
        mut self,
        from: PortSpec,
        nested: &NestedGraphHandle,
        port: &str,
    ) -> Result<Self, GraphBuildError> {
        let f_idx = self.try_find_index(&from.node)?;
        let targets =
            nested
                .inputs
                .get(port)
                .ok_or_else(|| GraphBuildError::MissingNestedInput {
                    alias: nested.alias.clone(),
                    port: port.to_string(),
                })?;

        for target in targets {
            self.edges.push(Edge {
                from: PortRef {
                    node: NodeRef(f_idx),
                    port: from.port.clone(),
                },
                to: target.clone(),
                metadata: BTreeMap::new(),
            });
        }
        Ok(self)
    }
}
