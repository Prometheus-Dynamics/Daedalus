use std::collections::BTreeMap;

use daedalus_planner::{Edge, Graph, NodeRef, PortRef, descriptor_dynamic_port_type};

use crate::handles::PortHandle;

use super::{GraphBuildError, GraphBuilder, IntoPortSpec, PortSpec};

impl GraphBuilder {
    pub fn input(&self, name: impl Into<String>) -> PortHandle {
        PortHandle::new(
            self.host_bridge_alias
                .clone()
                .unwrap_or_else(|| "host".to_string()),
            name,
        )
    }

    pub fn output(&self, name: impl Into<String>) -> PortHandle {
        PortHandle::new(
            self.host_bridge_alias
                .clone()
                .unwrap_or_else(|| "host".to_string()),
            name,
        )
    }

    pub fn node_port(&self, node_alias: impl Into<String>, port: impl Into<String>) -> PortHandle {
        PortHandle::new(node_alias, port)
    }

    /// Connect two ports using the builder's concise wiring syntax.
    ///
    /// Prefer `PortHandle` or `(node, port)` endpoints in release code. String
    /// endpoints are convenience shorthand and split on `.`.
    ///
    /// # Panics
    ///
    /// Panics when either endpoint references an unknown node alias, when nested
    /// graphs are connected directly to each other, or when a nested endpoint
    /// names a missing exposed port.
    pub fn connect<F, T>(self, from: F, to: T) -> Self
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        self.connect_ports(from, to)
    }

    pub fn try_connect<F, T>(self, from: F, to: T) -> Result<Self, GraphBuildError>
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        self.try_connect_ports(from, to)
    }

    /// Connect using explicit tuple arguments instead of a colon string, e.g.
    /// `.connect_ports(("src", "out"), ("dst", "inp"))`.
    ///
    /// # Panics
    ///
    /// Panics when either endpoint references an unknown node alias, when nested
    /// graphs are connected directly to each other, or when a nested endpoint
    /// names a missing exposed port.
    pub fn connect_ports<F, T>(self, from: F, to: T) -> Self
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        self.try_connect_ports(from, to)
            .unwrap_or_else(|err| panic!("{err}"))
    }

    pub fn try_connect_ports<F, T>(mut self, from: F, to: T) -> Result<Self, GraphBuildError>
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        self.try_connect_ports_in_place(from, to)?;
        Ok(self)
    }

    pub(super) fn try_connect_ports_in_place<F, T>(
        &mut self,
        from: F,
        to: T,
    ) -> Result<(), GraphBuildError>
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        let from_spec = from.into_spec();
        let to_spec = to.into_spec();
        let host_alias = self
            .host_bridge_alias
            .clone()
            .unwrap_or_else(|| "host".to_string());
        let from_spec = PortSpec {
            node: if from_spec.node.is_empty() {
                host_alias.clone()
            } else {
                from_spec.node
            },
            port: from_spec.port,
        };
        let to_spec = PortSpec {
            node: if to_spec.node.is_empty() {
                host_alias.clone()
            } else {
                to_spec.node
            },
            port: to_spec.port,
        };
        let from_nested = self.nested.get(&from_spec.node).cloned();
        let to_nested = self.nested.get(&to_spec.node).cloned();

        if from_nested.is_some() && to_nested.is_some() {
            return Err(GraphBuildError::NestedToNested {
                from: from_spec.node,
                to: to_spec.node,
            });
        }
        if let Some(nested) = from_nested {
            let backup = self.clone();
            let next = std::mem::replace(self, GraphBuilder::new(self.capabilities.clone()))
                .try_connect_from_nested_spec(&nested, &from_spec.port, to_spec);
            return match next {
                Ok(next) => {
                    *self = next;
                    Ok(())
                }
                Err(err) => {
                    *self = backup;
                    Err(err)
                }
            };
        }
        if let Some(nested) = to_nested {
            let backup = self.clone();
            let next = std::mem::replace(self, GraphBuilder::new(self.capabilities.clone()))
                .try_connect_to_nested_spec(from_spec, &nested, &to_spec.port);
            return match next {
                Ok(next) => {
                    *self = next;
                    Ok(())
                }
                Err(err) => {
                    *self = backup;
                    Err(err)
                }
            };
        }

        let from_is_host = from_spec.node == host_alias;
        let to_is_host = to_spec.node == host_alias;
        if !from_is_host {
            let f_idx = self.try_find_index(&from_spec.node)?;
            self.validate_declared_port(f_idx, &from_spec.node, &from_spec.port, true)?;
        }
        if !to_is_host {
            let t_idx = self.try_find_index(&to_spec.node)?;
            self.validate_declared_port(t_idx, &to_spec.node, &to_spec.port, false)?;
        }
        if from_is_host || to_is_host {
            *self = std::mem::replace(self, GraphBuilder::new(self.capabilities.clone()))
                .ensure_host_bridge(Some(host_alias.clone()));
        }
        if from_is_host {
            *self = std::mem::replace(self, GraphBuilder::new(self.capabilities.clone()))
                .ensure_host_bridge_port(true, &from_spec.port);
        }
        if to_is_host {
            *self = std::mem::replace(self, GraphBuilder::new(self.capabilities.clone()))
                .ensure_host_bridge_port(false, &to_spec.port);
        }
        let f_idx = self.try_find_index(&from_spec.node)?;
        let t_idx = self.try_find_index(&to_spec.node)?;
        self.edges.push(Edge {
            from: PortRef {
                node: NodeRef(f_idx),
                port: from_spec.port,
            },
            to: PortRef {
                node: NodeRef(t_idx),
                port: to_spec.port,
            },
            metadata: BTreeMap::new(),
        });
        Ok(())
    }

    pub(super) fn try_find_index(&self, name: &str) -> Result<usize, GraphBuildError> {
        self.nodes
            .iter()
            .position(|n| n.id.0 == name || n.label.as_deref() == Some(name))
            .ok_or_else(|| GraphBuildError::MissingNodeAlias {
                alias: name.to_string(),
            })
    }

    fn validate_declared_port(
        &self,
        node_idx: usize,
        alias: &str,
        port: &str,
        is_output: bool,
    ) -> Result<(), GraphBuildError> {
        let Some(node) = self.nodes.get(node_idx) else {
            return Ok(());
        };
        if super::nested::is_host_bridge(node) {
            return Ok(());
        }
        let Some(decl) = self.capabilities.nodes().get(&node.id) else {
            return Ok(());
        };
        if descriptor_dynamic_port_type(decl, !is_output).is_some() {
            return Ok(());
        }

        let matches_declared = if is_output {
            decl.outputs.iter().any(|candidate| candidate.name == port)
        } else {
            decl.inputs.iter().any(|candidate| candidate.name == port)
                || decl.fanin_inputs.iter().any(|fanin| {
                    crate::fanin::parse_indexed_port(&fanin.prefix, port)
                        .is_some_and(|index| index >= fanin.start)
                })
        };
        if matches_declared {
            return Ok(());
        }

        let mut available: Vec<String> = if is_output {
            decl.outputs
                .iter()
                .map(|candidate| candidate.name.clone())
                .collect()
        } else {
            decl.inputs
                .iter()
                .map(|candidate| candidate.name.clone())
                .collect()
        };
        if !is_output {
            available.extend(
                decl.fanin_inputs
                    .iter()
                    .map(|fanin| format!("{}{}+", fanin.prefix, fanin.start)),
            );
        }

        Err(GraphBuildError::MissingNodePort {
            alias: alias.to_string(),
            node_id: node.id.0.clone(),
            direction: if is_output { "output" } else { "input" }.to_string(),
            port: port.to_string(),
            available,
        })
    }

    pub fn build(self) -> Graph {
        let mut nodes = self.nodes;
        if !self.injected_node_metadata.is_empty()
            || !self.injected_node_metadata_overwrite.is_empty()
        {
            for node in &mut nodes {
                for (k, v) in &self.injected_node_metadata {
                    node.metadata.entry(k.clone()).or_insert_with(|| v.clone());
                }
                for (k, v) in &self.injected_node_metadata_overwrite {
                    node.metadata.insert(k.clone(), v.clone());
                }
            }
        }
        Graph {
            nodes,
            edges: self.edges,
            metadata: self.graph_metadata,
        }
    }

    /// Connect using PortHandle pairs for string-free wiring.
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`GraphBuilder::connect`].
    pub fn connect_handles(mut self, from: &PortHandle, to: &PortHandle) -> Self {
        self = self.connect_ports(from, to);
        self
    }

    /// Connect by explicit id/port tuples.
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`GraphBuilder::connect`].
    pub fn connect_by_id(
        mut self,
        from: (impl Into<String>, impl Into<String>),
        to: (impl Into<String>, impl Into<String>),
    ) -> Self {
        self = self.connect_ports((from.0.into(), from.1.into()), (to.0.into(), to.1.into()));
        self
    }
}
