use daedalus_data::model::Value;

use crate::plan::{
    EDGE_CAPACITY_KEY, EDGE_PRESSURE_BOUNDED, EDGE_PRESSURE_LATEST_ONLY, EDGE_PRESSURE_POLICY_KEY,
    RuntimeEdgePolicy,
};

use super::{GraphBuildError, GraphBuilder, IntoPortSpec, metadata::write_edge_policy_metadata};

impl GraphBuilder {
    /// Attach/override metadata for an existing connection edge.
    ///
    /// # Panics
    ///
    /// Panics when either endpoint references an unknown node alias.
    pub fn edge_metadata<F, T>(self, from: F, to: T, key: impl Into<String>, value: Value) -> Self
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        self.try_edge_metadata(from, to, key, value)
            .unwrap_or_else(|err| panic!("{err}"))
    }

    pub fn try_edge_metadata<F, T>(
        mut self,
        from: F,
        to: T,
        key: impl Into<String>,
        value: Value,
    ) -> Result<Self, GraphBuildError>
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        let from_spec = from.into_spec();
        let to_spec = to.into_spec();
        let f_idx = self.try_find_index(&from_spec.node)?;
        let t_idx = self.try_find_index(&to_spec.node)?;
        let key = key.into();
        for edge in &mut self.edges {
            if edge.from.node.0 == f_idx
                && edge.to.node.0 == t_idx
                && edge.from.port == from_spec.port
                && edge.to.port == to_spec.port
            {
                edge.metadata.insert(key.clone(), value.clone());
            }
        }
        Ok(self)
    }

    /// Mark an existing edge as latest-only.
    ///
    /// # Panics
    ///
    /// Panics when either endpoint references an unknown node alias.
    pub fn edge_latest_only<F, T>(self, from: F, to: T) -> Self
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        self.edge_metadata(
            from,
            to,
            EDGE_PRESSURE_POLICY_KEY,
            Value::String(EDGE_PRESSURE_LATEST_ONLY.into()),
        )
    }

    /// Mark an existing edge as bounded and set its capacity.
    ///
    /// # Panics
    ///
    /// Panics when either endpoint references an unknown node alias.
    pub fn edge_bounded<F, T>(mut self, from: F, to: T, capacity: usize) -> Self
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
    {
        let from = from.into_spec();
        let to = to.into_spec();
        self = self.edge_metadata(
            (from.node.clone(), from.port.clone()),
            (to.node.clone(), to.port.clone()),
            EDGE_PRESSURE_POLICY_KEY,
            Value::String(EDGE_PRESSURE_BOUNDED.into()),
        );
        self.edge_metadata(
            (from.node, from.port),
            (to.node, to.port),
            EDGE_CAPACITY_KEY,
            Value::Int(i64::try_from(capacity).unwrap_or(i64::MAX)),
        )
    }

    /// Attach a runtime edge policy to the most recently added edge.
    ///
    /// This supports the ergonomic `.connect(...).policy(RuntimeEdgePolicy::latest_only())`
    /// shape while preserving the low-level builder API.
    pub fn policy(mut self, policy: RuntimeEdgePolicy) -> Self {
        if let Some(edge) = self.edges.last_mut() {
            write_edge_policy_metadata(&mut edge.metadata, &policy);
        }
        self
    }

    /// Connect two ports and attach metadata to the edge.
    ///
    /// # Panics
    ///
    /// Panics under the same conditions as [`GraphBuilder::connect`].
    pub fn connect_with_metadata<F, T, K>(
        self,
        from: F,
        to: T,
        metadata: impl IntoIterator<Item = (K, Value)>,
    ) -> Self
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
        K: Into<String>,
    {
        self.try_connect_with_metadata(from, to, metadata)
            .unwrap_or_else(|err| panic!("{err}"))
    }

    pub fn try_connect_with_metadata<F, T, K>(
        mut self,
        from: F,
        to: T,
        metadata: impl IntoIterator<Item = (K, Value)>,
    ) -> Result<Self, GraphBuildError>
    where
        F: IntoPortSpec,
        T: IntoPortSpec,
        K: Into<String>,
    {
        let edge_idx = self.edges.len();
        self = self.try_connect_ports(from, to)?;
        let meta: Vec<(String, Value)> = metadata.into_iter().map(|(k, v)| (k.into(), v)).collect();
        for edge in self.edges.iter_mut().skip(edge_idx) {
            for (key, value) in &meta {
                edge.metadata.insert(key.clone(), value.clone());
            }
        }
        Ok(self)
    }
}
