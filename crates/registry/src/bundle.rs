//! Bundle loading (feature-gated).

use crate::diagnostics::RegistryResult;
use crate::store::{GroupDescriptor, NodeDescriptor, Registry};
use daedalus_data::convert::Converter;
use daedalus_data::descriptor::DataDescriptor;

/// A bundle of registry items to load deterministically.
pub struct Bundle {
    pub values: Vec<DataDescriptor>,
    pub nodes: Vec<NodeDescriptor>,
    pub groups: Vec<GroupDescriptor>,
    pub converters: Vec<Box<dyn Converter>>,
}

impl Bundle {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            nodes: Vec::new(),
            groups: Vec::new(),
            converters: Vec::new(),
        }
    }

    pub fn with_value(mut self, desc: DataDescriptor) -> Self {
        self.values.push(desc);
        self
    }

    pub fn with_node(mut self, desc: NodeDescriptor) -> Self {
        self.nodes.push(desc);
        self
    }

    pub fn with_group(mut self, desc: GroupDescriptor) -> Self {
        self.groups.push(desc);
        self
    }

    pub fn with_converter(mut self, conv: Box<dyn Converter>) -> Self {
        self.converters.push(conv);
        self
    }

    /// Load this bundle into the registry with deterministic ordering.
    pub fn load(self, registry: &mut Registry) -> RegistryResult<()> {
        let mut values = self.values;
        values.sort_by(|a, b| a.id.cmp(&b.id).then(a.version.cmp(&b.version)));
        for v in values {
            registry.register_value(v)?;
        }
        let mut nodes = self.nodes;
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        for n in nodes {
            registry.register_node(n)?;
        }
        let mut groups = self.groups;
        groups.sort_by(|a, b| a.id.cmp(&b.id));
        for g in groups {
            registry.register_group(g)?;
        }
        for c in self.converters {
            registry.register_converter(c)?;
        }
        Ok(())
    }
}

impl Default for Bundle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::NodeId;
    use daedalus_data::descriptor::DescriptorBuilder;
    use daedalus_data::model::{TypeExpr, ValueType};

    #[test]
    fn bundle_loads_deterministically() {
        let mut reg = Registry::new();
        let bundle = Bundle::new()
            .with_value(
                DescriptorBuilder::new("b", "1.0")
                    .type_expr(TypeExpr::Scalar(ValueType::Int))
                    .build()
                    .unwrap(),
            )
            .with_value(
                DescriptorBuilder::new("a", "1.0")
                    .type_expr(TypeExpr::Scalar(ValueType::Int))
                    .build()
                    .unwrap(),
            )
            .with_node(NodeDescriptor {
                id: NodeId::new("n2"),
                feature_flags: vec![],
                label: None,
                group: None,
                inputs: Vec::new(),
                fanin_inputs: Vec::new(),
                outputs: Vec::new(),
                default_compute: daedalus_core::compute::ComputeAffinity::CpuOnly,
                sync_groups: Vec::new(),
                metadata: Default::default(),
            })
            .with_node(NodeDescriptor {
                id: NodeId::new("n1"),
                feature_flags: vec![],
                label: None,
                group: None,
                inputs: Vec::new(),
                fanin_inputs: Vec::new(),
                outputs: Vec::new(),
                default_compute: daedalus_core::compute::ComputeAffinity::CpuOnly,
                sync_groups: Vec::new(),
                metadata: Default::default(),
            });
        bundle.load(&mut reg).unwrap();
        let view = reg.view();
        let keys: Vec<_> = view.values.keys().map(|(id, _)| id.0.clone()).collect();
        assert_eq!(keys, vec!["a".to_string(), "b".to_string()]);
        let node_keys: Vec<_> = view.nodes.keys().map(|id| id.0.clone()).collect();
        assert_eq!(node_keys, vec!["n1".to_string(), "n2".to_string()]);
    }
}
