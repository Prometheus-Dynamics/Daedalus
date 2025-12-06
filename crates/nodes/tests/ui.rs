use trybuild::TestCases;

#[test]
fn node_macro_ui() {
    let t = TestCases::new();
    t.pass("tests/ui/ok_node.rs");
    t.compile_fail("tests/ui/fail_missing_id.rs");
    t.compile_fail("tests/ui/fail_bad_compute.rs");
}

#[cfg(feature = "registry-adapter")]
#[test]
fn registry_builder_fills_id_and_label() {
    let desc = ui_helpers::starter_print::descriptor();
    let builder = daedalus_nodes::registry_adapter::registry_builder(&desc);
    let desc = builder
        .input(
            "in",
            daedalus_data::model::TypeExpr::Scalar(daedalus_data::model::ValueType::Int),
        )
        .output(
            "out",
            daedalus_data::model::TypeExpr::Scalar(daedalus_data::model::ValueType::Int),
        )
        .build()
        .expect("builds");
    assert_eq!(desc.id.0, "starter.print");
    assert_eq!(desc.label.as_deref(), None);
}

#[cfg(feature = "registry-adapter")]
mod ui_helpers {
    use daedalus_nodes::node;
    use daedalus_runtime::NodeError;

    #[node(id = "starter.print", bundle = "starter")]
    pub fn starter_print() -> Result<(), NodeError> {
        Ok(())
    }
}

#[cfg(feature = "planner-adapter")]
mod planner_helpers {
    use daedalus_nodes::{node, planner_adapter};
    use daedalus_runtime::NodeError;

    #[node(
        id = "starter.gpu",
        bundle = "starter",
        compute(::daedalus_core::compute::ComputeAffinity::GpuRequired),
        inputs("in"),
        outputs("out")
    )]
    pub fn starter_gpu(input: i32) -> Result<i32, NodeError> {
        Ok(input)
    }

    #[test]
    fn planner_adapter_maps_compute() {
        let desc = starter_gpu::descriptor();
        let inst = planner_adapter::node_instance(&desc, ["in"], ["out"]);
        assert_eq!(inst.id.0, "starter.gpu");
        assert!(matches!(
            inst.compute,
            daedalus_planner::ComputeAffinity::GpuRequired
        ));
        assert_eq!(inst.inputs, vec!["in".to_string()]);
        assert_eq!(inst.outputs, vec!["out".to_string()]);
    }
}

#[cfg(feature = "bundle-utils")]
#[test]
fn utils_bundle_registers_and_orders() {
    let nodes = daedalus_nodes::register_all();
    assert!(nodes.iter().any(|n| n.id.0 == "utils.identity"));
    assert!(nodes.iter().any(|n| n.id.0 == "utils.add"));
    // Ensure deterministic ordering by id.
    let mut sorted = nodes.clone();
    sorted.sort_by(|a, b| a.id.0.cmp(&b.id.0));
    assert_eq!(nodes, sorted);
}

#[cfg(all(feature = "bundle-starter", feature = "bundle-utils"))]
#[test]
fn multi_bundle_ordering_is_deterministic() {
    let nodes = daedalus_nodes::register_all();
    let mut sorted = nodes.clone();
    sorted.sort_by(|a, b| a.id.0.cmp(&b.id.0));
    assert_eq!(nodes, sorted);
}
