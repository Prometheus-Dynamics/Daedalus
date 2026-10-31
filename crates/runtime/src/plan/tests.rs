use super::*;
use crate::test_support::test_node;
use daedalus_planner::{Edge, Graph, NodeInstance, PortRef};

#[test]
fn stable_id_collision_returns_typed_runtime_plan_error() {
    let err = ensure_unique_stable_ids(&[test_node("first", 42), test_node("second", 42)])
        .expect_err("duplicate stable ids should fail");

    assert_eq!(
        err,
        RuntimePlanError::StableIdCollision {
            previous: "first".into(),
            current: "second".into(),
            stable_id: 42,
        }
    );
}

#[test]
fn try_from_execution_builds_empty_plan() {
    let plan = RuntimePlan::try_from_execution(&ExecutionPlan::new(Graph::default(), vec![]))
        .expect("empty plan should build");

    assert!(plan.nodes.is_empty());
}

#[test]
fn try_from_execution_reports_unknown_edge_pressure_policy() {
    let mut graph = graph_with_single_edge();
    graph.edges[0].metadata = BTreeMap::from([(
        EDGE_PRESSURE_POLICY_KEY.to_string(),
        Value::String("latest-typo".into()),
    )]);

    let err = RuntimePlan::try_from_execution(&ExecutionPlan::new(graph, vec![]))
        .expect_err("unknown edge pressure policy should fail");

    assert_eq!(
        err,
        RuntimePlanError::UnknownEdgePressurePolicy {
            edge_index: 0,
            policy: "latest-typo".into(),
        }
    );
}

#[test]
fn try_from_execution_reports_unknown_edge_freshness_policy() {
    let mut graph = graph_with_single_edge();
    graph.edges[0].metadata = BTreeMap::from([(
        EDGE_FRESHNESS_POLICY_KEY.to_string(),
        Value::String("latest-by-spelling-error".into()),
    )]);

    let err = RuntimePlan::try_from_execution(&ExecutionPlan::new(graph, vec![]))
        .expect_err("unknown edge freshness policy should fail");

    assert_eq!(
        err,
        RuntimePlanError::UnknownEdgeFreshnessPolicy {
            edge_index: 0,
            policy: "latest-by-spelling-error".into(),
        }
    );
}

fn graph_with_single_edge() -> Graph {
    let mut graph = Graph::default();
    graph
        .nodes
        .push(NodeInstance::new("src").with_outputs(["out"]));
    graph
        .nodes
        .push(NodeInstance::new("sink").with_inputs(["in"]));
    graph.edges.push(Edge {
        from: PortRef {
            node: NodeRef(0),
            port: "out".into(),
        },
        to: PortRef {
            node: NodeRef(1),
            port: "in".into(),
        },
        metadata: BTreeMap::from([(
            EDGE_FRESHNESS_POLICY_KEY.to_string(),
            Value::String(EDGE_FRESHNESS_LATEST_BY_SEQUENCE.into()),
        )]),
    });
    graph
}

fn step(
    kind: daedalus_transport::AdaptKind,
    from: &str,
    to: &str,
    residency: Option<daedalus_transport::Residency>,
) -> daedalus_registry::capability::AdapterPathStep {
    daedalus_registry::capability::AdapterPathStep {
        adapter: daedalus_transport::AdapterId::new(format!("{from}.to.{to}")),
        from: daedalus_transport::TypeKey::new(from),
        to: daedalus_transport::TypeKey::new(to),
        kind,
        access: daedalus_transport::AccessMode::Read,
        cost: daedalus_transport::AdaptCost::new(kind),
        requires_gpu: false,
        residency,
        layout: None,
    }
}

fn transport(path: Vec<daedalus_registry::capability::AdapterPathStep>) -> RuntimeEdgeTransport {
    RuntimeEdgeTransport {
        from_type: daedalus_data::model::TypeExpr::opaque("a"),
        to_type: daedalus_data::model::TypeExpr::opaque("b"),
        source_transport: None,
        target_transport: None,
        target_access: daedalus_transport::AccessMode::Read,
        target_exclusive: false,
        target_residency: None,
        transport_target: None,
        adapter_steps: path.iter().map(|step| step.adapter.clone()).collect(),
        adapter_path: path,
        expected_adapter_cost: None,
    }
}

#[test]
fn edge_transports_classify_copies_and_residency_changes() {
    use daedalus_transport::{AdaptKind, Residency};
    let view = transport(vec![step(
        AdaptKind::View,
        "cam:frame",
        "daedalus:frame",
        None,
    )]);
    assert!(!view.copies_data() && !view.crosses_residency() && view.carries_frame());
    assert_eq!(view.device_transfers(), (0, 0));

    let upload = transport(vec![
        step(
            AdaptKind::View,
            "cam:frame",
            "cam:frame_view",
            Some(Residency::External),
        ),
        step(
            AdaptKind::DeviceUpload,
            "cam:frame_view",
            "gpu:image",
            Some(Residency::Gpu),
        ),
    ]);
    assert!(upload.copies_data() && upload.crosses_residency());
    assert_eq!(upload.device_transfers(), (1, 0));

    let materialize = transport(vec![step(AdaptKind::Materialize, "bytes", "blob", None)]);
    assert!(materialize.copies_data() && !materialize.carries_frame());

    let mut reinterpret = transport(vec![step(
        AdaptKind::Reinterpret,
        "a",
        "b",
        Some(Residency::External),
    )]);
    assert!(!reinterpret.crosses_residency());
    reinterpret.target_residency = Some(Residency::Cpu);
    assert!(reinterpret.crosses_residency());
}

#[test]
fn explanation_flags_copying_and_crossing_frame_edges() {
    use daedalus_transport::{AdaptKind, Residency};
    let mut plan =
        RuntimePlan::try_from_execution(&ExecutionPlan::new(graph_with_single_edge(), vec![]))
            .expect("plan");
    plan.edge_transports = vec![Some(transport(vec![step(
        AdaptKind::DeviceUpload,
        "cam:frame",
        "gpu:frame",
        Some(Residency::Gpu),
    )]))];
    let explanation = plan.explain();
    let edge = &explanation.edges[0];
    assert!(edge.copies_frame && edge.crosses_residency);
    assert_eq!(explanation.copying_edges, vec![0]);
    assert_eq!(explanation.crossing_edges, vec![0]);
    let text = explanation.to_string();
    assert!(
        text.contains("copies_frame: edge 0 (src.out -> sink.in)"),
        "{text}"
    );
    assert!(
        text.contains("adapters=cam:frame.to.gpu:frame:device_upload"),
        "{text}"
    );
    let json = serde_json::to_value(&explanation).expect("json");
    assert_eq!(json["edges"][0]["copies_frame"], true);
    assert_eq!(json["crossing_edges"][0], 0);
}

fn edge(from: usize, from_port: &str, to: usize, to_port: &str) -> Edge {
    Edge {
        from: PortRef {
            node: NodeRef(from),
            port: from_port.into(),
        },
        to: PortRef {
            node: NodeRef(to),
            port: to_port.into(),
        },
        metadata: BTreeMap::new(),
    }
}

#[test]
fn explanation_lists_device_round_trips_across_nodes() {
    use daedalus_transport::{AdaptKind, Residency};
    // src -upload-> blur -device-> sharpen -download-> sink; blur -device-> preview (stays on
    // the device: no trip); src -> log (CPU only).
    let mut graph = Graph::default();
    for (id, inputs, outputs) in [
        ("src", vec![], vec!["out", "raw"]),
        ("blur", vec!["in"], vec!["out", "view"]),
        ("sharpen", vec!["in"], vec!["out"]),
        ("sink", vec!["in"], vec![]),
        ("preview", vec!["in"], vec![]),
        ("log", vec!["in"], vec![]),
    ] {
        graph.nodes.push(
            NodeInstance::new(id)
                .with_inputs(inputs)
                .with_outputs(outputs),
        );
    }
    graph.edges = vec![
        edge(0, "out", 1, "in"),
        edge(1, "out", 2, "in"),
        edge(2, "out", 3, "in"),
        edge(1, "view", 4, "in"),
        edge(0, "raw", 5, "in"),
    ];
    let mut plan =
        RuntimePlan::try_from_execution(&ExecutionPlan::new(graph, vec![])).expect("plan");
    let upload = || {
        transport(vec![step(
            AdaptKind::DeviceUpload,
            "cpu:img",
            "gpu:img",
            Some(Residency::Gpu),
        )])
    };
    let download = || {
        transport(vec![step(
            AdaptKind::DeviceDownload,
            "gpu:img",
            "cpu:img",
            Some(Residency::Cpu),
        )])
    };
    let index_of = |plan: &RuntimePlan, from: usize, to: usize| {
        plan.explain()
            .edges
            .iter()
            .find(|e| e.from_node == from && e.to_node == to)
            .map(|e| e.index)
            .expect("edge")
    };
    let (up, down) = (index_of(&plan, 0, 1), index_of(&plan, 2, 3));
    plan.edge_transports = vec![None; plan.edges.len()];
    plan.edge_transports[up] = Some(upload());
    plan.edge_transports[down] = Some(download());
    let trips = plan.explain().device_round_trips();
    assert_eq!(
        trips,
        vec![crate::plan::DeviceRoundTrip {
            upload_edge: up,
            download_edge: down,
            nodes: vec![1, 2],
            node_ids: vec!["blur".into(), "sharpen".into()],
        }]
    );

    // No download: the chain stays on the device.
    plan.edge_transports[down] = None;
    assert!(plan.explain().device_round_trips().is_empty());

    // One edge that uploads and downloads in its own path.
    let mut both = upload();
    both.adapter_path.extend(download().adapter_path);
    plan.edge_transports[up] = Some(both);
    let trips = plan.explain().device_round_trips();
    assert_eq!(trips.len(), 1);
    assert_eq!((trips[0].upload_edge, trips[0].download_edge), (up, up));
    assert!(trips[0].nodes.is_empty());
}
