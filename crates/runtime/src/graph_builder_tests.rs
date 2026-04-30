use super::*;
use crate::plan::{EDGE_CAPACITY_KEY, EDGE_PRESSURE_POLICY_KEY};
use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_registry::capability::{
    CapabilityRegistry, FanInDecl, NODE_EXECUTION_KIND_META_KEY, NodeDecl, NodeExecutionKind,
    PortDecl,
};

fn caps(nodes: impl IntoIterator<Item = NodeDecl>) -> CapabilityRegistry {
    let mut capabilities = CapabilityRegistry::new();
    for node in nodes {
        capabilities.register_node(node).unwrap();
    }
    capabilities
}

#[test]
fn applies_metadata_overrides() {
    let capabilities = caps([NodeDecl::new("demo.node").metadata("from_desc", Value::Bool(true))]);

    let graph = GraphBuilder::new(capabilities)
        .node_from_id("demo.node", "alias")
        .node_metadata_by_id("alias", "pos_x", Value::Int(10))
        .build();

    let meta = &graph.nodes[0].metadata;
    assert_eq!(meta.get("from_desc"), Some(&Value::Bool(true)));
    assert_eq!(meta.get("pos_x"), Some(&Value::Int(10)));
}

#[test]
fn applies_typed_node_execution_kind_metadata() {
    let capabilities = caps([NodeDecl::new("demo.noop").execution_kind(NodeExecutionKind::NoOp)]);

    let graph = GraphBuilder::new(capabilities)
        .node_from_id("demo.noop", "noop")
        .build();

    assert_eq!(
        graph.nodes[0].metadata.get(NODE_EXECUTION_KIND_META_KEY),
        Some(&Value::String("no_op".into()))
    );
}

#[test]
fn can_inject_graph_metadata_and_broadcast_to_nodes() {
    let capabilities =
        caps([NodeDecl::new("demo.node").metadata("existing", Value::String("keep".into()))]);

    let graph = GraphBuilder::new(capabilities)
        .graph_metadata("graph_run_id", "run-123")
        .graph_metadata_value("multiplier", Value::Int(3))
        .inject_node_metadata("trace_id", Value::String("trace-abc".into()))
        .inject_node_metadata_overwrite("existing", Value::String("overwrite".into()))
        .node_from_id("demo.node", "alias")
        .build();

    assert_eq!(
        graph.metadata.get("graph_run_id"),
        Some(&Value::String("run-123".into()))
    );
    assert_eq!(graph.metadata.get("multiplier"), Some(&Value::Int(3)));
    let meta = &graph.nodes[0].metadata;
    assert_eq!(
        meta.get("trace_id"),
        Some(&Value::String("trace-abc".into()))
    );
    assert_eq!(
        meta.get("existing"),
        Some(&Value::String("overwrite".into()))
    );
}

#[test]
fn nests_graph_and_exposes_ports() {
    let capabilities = CapabilityRegistry::new();

    let inner = GraphBuilder::new(capabilities.clone())
        .host_bridge("inner")
        .node_from_id("demo.add", "add")
        .connect_by_id(("inner", "lhs"), ("add", "lhs"))
        .connect_by_id(("inner", "rhs"), ("add", "rhs"))
        .connect_by_id(("add", "sum"), ("inner", "sum"))
        .build();
    let nested = NestedGraph::new(inner, "inner").expect("inner host bridge missing");

    let (builder, nested_handle) = GraphBuilder::new(capabilities)
        .node_from_id("demo.src", "src")
        .nest(&nested, "adder");

    let graph = builder
        .node_from_id("demo.sink", "sink")
        .connect(("src", "out_lhs"), &nested_handle.input("lhs"))
        .connect(("src", "out_rhs"), &nested_handle.input("rhs"))
        .connect(&nested_handle.output("sum"), ("sink", "in"))
        .build();

    assert!(nested_handle.inputs.contains_key("lhs"));
    assert!(nested_handle.inputs.contains_key("rhs"));
    assert!(nested_handle.outputs.contains_key("sum"));

    let find = |name: &str| {
        graph
            .nodes
            .iter()
            .position(|n| n.label.as_deref() == Some(name))
            .unwrap()
    };
    let src_idx = find("src");
    let sink_idx = find("sink");
    let add_idx = find("adder::add");

    let has_inbound = graph
        .edges
        .iter()
        .any(|e| e.from.node.0 == src_idx && e.to.node.0 == add_idx && e.to.port == "lhs");
    let has_outbound = graph
        .edges
        .iter()
        .any(|e| e.from.node.0 == add_idx && e.to.node.0 == sink_idx && e.from.port == "sum");

    assert!(has_inbound, "nested inputs should target inner nodes");
    assert!(has_outbound, "nested outputs should feed outer nodes");
}

#[test]
fn applies_edge_metadata() {
    let capabilities = caps([
        NodeDecl::new("demo.src")
            .input(
                PortDecl::new("camera", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            )
            .output(
                PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            ),
        NodeDecl::new("demo.sink")
            .input(
                PortDecl::new("in", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            )
            .output(
                PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            ),
    ]);

    let graph = GraphBuilder::new(capabilities)
        .node_from_id("demo.src", "a")
        .node_from_id("demo.sink", "b")
        .connect_with_metadata(
            ("a", "out"),
            ("b", "in"),
            [("ui.color", Value::String("red".into()))],
        )
        .build();
    assert_eq!(graph.edges.len(), 1);
    assert!(matches!(
        graph.edges[0].metadata.get("ui.color"),
        Some(Value::String(s)) if s.as_ref() == "red"
    ));
}

#[test]
fn supports_dot_paths_and_implicit_host_ports() {
    let capabilities = caps([
        NodeDecl::new("demo.src")
            .input(
                PortDecl::new("camera", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            )
            .output(
                PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            ),
        NodeDecl::new("demo.sink")
            .input(
                PortDecl::new("in", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            )
            .output(
                PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            ),
    ]);

    let graph = GraphBuilder::new(capabilities)
        .node_from_id("demo.src", "src")
        .node_from_id("demo.sink", "sink")
        .connect("camera", "src.camera")
        .connect("src.out", "sink.in")
        .connect("sink.out", "preview")
        .build();

    let host_idx = graph
        .nodes
        .iter()
        .position(is_host_bridge)
        .expect("host bridge");
    let src_idx = graph
        .nodes
        .iter()
        .position(|node| node.label.as_deref() == Some("src"))
        .expect("src");
    let sink_idx = graph
        .nodes
        .iter()
        .position(|node| node.label.as_deref() == Some("sink"))
        .expect("sink");

    assert!(graph.edges.iter().any(|edge| {
        edge.from.node.0 == host_idx
            && edge.from.port == "camera"
            && edge.to.node.0 == src_idx
            && edge.to.port == "camera"
    }));
    assert!(graph.edges.iter().any(|edge| {
        edge.from.node.0 == src_idx
            && edge.from.port == "out"
            && edge.to.node.0 == sink_idx
            && edge.to.port == "in"
    }));
    assert!(graph.edges.iter().any(|edge| {
        edge.from.node.0 == sink_idx
            && edge.from.port == "out"
            && edge.to.node.0 == host_idx
            && edge.to.port == "preview"
    }));
}

#[test]
fn policy_applies_to_latest_connection() {
    let capabilities = caps([
        NodeDecl::new("demo.src")
            .input(
                PortDecl::new("camera", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            )
            .output(
                PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            ),
        NodeDecl::new("demo.sink")
            .input(
                PortDecl::new("in", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            )
            .output(
                PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            ),
    ]);

    let graph = GraphBuilder::new(capabilities)
        .node_from_id("demo.src", "src")
        .node_from_id("demo.sink", "sink")
        .connect("src.out", "sink.in")
        .policy(RuntimeEdgePolicy::bounded(3))
        .build();

    assert_eq!(
        graph.edges[0].metadata.get(EDGE_PRESSURE_POLICY_KEY),
        Some(&Value::String("bounded".into()))
    );
    assert_eq!(
        graph.edges[0].metadata.get(EDGE_CAPACITY_KEY),
        Some(&Value::Int(3))
    );
}

#[test]
fn named_scoped_builder_supports_lookup_helpers() {
    let capabilities = caps([
        NodeDecl::new("demo.src")
            .input(
                PortDecl::new("camera", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            )
            .output(
                PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            ),
        NodeDecl::new("demo.sink")
            .input(
                PortDecl::new("in", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            )
            .output(
                PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                    .schema(TypeExpr::Scalar(ValueType::Bool)),
            ),
    ]);

    let graph = GraphBuilder::named(capabilities, "demo.graph")
        .inputs(|g| {
            g.input("camera");
        })
        .outputs(|g| {
            g.output("preview");
        })
        .nodes(|g| {
            g.add_node("src", "demo.src");
            g.add_node("sink", "demo.sink");
        })
        .edges(|g| {
            let src = g.node("src");
            let sink = g.node("sink");
            g.connect("camera", &src.input("camera"));
            g.connect(&src.output("out"), &sink.input("in"));
            g.connect(&sink.output("out"), "preview");
        })
        .build();

    assert_eq!(
        graph.metadata.get("name"),
        Some(&Value::String("demo.graph".into()))
    );
    assert_eq!(graph.edges.len(), 3);
}

#[test]
fn single_node_io_wires_common_host_roundtrip() {
    let capabilities = caps([NodeDecl::new("demo.double")
        .input(
            PortDecl::new("value", "typeexpr:{\"Scalar\":\"Int\"}")
                .schema(TypeExpr::Scalar(ValueType::Int)),
        )
        .output(
            PortDecl::new("value", "typeexpr:{\"Scalar\":\"Int\"}")
                .schema(TypeExpr::Scalar(ValueType::Int)),
        )]);
    let node = crate::handles::NodeHandle::new("demo.double").alias("double");

    let graph = GraphBuilder::new(capabilities)
        .try_single_node_io(&node, "in", "value", "value", "out")
        .expect("single-node graph")
        .build();

    let host_idx = graph
        .nodes
        .iter()
        .position(is_host_bridge)
        .expect("host bridge");
    let node_idx = graph
        .nodes
        .iter()
        .position(|node| node.label.as_deref() == Some("double"))
        .expect("double node");

    assert_eq!(graph.edges.len(), 2);
    assert!(graph.edges.iter().any(|edge| {
        edge.from.node.0 == host_idx
            && edge.from.port == "in"
            && edge.to.node.0 == node_idx
            && edge.to.port == "value"
    }));
    assert!(graph.edges.iter().any(|edge| {
        edge.from.node.0 == node_idx
            && edge.from.port == "value"
            && edge.to.node.0 == host_idx
            && edge.to.port == "out"
    }));
}

#[test]
fn fallible_single_node_io_reports_missing_node_id() {
    let node = crate::handles::NodeHandle::new("demo.missing").alias("missing");

    let err = match GraphBuilder::new(caps([]))
        .try_single_node_io(&node, "in", "value", "value", "out")
    {
        Ok(_) => panic!("missing node id should be reported"),
        Err(err) => err,
    };

    assert_eq!(
        err,
        GraphBuildError::MissingNodeId {
            id: "demo.missing".to_string(),
        }
    );
}

#[test]
fn single_node_ports_wires_typed_node_handles() {
    let capabilities = caps([NodeDecl::new("demo.double")
        .input(
            PortDecl::new("value", "typeexpr:{\"Scalar\":\"Int\"}")
                .schema(TypeExpr::Scalar(ValueType::Int)),
        )
        .output(
            PortDecl::new("value", "typeexpr:{\"Scalar\":\"Int\"}")
                .schema(TypeExpr::Scalar(ValueType::Int)),
        )]);
    let node = crate::handles::NodeHandle::new("demo.double").alias("double");

    let graph = GraphBuilder::new(capabilities)
        .try_single_node_ports(
            &node,
            "in",
            &node.input("value"),
            &node.output("value"),
            "out",
        )
        .expect("single-node graph")
        .build();

    let host_idx = graph
        .nodes
        .iter()
        .position(is_host_bridge)
        .expect("host bridge");
    let node_idx = graph
        .nodes
        .iter()
        .position(|node| node.label.as_deref() == Some("double"))
        .expect("double node");

    assert_eq!(graph.edges.len(), 2);
    assert!(graph.edges.iter().any(|edge| {
        edge.from.node.0 == host_idx
            && edge.from.port == "in"
            && edge.to.node.0 == node_idx
            && edge.to.port == "value"
    }));
    assert!(graph.edges.iter().any(|edge| {
        edge.from.node.0 == node_idx
            && edge.from.port == "value"
            && edge.to.node.0 == host_idx
            && edge.to.port == "out"
    }));
}

#[test]
fn fallible_connect_reports_missing_alias() {
    let capabilities = caps([
        NodeDecl::new("demo.src").output(
            PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                .schema(TypeExpr::Scalar(ValueType::Bool)),
        ),
        NodeDecl::new("demo.sink").input(
            PortDecl::new("in", "typeexpr:{\"Scalar\":\"Bool\"}")
                .schema(TypeExpr::Scalar(ValueType::Bool)),
        ),
    ]);

    let err = match GraphBuilder::new(capabilities)
        .node_from_id("demo.src", "src")
        .try_connect("src.out", "missing.in")
    {
        Ok(_) => panic!("connect should fail"),
        Err(err) => err,
    };

    assert_eq!(
        err,
        GraphBuildError::MissingNodeAlias {
            alias: "missing".into()
        }
    );
}

#[test]
fn fallible_connect_reports_missing_declared_output_port() {
    let capabilities = caps([
        NodeDecl::new("demo.src").output(
            PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                .schema(TypeExpr::Scalar(ValueType::Bool)),
        ),
        NodeDecl::new("demo.sink").input(
            PortDecl::new("in", "typeexpr:{\"Scalar\":\"Bool\"}")
                .schema(TypeExpr::Scalar(ValueType::Bool)),
        ),
    ]);

    let err = match GraphBuilder::new(capabilities)
        .node_from_id("demo.src", "src")
        .node_from_id("demo.sink", "sink")
        .try_connect("src.missing", "sink.in")
    {
        Ok(_) => panic!("connect should fail"),
        Err(err) => err,
    };

    assert_eq!(
        err,
        GraphBuildError::MissingNodePort {
            alias: "src".into(),
            node_id: "demo.src".into(),
            direction: "output".into(),
            port: "missing".into(),
            available: vec!["out".into()],
        }
    );
}

#[test]
fn fallible_connect_reports_missing_declared_input_port() {
    let capabilities = caps([
        NodeDecl::new("demo.src").output(
            PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                .schema(TypeExpr::Scalar(ValueType::Bool)),
        ),
        NodeDecl::new("demo.sink").input(
            PortDecl::new("in", "typeexpr:{\"Scalar\":\"Bool\"}")
                .schema(TypeExpr::Scalar(ValueType::Bool)),
        ),
    ]);

    let err = match GraphBuilder::new(capabilities)
        .node_from_id("demo.src", "src")
        .node_from_id("demo.sink", "sink")
        .try_connect("src.out", "sink.missing")
    {
        Ok(_) => panic!("connect should fail"),
        Err(err) => err,
    };

    assert_eq!(
        err,
        GraphBuildError::MissingNodePort {
            alias: "sink".into(),
            node_id: "demo.sink".into(),
            direction: "input".into(),
            port: "missing".into(),
            available: vec!["in".into()],
        }
    );
}

#[test]
fn fallible_connect_allows_fanin_and_dynamic_host_ports() {
    let capabilities = caps([
        NodeDecl::new("demo.src").output(
            PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                .schema(TypeExpr::Scalar(ValueType::Bool)),
        ),
        NodeDecl::new("demo.join").fanin_input(FanInDecl::new(
            "in",
            0,
            "typeexpr:{\"Scalar\":\"Bool\"}",
        )),
    ]);

    let graph = GraphBuilder::new(capabilities)
        .node_from_id("demo.src", "src")
        .node_from_id("demo.join", "join")
        .try_connect("camera", "join.in0")
        .expect("dynamic host output and fan-in input should be accepted")
        .try_connect("src.out", "join.in1")
        .expect("declared fan-in input should be accepted")
        .build();

    assert_eq!(graph.edges.len(), 2);
}

#[test]
fn fallible_node_insertion_reports_missing_node_id() {
    let capabilities = caps([NodeDecl::new("demo.known")]);

    let err = match GraphBuilder::new(capabilities).try_node_from_id("demo.missing", "missing") {
        Ok(_) => panic!("missing node id should fail"),
        Err(err) => err,
    };

    assert_eq!(
        err,
        GraphBuildError::MissingNodeId {
            id: "demo.missing".into()
        }
    );
}

#[test]
fn fallible_node_insertion_accepts_registered_node_id() {
    let capabilities = caps([NodeDecl::new("demo.known")]);

    let graph = GraphBuilder::new(capabilities)
        .try_node_from_id("demo.known", "known")
        .expect("registered node id")
        .build();

    assert_eq!(graph.nodes.len(), 1);
    assert_eq!(graph.nodes[0].id.0, "demo.known");
    assert_eq!(graph.nodes[0].label.as_deref(), Some("known"));
}

#[test]
fn graph_ctx_fallible_helpers_report_user_errors() {
    let capabilities = caps([
        NodeDecl::new("demo.src").output(
            PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                .schema(TypeExpr::Scalar(ValueType::Bool)),
        ),
        NodeDecl::new("demo.sink").input(
            PortDecl::new("in", "typeexpr:{\"Scalar\":\"Bool\"}")
                .schema(TypeExpr::Scalar(ValueType::Bool)),
        ),
    ]);

    let mut ctx = GraphCtx::new(capabilities, &["input"], &["output"]);
    let src = ctx.try_node_as("demo.src", "src").expect("src node");
    let sink = ctx.try_node_as("demo.sink", "sink").expect("sink node");

    let err = ctx
        .try_connect(&src.output("missing"), &sink.input("in"))
        .expect_err("missing output port should fail");
    assert_eq!(
        err,
        GraphBuildError::MissingNodePort {
            alias: "src".into(),
            node_id: "demo.src".into(),
            direction: "output".into(),
            port: "missing".into(),
            available: vec!["out".into()],
        }
    );

    let err = ctx
        .try_const_input(&src.input("missing"), Value::Bool(true))
        .expect_err("const input on non-input port should fail");
    assert_eq!(
        err,
        GraphBuildError::MissingNodePort {
            alias: "src".into(),
            node_id: "demo.src".into(),
            direction: "input".into(),
            port: "missing".into(),
            available: Vec::new(),
        }
    );
}

#[test]
fn graph_ctx_try_build_preserves_expected_host_ports() {
    let capabilities = caps([NodeDecl::new("demo.sink").input(
        PortDecl::new("in", "typeexpr:{\"Scalar\":\"Bool\"}")
            .schema(TypeExpr::Scalar(ValueType::Bool)),
    )]);

    let mut ctx = GraphCtx::new(capabilities, &["input"], &["output"]);
    let sink = ctx.try_node_as("demo.sink", "sink").expect("sink node");
    let input = ctx.input("input");
    ctx.try_connect(&input, &sink.input("in"))
        .expect("graph input should connect");
    let graph = ctx.try_build().expect("graph context should build");
    let host = graph
        .nodes
        .iter()
        .find(|node| is_host_bridge(node))
        .expect("host bridge");

    assert!(host.outputs.iter().any(|port| port == "input"));
    assert!(host.inputs.iter().any(|port| port == "output"));
}

#[test]
fn graph_scope_try_connect_error_leaves_scope_usable() {
    let capabilities = caps([
        NodeDecl::new("demo.src").output(
            PortDecl::new("out", "typeexpr:{\"Scalar\":\"Bool\"}")
                .schema(TypeExpr::Scalar(ValueType::Bool)),
        ),
        NodeDecl::new("demo.sink").input(
            PortDecl::new("in", "typeexpr:{\"Scalar\":\"Bool\"}")
                .schema(TypeExpr::Scalar(ValueType::Bool)),
        ),
    ]);

    let graph = GraphBuilder::new(capabilities)
        .try_edges(|scope| {
            let src = scope.add_node("src", "demo.src");
            let sink = scope.add_node("sink", "demo.sink");

            let err = scope
                .try_connect(&src.output("missing"), &sink.input("in"))
                .expect_err("missing output port should fail");
            assert_eq!(
                err,
                GraphBuildError::MissingNodePort {
                    alias: "src".into(),
                    node_id: "demo.src".into(),
                    direction: "output".into(),
                    port: "missing".into(),
                    available: vec!["out".into()],
                }
            );

            scope.try_connect(&src.output("out"), &sink.input("in"))
        })
        .expect("scope should remain usable after handled error")
        .build();

    assert_eq!(graph.edges.len(), 1);
}

#[test]
fn fallible_nested_helpers_report_user_errors() {
    let capabilities = CapabilityRegistry::new();
    let inner = GraphBuilder::new(capabilities.clone())
        .host_bridge("inner")
        .node_from_id("demo.echo", "echo")
        .connect_by_id(("inner", "input"), ("echo", "in"))
        .connect_by_id(("echo", "out"), ("inner", "output"))
        .build();
    let nested = NestedGraph::new(inner, "inner").expect("inner host bridge missing");

    let err = match GraphBuilder::new(capabilities.clone())
        .try_nest(&nested, "nested")
        .unwrap()
        .0
        .try_nest(&nested, "nested")
    {
        Ok(_) => panic!("duplicate nested alias should fail"),
        Err(err) => err,
    };
    assert_eq!(
        err,
        GraphBuildError::DuplicateNestedAlias {
            alias: "nested".into()
        }
    );

    let (builder, handle) = GraphBuilder::new(capabilities)
        .node_from_id("demo.src", "src")
        .try_nest(&nested, "nested")
        .unwrap();
    let err = match builder.try_connect_to_nested("src.out", &handle, "missing") {
        Ok(_) => panic!("missing nested input should fail"),
        Err(err) => err,
    };
    assert_eq!(
        err,
        GraphBuildError::MissingNestedInput {
            alias: "nested".into(),
            port: "missing".into()
        }
    );
}
