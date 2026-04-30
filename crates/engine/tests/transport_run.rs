#[cfg(all(feature = "plugins", feature = "gpu-mock"))]
use std::sync::atomic::{AtomicUsize, Ordering};
#[cfg(feature = "plugins")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "plugins")]
use daedalus_data::model::{TypeExpr, Value, ValueType};
#[cfg(all(feature = "plugins", feature = "gpu-mock"))]
use daedalus_engine::GpuBackend;
#[cfg(feature = "plugins")]
use daedalus_engine::{Engine, EngineConfig};
#[cfg(feature = "plugins")]
use daedalus_planner::{ComputeAffinity, Edge, Graph, NodeInstance, NodeRef, PortRef};
#[cfg(feature = "plugins")]
use daedalus_registry::capability::{NodeDecl, PortDecl};
#[cfg(feature = "plugins")]
use daedalus_registry::ids::NodeId;
#[cfg(feature = "plugins")]
use daedalus_runtime::RuntimeNode;
#[cfg(feature = "plugins")]
use daedalus_runtime::executor::{NodeError, NodeHandler};
#[cfg(feature = "plugins")]
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY, HostBridgeManager};
#[cfg(feature = "plugins")]
use daedalus_runtime::plugins::PluginRegistry;

#[test]
#[cfg(feature = "plugins")]
fn engine_run_plugin_registry_uses_registered_transport() {
    let mut plugins = PluginRegistry::new();
    plugins
        .register_node_decl(
            NodeDecl::new("source").output(
                PortDecl::new("out", "typeexpr:{\"Scalar\":\"Int\"}")
                    .schema(TypeExpr::Scalar(ValueType::Int)),
            ),
        )
        .unwrap();
    plugins
        .register_node_decl(
            NodeDecl::new("sink").input(
                PortDecl::new("in", "typeexpr:{\"Scalar\":\"String\"}")
                    .schema(TypeExpr::Scalar(ValueType::String)),
            ),
        )
        .unwrap();
    plugins
        .register_transport_adapter_fn(
            "int_to_string",
            TypeExpr::Scalar(ValueType::Int),
            TypeExpr::Scalar(ValueType::String),
            |payload, _request| {
                let value = payload.get_ref::<Value>().ok_or_else(|| {
                    daedalus_transport::TransportError::TypeMismatch {
                        expected: payload.type_key().clone(),
                        found: payload.type_key().clone(),
                    }
                })?;
                let Value::Int(value) = value else {
                    return Err(daedalus_transport::TransportError::Unsupported(
                        "expected int value".to_string(),
                    ));
                };
                Ok(daedalus_transport::Payload::owned(
                    "typeexpr:{\"Scalar\":\"String\"}",
                    Value::String(value.to_string().into()),
                ))
            },
        )
        .unwrap();

    let graph = Graph {
        nodes: vec![
            NodeInstance {
                id: NodeId::new("source"),
                bundle: None,
                label: None,
                inputs: vec![],
                outputs: vec!["out".into()],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
            NodeInstance {
                id: NodeId::new("sink"),
                bundle: None,
                label: None,
                inputs: vec!["in".into()],
                outputs: vec![],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
        ],
        edges: vec![Edge {
            from: PortRef {
                node: NodeRef(0),
                port: "out".into(),
            },
            to: PortRef {
                node: NodeRef(1),
                port: "in".into(),
            },
            metadata: Default::default(),
        }],
        metadata: Default::default(),
    };

    let seen = Arc::new(Mutex::new(None));
    let result = Engine::new(EngineConfig::default())
        .unwrap()
        .run_plugin_registry(
            &plugins,
            graph,
            TransportEngineHandler { seen: seen.clone() },
        )
        .unwrap();

    assert_eq!(seen.lock().unwrap().as_deref(), Some("42"));
    assert_eq!(result.runtime_plan.edge_transports.len(), 1);
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg(all(feature = "plugins", feature = "gpu-mock"))]
struct GpuI32(i32);

#[test]
#[cfg(all(feature = "plugins", feature = "gpu-mock"))]
fn engine_run_plugin_registry_executes_device_upload_download() {
    let cpu_ty = TypeExpr::opaque("test:i32");
    let device_ty = TypeExpr::opaque("test:i32@gpu");
    let mut plugins = PluginRegistry::new();
    plugins
        .register_node_decl(
            NodeDecl::new("source").output(PortDecl::new("out", "test:i32").schema(cpu_ty.clone())),
        )
        .unwrap();
    plugins
        .register_node_decl(
            NodeDecl::new("device_passthrough")
                .input(PortDecl::new("in", "test:i32@gpu").schema(device_ty.clone()))
                .output(PortDecl::new("out", "test:i32@gpu").schema(device_ty.clone())),
        )
        .unwrap();
    plugins
        .register_node_decl(
            NodeDecl::new("sink").input(PortDecl::new("in", "test:i32").schema(cpu_ty.clone())),
        )
        .unwrap();
    let uploads = Arc::new(AtomicUsize::new(0));
    let downloads = Arc::new(AtomicUsize::new(0));
    let upload_count = uploads.clone();
    let download_count = downloads.clone();
    plugins
        .register_typed_device_transport::<i32, GpuI32, _, _>(
            daedalus_runtime::plugins::TypedDeviceTransport::new(
                "test.device.i32",
                cpu_ty,
                device_ty,
                "test.i32.upload",
                "test.i32.download",
            ),
            move |value| {
                upload_count.fetch_add(1, Ordering::Relaxed);
                Ok(GpuI32(*value))
            },
            move |value| {
                download_count.fetch_add(1, Ordering::Relaxed);
                Ok(value.0)
            },
        )
        .unwrap();

    let graph = Graph {
        nodes: vec![
            NodeInstance {
                id: NodeId::new("source"),
                bundle: None,
                label: None,
                inputs: vec![],
                outputs: vec!["out".into()],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
            NodeInstance {
                id: NodeId::new("device_passthrough"),
                bundle: None,
                label: None,
                inputs: vec!["in".into()],
                outputs: vec!["out".into()],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
            NodeInstance {
                id: NodeId::new("sink"),
                bundle: None,
                label: None,
                inputs: vec!["in".into()],
                outputs: vec![],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
        ],
        edges: vec![
            Edge {
                from: PortRef {
                    node: NodeRef(0),
                    port: "out".into(),
                },
                to: PortRef {
                    node: NodeRef(1),
                    port: "in".into(),
                },
                metadata: Default::default(),
            },
            Edge {
                from: PortRef {
                    node: NodeRef(1),
                    port: "out".into(),
                },
                to: PortRef {
                    node: NodeRef(2),
                    port: "in".into(),
                },
                metadata: Default::default(),
            },
        ],
        metadata: Default::default(),
    };

    let seen = Arc::new(Mutex::new(None));
    let mut config = EngineConfig::default();
    config.planner.enable_gpu = true;
    config.gpu = GpuBackend::Mock;
    let result = Engine::new(config)
        .unwrap()
        .run_plugin_registry(
            &plugins,
            graph,
            DeviceTransportEngineHandler { seen: seen.clone() },
        )
        .unwrap();

    assert_eq!(*seen.lock().unwrap(), Some(42));
    assert_eq!(uploads.load(Ordering::Relaxed), 1);
    assert_eq!(
        downloads.load(Ordering::Relaxed),
        0,
        "download should use the CPU resident cached on the payload"
    );
    assert_eq!(result.runtime_plan.edge_transports.len(), 2);
    assert_eq!(
        result.runtime_plan.edge_transports[0]
            .as_ref()
            .unwrap()
            .adapter_steps[0]
            .as_str(),
        "test.i32.upload"
    );
    assert_eq!(
        result.runtime_plan.edge_transports[1]
            .as_ref()
            .unwrap()
            .adapter_steps[0]
            .as_str(),
        "test.i32.download"
    );
}

#[test]
#[cfg(feature = "plugins")]
fn host_graph_ticks_payloads_through_bridge() {
    let int_ty = TypeExpr::Scalar(ValueType::Int);
    let int_key = "typeexpr:{\"Scalar\":\"Int\"}";
    let mut plugins = PluginRegistry::new();
    plugins
        .register_node_decl(
            NodeDecl::new(HOST_BRIDGE_ID)
                .metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
                .input(PortDecl::new("out", int_key).schema(int_ty.clone()))
                .output(PortDecl::new("in", int_key).schema(int_ty.clone())),
        )
        .unwrap();
    plugins
        .register_node_decl(
            NodeDecl::new("inc")
                .input(PortDecl::new("in", int_key).schema(int_ty.clone()))
                .output(PortDecl::new("out", int_key).schema(int_ty)),
        )
        .unwrap();

    let graph = Graph {
        nodes: vec![
            NodeInstance {
                id: NodeId::new(HOST_BRIDGE_ID),
                bundle: None,
                label: Some("host".into()),
                inputs: vec!["out".into()],
                outputs: vec!["in".into()],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: [(HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true))]
                    .into_iter()
                    .collect(),
            },
            NodeInstance {
                id: NodeId::new("inc"),
                bundle: None,
                label: None,
                inputs: vec!["in".into()],
                outputs: vec!["out".into()],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
        ],
        edges: vec![
            Edge {
                from: PortRef {
                    node: NodeRef(0),
                    port: "in".into(),
                },
                to: PortRef {
                    node: NodeRef(1),
                    port: "in".into(),
                },
                metadata: Default::default(),
            },
            Edge {
                from: PortRef {
                    node: NodeRef(1),
                    port: "out".into(),
                },
                to: PortRef {
                    node: NodeRef(0),
                    port: "out".into(),
                },
                metadata: Default::default(),
            },
        ],
        metadata: Default::default(),
    };

    let bridges = HostBridgeManager::new();
    let mut graph = Engine::new(EngineConfig::default())
        .unwrap()
        .compile_host_graph_plugin_registry(&plugins, graph, IncrementHandler, bridges, "host")
        .unwrap();

    graph.push_payload(
        "in",
        daedalus_transport::Payload::owned(int_key, Value::Int(41)),
    );
    graph.tick().unwrap();

    let out = graph.drain_payloads("out");
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].get_ref::<Value>(), Some(&Value::Int(42)));
}

#[test]
#[cfg(feature = "plugins")]
fn cached_direct_host_route_reuses_direct_slots() {
    let int_ty = TypeExpr::Scalar(ValueType::Int);
    let int_key = "typeexpr:{\"Scalar\":\"Int\"}";
    let mut plugins = PluginRegistry::new();
    plugins
        .register_node_decl(
            NodeDecl::new(HOST_BRIDGE_ID)
                .metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
                .input(PortDecl::new("out", int_key).schema(int_ty.clone()))
                .output(PortDecl::new("in", int_key).schema(int_ty.clone())),
        )
        .unwrap();
    for id in ["inc_a", "inc_b"] {
        plugins
            .register_node_decl(
                NodeDecl::new(id)
                    .input(PortDecl::new("in", int_key).schema(int_ty.clone()))
                    .output(PortDecl::new("out", int_key).schema(int_ty.clone())),
            )
            .unwrap();
    }

    let graph = Graph {
        nodes: vec![
            NodeInstance {
                id: NodeId::new(HOST_BRIDGE_ID),
                bundle: None,
                label: Some("host".into()),
                inputs: vec!["out".into()],
                outputs: vec!["in".into()],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: [(HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true))]
                    .into_iter()
                    .collect(),
            },
            NodeInstance {
                id: NodeId::new("inc_a"),
                bundle: None,
                label: None,
                inputs: vec!["in".into()],
                outputs: vec!["out".into()],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
            NodeInstance {
                id: NodeId::new("inc_b"),
                bundle: None,
                label: None,
                inputs: vec!["in".into()],
                outputs: vec!["out".into()],
                compute: ComputeAffinity::CpuOnly,
                const_inputs: vec![],
                sync_groups: vec![],
                metadata: Default::default(),
            },
        ],
        edges: vec![
            Edge {
                from: PortRef {
                    node: NodeRef(0),
                    port: "in".into(),
                },
                to: PortRef {
                    node: NodeRef(1),
                    port: "in".into(),
                },
                metadata: Default::default(),
            },
            Edge {
                from: PortRef {
                    node: NodeRef(1),
                    port: "out".into(),
                },
                to: PortRef {
                    node: NodeRef(2),
                    port: "in".into(),
                },
                metadata: Default::default(),
            },
            Edge {
                from: PortRef {
                    node: NodeRef(2),
                    port: "out".into(),
                },
                to: PortRef {
                    node: NodeRef(0),
                    port: "out".into(),
                },
                metadata: Default::default(),
            },
        ],
        metadata: Default::default(),
    };

    let mut graph = Engine::new(EngineConfig::default())
        .unwrap()
        .compile_host_graph_plugin_registry(
            &plugins,
            graph,
            IncrementHandler,
            HostBridgeManager::new(),
            "host",
        )
        .unwrap();
    let route = graph
        .direct_host_route("in", "out")
        .expect("direct host route");

    for (input, expected) in [(40, 42), (100, 102)] {
        let (_, output) = graph
            .tick_direct_route(
                &route,
                daedalus_transport::Payload::owned(int_key, Value::Int(input)),
            )
            .unwrap();
        assert_eq!(
            output
                .as_ref()
                .and_then(|payload| payload.get_ref::<Value>()),
            Some(&Value::Int(expected))
        );
    }
}

#[cfg(feature = "plugins")]
struct TransportEngineHandler {
    seen: Arc<Mutex<Option<String>>>,
}

#[cfg(feature = "plugins")]
impl NodeHandler for TransportEngineHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "source" => io.push_value(Some("out"), Value::Int(42)),
            "sink" => {
                let value = io
                    .get_typed::<String>("in")
                    .ok_or_else(|| NodeError::InvalidInput("missing string input".to_string()))?;
                self.seen.lock().unwrap().replace(value);
            }
            _ => {}
        }
        Ok(())
    }
}

#[cfg(feature = "plugins")]
struct IncrementHandler;

#[cfg(feature = "plugins")]
impl NodeHandler for IncrementHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        if node.id.starts_with("inc") {
            let value = io
                .get_typed_ref::<Value>("in")
                .ok_or_else(|| NodeError::InvalidInput("missing input".to_string()))?;
            let Value::Int(value) = value else {
                return Err(NodeError::InvalidInput("expected int".to_string()));
            };
            io.push_payload(
                "out",
                daedalus_transport::Payload::owned(
                    "typeexpr:{\"Scalar\":\"Int\"}",
                    Value::Int(value + 1),
                ),
            );
        }
        Ok(())
    }
}

#[cfg(all(feature = "plugins", feature = "gpu-mock"))]
struct DeviceTransportEngineHandler {
    seen: Arc<Mutex<Option<i32>>>,
}

#[cfg(all(feature = "plugins", feature = "gpu-mock"))]
impl NodeHandler for DeviceTransportEngineHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "source" => {
                io.push_payload("out", daedalus_transport::Payload::owned("test:i32", 42i32))
            }
            "device_passthrough" => {
                let payload = io
                    .take_input_payload("in")
                    .ok_or_else(|| NodeError::InvalidInput("missing gpu input".to_string()))?;
                if payload.inner.get_ref::<GpuI32>().is_none() {
                    return Err(NodeError::InvalidInput("missing gpu input".to_string()));
                }
                io.push_correlated_payload("out", payload);
            }
            "sink" => {
                let value = io
                    .get_typed::<i32>("in")
                    .ok_or_else(|| NodeError::InvalidInput("missing cpu input".to_string()))?;
                self.seen.lock().unwrap().replace(value);
            }
            _ => {}
        }
        Ok(())
    }
}
