use std::sync::Mutex;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use daedalus::planner::{PlannerConfig, PlannerInput, build_plan, explain_plan};
use daedalus::runtime::plugins::RegistryPluginExt;
use daedalus::runtime::{SchedulerConfig, build_runtime, executor::Executor};
use daedalus::transport::{AdaptRequest, Payload, TransportError, TypeKey};
use daedalus::{PluginRegistry, adapt, device, macros::node, plugin, runtime::NodeError, type_key};
use image::{DynamicImage, ImageBuffer, Rgba};

#[type_key("test:frame")]
#[derive(Clone)]
pub struct TestFrame {
    _value: i32,
}

#[type_key("test:packet")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestPacket {
    value: i32,
}

#[type_key("test:summary")]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestSummary {
    text: String,
}

#[type_key("test:image:dynamic")]
#[derive(Clone)]
pub struct TestDynamicImage {
    image: DynamicImage,
}

#[type_key("test:image:rotated_180")]
#[derive(Clone)]
pub struct TestRotatedImage {
    image: DynamicImage,
}

static BRANCH_FRAME_CALLS: AtomicUsize = AtomicUsize::new(0);
static PACKET_TO_SUMMARY_CALLS: AtomicUsize = AtomicUsize::new(0);
static SUMMARY_SINK_VALUE: Mutex<Option<String>> = Mutex::new(None);
static IMAGE_ROTATE_CALLS: AtomicUsize = AtomicUsize::new(0);
static ROTATED_IMAGE_PIXELS: Mutex<Option<Vec<[u8; 4]>>> = Mutex::new(None);

#[adapt(
    id = "test.i32_to_string",
    from = "test:i32",
    to = "test:string",
    cost = 0
)]
fn i32_to_string(value: &i32) -> Result<String, TransportError> {
    Ok(value.to_string())
}

#[adapt(id = "test.bump_i32", from = "test:i32")]
fn bump_i32(value: &mut i32) -> Result<(), TransportError> {
    *value += 1;
    Ok(())
}

#[adapt(
    id = "test.arc_i32_to_string",
    from = "test:i32_arc",
    to = "test:string"
)]
fn arc_i32_to_string(value: Arc<i32>) -> Result<String, TransportError> {
    Ok(value.to_string())
}

#[adapt(
    id = "test.owned_i32_to_string",
    from = "test:i32_owned",
    to = "test:string"
)]
fn owned_i32_to_string(value: i32) -> Result<String, TransportError> {
    Ok(value.to_string())
}

#[adapt(
    id = "test.i32_to_shared_string",
    from = "test:i32_shared_out",
    to = "test:string_shared"
)]
fn i32_to_shared_string(value: &i32) -> Result<Arc<String>, TransportError> {
    Ok(Arc::new(value.to_string()))
}

#[adapt(
    id = "test.gpu_i32_to_string",
    from = "test:i32_gpu",
    to = "test:string_gpu",
    cost = 7,
    kind = "device_transfer",
    residency = "gpu",
    layout = "test-layout",
    requires_gpu = true,
    features = "gpu, fast"
)]
fn gpu_i32_to_string(value: &i32) -> Result<String, TransportError> {
    Ok(value.to_string())
}

#[adapt(
    id = "test.frame_branch",
    from = "test:frame",
    to = "test:frame",
    kind = "branch"
)]
fn branch_frame(value: &TestFrame) -> Result<TestFrame, TransportError> {
    BRANCH_FRAME_CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(value.clone())
}

#[adapt(
    id = "test.packet_to_summary",
    from = "test:packet",
    to = "test:summary",
    cost = 0
)]
fn packet_to_summary(packet: &TestPacket) -> Result<TestSummary, TransportError> {
    PACKET_TO_SUMMARY_CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(TestSummary {
        text: format!("packet={}", packet.value),
    })
}

#[adapt(
    id = "test.image.rotate_180",
    from = "test:image:dynamic",
    to = "test:image:rotated_180",
    cost = 0
)]
fn rotate_dynamic_image_180(input: &TestDynamicImage) -> Result<TestRotatedImage, TransportError> {
    IMAGE_ROTATE_CALLS.fetch_add(1, Ordering::SeqCst);
    Ok(TestRotatedImage {
        image: input.image.rotate180(),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TestGpuI32(i32);

fn download_i32(value: &TestGpuI32) -> Result<i32, TransportError> {
    Ok(value.0)
}

#[device(
    id = "test.device.i32",
    cpu = "test:i32",
    device = "test:i32@gpu",
    download = download_i32
)]
fn upload_i32(value: &i32) -> Result<TestGpuI32, TransportError> {
    Ok(TestGpuI32(*value))
}

#[node(id = "test.adapter_noop", outputs("out"))]
fn adapter_noop() -> Result<i32, NodeError> {
    Ok(1)
}

#[node(id = "test.frame_source", outputs("frame"))]
fn frame_source() -> Result<TestFrame, NodeError> {
    Ok(TestFrame { _value: 1 })
}

#[node(id = "test.read_frame", inputs("frame"))]
fn read_frame(_frame: &TestFrame) -> Result<(), NodeError> {
    Ok(())
}

#[node(id = "test.mut_frame", inputs("frame"))]
fn mut_frame(frame: &mut TestFrame) -> Result<(), NodeError> {
    frame._value += 1;
    Ok(())
}

#[node(id = "test.packet_source", outputs("packet"))]
fn packet_source() -> Result<TestPacket, NodeError> {
    Ok(TestPacket { value: 7 })
}

#[node(id = "test.summary_sink", inputs("summary"))]
fn summary_sink(summary: TestSummary) -> Result<(), NodeError> {
    SUMMARY_SINK_VALUE
        .lock()
        .map_err(|_| NodeError::Handler("summary sink lock poisoned".into()))?
        .replace(summary.text);
    Ok(())
}

#[node(id = "test.dynamic_image_source", outputs("image"))]
fn dynamic_image_source() -> Result<TestDynamicImage, NodeError> {
    let image = ImageBuffer::from_fn(2, 2, |x, y| match (x, y) {
        (0, 0) => Rgba([255, 0, 0, 255]),
        (1, 0) => Rgba([0, 255, 0, 255]),
        (0, 1) => Rgba([0, 0, 255, 255]),
        (1, 1) => Rgba([255, 255, 255, 255]),
        _ => unreachable!(),
    });
    Ok(TestDynamicImage {
        image: DynamicImage::ImageRgba8(image),
    })
}

#[node(id = "test.rotated_image_sink", inputs("image"))]
fn rotated_image_sink(image: TestRotatedImage) -> Result<(), NodeError> {
    let rgba = image.image.to_rgba8();
    let pixels = rgba.pixels().map(|pixel| pixel.0).collect::<Vec<_>>();
    ROTATED_IMAGE_PIXELS
        .lock()
        .map_err(|_| NodeError::Handler("rotated image sink lock poisoned".into()))?
        .replace(pixels);
    Ok(())
}

#[plugin(
    id = "test.adapt_attr",
    types(TestFrame),
    nodes(adapter_noop),
    adapters(i32_to_string, bump_i32, arc_i32_to_string, owned_i32_to_string)
)]
pub struct AdaptAttrPlugin;

#[plugin(
    id = "test.fanout_mut",
    types(TestFrame),
    nodes(frame_source, read_frame, mut_frame),
    adapters(branch_frame)
)]
pub struct FanoutMutPlugin;

#[plugin(
    id = "test.end_to_end_transport",
    types(TestPacket, TestSummary),
    nodes(packet_source, summary_sink),
    adapters(packet_to_summary)
)]
pub struct EndToEndTransportPlugin;

#[plugin(
    id = "test.dynamic_image_transport",
    types(TestDynamicImage, TestRotatedImage),
    nodes(dynamic_image_source, rotated_image_sink),
    adapters(rotate_dynamic_image_180)
)]
pub struct DynamicImageTransportPlugin;

fn install_transport_only_setup(registry: &mut PluginRegistry) -> Result<(), &'static str> {
    registry.register_capability_typed::<i32, _>("hook-called", |a, b| Ok(*a + *b));
    Ok(())
}

#[plugin(
    id = "test.transport_only",
    install = install_transport_only_setup,
    types(TestFrame),
    adapters(i32_to_string),
    devices(upload_i32)
)]
pub struct TransportOnlyPlugin;

#[test]
fn adapt_macro_registers_read_adapter() {
    let mut registry = PluginRegistry::new();
    register_i32_to_string_adapter(&mut registry).expect("register adapter");

    let path = registry
        .transport_capabilities
        .resolve_adapter_path_for(
            &TypeKey::new("test:i32"),
            &AdaptRequest::new(TypeKey::new("test:string")),
        )
        .expect("adapter path");
    assert_eq!(path.steps[0].as_str(), "test.i32_to_string");
    assert!(
        registry
            .transport_capabilities
            .adapter_decl(&daedalus::transport::AdapterId::new("test.i32_to_string"))
            .is_some()
    );

    let output = registry
        .runtime_transport
        .execute_adapter_path(
            Payload::owned("test:i32", 42i32),
            &path.steps,
            &AdaptRequest::new(TypeKey::new("test:string")),
        )
        .expect("execute adapter");

    assert_eq!(output.get_ref::<String>().map(String::as_str), Some("42"));
}

#[test]
fn node_macro_marks_mut_ref_inputs_as_mut_borrowed() {
    let decl = MutFrameNode::node_decl().expect("node decl");
    assert_eq!(
        decl.inputs[0].access,
        daedalus::transport::AccessMode::Modify
    );
}

#[test]
fn branch_adapt_macro_registers_modify_capable_adapter() {
    let mut registry = PluginRegistry::new();
    register_branch_frame_adapter(&mut registry).expect("register branch adapter");

    let adapter = registry
        .transport_capabilities
        .adapter_decl(&daedalus::transport::AdapterId::new("test.frame_branch"))
        .expect("adapter capability");
    assert_eq!(adapter.kind, daedalus::transport::AdaptKind::Branch);
    assert_eq!(adapter.access, daedalus::transport::AccessMode::Modify);
}

#[test]
fn plugin_graph_plans_branch_for_fanout_into_mut_node() {
    let mut registry = PluginRegistry::new();
    let plugin = FanoutMutPlugin::new();
    registry.install_plugin(&plugin).expect("install plugin");

    let source = plugin.frame_source.alias("source");
    let read = plugin.read_frame.alias("read");
    let mutate = plugin.mut_frame.alias("mutate");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .node(&source)
        .node(&read)
        .node(&mutate)
        .connect(&source.outputs.frame, &read.inputs.frame)
        .connect(&source.outputs.frame, &mutate.inputs.frame)
        .build();
    let planned = build_plan(
        PlannerInput { graph },
        registry
            .planner_config_with_transport(PlannerConfig::default())
            .expect("planner config"),
    );
    assert!(planned.diagnostics.is_empty(), "{:?}", planned.diagnostics);

    let explanation = explain_plan(&planned.plan.graph);
    let mut_edge = explanation
        .edges
        .iter()
        .find(|edge| edge.to_node == "test.fanout_mut:test.mut_frame")
        .expect("mutating edge");
    assert_eq!(mut_edge.converter_steps, vec!["test.frame_branch"]);
    assert!(mut_edge.target_exclusive);
    assert_eq!(
        mut_edge.target_access,
        daedalus::transport::AccessMode::Modify
    );
}

#[test]
fn plugin_graph_executes_branch_adapter_for_fanout_into_mut_node() {
    BRANCH_FRAME_CALLS.store(0, Ordering::SeqCst);

    let mut registry = PluginRegistry::new();
    let plugin = FanoutMutPlugin::new();
    registry.install_plugin(&plugin).expect("install plugin");

    let source = plugin.frame_source.alias("source");
    let read = plugin.read_frame.alias("read");
    let mutate = plugin.mut_frame.alias("mutate");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .node(&source)
        .node(&read)
        .node(&mutate)
        .connect(&source.outputs.frame, &read.inputs.frame)
        .connect(&source.outputs.frame, &mutate.inputs.frame)
        .build();
    let planned = build_plan(
        PlannerInput { graph },
        registry
            .planner_config_with_transport(PlannerConfig::default())
            .expect("planner config"),
    );
    assert!(planned.diagnostics.is_empty(), "{:?}", planned.diagnostics);
    let runtime_plan = build_runtime(&planned.plan, &SchedulerConfig::default());
    let handlers = registry.take_handlers();

    Executor::new(&runtime_plan, handlers)
        .with_runtime_transport(registry.runtime_transport.clone())
        .run()
        .expect("execute graph");

    assert_eq!(BRANCH_FRAME_CALLS.load(Ordering::SeqCst), 1);
}

#[test]
fn plugin_graph_runs_macro_nodes_through_planned_transport_adapter() {
    PACKET_TO_SUMMARY_CALLS.store(0, Ordering::SeqCst);
    SUMMARY_SINK_VALUE.lock().unwrap().take();

    let mut registry = PluginRegistry::new();
    let plugin = EndToEndTransportPlugin::new();
    registry.install_plugin(&plugin).expect("install plugin");

    let source = plugin.packet_source.alias("source");
    let sink = plugin.summary_sink.alias("sink");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .node(&source)
        .node(&sink)
        .connect(&source.outputs.packet, &sink.inputs.summary)
        .build();
    let planned = build_plan(
        PlannerInput { graph },
        registry
            .planner_config_with_transport(PlannerConfig::default())
            .expect("planner config"),
    );
    assert!(planned.diagnostics.is_empty(), "{:?}", planned.diagnostics);

    let explanation = explain_plan(&planned.plan.graph);
    let edge = explanation.edges.first().expect("transport edge");
    assert_eq!(edge.converter_steps, vec!["test.packet_to_summary"]);

    let runtime_plan = build_runtime(&planned.plan, &SchedulerConfig::default());
    let handlers = registry.take_handlers();
    Executor::new(&runtime_plan, handlers)
        .with_runtime_transport(registry.runtime_transport.clone())
        .run()
        .expect("execute graph");

    assert_eq!(PACKET_TO_SUMMARY_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(
        SUMMARY_SINK_VALUE.lock().unwrap().as_deref(),
        Some("packet=7")
    );
}

#[test]
fn plugin_graph_rotates_dynamic_image_180_through_transport_adapter() {
    IMAGE_ROTATE_CALLS.store(0, Ordering::SeqCst);
    ROTATED_IMAGE_PIXELS.lock().unwrap().take();

    let mut registry = PluginRegistry::new();
    let plugin = DynamicImageTransportPlugin::new();
    registry.install_plugin(&plugin).expect("install plugin");

    let source = plugin.dynamic_image_source.alias("source");
    let sink = plugin.rotated_image_sink.alias("sink");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .node(&source)
        .node(&sink)
        .connect(&source.outputs.image, &sink.inputs.image)
        .build();
    let planned = build_plan(
        PlannerInput { graph },
        registry
            .planner_config_with_transport(PlannerConfig::default())
            .expect("planner config"),
    );
    assert!(planned.diagnostics.is_empty(), "{:?}", planned.diagnostics);

    let explanation = explain_plan(&planned.plan.graph);
    let edge = explanation.edges.first().expect("transport edge");
    assert_eq!(edge.converter_steps, vec!["test.image.rotate_180"]);

    let runtime_plan = build_runtime(&planned.plan, &SchedulerConfig::default());
    let handlers = registry.take_handlers();
    Executor::new(&runtime_plan, handlers)
        .with_runtime_transport(registry.runtime_transport.clone())
        .run()
        .expect("execute graph");

    assert_eq!(IMAGE_ROTATE_CALLS.load(Ordering::SeqCst), 1);
    assert_eq!(
        ROTATED_IMAGE_PIXELS.lock().unwrap().as_ref(),
        Some(&vec![
            [255, 255, 255, 255],
            [0, 0, 255, 255],
            [0, 255, 0, 255],
            [255, 0, 0, 255],
        ])
    );
}

#[test]
fn adapt_macro_registers_mut_adapter() {
    let mut registry = PluginRegistry::new();
    register_bump_i32_adapter(&mut registry).expect("register adapter");

    let output = registry
        .runtime_transport
        .execute_adapter_path(
            Payload::owned("test:i32", 41i32),
            &[daedalus::transport::AdapterId::new("test.bump_i32")],
            &AdaptRequest::new(TypeKey::new("test:i32")),
        )
        .expect("execute adapter");

    assert_eq!(output.get_ref::<i32>(), Some(&42));
}

#[test]
fn adapt_macro_registers_arc_adapter() {
    let mut registry = PluginRegistry::new();
    register_arc_i32_to_string_adapter(&mut registry).expect("register adapter");

    let input = Arc::new(42i32);
    let output = registry
        .runtime_transport
        .execute_adapter_path(
            Payload::shared("test:i32_arc", input.clone()),
            &[daedalus::transport::AdapterId::new(
                "test.arc_i32_to_string",
            )],
            &AdaptRequest::new(TypeKey::new("test:string")),
        )
        .expect("execute adapter");

    assert_eq!(Arc::strong_count(&input), 1);
    assert_eq!(output.get_ref::<String>().map(String::as_str), Some("42"));
}

#[test]
fn adapt_macro_registers_owned_adapter() {
    let mut registry = PluginRegistry::new();
    register_owned_i32_to_string_adapter(&mut registry).expect("register adapter");

    let output = registry
        .runtime_transport
        .execute_adapter_path(
            Payload::owned("test:i32_owned", 42i32),
            &[daedalus::transport::AdapterId::new(
                "test.owned_i32_to_string",
            )],
            &AdaptRequest::new(TypeKey::new("test:string")),
        )
        .expect("execute adapter");

    assert_eq!(output.get_ref::<String>().map(String::as_str), Some("42"));
}

#[test]
fn adapt_macro_owned_adapter_rejects_shared_payload() {
    let mut registry = PluginRegistry::new();
    register_owned_i32_to_string_adapter(&mut registry).expect("register adapter");

    let input = Arc::new(42i32);
    let err = registry
        .runtime_transport
        .execute_adapter_path(
            Payload::shared("test:i32_owned", input.clone()),
            &[daedalus::transport::AdapterId::new(
                "test.owned_i32_to_string",
            )],
            &AdaptRequest::new(TypeKey::new("test:string")),
        )
        .expect_err("shared payload should not satisfy owned adapter");

    assert!(err.to_string().contains("requires a unique payload"));
}

#[test]
fn adapt_macro_registers_shared_output_adapter() {
    let mut registry = PluginRegistry::new();
    register_i32_to_shared_string_adapter(&mut registry).expect("register adapter");

    let output = registry
        .runtime_transport
        .execute_adapter_path(
            Payload::owned("test:i32_shared_out", 42i32),
            &[daedalus::transport::AdapterId::new(
                "test.i32_to_shared_string",
            )],
            &AdaptRequest::new(TypeKey::new("test:string_shared")),
        )
        .expect("execute adapter");

    assert_eq!(
        output.get_arc::<String>().as_deref().map(String::as_str),
        Some("42")
    );
    assert!(output.get_ref::<Arc<String>>().is_none());
}

#[test]
fn adapt_macro_registers_transport_metadata() {
    let mut registry = PluginRegistry::new();
    register_gpu_i32_to_string_adapter(&mut registry).expect("register adapter");

    let adapter = registry
        .transport_capabilities
        .adapter_decl(&daedalus::transport::AdapterId::new(
            "test.gpu_i32_to_string",
        ))
        .expect("adapter capability");
    assert_eq!(adapter.cost.cpu_ns, 7);
    assert_eq!(adapter.kind, daedalus::transport::AdaptKind::DeviceTransfer);
    assert_eq!(adapter.access, daedalus::transport::AccessMode::Move);
    assert_eq!(adapter.residency, Some(daedalus::transport::Residency::Gpu));
    assert_eq!(
        adapter.layout.as_ref().map(|layout| layout.as_str()),
        Some("test-layout")
    );
    assert!(adapter.requires_gpu);
    assert_eq!(adapter.feature_flags, vec!["fast", "gpu"]);

    assert!(
        registry
            .transport_capabilities
            .resolve_adapter_path_for_with_context(
                &TypeKey::new("test:i32_gpu"),
                &AdaptRequest::new(TypeKey::new("test:string_gpu")),
                &["gpu".to_string(), "fast".to_string()],
                false,
            )
            .is_err()
    );
    let path = registry
        .transport_capabilities
        .resolve_adapter_path_for_with_context(
            &TypeKey::new("test:i32_gpu"),
            &AdaptRequest::new(TypeKey::new("test:string_gpu")),
            &["gpu".to_string(), "fast".to_string()],
            true,
        )
        .expect("gpu adapter path");
    assert_eq!(path.steps[0].as_str(), "test.gpu_i32_to_string");
}

#[test]
fn device_macro_registers_upload_download_device() {
    let mut registry = PluginRegistry::new();
    register_upload_i32_device(&mut registry).expect("register device");

    let snapshot = registry
        .transport_capabilities
        .freeze()
        .expect("transport capabilities freeze");
    assert_eq!(snapshot.devices.len(), 1);
    assert_eq!(snapshot.devices[0].id, "test.device.i32");
    assert!(
        snapshot
            .adapters
            .iter()
            .any(|adapter| adapter.id.as_str() == "test.device.i32.upload"
                && adapter.residency == Some(daedalus::transport::Residency::Gpu)
                && adapter.requires_gpu)
    );

    let mut request = AdaptRequest::new(TypeKey::new("test:i32@gpu"));
    request.residency = Some(daedalus::transport::Residency::Gpu);
    let path = registry
        .transport_capabilities
        .resolve_adapter_path_for_with_context(&TypeKey::new("test:i32"), &request, &[], true)
        .expect("upload path");
    assert_eq!(path.steps[0].as_str(), "test.device.i32.upload");

    let uploaded = registry
        .runtime_transport
        .execute_adapter_path(Payload::owned("test:i32", 42i32), &path.steps, &request)
        .expect("execute upload");
    assert_eq!(uploaded.get_ref::<TestGpuI32>(), Some(&TestGpuI32(42)));
}

#[test]
fn declare_plugin_installs_adapt_macro_adapters() {
    let mut registry = PluginRegistry::new();
    registry
        .install_plugin(&AdaptAttrPlugin::new())
        .expect("install plugin");

    let path = registry
        .combined_transport_capabilities()
        .expect("combined capabilities")
        .resolve_adapter_path_for(
            &TypeKey::new("test:i32"),
            &AdaptRequest::new(TypeKey::new("test:string")),
        )
        .expect("adapter path");
    assert_eq!(path.steps[0].as_str(), "test.i32_to_string");
    assert!(
        registry
            .transport_capabilities
            .node_decl(&daedalus::registry::ids::NodeId::new(
                "test.adapt_attr:test.adapter_noop"
            ))
            .is_some()
    );
}

#[test]
fn type_key_macro_registers_opaque_type() {
    let mut registry = PluginRegistry::new();
    register_test_frame_type(&mut registry).expect("register type");

    assert_eq!(
        daedalus::data::typing::type_expr::<TestFrame>(),
        daedalus::data::model::TypeExpr::opaque("test:frame")
    );
    assert_eq!(
        registry
            .named_type_registry
            .lookup("test:frame")
            .expect("named type")
            .expr,
        daedalus::data::model::TypeExpr::opaque("test:frame")
    );
}

#[test]
fn plugin_attr_installs_adapt_macro_adapters() {
    let mut registry = PluginRegistry::new();
    registry
        .install_plugin(&AdaptAttrPlugin::new())
        .expect("install plugin");

    let path = registry
        .combined_transport_capabilities()
        .expect("combined capabilities")
        .resolve_adapter_path_for(
            &TypeKey::new("test:i32"),
            &AdaptRequest::new(TypeKey::new("test:string")),
        )
        .expect("adapter path");
    assert_eq!(path.steps[0].as_str(), "test.i32_to_string");
    assert_eq!(
        daedalus::data::typing::type_expr::<TestFrame>(),
        daedalus::data::model::TypeExpr::opaque("test:frame")
    );
    assert!(
        registry
            .transport_capabilities
            .node_decl(&daedalus::registry::ids::NodeId::new(
                "test.adapt_attr:test.adapter_noop"
            ))
            .is_some()
    );
}

#[test]
fn plugin_attr_supports_transport_only_plugins_and_install_hook() {
    let mut registry = PluginRegistry::bare();
    registry
        .install_plugin(&TransportOnlyPlugin::new())
        .expect("install plugin");

    let path = registry
        .combined_transport_capabilities()
        .expect("combined capabilities")
        .resolve_adapter_path_for(
            &TypeKey::new("test:i32"),
            &AdaptRequest::new(TypeKey::new("test:string")),
        )
        .expect("adapter path");
    assert_eq!(path.steps[0].as_str(), "test.i32_to_string");
    assert!(
        registry
            .combined_transport_capabilities()
            .expect("combined capabilities")
            .snapshot()
            .nodes
            .is_empty()
    );
    assert!(
        registry
            .capabilities
            .get("hook-called")
            .is_some_and(|entries| !entries.is_empty())
    );
    assert_eq!(
        registry
            .transport_capabilities
            .snapshot()
            .devices
            .iter()
            .filter(|device| device.id == "test.device.i32")
            .count(),
        1
    );
}
