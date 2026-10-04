//! Loads `examples/plugins/stable_abi` as a `cdylib` whose descriptor claims another rustc (a
//! plugin built by another toolchain), installs it through the stable handler path, and checks
//! its nodes produce what the statically installed plugin produces: scalars (`u64` included), a
//! derived struct, bytes, an optional input, node state, a host frame read in place through
//! `daedalus:frame`, conditional outputs and a `fire = "all"` join. A panic inside the plugin
//! becomes a node error the host survives.

#[path = "support/cdylib.rs"]
mod cdylib;

use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use daedalus::data::model::{TypeExpr, Value, ValueType};
use daedalus::data::to_value::ToValue;
use daedalus::dylib::{InstallPath, RustAbiMismatch};
use daedalus::engine::{Engine, EngineConfig, HostGraph};
use daedalus::macros::plugin;
use daedalus::registry::ids::NodeId;
use daedalus::runtime::NODE_FIRE_META_KEY;
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus::transport::{
    FrameInterface, FramePlane, FrameResidency, FrameSource, Payload, Residency, fourcc,
};
use daedalus::{PluginLibrary, type_key};
use daedalus_plugins_stable_abi::{Point, SIMULATED_RUSTC, StableAbiPlugin};

const FRAME_KEY: &str = "test:stable:gray_frame";

/// A host-owned frame the plugin reads through `daedalus:frame`.
#[type_key(FRAME_KEY)]
struct GrayFrame {
    pixels: Vec<u8>,
    width: u32,
}

impl FrameSource for GrayFrame {
    fn width(&self) -> u32 {
        self.width
    }
    fn height(&self) -> u32 {
        self.pixels.len() as u32 / self.width
    }
    fn format(&self) -> u32 {
        fourcc(b"R8  ")
    }
    fn residency(&self) -> FrameResidency {
        FrameResidency::Cpu
    }
    fn plane_count(&self) -> u32 {
        1
    }
    fn plane(&self, index: u32) -> Option<FramePlane<'_>> {
        (index == 0).then(|| FramePlane::mapped(&self.pixels, self.width))
    }
}

#[plugin(
    id = "test.stable.frames",
    types(GrayFrame),
    foreign_providers(GrayFrame => FrameInterface)
)]
struct FramesPlugin;

fn stable_cdylib() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    // Only `dylib`: the plugin is built with its own feature set, not the host's.
    PATH.get_or_init(|| {
        cdylib::build_cdylib(
            "daedalus-plugins-stable-abi",
            "daedalus_plugins_stable_abi",
            "dylib",
        )
    })
}

fn library() -> &'static PluginLibrary {
    static LIBRARY: OnceLock<PluginLibrary> = OnceLock::new();
    // Safety: a trusted test plugin.
    LIBRARY.get_or_init(|| unsafe { PluginLibrary::load(stable_cdylib()) }.unwrap())
}

fn host_registry() -> PluginRegistry {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&FramesPlugin::new()).unwrap();
    registry
}

fn static_registry() -> PluginRegistry {
    let mut registry = host_registry();
    registry.install_plugin(&StableAbiPlugin::new()).unwrap();
    registry
}

fn stable_registry() -> PluginRegistry {
    let mut registry = host_registry();
    assert_eq!(
        library().install_into(&mut registry).unwrap(),
        InstallPath::Stable
    );
    registry
}

fn compile(
    registry: &PluginRegistry,
    wire: &[(&str, &str)],
    inputs: &[(&str, TypeExpr)],
) -> HostGraph<HandlerRegistry> {
    let mut builder = registry.graph_builder().unwrap();
    for (name, ty) in inputs {
        builder = builder.input_as(*name, ty.clone());
    }
    let mut aliases: Vec<&str> = Vec::new();
    for (from, to) in wire {
        for port in [from, to] {
            if let Some((alias, _)) = port.split_once('.')
                && !aliases.contains(&alias)
            {
                aliases.push(alias);
                // An alias is its node's name, plus digits for further instances (`every2`).
                let node = alias.trim_end_matches(|c: char| c.is_ascii_digit());
                builder = builder.node_id(&format!("stable_abi:{node}"), alias);
            }
        }
    }
    for (from, to) in wire {
        builder = builder.connect(*from, *to);
    }
    Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(registry, builder.build())
        .unwrap()
}

fn scalar(ty: ValueType) -> TypeExpr {
    TypeExpr::Scalar(ty)
}

/// Every node but `explode`, fed from host inputs.
fn full_graph(registry: &PluginRegistry) -> HostGraph<HandlerRegistry> {
    compile(
        registry,
        &[
            ("value", "scale.value"),
            ("factor", "scale.factor"),
            ("scale.out", "scaled"),
            ("x", "point.x"),
            ("y", "point.y"),
            ("label", "point.label"),
            ("point.point", "point_out"),
            ("point.point", "describe.point"),
            ("describe.text", "text"),
            ("describe.norm", "norm"),
            ("bytes", "checksum.bytes"),
            ("checksum.sum", "sum"),
            ("checksum.reversed", "reversed"),
            ("trigger", "or_default.trigger"),
            ("or_default.out", "defaulted"),
            ("step", "accumulate.step"),
            ("accumulate.total", "total"),
            ("frame", "frame_sum.frame"),
            ("frame_sum.sum", "frame_total"),
            ("frame_sum.ptr", "frame_ptr"),
        ],
        &[
            ("value", scalar(ValueType::I32)),
            ("factor", scalar(ValueType::Float)),
            ("x", scalar(ValueType::Int)),
            ("y", scalar(ValueType::Float)),
            ("label", scalar(ValueType::String)),
            ("bytes", scalar(ValueType::Bytes)),
            ("trigger", scalar(ValueType::Bool)),
            ("step", scalar(ValueType::Int)),
            ("frame", TypeExpr::opaque(FRAME_KEY)),
        ],
    )
}

#[derive(Debug, PartialEq)]
struct Outputs {
    scaled: Option<f64>,
    point: Option<Value>,
    text: Option<String>,
    norm: Option<f64>,
    sum: Option<u64>,
    reversed: Option<Vec<u8>>,
    defaulted: Option<i64>,
    totals: Vec<i64>,
    frame_total: Option<i64>,
    frame_read_in_place: bool,
}

/// Two ticks of the full graph; the frame is checked for being read in place and released.
fn run(registry: &PluginRegistry) -> Outputs {
    let mut host = full_graph(registry);
    let frame = Arc::new(GrayFrame {
        pixels: vec![1, 2, 3, 4, 5, 6],
        width: 3,
    });
    let push_all = |host: &HostGraph<HandlerRegistry>, step: i64| {
        host.push("value", 7_i32);
        host.push("factor", 1.5_f64);
        host.push("x", 3_i64);
        host.push("y", 4.0_f64);
        host.push("label", String::from("p"));
        host.push("bytes", vec![1_u8, 2, 250]);
        host.push("trigger", true);
        host.push("step", step);
        host.push_payload(
            "frame",
            Payload::shared_with(FRAME_KEY, frame.clone(), Residency::Cpu, None, None),
        );
    };
    push_all(&host, 2);
    host.tick().unwrap();
    // A typed `Point` statically, a `Value` through the stable path (the host lacks the type).
    let point = host.take_payload("point_out").map(|payload| {
        payload
            .get_ref::<Point>()
            .map(ToValue::to_value)
            .or_else(|| payload.get_ref::<Value>().cloned())
            .expect("a Point or its Value")
    });
    let outputs = Outputs {
        scaled: host.take("scaled"),
        point,
        text: host.take("text"),
        norm: host.take("norm"),
        sum: host.take("sum"),
        reversed: host.take("reversed"),
        defaulted: host.take("defaulted"),
        totals: Vec::new(),
        frame_total: host.take("frame_total"),
        frame_read_in_place: host.take::<i64>("frame_ptr") == Some(frame.pixels.as_ptr() as i64),
    };
    let first_total = host.take::<i64>("total");
    push_all(&host, 3);
    host.tick().unwrap();
    let totals = [first_total, host.take::<i64>("total")]
        .into_iter()
        .flatten()
        .collect();
    drop(host);
    assert_eq!(
        Arc::strong_count(&frame),
        1,
        "every frame handle was released"
    );
    Outputs { totals, ..outputs }
}

#[test]
fn mismatched_toolchain_plugin_runs_through_the_stable_path_like_the_static_install() {
    let library = library();
    assert!(
        matches!(library.rust_abi(), Err(RustAbiMismatch::Rustc { found, .. }) if found == SIMULATED_RUSTC),
        "{:?}",
        library.rust_abi()
    );
    assert_eq!(library.install_mode(), Some(InstallPath::Stable));

    let expected = run(&static_registry());
    assert_eq!(
        expected,
        Outputs {
            scaled: Some(10.5),
            point: Some(
                Point {
                    x: 3,
                    y: 4.0,
                    label: "p".into()
                }
                .to_value()
            ),
            text: Some("p@(3, 4)".into()),
            norm: Some(5.0),
            sum: Some(253),
            reversed: Some(vec![250, 2, 1]),
            defaulted: Some(-1),
            totals: vec![2, 5],
            frame_total: Some(21),
            frame_read_in_place: true,
        }
    );
    assert_eq!(run(&stable_registry()), expected);
}

/// Join outputs over six ticks (`every2` and `every3` feed a `fire = "all"` join through
/// conditional outputs), and `successor(u64::MAX - 1)`.
fn run_joins(registry: &PluginRegistry) -> (Vec<Option<i64>>, Option<u64>) {
    let mut host = compile(
        registry,
        &[
            ("tick", "every2.tick"),
            ("two", "every2.every"),
            ("tick", "every3.tick"),
            ("three", "every3.every"),
            ("every2.out", "join.a"),
            ("every3.out", "join.b"),
            ("join.out", "out"),
            ("big", "successor.value"),
            ("successor.out", "next"),
        ],
        &[
            ("tick", scalar(ValueType::Int)),
            ("two", scalar(ValueType::Int)),
            ("three", scalar(ValueType::Int)),
            ("big", scalar(ValueType::U64)),
        ],
    );
    let joined = (1..=6_i64)
        .map(|tick| {
            host.push("tick", tick);
            host.push("two", 2_i64);
            host.push("three", 3_i64);
            host.tick().unwrap();
            host.take::<i64>("out")
        })
        .collect();
    host.push("big", u64::MAX - 1);
    host.tick().unwrap();
    (joined, host.take::<u64>("next"))
}

#[test]
fn joins_conditional_outputs_and_u64_behave_like_the_static_install() {
    let (static_registry, stable_registry) = (static_registry(), stable_registry());
    let expected = run_joins(&static_registry);
    assert_eq!(expected.1, Some(u64::MAX));
    assert!(expected.0.contains(&Some(203)), "{:?}", expected.0);
    assert!(expected.0.contains(&None), "{:?}", expected.0);
    assert_eq!(run_joins(&stable_registry), expected);

    // The fire mode and conditional-output metadata cross through the schema.
    for (node, key) in [
        ("stable_abi:join", NODE_FIRE_META_KEY.to_string()),
        ("stable_abi:every", "outputs.out.conditional".to_string()),
    ] {
        let metadata = |registry: &PluginRegistry| {
            let decl = registry
                .transport_capabilities
                .node_decl(&NodeId::new(node))
                .unwrap_or_else(|| panic!("{node} declared"));
            let json = decl
                .metadata_json
                .get(&key)
                .unwrap_or_else(|| panic!("{node} {key}"));
            serde_json::from_str::<Value>(json).unwrap()
        };
        assert_eq!(
            metadata(&stable_registry),
            metadata(&static_registry),
            "{node} {key}"
        );
    }
}

#[test]
fn a_panicking_plugin_node_fails_the_tick_and_the_host_keeps_running() {
    let registry = stable_registry();
    let mut host = compile(
        &registry,
        &[("value", "explode.value"), ("explode.out", "out")],
        &[("value", scalar(ValueType::Int))],
    );
    host.push("value", 13_i64);
    let err = host.tick().unwrap_err().to_string();
    assert!(
        err.contains("plugin `stable_abi` node `stable_abi:explode`")
            && err.contains("unlucky input")
            && err.contains("panicked"),
        "{err}"
    );
    host.push("value", 5_i64);
    host.tick().unwrap();
    assert_eq!(host.take::<i64>("out"), Some(5));
}

#[test]
fn the_schema_lists_the_plugin_and_its_nodes() {
    let schema = library().schema();
    assert_eq!(schema.plugin.name, "stable_abi");
    let ids: Vec<_> = schema.nodes.iter().map(|node| node.id.as_str()).collect();
    assert!(ids.contains(&"stable_abi:frame_sum"), "{ids:?}");
    assert_eq!(library().foreign_interfaces().len(), 1);
}

/// A graph to time: its wiring, host inputs and what it pushes per tick.
struct Case {
    name: &'static str,
    wire: Vec<(&'static str, &'static str)>,
    inputs: Vec<(&'static str, TypeExpr)>,
    push: fn(&HostGraph<HandlerRegistry>),
}

const TIMED_RUNS: u32 = 20_000;

/// Mean time of `once` over [`TIMED_RUNS`] runs, after a warm-up.
fn time_ns(mut once: impl FnMut()) -> f64 {
    (0..1_000).for_each(|_| once());
    let start = Instant::now();
    (0..TIMED_RUNS).for_each(|_| once());
    start.elapsed().as_nanos() as f64 / f64::from(TIMED_RUNS)
}

fn time_case(registry: &PluginRegistry, case: &Case) -> f64 {
    let mut host = compile(registry, &case.wire, &case.inputs);
    time_ns(|| {
        (case.push)(&host);
        host.tick().unwrap();
        host.take_payload("out");
    })
}

/// The `scale` handler alone, outside the executor.
fn time_scale_handler(registry: &PluginRegistry) -> f64 {
    use daedalus::runtime::executor::{CorrelatedPayload, NodeHandler};
    use daedalus::runtime::{ExecutionContext, NodeIo, RuntimeNode, StateStore};
    let node = RuntimeNode::new("stable_abi:scale");
    let ctx = ExecutionContext::detached(StateStore::default(), "scale".into());
    let input = |port: &'static str, payload| (port.into(), CorrelatedPayload::from_edge(payload));
    time_ns(|| {
        let mut io = NodeIo::from_inputs([
            input("value", Payload::owned("i32", 7_i32)),
            input("factor", Payload::owned("f64", 1.5_f64)),
        ]);
        registry.handlers.run(&node, &ctx, &mut io).unwrap();
        assert_eq!(io.outputs().len(), 1);
    })
}

/// Per-call cost of the stable path over a statically installed (equivalently, Rust-ABI) node:
/// `cargo test -p daedalus-rs --features engine,dylib-plugins --release --test dylib_stable --
/// --ignored --nocapture`.
#[test]
#[ignore = "timing; run manually"]
#[allow(clippy::print_stdout)]
fn stable_call_overhead() {
    let cases = [
        Case {
            name: "scale (i32, f64 -> f64)",
            wire: vec![
                ("value", "scale.value"),
                ("factor", "scale.factor"),
                ("scale.out", "out"),
            ],
            inputs: vec![
                ("value", scalar(ValueType::I32)),
                ("factor", scalar(ValueType::Float)),
            ],
            push: |host| {
                host.push("value", 7_i32);
                host.push("factor", 1.5_f64);
            },
        },
        Case {
            name: "checksum (4 KiB Vec<u8>)",
            wire: vec![("bytes", "checksum.bytes"), ("checksum.sum", "out")],
            inputs: vec![("bytes", scalar(ValueType::Bytes))],
            push: |host| {
                host.push("bytes", vec![7_u8; 4096]);
            },
        },
        Case {
            name: "point -> describe (derived struct, two nodes)",
            wire: vec![
                ("x", "point.x"),
                ("y", "point.y"),
                ("label", "point.label"),
                ("point.point", "describe.point"),
                ("describe.norm", "out"),
            ],
            inputs: vec![
                ("x", scalar(ValueType::Int)),
                ("y", scalar(ValueType::Float)),
                ("label", scalar(ValueType::String)),
            ],
            push: |host| {
                host.push("x", 3_i64);
                host.push("y", 4.0_f64);
                host.push("label", String::from("p"));
            },
        },
    ];
    let (static_registry, stable_registry) = (static_registry(), stable_registry());
    let (base, stable) = (
        time_scale_handler(&static_registry),
        time_scale_handler(&stable_registry),
    );
    println!(
        "scale handler call: static {base:.0} ns, stable {stable:.0} ns, +{:.0} ns",
        stable - base
    );
    for case in &cases {
        let base = time_case(&static_registry, case);
        let stable = time_case(&stable_registry, case);
        println!(
            "{}: static {base:.0} ns/tick, stable {stable:.0} ns/tick, +{:.0} ns",
            case.name,
            stable - base
        );
    }
}
