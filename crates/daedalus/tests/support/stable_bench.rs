//! The cases of the stable-path benchmark (`dylib_stable_bench`, `dylib_stable_bench_allocations`):
//! graphs over `daedalus-plugins-stable-bench`, built against the static install or the stable
//! path, and the step one tick of each runs. Every case returns an `i64` output, so a test can
//! check that the two installs agree before it times them.

use std::borrow::Cow;

use daedalus::PluginLibrary;
use daedalus::data::model::{TypeExpr, Value, ValueType};
use daedalus::engine::{Engine, EngineConfig, HostGraph};
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus_plugins_stable_bench::StableBenchPlugin;

use crate::frames::{COUNTED_KEY, Counted, FramesHost};

/// A compiled host graph.
pub type Host = HostGraph<HandlerRegistry>;

/// One tick of a case: pushes its inputs, ticks, and returns its `out` output.
pub type Step = Box<dyn FnMut(&mut Host) -> i64>;

/// Constant values for the config nodes, typed as the fixture declares them: scalars cycle
/// `i64`, `f64`, `bool`; with `mixed`, every fourth constant (index 3, 7, ...) is a string.
pub fn const_values(mixed: bool, n: usize) -> Vec<Value> {
    (0..n)
        .map(|i| {
            if mixed && i % 4 == 3 {
                Value::String(Cow::Borrowed("enum_variant_name"))
            } else {
                match i % 3 {
                    0 => Value::Int(i as i64 * 7 + 3),
                    1 => Value::Float(i as f64 * 0.5 + 0.25),
                    _ => Value::Bool(i % 2 == 0),
                }
            }
        })
        .collect()
}

/// The static install of the fixture and the host's frame provider.
pub fn static_registry() -> PluginRegistry {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&FramesHost::new()).unwrap();
    registry.install_plugin(&StableBenchPlugin::new()).unwrap();
    registry
}

/// The fixture installed through its `cdylib` on the stable handler path.
pub fn stable_registry(library: &PluginLibrary) -> PluginRegistry {
    use daedalus::dylib::InstallPath;
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&FramesHost::new()).unwrap();
    assert_eq!(
        library.install_into(&mut registry).unwrap(),
        InstallPath::Stable
    );
    registry
}

/// The wiring of one case: nodes by alias, host inputs, edges, and constants by alias and port.
#[derive(Default)]
struct Spec {
    nodes: Vec<(String, &'static str)>,
    inputs: Vec<(&'static str, TypeExpr)>,
    edges: Vec<(String, String)>,
    consts: Vec<(String, String, Value)>,
}

impl Spec {
    fn node(mut self, alias: impl Into<String>, id: &'static str) -> Self {
        self.nodes.push((alias.into(), id));
        self
    }
    fn input(mut self, name: &'static str, ty: TypeExpr) -> Self {
        self.inputs.push((name, ty));
        self
    }
    fn edge(mut self, from: impl Into<String>, to: impl Into<String>) -> Self {
        self.edges.push((from.into(), to.into()));
        self
    }
    fn constant(mut self, alias: impl Into<String>, port: impl Into<String>, value: Value) -> Self {
        self.consts.push((alias.into(), port.into(), value));
        self
    }

    fn compile(self, registry: &PluginRegistry) -> Host {
        let mut builder = registry.graph_builder().unwrap();
        for (name, ty) in self.inputs {
            builder = builder.input_as(name, ty);
        }
        for (alias, id) in &self.nodes {
            builder = builder.node_id(&format!("stable_bench:{id}"), alias.as_str());
        }
        for (from, to) in &self.edges {
            builder = builder.connect(from.as_str(), to.as_str());
        }
        for (alias, port, value) in self.consts {
            builder = builder.const_input_by_id(alias, port, Some(value));
        }
        Engine::new(EngineConfig::default())
            .unwrap()
            .compile_registry(registry, builder.build())
            .unwrap()
    }
}

fn int() -> TypeExpr {
    TypeExpr::Scalar(ValueType::Int)
}

/// `config_s<n>` / `config_m<n>`: one `tick` host input and `n` constants, fed to `out`.
/// `config_s0` (no constants) is the baseline for both variants.
pub fn config_case(registry: &PluginRegistry, mixed: bool, n: usize) -> (Host, Step) {
    let id: &'static str = match (mixed && n > 0, n) {
        (_, 0) => "config_s0",
        (false, 8) => "config_s8",
        (false, 16) => "config_s16",
        (false, 49) => "config_s49",
        (true, 8) => "config_m8",
        (true, 16) => "config_m16",
        (true, 49) => "config_m49",
        _ => panic!("no config node with {n} constants"),
    };
    let mut spec = Spec::default()
        .node("cfg", id)
        .input("tick", int())
        .edge("tick", "cfg.tick")
        .edge("cfg.out", "out");
    for (i, value) in const_values(mixed && n > 0, n).into_iter().enumerate() {
        spec = spec.constant("cfg", format!("c{i}"), value);
    }
    let mut tick = 0_i64;
    let step: Step = Box::new(move |host| {
        tick += 1;
        host.push("tick", tick);
        host.tick().unwrap();
        host.take::<i64>("out").unwrap_or(-1)
    });
    (spec.compile(registry), step)
}

/// `Detections` through `Value` (`handle = false`) or as a plugin-local handle, from a source to a
/// sink; `tags` tags per frame.
pub fn detections_case(registry: &PluginRegistry, handle: bool, tags: i64) -> (Host, Step) {
    let (source, sink) = match handle {
        true => ("handle_source", "handle_sink"),
        false => ("value_source", "value_sink"),
    };
    let host = Spec::default()
        .node("src", source)
        .node("sink", sink)
        .input("tags", int())
        .edge("tags", "src.tags")
        .edge("src.det", "sink.det")
        .edge("sink.out", "out")
        .compile(registry);
    let step: Step = Box::new(move |host| {
        host.push("tags", tags);
        host.tick().unwrap();
        host.take::<i64>("out").unwrap_or(-1)
    });
    (host, step)
}

/// The `Detections` tags as a `Vec` of `StablePod` structs, source to sink.
#[allow(dead_code)] // Not every bench binary runs it.
pub fn pod_case(registry: &PluginRegistry, tags: i64) -> (Host, Step) {
    let host = Spec::default()
        .node("src", "pod_source")
        .node("sink", "pod_sink")
        .input("tags", int())
        .edge("tags", "src.tags")
        .edge("src.det", "sink.det")
        .edge("sink.out", "out")
        .compile(registry);
    let step: Step = Box::new(move |host| {
        host.push("tags", tags);
        host.tick().unwrap();
        host.take::<i64>("out").unwrap_or(-1)
    });
    (host, step)
}

/// The staged shape: a source, twelve `stage` nodes (one handle in, one out, the frame, and
/// ten constants or none), and a sink. `tags` is 4. The frame is `Counted`, read in place.
pub fn staged_case(registry: &PluginRegistry, constants: bool) -> (Host, Step) {
    const STAGES: usize = 12;
    let id = if constants { "stage10" } else { "stage0" };
    let mut spec = Spec::default()
        .node("src", "handle_source")
        .node("sink", "handle_sink")
        .input("tags", int())
        .input("frame", TypeExpr::opaque(COUNTED_KEY))
        .edge("tags", "src.tags")
        .edge("src.det", "stage_0.det");
    for i in 0..STAGES {
        spec = spec
            .node(format!("stage_{i}"), id)
            .edge("frame", format!("stage_{i}.frame"));
        if i + 1 < STAGES {
            spec = spec.edge(format!("stage_{i}.det"), format!("stage_{}.det", i + 1));
        }
        if constants {
            for (c, value) in const_values(true, 10).into_iter().enumerate() {
                spec = spec.constant(format!("stage_{i}"), format!("c{c}"), value);
            }
        }
    }
    let spec = spec
        .edge(format!("stage_{}.det", STAGES - 1), "sink.det")
        .edge("sink.out", "out");
    let host = spec.compile(registry);
    let (frame, _) = Counted::nv12(64, 32, 1);
    let step: Step = Box::new(move |host| {
        host.push("tags", 4_i64);
        host.push_payload("frame", frame.payload());
        host.tick().unwrap();
        host.take::<i64>("out").unwrap_or(-1)
    });
    (host, step)
}
