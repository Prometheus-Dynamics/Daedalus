//! A call that fails after its config is decoded (a required input that no edge or const
//! supplies, so the argument fetch fails) keeps the node's state, decoded config included: the
//! next call does not decode the config again.

use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus::{
    data::typing::TypeRegistry,
    engine::{Engine, EngineConfig, HostGraph},
    macros::{node, plugin},
    registry::capability::PortDecl,
    runtime::{
        NodeError,
        config::{ConfigError, NodeConfig, Sanitized},
        handler_registry::HandlerRegistry,
        io::NodeIo,
        plugins::PluginRegistry,
    },
};

/// Config decodes (one per `from_io`).
static DECODES: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone, Debug)]
struct ErrCfg {
    factor: i64,
}

impl NodeConfig for ErrCfg {
    fn ports(_: &TypeRegistry) -> Vec<PortDecl> {
        Vec::new()
    }
    fn port_names() -> &'static [&'static str] {
        &["factor"]
    }
    fn from_io(io: &NodeIo) -> Result<Self, NodeError> {
        DECODES.fetch_add(1, Ordering::SeqCst);
        Ok(Self {
            factor: io.get_typed::<i64>("factor").unwrap_or(10),
        })
    }
    fn sanitize(self) -> Result<Sanitized<Self>, ConfigError> {
        Ok(Sanitized {
            value: self,
            changes: Vec::new(),
        })
    }
    fn validate(&self) -> Result<(), ConfigError> {
        Ok(())
    }
}

/// `extra` is required and never supplied, so every call fails in its argument fetch, after the
/// config has been decoded.
#[node(id = "test.state.scale", inputs("value", "extra", config = ErrCfg), outputs("out"))]
fn scale(value: i64, extra: i64, cfg: ErrCfg) -> Result<i64, NodeError> {
    Ok(value * extra * cfg.factor)
}

#[plugin(id = "test.state.error_path", nodes(scale))]
struct ErrorPathPlugin;

fn host() -> HostGraph<HandlerRegistry> {
    let mut registry = PluginRegistry::new();
    let plugin = ErrorPathPlugin::new();
    registry.install(&plugin).expect("install plugin");
    let scale = plugin.scale.alias("scale");
    let graph = registry
        .graph_builder()
        .expect("graph builder")
        .try_node(&scale)
        .expect("scale node")
        .try_connect("in", &scale.inputs.value)
        .expect("input edge")
        .try_connect(&scale.outputs.out, "out")
        .expect("output edge")
        .build();
    Engine::new(EngineConfig::default())
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile graph")
}

#[test]
fn a_failed_call_keeps_its_decoded_config() {
    let mut host = host();
    let before = DECODES.load(Ordering::SeqCst);
    for n in 0..5i64 {
        host.push("in", n);
        // Each call fails in its argument fetch; the outcome itself is not the point.
        let _ = host.tick();
    }
    assert_eq!(
        DECODES.load(Ordering::SeqCst) - before,
        1,
        "the state survives the failed calls, so the config is decoded once"
    );
}
