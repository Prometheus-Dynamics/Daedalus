//! `NodeRecovery::Retry` re-runs a `#[node]` whose input is taken by value: the executor keeps a
//! copy of the input for later attempts, so the generated handler clones a shared `Clone` input
//! instead of failing to move it.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use daedalus::{
    engine::{Engine, EngineConfig},
    macros::{node, plugin},
    runtime::{NodeError, NodeRecovery, plugins::PluginRegistry},
};

static CALLS: AtomicUsize = AtomicUsize::new(0);

/// Fails its first call, then echoes `text` (taken by value) with a suffix.
#[node(id = "test.retry_by_value.flaky", inputs("text"), outputs("out"))]
fn flaky(text: String) -> Result<String, NodeError> {
    if CALLS.fetch_add(1, Ordering::SeqCst) == 0 {
        return Err(NodeError::Handler("first attempt fails".into()));
    }
    Ok(format!("{text}!"))
}

#[plugin(id = "test.retry_by_value", nodes(flaky))]
struct RetryPlugin;

#[test]
fn retry_reruns_a_by_value_node_on_its_input() {
    let mut registry = PluginRegistry::new();
    let plugin = RetryPlugin::new();
    registry.install(&plugin).expect("install");
    let flaky = plugin.flaky.alias("flaky");
    let graph = registry
        .graph_builder()
        .expect("builder")
        .input_typed::<String>("text")
        .and_then(|b| b.try_node(&flaky))
        .and_then(|b| b.try_connect("text", &flaky.inputs.text))
        .and_then(|b| b.try_connect(&flaky.outputs.out, "out"))
        .expect("wire")
        .build();
    let config = EngineConfig::default().with_recovery(NodeRecovery::Retry {
        max: 2,
        backoff: Duration::ZERO,
    });
    let mut host = Engine::new(config)
        .expect("engine")
        .compile_registry(&registry, graph)
        .expect("compile");

    host.push("text", String::from("hi"));
    host.tick().expect("tick");
    assert_eq!(host.take::<String>("out"), Some(String::from("hi!")));
    assert_eq!(
        CALLS.load(Ordering::SeqCst),
        2,
        "one failed attempt, one retry"
    );
}
