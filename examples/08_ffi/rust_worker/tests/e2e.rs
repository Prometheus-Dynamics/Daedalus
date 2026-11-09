//! End to end: the host installs the example package, starts the real `rust_worker` binary as a
//! persistent worker, and runs a graph of invokes through it.

use std::collections::BTreeMap;

use daedalus::registry::capability::CapabilityRegistry;
use daedalus_ffi_core::{InvokeRequest, WORKER_PROTOCOL_VERSION, WireValue};
use daedalus_ffi_host::{FfiHost, PersistentWorkerRunnerFactory};
use daedalus_plugins_rust_worker::{ACCUMULATE, PLUGIN_ID, package};

fn invoke(
    host: &FfiHost,
    node: &str,
    args: &[(&str, WireValue)],
    state: Option<WireValue>,
) -> Result<daedalus_ffi_core::InvokeResponse, String> {
    host.invoke(
        &format!("{PLUGIN_ID}:{node}"),
        InvokeRequest {
            protocol_version: WORKER_PROTOCOL_VERSION,
            node_id: String::new(),
            correlation_id: Some(format!("{node}-call")),
            args: args
                .iter()
                .map(|(name, value)| ((*name).to_string(), value.clone()))
                .collect(),
            state,
            context: BTreeMap::new(),
        },
    )
    .map_err(|err| err.to_string())
}

fn int(response: &daedalus_ffi_core::InvokeResponse, port: &str) -> i64 {
    match response.outputs.get(port) {
        Some(WireValue::Int(value)) => *value,
        other => panic!("output `{port}` is {other:?}"),
    }
}

#[test]
fn a_graph_runs_through_the_real_worker() {
    let package = package(env!("CARGO_BIN_EXE_rust_worker")).expect("package");
    let mut registry = CapabilityRegistry::new();
    let host = FfiHost::install_package(
        &mut registry,
        &package,
        &PersistentWorkerRunnerFactory::new(),
    )
    .expect("the host installs the package and starts the worker");

    // add(2, 40) -> scale(42, factor 2) -> accumulate (state 0 + 84) -> accumulate (84 + 84).
    let added = invoke(
        &host,
        "add",
        &[("a", WireValue::Int(2)), ("b", WireValue::Int(40))],
        None,
    )
    .expect("add");
    assert_eq!(int(&added, "out"), 42);

    let scaled = invoke(
        &host,
        "scale",
        &[
            ("value", WireValue::Int(int(&added, "out"))),
            // A `NodeConfig` is flattened into one port per field.
            ("factor", WireValue::Int(2)),
        ],
        None,
    )
    .expect("scale");
    assert_eq!(int(&scaled, "out"), 84);

    let first = invoke(
        &host,
        "accumulate",
        &[("value", WireValue::Int(int(&scaled, "out")))],
        None,
    )
    .expect("accumulate");
    assert_eq!(int(&first, "sum"), 84);
    let second =
        invoke(&host, "accumulate", &[("value", WireValue::Int(84))], None).expect("accumulate");
    assert_eq!(
        int(&second, "sum"),
        168,
        "the worker keeps the node's state between invokes"
    );

    // The host's state sync: a state the host sends is imported before the call, and the new state
    // comes back in the response.
    let imported = invoke(
        &host,
        "accumulate",
        &[("value", WireValue::Int(1))],
        Some(WireValue::Record(BTreeMap::from([(
            "sum".into(),
            WireValue::Int(1000),
        )]))),
    )
    .expect("accumulate with imported state");
    assert_eq!(int(&imported, "sum"), 1001);
    assert_eq!(
        imported.state,
        Some(WireValue::Record(BTreeMap::from([(
            "sum".into(),
            WireValue::Int(1001)
        )])))
    );
    assert!(host.contains_node(ACCUMULATE));

    // A node that emits an event: the event comes back with the output.
    let described =
        invoke(&host, "describe", &[("value", WireValue::Int(7))], None).expect("describe");
    assert_eq!(
        described.outputs.get("text"),
        Some(&WireValue::String("value is 7".into()))
    );
    assert_eq!(described.events.len(), 1);
    assert_eq!(described.events[0].message, "describing 7");

    // A node that panics is an error for that call; the worker keeps serving.
    let panicked = invoke(&host, "boom", &[("value", WireValue::Int(-1))], None);
    let message = panicked.expect_err("a panicking node fails the call");
    assert!(message.contains("negative input"), "{message}");
    let after = invoke(
        &host,
        "add",
        &[("a", WireValue::Int(1)), ("b", WireValue::Int(1))],
        None,
    )
    .expect("the worker serves after a panic");
    assert_eq!(int(&after, "out"), 2);

    host.shutdown().expect("clean shutdown");
}
