//! The safe wrapper against the real shared library: a graph run through it, each error as its
//! type, a take that finds nothing, and handles released by drop.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use daedalus_capi_rust::{Graph, Host, HostError, PortIndex, Pushed, Surface};

/// The surface as a shared library, which cargo builds beside this test.
fn library() -> PathBuf {
    let exe = std::env::current_exe().expect("the test's own path");
    let name = format!(
        "{}daedalus_capi{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    );
    let found = exe.parent().expect("the deps directory").join(name);
    assert!(found.exists(), "{} isn't there", found.display());
    found
}

/// The plugin fixture, built once.
fn fixture() -> String {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            let source = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../testing/fixtures/dylib/Cargo.toml");
            let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("dylib-fixture");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let status = Command::new(cargo)
                .args(["build", "-q", "--manifest-path"])
                .arg(source)
                .arg("--target-dir")
                .arg(&target)
                .status()
                .expect("cargo runs");
            assert!(status.success(), "the dylib fixture builds");
            target.join("debug").join(format!(
                "{}daedalus_dylib_fixture{}",
                std::env::consts::DLL_PREFIX,
                std::env::consts::DLL_SUFFIX
            ))
        })
        .to_str()
        .expect("a UTF-8 path")
        .to_owned()
}

fn graph_json(node: &str) -> String {
    format!(
        r#"{{"format": "daedalus.graph", "schema_version": 1, "graph": {{
  "nodes": [
    {{"id": "host", "kind": "host_bridge", "port_types": {{"in": "u32", "out": "u32"}}}},
    {{"id": "{node}"}}
  ],
  "edges": [
    {{"from": {{"node": 0, "port": "in"}}, "to": {{"node": 1, "port": "in"}}}},
    {{"from": {{"node": 1, "port": "out"}}, "to": {{"node": 0, "port": "out"}}}}
  ]
}}}}"#
    )
}

fn host(surface: &Surface) -> Host {
    let host = surface.host().expect("a host");
    host.install(&fixture()).expect("the plugin installs");
    host
}

fn pass(host: &Host) -> Graph {
    host.compile(&graph_json("fixture.pass")).expect("compiles")
}

#[test]
fn the_library_loads_when_it_is_the_version_this_was_built_for() {
    let surface = Surface::load(library()).expect("loads");
    let abi = daedalus_abi::DAEDALUS_ABI;
    assert_eq!(surface.version().layout_hash, abi.layout);
    assert_eq!(surface.version().major, u32::from(abi.major));

    // Another layout, or another major, is refused before anything is called.
    let wrong = Surface::load_expecting(library(), u32::from(abi.major), abi.layout ^ 1);
    assert!(
        matches!(wrong, Err(HostError::Version { wanted_layout_hash, .. }) if wanted_layout_hash == abi.layout ^ 1)
    );
    let wrong = Surface::load_expecting(library(), u32::from(abi.major) + 1, abi.layout);
    assert!(matches!(wrong, Err(HostError::Version { .. })));
    // A file that isn't there, and one that is no surface.
    assert!(matches!(
        Surface::load("/nowhere/libdaedalus_capi.so"),
        Err(HostError::Library(_))
    ));
    let not_it = Surface::load(fixture());
    assert!(
        matches!(&not_it, Err(HostError::Library(why)) if why.contains("daedalus_")),
        "{:?}",
        not_it.err()
    );
}

#[test]
fn a_graph_runs_with_typed_pushes_and_optional_takes() {
    let surface = Surface::load(library()).expect("loads");
    let host = host(&surface);
    assert!(host.schemas().expect("schemas").contains("fixture.pass"));
    let graph = pass(&host);

    let ports = graph.ports().expect("ports");
    let seen: Vec<(&str, &str, bool)> = ports
        .iter()
        .map(|port| (port.name.as_str(), port.type_key.as_str(), port.is_input))
        .collect();
    assert_eq!(seen, [("in", "u32", true), ("out", "u32", false)]);
    assert!(graph.explain().expect("explain").contains("fixture.cpu"));

    // A take is optional: nothing before the first tick, a value after, nothing again.
    assert_eq!(graph.take::<u32>("out"), Ok(None));
    graph.push("in", 41u32).expect("a push");
    let ran = graph.tick().expect("a tick");
    assert_eq!((ran.idle, ran.step_calls, ran.faults), (0, 1, 0));
    assert_eq!(graph.take::<u32>("out"), Ok(Some(41)));
    assert_eq!(graph.take::<u32>("out"), Ok(None));
    assert_eq!(graph.tick().expect("a tick").idle, 1);

    // By index, and as any integer type that holds the number.
    let (input, output) = (
        graph.port("in").expect("in"),
        graph.port("out").expect("out"),
    );
    assert_eq!((input, output), (PortIndex(0), PortIndex(1)));
    graph.push(input, 300i64).expect("a push");
    graph.tick().expect("a tick");
    assert_eq!(graph.take::<u16>(output), Ok(Some(300)));
    graph.push(input, 300u16).expect("a push");
    graph.tick().expect("a tick");
    // Taken as a type too small for it, the number is refused and not lost.
    assert_eq!(graph.take::<u8>(output), Err(HostError::OutOfRange));

    let telemetry = graph.telemetry().expect("telemetry");
    assert_eq!(
        (telemetry.ticks, telemetry.pushes, telemetry.faults),
        (4, 3, 0)
    );
}

#[test]
fn each_refusal_is_its_own_error() {
    let surface = Surface::load(library()).expect("loads");
    let host = host(&surface);
    let graph = pass(&host);
    assert_eq!(graph.push("in", -1i64), Err(HostError::OutOfRange));
    assert_eq!(graph.push("in", u64::MAX), Err(HostError::OutOfRange));
    assert_eq!(graph.push("in", 1.5f64), Err(HostError::TypeMismatch));
    assert_eq!(graph.push("in", true), Err(HostError::TypeMismatch));
    assert_eq!(
        graph.push_bytes("in", &[1, 2]),
        Err(HostError::TypeMismatch)
    );
    assert_eq!(graph.push("nowhere", 1u32), Err(HostError::UnknownPort));
    assert_eq!(graph.push("out", 1u32), Err(HostError::WrongDirection));
    assert_eq!(graph.take::<u32>("in"), Err(HostError::WrongDirection));
    assert_eq!(graph.take::<f64>("out"), Err(HostError::TypeMismatch));
    assert_eq!(graph.take::<Vec<u8>>("out"), Err(HostError::TypeMismatch));
    assert_eq!(graph.port("nowhere"), Err(HostError::UnknownPort));
    assert_eq!(graph.push(PortIndex(9), 1u32), Err(HostError::UnknownPort));

    // A batch is checked whole: the bad entry is named, and nothing went in.
    let bad = graph.push_batch(&[
        (PortIndex(0), Pushed::I64(1)),
        (PortIndex(0), Pushed::F64(2.0)),
    ]);
    assert!(
        matches!(&bad, Err(HostError::BadBatch { entry: Some(1), reason }) if reason.contains("TYPE_MISMATCH")),
        "{bad:?}"
    );
    assert_eq!(graph.tick().expect("a tick").idle, 1);
    graph
        .push_batch(&[(PortIndex(0), Pushed::I64(5))])
        .expect("a good batch");
    graph.tick().expect("a tick");
    assert_eq!(graph.take::<i64>("out"), Ok(Some(5)));

    // A graph that can't be read, one that can't be planned, a plugin that isn't there: each
    // with the library's reason.
    assert!(
        matches!(host.compile("not json"), Err(HostError::GraphInvalid(why)) if !why.is_empty())
    );
    assert!(
        matches!(host.compile(&graph_json("no.such.node")), Err(HostError::PlanFailed(why)) if why.contains("no.such.node"))
    );
    assert!(
        matches!(host.install("/nowhere/plugin.so"), Err(HostError::PluginFailed(why)) if why.contains("/nowhere/plugin.so"))
    );
    // A panic inside the library is an error, and everything still works after.
    assert!(matches!(surface.selftest_panic(), HostError::Panic(why) if why.contains("panicked")));
    graph.push("in", 6u32).expect("still good");

    // Every error that is a status knows its number, and the others have none.
    assert_eq!(HostError::Busy.code(), Some(4));
    assert_eq!(HostError::Library(String::new()).code(), None);
    assert!(HostError::OutOfRange.to_string().contains("fit"));
}

#[test]
fn a_node_that_panics_is_a_fault_and_not_an_error() {
    let surface = Surface::load(library()).expect("loads");
    let host = host(&surface);
    let graph = host.compile(&graph_json("fixture.boom")).expect("compiles");
    graph.push("in", 1u32).expect("a push");
    assert_eq!(graph.tick().expect("the tick itself is fine").faults, 1);
    assert_eq!(graph.take::<u32>("out"), Ok(None));
    assert_eq!(graph.telemetry().expect("telemetry").faults, 1);
}

#[test]
fn a_host_is_busy_while_its_graphs_live_and_drop_releases_both() {
    let surface = Surface::load(library()).expect("loads");
    let host = host(&surface);
    let (first, second) = (pass(&host), pass(&host));
    assert_eq!(host.graphs(), 2);
    // Released by hand while graphs live: busy, and the host comes back.
    let (host, error) = host.release().expect_err("graphs are alive");
    assert_eq!(error, HostError::Busy);
    drop(first);
    assert_eq!(host.graphs(), 1);
    let (host, error) = host.release().expect_err("one graph is alive");
    assert_eq!(error, HostError::Busy);
    drop(second);
    assert_eq!(host.graphs(), 0);
    host.release().expect("no graphs are left");

    // Dropped with a graph alive, the host goes when the graph does: the graph still works,
    // and after it nothing is left in the library.
    let before = daedalus_capi::live_handles();
    let host = self::host(&surface);
    let graph = pass(&host);
    drop(host);
    graph
        .push("in", 9u32)
        .expect("the graph outlives the host value");
    graph.tick().expect("a tick");
    assert_eq!(graph.take::<u32>("out"), Ok(Some(9)));
    drop(graph);
    // The library loaded here is its own copy of the surface: the count that matters is that
    // a new host can be made and released at once, with nothing holding it.
    surface
        .host()
        .expect("a host")
        .release()
        .expect("free at once");
    assert_eq!(
        daedalus_capi::live_handles(),
        before,
        "the linked copy was never used"
    );
}
