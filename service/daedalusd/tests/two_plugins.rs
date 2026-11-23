//! Two plugins in one daemon: the dylib fixture and the fake device's plugin, loaded from the
//! plugin directory. A client that links neither builds graphs over both and runs them: a number
//! through a node of each, a megabyte one hands over, and one plugin's plain-data type reaching
//! the other's node through its codec.

#![cfg(all(unix, feature = "planner"))]

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use daedalus_client::{Client, Endpoint, Options};
use daedalus_graph::{
    EdgeDoc, EdgePolicyDoc, GraphBody, GraphDoc, NodeDoc, NodeKindDoc, PortRefDoc,
};
use daedalus_wire::service::{InspectTarget, Inspection, StartConfig, TypedValue};
use daedalus_wire::{WireType, WireValue};

/// The dylib fixture, built into the directory the other tests share.
fn fixture() -> PathBuf {
    let source =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../testing/fixtures/dylib/Cargo.toml");
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
    library(&target.join("debug"), "daedalus_dylib_fixture")
}

fn library(dir: &Path, name: &str) -> PathBuf {
    dir.join(format!(
        "{}{name}{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ))
}

/// The fake device's plugin. This test depends on its crate, so cargo builds its library
/// beside the test's own binary.
fn device() -> PathBuf {
    let exe = std::env::current_exe().expect("the test's own path");
    let path = library(
        exe.parent().expect("a directory"),
        "daedalus_fake_npu_plugin",
    );
    assert!(path.exists(), "{} was not built", path.display());
    path
}

/// A daemon child, stopped on drop.
struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts a daemon whose plugin directory holds both libraries.
fn start(name: &str) -> (Daemon, Client) {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("two-plugins-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    let plugins = dir.join("plugins");
    std::fs::create_dir_all(&plugins).unwrap();
    let suffix = std::env::consts::DLL_SUFFIX;
    std::fs::copy(fixture(), plugins.join(format!("fixture{suffix}"))).unwrap();
    std::fs::copy(device(), plugins.join(format!("device{suffix}"))).unwrap();
    let socket = dir.join("d.sock");
    let config = dir.join("daedalusd.toml");
    let text = format!(
        "listen = [\"unix:{}\"]\nplugin_dir = \"{}\"\n",
        socket.display(),
        plugins.display()
    );
    std::fs::write(&config, text).unwrap();
    let child = Command::new(env!("CARGO_BIN_EXE_daedalusd"))
        .arg("--config")
        .arg(&config)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(File::create(dir.join("daedalusd.log")).unwrap())
        .spawn()
        .expect("daedalusd starts");
    let mut daemon = Daemon(child);
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        if let Ok(Some(status)) = daemon.0.try_wait() {
            panic!("daedalusd exited early: {status:?}");
        }
        if let Ok(client) = Client::connect(Endpoint::unix(&socket), Options::default()) {
            return (daemon, client);
        }
        assert!(Instant::now() < deadline, "the daemon never listened");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// `host.in -> each of nodes in turn -> host.out`, with the host's `out` of type `out`. Each
/// node is given as its id and the names of its input and output.
fn chain(nodes: &[(&str, &str, &str)], out: &str) -> String {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    bridge
        .port_types
        .insert("in".to_string(), "u32".to_string());
    bridge.port_types.insert("out".to_string(), out.to_string());
    let edge = |from: usize, from_port: &str, to: usize, to_port: &str| EdgeDoc {
        from: PortRefDoc {
            node: from as u32,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to as u32,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    };
    let mut docs = vec![bridge];
    let mut edges = Vec::new();
    let mut last = (0, "in");
    for (at, (id, input, output)) in nodes.iter().enumerate() {
        docs.push(NodeDoc::new(*id));
        edges.push(edge(last.0, last.1, at + 1, input));
        last = (at + 1, output);
    }
    edges.push(edge(last.0, last.1, 0, "out"));
    let body = GraphBody {
        nodes: docs,
        edges,
        groups: Vec::new(),
    };
    GraphDoc::new(body).to_json().unwrap()
}

fn u32_value(value: u32) -> TypedValue {
    TypedValue::new(
        WireType::Scalar(daedalus_types::Scalar::U32),
        WireValue::U32(value),
    )
}

/// One value in, one tick, one value out.
fn through(client: &Client, graph: &str, value: u32) -> Option<TypedValue> {
    let run = client.start(graph, StartConfig::default()).unwrap();
    client.feed(run, "in", u32_value(value)).unwrap();
    client.step(run, 1).unwrap();
    client.take(run, "out").unwrap()
}

#[test]
fn a_daemon_runs_graphs_over_two_plugins_with_numbers_bytes_and_a_codec() {
    let (_daemon, client) = start("run");
    let mut plugins = match client.inspect(InspectTarget::Host).unwrap() {
        Inspection::Host(info) => info.plugins,
        other => panic!("expected the host, got {other:?}"),
    };
    plugins.sort();
    assert_eq!(plugins, ["dylib-fixture", "fake-npu-plugin"]);

    // A number through a node of each plugin.
    let both = chain(
        &[("fixture.pass", "in", "out"), ("npu.scale", "x", "y")],
        "u32",
    );
    assert!(client.put_graph("both", &both).unwrap().is_empty());
    assert_eq!(through(&client, "both", 21), Some(u32_value(42)));

    // A megabyte one plugin hands over, taken by the client.
    let bytes = chain(&[("fixture.handover", "in", "out")], "bytes");
    assert!(client.put_graph("bytes", &bytes).unwrap().is_empty());
    let handed = through(&client, "bytes", 1).expect("the megabyte");
    match handed.value {
        WireValue::Bytes(bytes) => assert_eq!((bytes.len(), bytes[0]), (1 << 20, 7)),
        other => panic!("expected bytes, got {other:?}"),
    }

    // One plugin's plain-data type into the other plugin's node: the daemon places the
    // codec's conversions and runs them. The number is one less than its value on the way
    // in and the value again on the way out.
    let codec = chain(&[("npu.number", "x", "y")], "u32");
    let said = client.put_graph("codec", &codec).unwrap();
    assert!(said.is_empty(), "{said:?}");
    assert_eq!(through(&client, "codec", 9), Some(u32_value(9)));
}
