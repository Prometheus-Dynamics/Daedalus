//! A made plan through the daemon as a process. A daemon with the planner makes the plan of a graph
//! with the dylib fixture loaded, and runs a value through it. A daemon with `plan_dir` loads the
//! made plan from its directory, with no graph and no planner needed, and must give the same
//! value. The full build does both in one test, in order. The lean build runs the second half
//! alone, against the plan the full run left, so the full run (`cargo test` without
//! `--no-default-features`) must come first.

#![cfg(unix)]

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use daedalus_client::{Client, Endpoint, Options};
#[cfg(feature = "planner")]
use daedalus_graph::{
    EdgeDoc, EdgePolicyDoc, GraphBody, GraphDoc, NodeDoc, NodeKindDoc, PortRefDoc,
};
use daedalus_wire::service::{StartConfig, TypedValue};
use daedalus_wire::{WireType, WireValue};

/// Where the made plan and the value it gives are kept between the two halves.
fn shared() -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("made-plans");
    std::fs::create_dir_all(&dir).expect("the shared directory");
    dir
}

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
    target.join("debug").join(format!(
        "{}daedalus_dylib_fixture{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ))
}

/// A fresh directory with the fixture library in `plugins`.
fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("made-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("plugins")).unwrap();
    std::fs::copy(
        fixture(),
        dir.join("plugins")
            .join(format!("fixture{}", std::env::consts::DLL_SUFFIX)),
    )
    .unwrap();
    dir
}

#[cfg(feature = "planner")]
fn graph(node: &str) -> String {
    let mut bridge = NodeDoc::new("host");
    bridge.kind = NodeKindDoc::HostBridge;
    bridge.label = Some("io".to_string());
    for port in ["in", "out"] {
        bridge
            .port_types
            .insert(port.to_string(), "u32".to_string());
    }
    let mut inner = NodeDoc::new(node);
    inner.label = Some("work".to_string());
    let edge = |from: u32, from_port: &str, to: u32, to_port: &str| EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    };
    let body = GraphBody {
        nodes: vec![bridge, inner],
        edges: vec![edge(0, "in", 1, "in"), edge(1, "out", 0, "out")],
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

/// A daemon child, stopped on drop.
struct Daemon(Child);

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// Starts a daemon on `dir`'s socket, with its plugins, and `plan_dir` when given.
fn start(dir: &Path, plan_dir: Option<&Path>) -> (Daemon, Client) {
    let socket = dir.join("d.sock");
    let config = dir.join("daedalusd.toml");
    let mut text = format!(
        "listen = [\"unix:{}\"]\nplugin_dir = \"{}\"\n",
        socket.display(),
        dir.join("plugins").display()
    );
    if let Some(plan_dir) = plan_dir {
        text.push_str(&format!("plan_dir = \"{}\"\n", plan_dir.display()));
    }
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

/// Feeds, steps and takes a few values through a run of `graph`, and says what came out.
fn script(client: &Client, graph: &str) -> Vec<String> {
    let run = client.start(graph, StartConfig::default()).unwrap();
    let mut seen = Vec::new();
    for value in [3u32, 41, 7] {
        client.feed(run, "in", u32_value(value)).unwrap();
        client.step(run, 1).unwrap();
        seen.push(format!("{:?}", client.take(run, "out").unwrap()));
    }
    seen
}

/// Writes `bytes` to `path` whole: a reader sees the old file or the new one, never a part.
#[cfg(feature = "planner")]
fn write_whole(path: &Path, bytes: &[u8]) {
    let part = path.with_extension("part");
    std::fs::write(&part, bytes).unwrap();
    std::fs::rename(&part, path).unwrap();
}

/// One test, in order: the first daemon makes the plan and runs the graph, then a daemon with
/// `plan_dir` runs the stored plan and must give the same values. The plan and the values are
/// left in `shared()` for the lean build's test.
#[cfg(feature = "planner")]
#[test]
fn a_made_plan_from_a_plan_dir_runs_the_same_as_its_graph() {
    let made = scratch("make");
    let (daemon, client) = start(&made, None);
    client.put_graph("g", &graph("fixture.pass")).unwrap();
    let (_hash, bytes) = client.make_plan("g", StartConfig::default()).unwrap();
    let from_graph = script(&client, "g");
    assert_eq!(from_graph.len(), 3);
    assert!(
        from_graph.iter().all(|line| line.contains("Some")),
        "{from_graph:?}"
    );
    drop(client);
    drop(daemon);

    let dir = scratch("load");
    let plans = scratch("load-plans");
    std::fs::write(plans.join("g.plan"), &bytes).unwrap();
    let (_daemon, client) = start(&dir, Some(&plans));
    assert_eq!(script(&client, "g"), from_graph);

    let out = shared();
    write_whole(&out.join("g.plan"), &bytes);
    write_whole(&out.join("expected.txt"), from_graph.join("\n").as_bytes());
}

/// The lean build has no planner: the stored plan runs, and planning ops are refused.
#[cfg(not(feature = "planner"))]
#[test]
fn a_daemon_without_the_planner_runs_a_stored_plan_and_refuses_to_plan() {
    let plan = shared().join("g.plan");
    assert!(
        plan.exists(),
        "run the full test first: it writes {}",
        plan.display()
    );
    let dir = scratch("lean");
    let plans = scratch("lean-plans");
    std::fs::copy(&plan, plans.join("g.plan")).unwrap();
    let (_daemon, client) = start(&dir, Some(&plans));
    let expected = std::fs::read_to_string(shared().join("expected.txt")).unwrap();
    assert_eq!(script(&client, "g").join("\n"), expected);
    let refused = client.plan("g", Default::default()).unwrap_err();
    assert!(
        format!("{refused:?}").contains("Unsupported"),
        "{refused:?}"
    );
}
