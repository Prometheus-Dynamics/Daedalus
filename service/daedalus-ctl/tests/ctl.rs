//! Every command against a daemon started here with the dylib fixture loaded: exit codes, text
//! output, and the JSON's fixed shape.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use serde_json::Value;

fn ctl() -> Command {
    Command::new(env!("CARGO_BIN_EXE_daedalus-ctl"))
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

/// A directory of this test's own, emptied first.
fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("ctl-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

#[test]
fn a_usage_error_exits_2_and_a_daemon_that_is_not_there_exits_3() {
    let dir = scratch("usage");
    let usage: &[&[&str]] = &[
        &[],
        &["frobnicate"],
        &["--loud", "ping"],
        &["ping", "extra"],
        &["step"],
        &["step", "one"],
        &["feed", "1", "in"],
        &["feed", "1", "in", "u32:many"],
        &["feed", "1", "in", "widget:1"],
        &["feed", "1", "in", "bytes:0g"],
        &["plan", "g", "--form", "wasm"],
        &["start", "g", "--telemetry", "loud"],
        &["start", "g", "--fifo"],
        &["watch", "1", "--kinds", "noise"],
        &["inspect", "everything"],
        &["cache", "drop"],
        &["stop", "1", "--now"],
        &["--socket", "/a", "--tcp", "127.0.0.1:1", "ping"],
        &["--tcp", "192.0.2.1:4100", "ping"],
        &["--tcp", "nowhere", "ping"],
        &["--socket"],
    ];
    for args in usage {
        let output = ctl().args(*args).output().unwrap();
        assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
        assert!(stderr(&output).starts_with("daedalus-ctl: "), "{args:?}");
        assert!(output.stdout.is_empty(), "{args:?}");
    }
    // A usage error in JSON goes to stdout, in the envelope.
    let output = ctl().args(["--json", "step", "one"]).output().unwrap();
    assert_eq!(output.status.code(), Some(2));
    let shown: Value = serde_json::from_str(&stdout(&output)).expect("json");
    assert_eq!(shown["ok"], false);
    assert_eq!(shown["command"], "step");
    assert_eq!(shown["error"]["kind"], "usage");
    assert_eq!(shown["error"]["message"], "RUN must be a number, not `one`");

    let help = ctl().arg("--help").output().unwrap();
    assert_eq!(help.status.code(), Some(0));
    assert!(stdout(&help).starts_with("usage: daedalus-ctl"));
    assert!(stdout(&help).contains("cache inspect | clear"));

    // Nothing listens on a port that was just closed.
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let address = format!("127.0.0.1:{port}");
    let output = ctl().args(["--tcp", &address, "ping"]).output().unwrap();
    assert_eq!(output.status.code(), Some(3), "{output:?}");
    let message = stderr(&output);
    assert!(
        message.starts_with(&format!(
            "daedalus-ctl: could not connect to tcp:{address}: "
        )),
        "{message}"
    );
    let output = ctl()
        .args(["--json", "--tcp", &address, "ping"])
        .output()
        .unwrap();
    assert_eq!(output.status.code(), Some(3));
    let shown: Value = serde_json::from_str(&stdout(&output)).expect("json");
    assert_eq!(shown["error"]["kind"], "connect");

    // A file the command names is read before anything is sent, so this is a usage error too.
    let missing = dir.join("absent.json");
    let output = ctl()
        .args(["--tcp", &address, "put-graph", "g"])
        .arg(&missing)
        .output()
        .unwrap();
    assert_eq!(
        output.status.code(),
        Some(3),
        "the daemon is tried first: {output:?}"
    );
}

#[cfg(unix)]
mod with_daemon {
    use std::collections::BTreeSet;
    use std::fs::File;
    use std::io::{BufRead, BufReader};
    use std::process::{Child, Stdio};
    use std::sync::OnceLock;
    use std::time::{Duration, Instant};

    use daedalus_graph::{
        EdgeDoc, EdgePolicyDoc, GraphBody, GraphDoc, NodeDoc, NodeKindDoc, PortRefDoc,
    };

    use super::*;

    fn cargo_build(args: &[&str], manifest: &Path, target: &Path) {
        let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
        let status = Command::new(cargo)
            .args(["build", "-q", "--manifest-path"])
            .arg(manifest)
            .args(args)
            .arg("--target-dir")
            .arg(target)
            .status()
            .expect("cargo runs");
        assert!(
            status.success(),
            "cargo build {args:?} in {}",
            manifest.display()
        );
    }

    /// The daemon and the dylib fixture, each built once per test binary into a directory of
    /// this package's own. Another package's binary is not given to a test, so cargo builds it.
    fn built() -> &'static (PathBuf, PathBuf) {
        static BUILT: OnceLock<(PathBuf, PathBuf)> = OnceLock::new();
        BUILT.get_or_init(|| {
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
            let tmp = Path::new(env!("CARGO_TARGET_TMPDIR"));
            let daemon_dir = tmp.join("ctl-daemon");
            cargo_build(&["-p", "daedalusd"], &root.join("Cargo.toml"), &daemon_dir);
            let fixture_dir = tmp.join("ctl-fixture");
            cargo_build(
                &[],
                &root.join("testing/fixtures/dylib/Cargo.toml"),
                &fixture_dir,
            );
            let exe = std::env::consts::EXE_SUFFIX;
            (
                daemon_dir.join("debug").join(format!("daedalusd{exe}")),
                fixture_dir.join("debug").join(format!(
                    "{}daedalus_dylib_fixture{}",
                    std::env::consts::DLL_PREFIX,
                    std::env::consts::DLL_SUFFIX
                )),
            )
        })
    }

    /// A host bridge with ports `in` and `out` around the fixture's pass node.
    fn graph() -> String {
        let mut bridge = NodeDoc::new("host");
        bridge.kind = NodeKindDoc::HostBridge;
        bridge.label = Some("io".to_string());
        for port in ["in", "out"] {
            bridge
                .port_types
                .insert(port.to_string(), "u32".to_string());
        }
        let mut inner = NodeDoc::new("fixture.pass");
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

    /// A running daemon, stopped when the test ends.
    struct Daemon {
        child: Child,
        dir: PathBuf,
        json: bool,
    }

    impl Drop for Daemon {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    impl Daemon {
        /// Starts a daemon in `dir` with the fixture in its plugin directory, and waits until
        /// it answers.
        fn start(name: &str) -> Daemon {
            let (daemon, fixture) = built();
            let dir = scratch(name);
            let plugins = dir.join("plugins");
            std::fs::create_dir_all(&plugins).unwrap();
            let suffix = std::env::consts::DLL_SUFFIX;
            std::fs::copy(fixture, plugins.join(format!("fixture{suffix}"))).unwrap();
            std::fs::copy(fixture, dir.join(format!("again{suffix}"))).unwrap();
            std::fs::write(dir.join("g.json"), graph()).unwrap();
            std::fs::write(
                dir.join("patch.json"),
                r#"{"version":1,"ops":[{"type":"replace_node_id","node":{"index":1},"new_id":"fixture.pass"}]}"#,
            )
            .unwrap();
            let config = dir.join("daedalusd.toml");
            std::fs::write(
                &config,
                format!(
                    "listen = [\"unix:{}\"]\nplugin_dir = \"{}\"\n",
                    dir.join("d.sock").display(),
                    plugins.display()
                ),
            )
            .unwrap();
            let child = Command::new(daemon)
                .arg("--config")
                .arg(&config)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(File::create(dir.join("daedalusd.log")).unwrap())
                .spawn()
                .expect("daedalusd starts");
            let mut daemon = Daemon {
                child,
                dir,
                json: false,
            };
            let deadline = Instant::now() + Duration::from_secs(300);
            while daemon.run(&["ping"]).status.code() != Some(0) {
                if let Ok(Some(status)) = daemon.child.try_wait() {
                    panic!("daedalusd exited early: {status:?}");
                }
                assert!(Instant::now() < deadline, "the daemon never answered");
                std::thread::sleep(Duration::from_millis(10));
            }
            daemon
        }

        fn command(&self, args: &[&str]) -> Command {
            let mut command = ctl();
            command.arg("--socket").arg(self.dir.join("d.sock"));
            if self.json {
                command.arg("--json");
            }
            command.args(args);
            command
        }

        fn run(&self, args: &[&str]) -> Output {
            self.command(args).output().expect("daedalus-ctl runs")
        }

        /// Runs a command that must succeed, and gives what it printed.
        fn ok(&self, args: &[&str]) -> String {
            let output = self.run(args);
            assert_eq!(
                output.status.code(),
                Some(0),
                "{args:?}: {}",
                stderr(&output)
            );
            assert!(output.stderr.is_empty(), "{args:?}: {}", stderr(&output));
            stdout(&output)
        }

        /// Runs a command with `--json` that must succeed, and gives its result.
        fn result(&self, args: &[&str]) -> Value {
            let shown: Value = serde_json::from_str(&self.ok(args)).expect("one JSON object");
            assert_eq!(shown["ok"], true, "{args:?}");
            assert_eq!(shown["command"], args[0], "{args:?}");
            assert_eq!(keys(&shown), ["command", "ok", "result"], "{args:?}");
            shown["result"].clone()
        }

        fn path(&self, name: &str) -> String {
            self.dir.join(name).display().to_string()
        }
    }

    fn keys(value: &Value) -> Vec<&str> {
        value
            .as_object()
            .map(|fields| fields.keys().map(String::as_str).collect())
            .unwrap_or_default()
    }

    /// Reads one paragraph of a watch's text output: the lines up to a blank one.
    fn paragraph(lines: &mut impl Iterator<Item = std::io::Result<String>>) -> Vec<String> {
        let mut block = Vec::new();
        for line in lines {
            let line = line.expect("a line");
            if line.is_empty() {
                break;
            }
            block.push(line);
        }
        block
    }

    #[test]
    fn every_command_runs_against_the_daemon() {
        let daemon = Daemon::start("commands");
        let (g, patch, again) = (daemon.path("g.json"), daemon.path("patch.json"), {
            daemon.path(&format!("again{}", std::env::consts::DLL_SUFFIX))
        });
        assert_eq!(daemon.ok(&["ping"]), "version: 1\n");
        let loaded = daemon.ok(&["load-plugin", &again]);
        assert!(loaded.contains("name: dylib-fixture\n"), "{loaded}");
        assert!(loaded.contains("generation: 2\n"), "{loaded}");
        assert!(loaded.contains("backends: fixture.cpu\n"), "{loaded}");
        assert_eq!(
            daemon.ok(&["put-graph", "g", &g]),
            "diagnostics: []\ngraph: g\n"
        );
        assert_eq!(
            daemon.ok(&["patch-graph", "g", &patch]),
            "diagnostics: []\ngraph: g\n"
        );

        let planned = daemon.ok(&["plan", "g"]);
        assert!(
            planned.contains("cached: false\n") && planned.contains("regions: 1\n"),
            "{planned}"
        );
        assert!(planned.contains("explain: null\n"), "{planned}");
        assert!(daemon.ok(&["plan", "g"]).contains("cached: true\n"));
        let inspected = daemon.ok(&[
            "plan",
            "g",
            "--inspect",
            "--form",
            "steps",
            "--priority",
            "fixture.cpu",
        ]);
        assert!(inspected.contains("explain:\n  edges:"), "{inspected}");

        assert_eq!(
            daemon.ok(&["start", "g", "--telemetry", "detailed", "--fifo", "8"]),
            "run: 1\n"
        );
        assert_eq!(
            daemon.ok(&["feed", "1", "in", "u32:5"]),
            "dropped: false\nwoke: 1\n"
        );
        let stepped = daemon.ok(&["step", "1"]);
        assert!(
            stepped.contains("ticks: 1\n") && stepped.contains("step_calls: 1\n"),
            "{stepped}"
        );
        assert_eq!(
            daemon.ok(&["take", "1", "out"]),
            "value:\n  type: u32\n  value: 5\n"
        );
        assert_eq!(daemon.ok(&["take", "1", "out"]), "value: null\n");
        assert!(daemon.ok(&["step", "1", "3"]).contains("ticks: 3\n"));

        let reloaded = daemon.ok(&["reload", "1", "g"]);
        assert!(
            reloaded.contains("carried:\n  names: io, work\n  total: 2\n"),
            "{reloaded}"
        );

        let run = daemon.ok(&["inspect", "run", "1"]);
        assert!(
            run.contains("state: running\n") && run.contains("graph: g\n"),
            "{run}"
        );
        assert!(
            run.contains("  - input: true\n    name: in\n    type: u32\n"),
            "{run}"
        );
        assert!(daemon.ok(&["inspect", "graph", "g"]).contains("name: g\n"));
        let telemetry = daemon.ok(&["inspect", "telemetry", "1"]);
        assert!(
            telemetry.contains("level: detailed\n") && telemetry.contains("nodes: work\n"),
            "{telemetry}"
        );
        let host = daemon.ok(&["inspect", "host"]);
        assert!(
            host.contains("plugins: dylib-fixture\n") && host.contains("graphs: g\n"),
            "{host}"
        );
        assert!(daemon.ok(&["cache", "inspect"]).contains("entries: 1\n"));
        assert_eq!(daemon.ok(&["cache", "clear"]), "ok\n");
        assert!(daemon.ok(&["cache", "inspect"]).contains("entries: 0\n"));
        let trace = daemon.ok(&["trace", "2"]);
        assert!(
            trace.contains("op: cache\n") && trace.contains("op: inspect\n"),
            "{trace}"
        );
        assert_eq!(trace.matches("seq: ").count(), 2, "{trace}");
        assert!(daemon.ok(&["trace"]).contains("op: put_graph\n"));

        // A watch prints as events arrive, and ends when its run does.
        let mut watching = daemon
            .command(&["watch", "1"])
            .stdout(Stdio::piped())
            .spawn()
            .expect("watch starts");
        let mut lines = BufReader::new(watching.stdout.take().unwrap()).lines();
        let first = paragraph(&mut lines);
        assert_eq!(first.len(), 1);
        assert!(first[0].starts_with("watching: "), "{first:?}");
        daemon.ok(&["step", "1"]);
        let event = paragraph(&mut lines);
        assert!(event.contains(&"event: telemetry".to_string()), "{event:?}");
        assert!(event.contains(&"run: 1".to_string()), "{event:?}");
        let stopped = daemon.ok(&["stop", "1"]);
        assert!(
            stopped.contains("run: 1\n") && stopped.contains("dropped: 0\n"),
            "{stopped}"
        );
        let event = paragraph(&mut lines);
        assert!(event.contains(&"event: ended".to_string()), "{event:?}");
        assert!(event.contains(&"reason: stopped".to_string()), "{event:?}");
        assert_eq!(watching.wait().unwrap().code(), Some(0));

        // A second run, in another session's eyes: cancelled, then stopped at once.
        assert_eq!(daemon.ok(&["start", "g"]), "run: 2\n");
        assert_eq!(daemon.ok(&["--session", "other", "cancel", "2"]), "ok\n");
        assert!(
            daemon
                .ok(&["inspect", "run", "2"])
                .contains("state: cancelled\n")
        );
        assert!(
            daemon
                .ok(&["stop", "2", "--immediate"])
                .contains("run: 2\n")
        );
        // A watch with a count ends after that many events.
        assert_eq!(daemon.ok(&["start", "g"]), "run: 3\n");
        let mut counted = daemon
            .command(&["watch", "all", "--kinds", "telemetry", "--count", "2"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(counted.stdout.take().unwrap()).lines();
        assert!(paragraph(&mut lines)[0].starts_with("watching: "));
        daemon.ok(&["step", "3"]);
        daemon.ok(&["step", "3"]);
        assert_eq!(counted.wait().unwrap().code(), Some(0));
    }

    #[test]
    fn a_daemon_error_exits_1_with_its_code() {
        let daemon = Daemon::start("errors");
        let g = daemon.path("g.json");
        let cases: &[(&[&str], &str)] = &[
            (&["step", "99"], "unknown_run: no run 99"),
            (&["plan", "absent"], "unknown_graph: "),
            (&["start", "absent"], "unknown_graph: "),
            (&["load-plugin", "/nowhere/libabsent.so"], "plugin_error: "),
            (&["inspect", "graph", "absent"], "unknown_graph: "),
            (&["--session", "other", "plan", "g"], "unknown_graph: "),
        ];
        daemon.ok(&["put-graph", "g", &g]);
        for (args, expected) in cases {
            let output = daemon.run(args);
            assert_eq!(output.status.code(), Some(1), "{args:?}");
            let message = stderr(&output);
            assert!(
                message.starts_with(&format!("daedalus-ctl: {expected}")),
                "{args:?}: {message}"
            );
            assert!(output.stdout.is_empty(), "{args:?}");
        }
        daemon.ok(&["start", "g"]);
        for (args, expected) in [
            (&["feed", "1", "nope", "u32:1"][..], "unknown_port: "),
            (&["feed", "1", "in", "string:five"][..], "value_type: "),
            (&["take", "1", "in"][..], "unknown_port: "),
            (&["plan", "g", "--form", "source"][..], "planning_failed: "),
        ] {
            let output = daemon.run(args);
            assert_eq!(output.status.code(), Some(1), "{args:?}");
            assert!(
                stderr(&output).starts_with(&format!("daedalus-ctl: {expected}")),
                "{args:?}"
            );
        }

        // A graph that does not plan: the diagnostics are shown, one a line.
        let lost = daemon.path("lost.json");
        std::fs::write(&lost, graph().replace("fixture.pass", "fixture.lost")).unwrap();
        assert!(
            daemon
                .ok(&["put-graph", "lost", &lost])
                .contains("code: node_missing\n")
        );
        let output = daemon.run(&["start", "lost"]);
        assert_eq!(output.status.code(), Some(1));
        let message = stderr(&output);
        assert!(
            message.starts_with("daedalus-ctl: planning_failed: "),
            "{message}"
        );
        assert!(message.contains("\n  node_missing: "), "{message}");

        // The same in JSON: the envelope on stdout, the same exit code, and nothing on stderr.
        let output = daemon.run(&["--json", "start", "lost"]);
        assert_eq!(output.status.code(), Some(1));
        assert!(output.stderr.is_empty());
        let shown: Value = serde_json::from_str(&stdout(&output)).expect("json");
        assert_eq!(keys(&shown), ["command", "error", "ok"]);
        assert_eq!(shown["ok"], false);
        assert_eq!(
            keys(&shown["error"]),
            ["code", "diagnostics", "kind", "message"]
        );
        assert_eq!(shown["error"]["kind"], "daemon");
        assert_eq!(shown["error"]["code"], "planning_failed");
        assert_eq!(
            keys(&shown["error"]["diagnostics"][0]),
            ["code", "message", "node", "port", "severity"]
        );

        // A file that cannot be read is a usage error, found once the daemon is reached.
        let output = daemon.run(&["put-graph", "g", &daemon.path("absent.json")]);
        assert_eq!(output.status.code(), Some(2), "{output:?}");

        // With the daemon gone, the same command cannot connect.
        let socket = daemon.dir.join("d.sock");
        drop(daemon);
        let output = ctl()
            .arg("--socket")
            .arg(&socket)
            .arg("ping")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(3), "{output:?}");
    }

    #[test]
    fn json_output_has_a_fixed_schema() {
        let mut daemon = Daemon::start("json");
        daemon.json = true;
        let (g, patch, again) = (daemon.path("g.json"), daemon.path("patch.json"), {
            daemon.path(&format!("again{}", std::env::consts::DLL_SUFFIX))
        });
        let names = ["names", "total"];
        let shape = |args: &[&str], expected: &[&str]| -> Value {
            let result = daemon.result(args);
            assert_eq!(keys(&result), expected, "{args:?}");
            result
        };

        assert_eq!(shape(&["ping"], &["version"])["version"], 1);
        let loaded = shape(
            &["load-plugin", &again],
            &["backends", "generation", "name", "nodes"],
        );
        assert_eq!(loaded["generation"], 2);
        shape(&["put-graph", "g", &g], &["diagnostics", "graph"]);
        shape(&["patch-graph", "g", &patch], &["diagnostics", "graph"]);
        let plan_keys = [
            "cached",
            "diagnostics",
            "edges",
            "explain",
            "form",
            "graph",
            "hash",
            "nodes",
            "regions",
            "steps",
        ];
        let planned = shape(&["plan", "g"], &plan_keys);
        assert_eq!(planned["explain"], Value::Null);
        assert!(planned["hash"].as_str().unwrap().starts_with("0x"));
        assert_eq!(planned["form"], "steps");
        let inspected = shape(&["plan", "g", "--inspect"], &plan_keys);
        assert_eq!(
            keys(&inspected["explain"]),
            ["edges", "nodes", "regions", "units"]
        );
        assert_eq!(
            shape(&["start", "g", "--telemetry", "detailed"], &["run"])["run"],
            1
        );
        shape(&["feed", "1", "in", "u32:7"], &["dropped", "woke"]);
        shape(
            &["step", "1"],
            &["faults", "idle", "skipped", "step_calls", "tick", "ticks"],
        );
        let taken = shape(&["take", "1", "out"], &["value"]);
        assert_eq!(keys(&taken["value"]), ["type", "value"]);
        assert_eq!(taken["value"]["type"], "u32");
        assert_eq!(taken["value"]["value"], 7);
        assert_eq!(
            shape(&["take", "1", "out"], &["value"])["value"],
            Value::Null
        );
        let reloaded = shape(
            &["reload", "1", "g"],
            &[
                "added",
                "carried",
                "dropped_values",
                "fresh",
                "removed",
                "removed_ports",
            ],
        );
        for list in ["added", "carried", "fresh", "removed", "removed_ports"] {
            assert_eq!(keys(&reloaded[list]), names, "{list}");
        }
        let run = shape(
            &["inspect", "run", "1"],
            &[
                "faults", "graph", "nodes", "ports", "run", "session", "state", "tick",
            ],
        );
        assert_eq!(keys(&run["ports"][0]), ["input", "name", "type"]);
        assert_eq!(run["state"], "running");
        let stored = shape(&["inspect", "graph", "g"], &["graph", "name"]);
        assert!(
            stored["graph"].is_object(),
            "the document is JSON, not text"
        );
        let telemetry = shape(
            &["inspect", "telemetry", "1"],
            &[
                "faults",
                "level",
                "rows",
                "run",
                "skipped",
                "step_calls",
                "tick",
            ],
        );
        assert_eq!(
            keys(&telemetry["rows"][0]),
            [
                "auto_off",
                "backing_off",
                "calls",
                "disabled",
                "failures",
                "nodes",
                "step"
            ]
        );
        let host = shape(
            &["inspect", "host"],
            &["graphs", "plugins", "runs", "sessions"],
        );
        assert_eq!(keys(&host["runs"][0]), ["graph", "run", "session", "state"]);
        shape(&["cache", "inspect"], &["entries", "hits", "misses"]);
        shape(&["cache", "clear"], &[]);
        let trace = shape(&["trace", "3"], &["entries"]);
        assert_eq!(trace["entries"].as_array().unwrap().len(), 3);
        assert_eq!(
            keys(&trace["entries"][0]),
            ["error", "id", "op", "seq", "session", "target"]
        );

        // A watch in JSON is one object a line, and every event has `event`, `sub` and `run`.
        let mut watching = daemon
            .command(&["watch", "1"])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut lines = BufReader::new(watching.stdout.take().unwrap()).lines();
        let mut next = || -> Value {
            serde_json::from_str(&lines.next().expect("a line").unwrap()).expect("json")
        };
        assert_eq!(keys(&next()), ["watching"]);
        daemon.result(&["step", "1"]);
        let event = next();
        assert_eq!(
            keys(&event),
            [
                "event",
                "faults",
                "run",
                "skipped",
                "step_calls",
                "sub",
                "tick"
            ]
        );
        assert_eq!(event["event"], "telemetry");
        shape(&["cancel", "1"], &[]);
        let event = next();
        assert_eq!(keys(&event), ["event", "reason", "run", "sub"]);
        assert_eq!(
            (&event["event"], &event["reason"]),
            (&Value::from("ended"), &Value::from("cancelled"))
        );
        assert_eq!(watching.wait().unwrap().code(), Some(0));
        shape(&["stop", "1", "--immediate"], &["dropped", "run", "tick"]);

        // Every command was covered: the usage text lists these and no others.
        let listed: BTreeSet<&str> = [
            "ping",
            "load-plugin",
            "put-graph",
            "patch-graph",
            "plan",
            "start",
            "feed",
            "step",
            "take",
            "watch",
            "reload",
            "cancel",
            "stop",
            "inspect",
            "trace",
            "cache",
        ]
        .into();
        let help = stdout(&ctl().arg("--help").output().unwrap());
        let shown: BTreeSet<&str> = help
            .lines()
            .skip_while(|line| *line != "commands:")
            .skip(1)
            .filter(|line| line.starts_with("  ") && !line.starts_with("   "))
            .filter_map(|line| line.split_whitespace().next())
            .collect();
        assert_eq!(shown, listed);
    }
}
