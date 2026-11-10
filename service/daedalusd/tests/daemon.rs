//! The daemon as a process: its config check, a stdio session, and on unix a full run over a
//! unix socket with the dylib fixture loaded, ended by a signal.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};

use daedalus_wire::service::{ClientMsg, Op, Reply, Request, ServerMsg, ServiceHello};
use daedalus_wire::{DEFAULT_CAP, Schema, read_packet, write_packet};

fn daemon() -> Command {
    Command::new(env!("CARGO_BIN_EXE_daedalusd"))
}

/// A directory of this test's own, emptied first.
fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("daedalusd-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn check(dir: &Path, name: &str, text: &str) -> Output {
    let path = dir.join(name);
    std::fs::write(&path, text).unwrap();
    daemon()
        .arg("--check-config")
        .arg("--config")
        .arg(&path)
        .output()
        .expect("daedalusd runs")
}

#[test]
fn check_config_accepts_a_good_file_and_rejects_a_bad_one_with_a_message() {
    let dir = scratch("check");
    let good = check(
        &dir,
        "good.toml",
        "listen = [\"tcp:127.0.0.1:4100\", \"tcp:[::1]:4100\"]\nplugin_dir = \"/usr/lib/daedalusd/plugins\"\nmax_runs = 8\nlog_level = \"debug\"\n",
    );
    assert!(good.status.success(), "{good:?}");
    let shown = String::from_utf8_lossy(&good.stdout);
    assert!(shown.starts_with("config ok: "), "{shown}");
    assert!(
        shown.contains("listen: tcp:127.0.0.1:4100, tcp:[::1]:4100"),
        "{shown}"
    );
    assert!(shown.contains("max_runs: 8"), "{shown}");
    assert!(shown.contains("log_level: debug"), "{shown}");

    let cases = [
        (
            "listen = [\"tcp:0.0.0.0:4100\"]\n",
            "not a loopback address",
        ),
        (
            "listen = [\"tcp:192.0.2.1:4100\"]\n",
            "not a loopback address",
        ),
        (
            "listen = [\"udp:127.0.0.1:1\"]\n",
            "must start with `unix:` or `tcp:`",
        ),
        (
            "listen = [\"unix:relative.sock\"]\n",
            "not an absolute path",
        ),
        (
            "log_level = \"loud\"\n",
            "log_level: `loud` is not one of error, warn, info, debug",
        ),
        (
            "plugin_dir = \"plugins\"\n",
            "plugin_dir: `plugins` is not an absolute path",
        ),
        ("max_run = 3\n", "unknown field `max_run`"),
        ("max_runs = \"many\"\n", "max_runs"),
        ("listen = [\n", "listen"),
    ];
    for (text, expected) in cases {
        let bad = check(&dir, "bad.toml", text);
        assert_eq!(bad.status.code(), Some(1), "{text}: {bad:?}");
        let message = String::from_utf8_lossy(&bad.stderr);
        assert!(message.starts_with("daedalusd: config: "), "{message}");
        assert!(message.contains("bad.toml"), "the file is named: {message}");
        assert!(message.contains(expected), "{text}: {message}");
        assert!(bad.stdout.is_empty());
    }

    // A file that is not there, and an argument that does not exist.
    let missing = daemon()
        .args(["--check-config", "--config"])
        .arg(dir.join("absent.toml"))
        .output()
        .unwrap();
    assert_eq!(missing.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&missing.stderr).contains("absent.toml"));
    let usage = daemon().arg("--serve").output().unwrap();
    assert_eq!(usage.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&usage.stderr).contains("unknown argument `--serve`"));
    let help = daemon().arg("--help").output().unwrap();
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).starts_with("usage: daedalusd"));
}

/// Stops the child if a test fails before it has stopped it.
struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[test]
fn a_stdio_session_is_served_and_the_daemon_exits_when_stdin_closes() {
    let dir = scratch("stdio");
    let config = dir.join("daedalusd.toml");
    std::fs::write(&config, "log_level = \"warn\"\n").unwrap();
    let mut running = Running(
        daemon()
            .arg("--stdio")
            .arg("--config")
            .arg(&config)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("daedalusd starts"),
    );
    let mut stdin = running.0.stdin.take().unwrap();
    let mut stdout = running.0.stdout.take().unwrap();
    let mut buf = Vec::new();
    let mut ask = |message: ClientMsg| -> ServerMsg {
        let body = message.encode(&Schema::new()).unwrap();
        write_packet(&mut stdin, &body, DEFAULT_CAP).unwrap();
        read_packet(&mut stdout, DEFAULT_CAP, &mut buf).unwrap();
        ServerMsg::decode(&buf, &Schema::new()).unwrap()
    };
    assert!(matches!(
        ask(ClientMsg::Hello(ServiceHello::client())),
        ServerMsg::Hello(hello) if hello.check().is_ok()
    ));
    match ask(ClientMsg::Request(Request::new(1, Op::Ping))) {
        ServerMsg::Response(response) => {
            assert_eq!(response.id, 1);
            assert!(matches!(response.result, Ok(Reply::Pong { version: 1 })));
        }
        other => panic!("expected a response, got {other:?}"),
    }
    drop(ask);
    stdin.flush().unwrap();
    drop(stdin);
    let status = running.0.wait().expect("daedalusd exits");
    assert!(status.success(), "{status:?}");
}

#[cfg(unix)]
mod unix {
    use std::fs::File;
    use std::sync::OnceLock;
    use std::time::{Duration, Instant};

    use daedalus_client::{Client, Endpoint, Options};
    use daedalus_graph::{
        EdgeDoc, EdgePolicyDoc, GraphBody, GraphDoc, NodeDoc, NodeKindDoc, PortRefDoc,
    };
    use daedalus_wire::service::{
        EventBody, EventKinds, InspectTarget, Inspection, StartConfig, StopMode, TypedValue,
        WatchTarget,
    };
    use daedalus_wire::{WireType, WireValue};

    use super::*;

    /// The dylib fixture, built once per test binary into a directory of its own.
    fn fixture() -> &'static Path {
        static BUILT: OnceLock<PathBuf> = OnceLock::new();
        BUILT.get_or_init(|| {
            let source = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../testing/fixtures/dylib/Cargo.toml");
            let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("daedalusd-fixture");
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
    }

    fn u32_value(value: u32) -> TypedValue {
        TypedValue::new(
            WireType::Scalar(daedalus_types::Scalar::U32),
            WireValue::U32(value),
        )
    }

    /// A host bridge with ports `in` and `out` around one plugin node.
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

    /// Waits for `done`, however slow the machine is. Fails only if it never happens.
    fn until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(300);
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    struct Daemon {
        running: Running,
        socket: PathBuf,
        log: PathBuf,
    }

    impl Daemon {
        /// Starts the daemon in `dir` with the plugins in `dir/plugins`.
        fn start(dir: &Path) -> Daemon {
            let socket = dir.join("d.sock");
            let config = dir.join("daedalusd.toml");
            std::fs::write(
                &config,
                format!(
                    "listen = [\"unix:{}\"]\nplugin_dir = \"{}\"\nlog_level = \"debug\"\n",
                    socket.display(),
                    dir.join("plugins").display()
                ),
            )
            .unwrap();
            let log = dir.join("daedalusd.log");
            let child = daemon()
                .arg("--config")
                .arg(&config)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(File::create(&log).unwrap())
                .spawn()
                .expect("daedalusd starts");
            Daemon {
                running: Running(child),
                socket,
                log,
            }
        }

        fn log(&self) -> String {
            std::fs::read_to_string(&self.log).unwrap_or_default()
        }

        /// Connects once the daemon is listening.
        fn client(&mut self) -> Client {
            let mut client = None;
            until("the daemon to listen", || {
                if let Ok(Some(status)) = self.running.0.try_wait() {
                    panic!("daedalusd exited early: {status:?}\n{}", self.log());
                }
                client = Client::connect(Endpoint::unix(&self.socket), Options::default()).ok();
                client.is_some()
            });
            client.unwrap()
        }

        /// Sends the daemon a signal, by its pid, and waits for it to exit.
        fn signal(mut self, signal: &str) -> (Option<i32>, String) {
            let pid = self.running.0.id().to_string();
            let sent = Command::new("kill").args([signal, &pid]).status().unwrap();
            assert!(sent.success(), "kill {signal} {pid}");
            let status = self.running.0.wait().expect("daedalusd exits");
            (status.code(), self.log())
        }
    }

    fn plugins(client: &Client) -> Vec<String> {
        match client.inspect(InspectTarget::Host).unwrap() {
            Inspection::Host(info) => info.plugins,
            other => panic!("expected the host, got {other:?}"),
        }
    }

    #[test]
    fn a_daemon_on_a_unix_socket_runs_a_plugin_graph_and_sigterm_exits_0() {
        let dir = scratch("run");
        std::fs::create_dir_all(dir.join("plugins")).unwrap();
        let library = dir
            .join("plugins")
            .join(format!("fixture{}", std::env::consts::DLL_SUFFIX));
        std::fs::copy(fixture(), &library).unwrap();
        std::fs::write(dir.join("plugins").join("README.txt"), "not a library").unwrap();
        let mut daemon = Daemon::start(&dir);
        let client = daemon.client();

        // The plugin directory was loaded before the socket opened.
        assert_eq!(plugins(&client), ["dylib-fixture"]);
        assert!(
            client
                .put_graph("g", &graph("fixture.pass"))
                .unwrap()
                .is_empty()
        );
        let run = client.start("g", StartConfig::default()).unwrap();
        client.feed(run, "in", u32_value(5)).unwrap();
        assert_eq!(client.step(run, 1).unwrap().step_calls, 1);
        assert_eq!(client.take(run, "out").unwrap(), Some(u32_value(5)));

        // Many values through the plugin's node, each fed, stepped and taken. A number travels
        // in place, as its bytes, which is the form a plugin reads and forwards.
        for value in 0..2000 {
            client.feed(run, "in", u32_value(value)).unwrap();
            client.step(run, 1).unwrap();
            assert_eq!(client.take(run, "out").unwrap(), Some(u32_value(value)));
        }

        // Reload onto the same graph: every node is carried.
        let report = client.reload(run, "g").unwrap();
        assert_eq!(report.carried.names, ["io", "work"]);

        // Loading the library again is a second generation, and the run moves onto it.
        let again = dir.join(format!("again{}", std::env::consts::DLL_SUFFIX));
        std::fs::copy(fixture(), &again).unwrap();
        let loaded = client.load_plugin(again.to_str().unwrap()).unwrap();
        assert_eq!(
            (loaded.name.as_str(), loaded.generation),
            ("dylib-fixture", 2)
        );
        assert!(loaded.nodes.contains(&"fixture.pass".to_string()));
        assert_eq!(loaded.backends, ["fixture.cpu"]);
        client.reload(run, "g").unwrap();
        client.feed(run, "in", u32_value(7)).unwrap();
        client.step(run, 1).unwrap();
        assert_eq!(client.take(run, "out").unwrap(), Some(u32_value(7)));

        // A node that panics is a fault the watcher hears of, and the daemon goes on.
        client.put_graph("boom", &graph("fixture.boom")).unwrap();
        let boom = client.start("boom", StartConfig::default()).unwrap();
        let watch = client
            .watch(WatchTarget::Run(boom), EventKinds::FAULT)
            .unwrap();
        client.feed(boom, "in", u32_value(1)).unwrap();
        assert_eq!(client.step(boom, 1).unwrap().faults, 1);
        assert!(matches!(
            watch.next_event().expect("a fault").body,
            EventBody::Fault(fault) if fault.covers == ["work"]
        ));
        assert_eq!(client.ping().unwrap(), 1);

        assert_eq!(client.stop(run, StopMode::Drain).unwrap().dropped, 0);
        // The other run is left running: stopping the daemon cancels and drops it.
        let (code, log) = daemon.signal("-TERM");
        assert_eq!(code, Some(0), "{log}");
        assert!(
            log.contains("loaded plugin dylib-fixture generation 1"),
            "{log}"
        );
        assert!(log.contains("stopped: 1 runs dropped"), "{log}");
        assert!(!log.contains("was not released"), "{log}");
        assert!(!dir.join("d.sock").exists(), "the socket file is removed");
        assert_eq!(
            watch.next_event().map(|event| event.body),
            Some(EventBody::Ended {
                run: 0,
                reason: daedalus_wire::service::EndReason::Shutdown,
            })
        );
    }

    #[test]
    fn a_plugin_that_fails_to_load_is_retried_and_never_stops_the_daemon() {
        let dir = scratch("retry");
        let plugin_dir = dir.join("plugins");
        std::fs::create_dir_all(&plugin_dir).unwrap();
        let suffix = std::env::consts::DLL_SUFFIX;
        let broken = plugin_dir.join(format!("broken{suffix}"));
        let hopeless = plugin_dir.join(format!("hopeless{suffix}"));
        std::fs::write(&broken, "this is not a library").unwrap();
        std::fs::write(&hopeless, "neither is this").unwrap();
        let mut daemon = Daemon::start(&dir);
        let client = daemon.client();

        // Both failed, and the daemon serves all the same.
        assert!(plugins(&client).is_empty());
        assert_eq!(client.ping().unwrap(), 1);
        let log = daemon.log();
        assert!(
            log.contains("broken") && log.contains("did not load"),
            "{log}"
        );

        // The file is put right. Nobody asks: the daemon's own retry loads it.
        let staged = dir.join("staged");
        std::fs::copy(fixture(), &staged).unwrap();
        std::fs::rename(&staged, &broken).unwrap();
        until("the retry to load the plugin", || {
            plugins(&client) == ["dylib-fixture"]
        });
        until("the retry to be logged", || {
            daemon.log().contains("after retrying")
        });

        // Removing a file is how an operator stops the retries for it.
        std::fs::remove_file(&hopeless).unwrap();
        until("the retries for the removed file to stop", || {
            daemon.log().contains("was removed: no longer retried")
        });

        client.put_graph("g", &graph("fixture.pass")).unwrap();
        let run = client.start("g", StartConfig::default()).unwrap();
        client.feed(run, "in", u32_value(9)).unwrap();
        client.step(run, 1).unwrap();
        assert_eq!(client.take(run, "out").unwrap(), Some(u32_value(9)));
        client.stop(run, StopMode::Immediate).unwrap();
        // SIGINT stops it the same way.
        let (code, log) = daemon.signal("-INT");
        assert_eq!(code, Some(0), "{log}");
    }

    #[test]
    fn a_socket_passed_by_a_service_manager_is_served() {
        let activator = "systemd-socket-activate";
        if Command::new(activator).arg("--version").output().is_err() {
            // Without the tool there is nothing here that can pass a socket to a child.
            return;
        }
        let dir = scratch("activated");
        let socket = dir.join("passed.sock");
        let config = dir.join("daedalusd.toml");
        // The config names another socket. A passed one is used in its place.
        let unused = dir.join("unused.sock");
        std::fs::write(
            &config,
            format!("listen = [\"unix:{}\"]\n", unused.display()),
        )
        .unwrap();
        let log = dir.join("daedalusd.log");
        let child = Command::new(activator)
            .arg("-l")
            .arg(&socket)
            .arg(env!("CARGO_BIN_EXE_daedalusd"))
            .arg("--config")
            .arg(&config)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(File::create(&log).unwrap())
            .spawn()
            .expect("the activator starts");
        let mut daemon = Daemon {
            running: Running(child),
            socket: socket.clone(),
            log,
        };
        // The activator starts the daemon on the first connection, in its own place.
        let client = daemon.client();
        assert_eq!(client.ping().unwrap(), 1);
        assert!(plugins(&client).is_empty());
        assert!(!unused.exists());
        let (code, log) = daemon.signal("-TERM");
        assert_eq!(code, Some(0), "{log}");
        assert!(
            socket.exists(),
            "the passed socket's file is the activator's to remove"
        );
    }
}
