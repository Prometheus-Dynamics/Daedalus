//! The daemon with worker programs from its config: a worker's nodes run in a graph, a worker is
//! started again after it is killed, a missing program is tried again until it appears, Locked
//! integrity starts an approved worker and refuses a changed one, and a daemon built without
//! `workers` refuses a config that names workers.

use std::path::{Path, PathBuf};
use std::process::{Command, Output};

fn daemon() -> Command {
    Command::new(env!("CARGO_BIN_EXE_daedalusd"))
}

/// A directory of this test's own, emptied first.
fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join(format!("daedalusd-workers-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn check(dir: &Path, text: &str) -> Output {
    let path = dir.join("check.toml");
    std::fs::write(&path, text).unwrap();
    daemon()
        .arg("--check-config")
        .arg("--config")
        .arg(&path)
        .output()
        .expect("daedalusd runs")
}

#[test]
fn check_config_names_its_workers_or_refuses_them_when_built_without_them() {
    let dir = scratch("check");
    let relative = check(&dir, "[[worker]]\nprogram = \"bin/worker\"\n");
    assert_eq!(relative.status.code(), Some(1), "{relative:?}");
    assert!(
        String::from_utf8_lossy(&relative.stderr)
            .contains("worker 0: program `bin/worker` is not an absolute path or a bare name"),
        "{relative:?}"
    );

    // An absolute path on this platform: `/usr/bin/tool` has no drive, so Windows calls it relative.
    let tool = dir.join("tool");
    let named = check(
        &dir,
        &format!(
            "[[worker]]\nprogram = \"python3\"\nargs = [\"w.py\"]\n[[worker]]\nprogram = '{}'\ncwd = 'relative'\n",
            tool.display()
        ),
    );
    assert_eq!(named.status.code(), Some(1), "{named:?}");
    assert!(
        String::from_utf8_lossy(&named.stderr)
            .contains("worker 1: cwd `relative` is not an absolute path"),
        "{named:?}"
    );

    let good = check(
        &dir,
        "[[worker]]\nprogram = \"python3\"\nargs = [\"w.py\"]\n[worker.env]\nMODE = \"x\"\n",
    );
    if cfg!(feature = "workers") {
        assert!(good.status.success(), "{good:?}");
        let shown = String::from_utf8_lossy(&good.stdout);
        assert!(shown.contains("workers: python3 w.py"), "{shown}");
    } else {
        assert_eq!(good.status.code(), Some(1), "{good:?}");
        let message = String::from_utf8_lossy(&good.stderr);
        assert!(
            message.contains("this daemon was built without `workers`"),
            "{message}"
        );
        assert!(good.stdout.is_empty());
    }
    let _ = std::fs::remove_dir_all(dir);
}

/// Where the made plan of the worker graph is kept between the full build and the lean one.
#[cfg(unix)]
#[allow(dead_code, reason = "only the workers and the lean tests read it")]
fn shared_plan() -> PathBuf {
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("made-workers");
    std::fs::create_dir_all(&dir).expect("the shared directory");
    dir.join("add.plan")
}

#[cfg(unix)]
#[allow(
    dead_code,
    reason = "the helpers serve the tests that need the workers feature"
)]
mod running {
    use std::fs::File;
    use std::sync::OnceLock;
    use std::time::{Duration, Instant};

    use daedalus_client::{Client, Endpoint, Options};
    use daedalus_graph::{
        EdgeDoc, EdgePolicyDoc, GraphBody, GraphDoc, NodeDoc, NodeKindDoc, PortRefDoc,
    };
    use daedalus_types::Scalar;
    use daedalus_wire::service::{InspectTarget, Inspection, StartConfig, StopMode, TypedValue};
    use daedalus_wire::{WireType, WireValue};
    use std::process::{Child, Stdio};

    use super::*;

    /// The Rust fixture worker, built once per test binary into a directory of its own.
    fn fixture_worker() -> &'static Path {
        static BUILT: OnceLock<PathBuf> = OnceLock::new();
        BUILT.get_or_init(|| {
            let source = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../testing/crates/worker-fixture/Cargo.toml");
            let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("worker-fixture");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let status = Command::new(cargo)
                .args(["build", "-q", "--manifest-path"])
                .arg(source)
                .arg("--target-dir")
                .arg(&target)
                .status()
                .expect("cargo runs");
            assert!(status.success(), "the worker fixture builds");
            target.join("debug").join(format!(
                "daedalus-worker-fixture{}",
                std::env::consts::EXE_SUFFIX
            ))
        })
    }

    /// A copy of the fixture worker at `path`, so a test can change or remove it.
    fn copy_worker(path: &Path) {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir).unwrap();
        }
        std::fs::copy(fixture_worker(), path).expect("the worker is copied");
    }

    /// Waits for `done`, however slow the machine is. Fails only if it never happens.
    fn until(what: &str, mut done: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(300);
        while !done() {
            assert!(Instant::now() < deadline, "timed out waiting for {what}");
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// Waits until the daemon's one worker reads as `want` in `inspect host`: its state line
    /// contains `want`. Bounded at 90 s, which a gate under load needs. On timeout it fails with the
    /// last lines seen, so the state it was in is in the failure.
    fn wait_worker(client: &Client, what: &str, want: &str) {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let lines = workers(client);
            if lines.first().is_some_and(|line| line.contains(want)) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what} (wanted `{want}`); last state: {lines:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn i64_value(value: i64) -> TypedValue {
        TypedValue::new(WireType::Scalar(Scalar::Int), WireValue::I64(value))
    }

    /// Two host inputs, `a` and `b`, around the worker's `fixture.scalar_add`, whose output `out`
    /// is the host's.
    fn add_graph() -> String {
        let mut bridge = NodeDoc::new("host");
        bridge.kind = NodeKindDoc::HostBridge;
        bridge.label = Some("io".to_string());
        for port in ["a", "b", "out"] {
            bridge
                .port_types
                .insert(port.to_string(), "i64".to_string());
        }
        let mut inner = NodeDoc::new("fixture.scalar_add");
        inner.label = Some("add".to_string());
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
            edges: vec![
                edge(0, "a", 1, "a"),
                edge(0, "b", 1, "b"),
                edge(1, "out", 0, "out"),
            ],
            groups: Vec::new(),
        };
        GraphDoc::new(body).to_json().unwrap()
    }

    /// A daemon with `extra` config lines, started with its own config in `dir`.
    struct Daemon {
        child: Child,
        socket: PathBuf,
        log: PathBuf,
    }

    impl Daemon {
        fn start(dir: &Path, extra: &str) -> Daemon {
            let socket = dir.join("d.sock");
            let config = dir.join("daedalusd.toml");
            std::fs::write(
                &config,
                format!(
                    "listen = [\"unix:{}\"]\nlog_level = \"debug\"\n{extra}",
                    socket.display()
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
            Daemon { child, socket, log }
        }

        fn log(&self) -> String {
            std::fs::read_to_string(&self.log).unwrap_or_default()
        }

        fn client(&mut self) -> Client {
            let mut client = None;
            until("the daemon to listen", || {
                if let Ok(Some(status)) = self.child.try_wait() {
                    panic!("daedalusd exited early: {status:?}\n{}", self.log());
                }
                client = Client::connect(Endpoint::unix(&self.socket), Options::default()).ok();
                client.is_some()
            });
            client.unwrap()
        }

        /// Stops the daemon with SIGTERM and returns its exit code and log.
        fn stop(mut self) -> (Option<i32>, String) {
            let pid = self.child.id().to_string();
            let sent = Command::new("kill").args(["-TERM", &pid]).status().unwrap();
            assert!(sent.success(), "kill -TERM {pid}");
            let status = self.child.wait().expect("daedalusd exits");
            (status.code(), self.log())
        }
    }

    impl Drop for Daemon {
        fn drop(&mut self) {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }

    /// The `inspect host` lines that name workers.
    fn workers(client: &Client) -> Vec<String> {
        match client.inspect(InspectTarget::Host).unwrap() {
            Inspection::Host(info) => info
                .plugins
                .into_iter()
                .filter(|line| line.starts_with("worker "))
                .collect(),
            other => panic!("expected the host, got {other:?}"),
        }
    }

    /// Runs the add graph once, with `a` and `b`, and returns what came out.
    fn add_once(client: &Client, a: i64, b: i64) -> Option<TypedValue> {
        add_once_named(client, "g", a, b)
    }

    /// [`add_once`] for the graph or plan named `name`.
    fn add_once_named(client: &Client, name: &str, a: i64, b: i64) -> Option<TypedValue> {
        let run = client.start(name, StartConfig::default()).unwrap();
        client.feed(run, "a", i64_value(a)).unwrap();
        client.feed(run, "b", i64_value(b)).unwrap();
        client.step(run, 1).unwrap();
        let out = client.take(run, "out").unwrap();
        client.stop(run, StopMode::Drain).unwrap();
        out
    }

    /// The text the worker is listed under, for a program and its arguments.
    fn label(program: &Path, args: &[&str]) -> String {
        std::iter::once(program.display().to_string())
            .chain(args.iter().map(|arg| arg.to_string()))
            .collect::<Vec<_>>()
            .join(" ")
    }

    #[test]
    #[cfg(all(feature = "workers", feature = "planner"))]
    fn a_rust_worker_node_runs_through_the_daemon_and_is_listed() {
        let dir = scratch("rust");
        let program = dir.join("worker");
        copy_worker(&program);
        let mut daemon = Daemon::start(
            &dir,
            &format!("\n[[worker]]\nprogram = '{}'\n", program.display()),
        );
        let client = daemon.client();
        wait_worker(&client, "the worker to be running", "running, serving");

        let listed = workers(&client);
        assert_eq!(listed.len(), 1, "{listed:?}");
        assert!(
            listed[0].starts_with(&format!("worker {}: running, serving", program.display())),
            "{listed:?}"
        );
        assert!(listed[0].contains("fixture.scalar_add"), "{listed:?}");

        client.put_graph("g", &add_graph()).unwrap();
        assert_eq!(add_once(&client, 2, 40), Some(i64_value(42)));
        // Many values, each fed, stepped and taken through the worker's node.
        let run = client.start("g", StartConfig::default()).unwrap();
        for value in 0..500 {
            client.feed(run, "a", i64_value(value)).unwrap();
            client.feed(run, "b", i64_value(1)).unwrap();
            assert_eq!(client.step(run, 1).unwrap().faults, 0);
            assert_eq!(client.take(run, "out").unwrap(), Some(i64_value(value + 1)));
        }
        client.stop(run, StopMode::Drain).unwrap();

        let (code, log) = daemon.stop();
        assert_eq!(code, Some(0), "{log}");
        assert!(
            log.contains("worker ") && log.contains("started, serving"),
            "{log}"
        );
    }

    #[test]
    #[cfg(all(feature = "workers", feature = "planner"))]
    fn a_python_worker_node_runs_through_the_daemon() {
        // Skips where there is no interpreter: the test says nothing then.
        if Command::new("python3").arg("--version").output().is_err() {
            return;
        }
        let dir = scratch("python");
        let script =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../languages/python/fixtures/worker.py");
        let mut daemon = Daemon::start(
            &dir,
            &format!(
                "\n[[worker]]\nprogram = \"python3\"\nargs = ['{}']\n",
                script.display()
            ),
        );
        let client = daemon.client();
        wait_worker(
            &client,
            "the python worker to be running",
            "running, serving",
        );
        assert!(
            workers(&client)[0].starts_with("worker python3 "),
            "{:?}",
            workers(&client)
        );
        client.put_graph("g", &add_graph()).unwrap();
        assert_eq!(add_once(&client, 2, 40), Some(i64_value(42)));
        let (code, log) = daemon.stop();
        assert_eq!(code, Some(0), "{log}");
    }

    #[test]
    #[cfg(all(feature = "workers", feature = "planner"))]
    fn a_worker_killed_while_the_daemon_runs_is_started_again() {
        let dir = scratch("killed");
        let program = dir.join("worker");
        copy_worker(&program);
        // The shell writes its pid and becomes the worker, so the pid is the worker's own.
        let pidfile = dir.join("worker.pid");
        let script = dir.join("worker.sh");
        std::fs::write(
            &script,
            format!(
                "echo $$ > '{}'\nexec '{}'\n",
                pidfile.display(),
                program.display()
            ),
        )
        .unwrap();
        let mut daemon = Daemon::start(
            &dir,
            &format!(
                "\n[[worker]]\nprogram = \"/bin/sh\"\nargs = ['{}']\n",
                script.display()
            ),
        );
        let client = daemon.client();
        wait_worker(&client, "the worker to be running", "running, serving");
        client.put_graph("g", &add_graph()).unwrap();
        assert_eq!(add_once(&client, 2, 40), Some(i64_value(42)));

        let first: u32 = std::fs::read_to_string(&pidfile)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        // The worker is killed, by the pid it wrote for itself.
        let killed = Command::new("kill")
            .args(["-KILL", &first.to_string()])
            .status()
            .unwrap();
        assert!(killed.success());

        // A step that finds the worker down faults and names it. Later steps start it again, and
        // a new pid is written by the worker that runs.
        let mut answer = None;
        until("the worker to run again", || {
            answer = add_once(&client, 5, 6);
            answer == Some(i64_value(11))
        });
        let second: u32 = std::fs::read_to_string(&pidfile)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        assert_ne!(first, second, "a new worker process runs");
        assert_eq!(add_once(&client, 1, 1), Some(i64_value(2)));
        let (code, log) = daemon.stop();
        assert_eq!(code, Some(0), "{log}");
    }

    #[test]
    #[cfg(all(feature = "workers", feature = "planner"))]
    fn a_missing_worker_is_tried_again_and_comes_up_when_its_program_appears() {
        let dir = scratch("missing");
        let program = dir.join("late").join("worker");
        let mut daemon = Daemon::start(
            &dir,
            &format!("\n[[worker]]\nprogram = '{}'\n", program.display()),
        );
        let client = daemon.client();

        // Down: listed as waiting, and a graph that needs its node is refused by name.
        wait_worker(&client, "the first try to fail", "No such file");
        let listed = workers(&client);
        assert!(
            listed[0].contains("waiting, not started") && listed[0].contains("No such file"),
            "{listed:?}"
        );
        let refused = match client.put_graph("g", &add_graph()) {
            Err(error) => error.to_string(),
            Ok(_) => client
                .start("g", StartConfig::default())
                .expect_err("a node of a worker that is not up is refused")
                .to_string(),
        };
        assert!(
            refused.contains(&format!("not running: worker {}", program.display())),
            "{refused}"
        );

        // The program appears, and the daemon starts it on a later try.
        copy_worker(&program);
        wait_worker(&client, "the worker to come up", "running, serving");
        client.put_graph("g", &add_graph()).unwrap();
        assert_eq!(add_once(&client, 2, 40), Some(i64_value(42)));
        let (code, log) = daemon.stop();
        assert_eq!(code, Some(0), "{log}");
        assert!(log.contains("is not up"), "{log}");
    }

    #[test]
    #[cfg(all(feature = "workers", feature = "planner"))]
    fn locked_integrity_starts_an_approved_worker_and_refuses_a_changed_one() {
        let dir = scratch("locked");
        let program = dir.join("worker");
        copy_worker(&program);
        let lock = dir.join("daedalus.lock");
        let worker = format!("\n[[worker]]\nprogram = '{}'\n", program.display());

        // Record writes the worker into the lock as it starts.
        let mut recording = Daemon::start(
            &dir,
            &format!(
                "integrity = \"record\"\nlock_file = '{}'\n{worker}",
                lock.display()
            ),
        );
        let client = recording.client();
        wait_worker(&client, "the worker to be recorded", "running, serving");
        let (code, log) = recording.stop();
        assert_eq!(code, Some(0), "{log}");
        assert!(lock.exists(), "the lock was written");

        // Locked: the approved worker starts.
        let mut approved = Daemon::start(
            &dir,
            &format!(
                "integrity = \"locked\"\nlock_file = '{}'\n{worker}",
                lock.display()
            ),
        );
        let client = approved.client();
        wait_worker(&client, "the approved worker to start", "running, serving");
        client.put_graph("g", &add_graph()).unwrap();
        assert_eq!(add_once(&client, 2, 40), Some(i64_value(42)));
        let (code, log) = approved.stop();
        assert_eq!(code, Some(0), "{log}");

        // A changed program is refused before it starts, and the daemon serves on without it.
        let mut bytes = std::fs::read(&program).unwrap();
        bytes.push(0);
        std::fs::write(&program, bytes).unwrap();
        let mut refused = Daemon::start(
            &dir,
            &format!(
                "integrity = \"locked\"\nlock_file = '{}'\n{worker}",
                lock.display()
            ),
        );
        let client = refused.client();
        wait_worker(&client, "the changed worker to be refused", "refused");
        let listed = workers(&client);
        assert!(
            listed[0].contains("is not the file the lock approves"),
            "{listed:?}"
        );
        assert_eq!(client.ping().unwrap(), 1, "the daemon serves on");
        let (code, log) = refused.stop();
        assert_eq!(code, Some(0), "{log}");
    }

    /// A worker that never says hello is being started: the daemon answers a request while it waits
    /// for the hello, and the worker is named as not up once the start gives up.
    #[test]
    #[cfg(all(feature = "workers", feature = "planner"))]
    fn the_daemon_answers_while_a_worker_that_never_says_hello_is_started() {
        let dir = scratch("silent");
        let mut daemon = Daemon::start(
            &dir,
            "\n[[worker]]\nprogram = \"/bin/sleep\"\nargs = [\"600\"]\n",
        );
        let client = daemon.client();
        let asked = Instant::now();
        let listed = workers(&client);
        assert!(
            asked.elapsed() < Duration::from_secs(2),
            "the request waited {:?} for the worker",
            asked.elapsed()
        );
        assert!(!listed[0].contains("running"), "{listed:?}");
        wait_worker(
            &client,
            "the silent worker to be named as not up",
            "waiting, not started",
        );
        assert_eq!(client.ping().unwrap(), 1, "the daemon serves on");
        let (code, log) = daemon.stop();
        assert_eq!(code, Some(0), "{log}");
    }

    #[test]
    #[cfg(all(feature = "workers", feature = "planner"))]
    fn a_made_plan_of_a_worker_node_is_written_for_the_lean_build() {
        let dir = scratch("made-write");
        let program = dir.join("worker");
        copy_worker(&program);
        let mut daemon = Daemon::start(
            &dir,
            &format!("\n[[worker]]\nprogram = '{}'\n", program.display()),
        );
        let client = daemon.client();
        wait_worker(&client, "the worker to start", "running, serving");
        client.put_graph("add", &add_graph()).unwrap();
        let (_hash, bytes) = client.make_plan("add", StartConfig::default()).unwrap();
        std::fs::write(shared_plan(), &bytes).unwrap();
        // The graph itself, for the resident-memory measurement, which runs it through `daedalus-ctl`.
        std::fs::write(shared_plan().with_file_name("add.json"), add_graph()).unwrap();
        assert_eq!(add_once_named(&client, "add", 2, 40), Some(i64_value(42)));
        let (code, log) = daemon.stop();
        assert_eq!(code, Some(0), "{log}");
    }

    /// The daemon built without `workers` (and without the planner) loads the plan the full
    /// build wrote, and says at bind that it has no worker backend to run it on.
    #[test]
    #[cfg(all(not(feature = "workers"), not(feature = "planner")))]
    fn a_daemon_without_workers_refuses_a_made_plan_that_needs_one() {
        let plan = shared_plan();
        assert!(
            plan.exists(),
            "run the workers test first: it writes {}",
            plan.display()
        );
        let dir = scratch("made-lean");
        let plans = dir.join("plans");
        std::fs::create_dir_all(&plans).unwrap();
        std::fs::copy(&plan, plans.join("add.plan")).unwrap();
        let mut daemon = Daemon::start(&dir, &format!("plan_dir = '{}'\n", plans.display()));
        let client = daemon.client();
        let refused = client
            .start("add", StartConfig::default())
            .expect_err("a plan that needs a worker is refused without one")
            .to_string();
        assert!(
            refused.contains("the daemon was built without `workers`"),
            "{refused}"
        );
        assert_eq!(client.ping().unwrap(), 1, "the daemon serves on");
    }
}
