//! A Styx-style host loop: one `poll(2)` over the host's own descriptor (a pipe standing in for a
//! camera) and the graph's inbound fd, driving ticks without a waiter thread or async runtime.
#![cfg(all(feature = "plugins", feature = "std", target_os = "linux"))]

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::thread;
use std::time::Duration;

use daedalus_data::model::{TypeExpr, Value, ValueType};
use daedalus_engine::{Engine, EngineConfig, HostGraph};
use daedalus_planner::{Edge, Graph, NodeInstance};
use daedalus_registry::capability::{NodeDecl, PortDecl};
use daedalus_runtime::RuntimeNode;
use daedalus_runtime::executor::{NodeError, NodeHandler};
use daedalus_runtime::host_bridge::{HOST_BRIDGE_ID, HOST_BRIDGE_META_KEY, HostBridgeManager};
use daedalus_runtime::plugins::PluginRegistry;

const INT_KEY: &str = "typeexpr:{\"Scalar\":\"Int\"}";

struct Increment;

impl NodeHandler for Increment {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        if node.id == "inc" {
            let Some(Value::Int(value)) = io.get_typed_ref::<Value>("in") else {
                return Err(NodeError::InvalidInput("expected int".to_string()));
            };
            let next = Value::Int(value + 1);
            io.push_as_to("out", INT_KEY.into(), next);
        }
        Ok(())
    }
}

fn increment_graph() -> HostGraph<Increment> {
    let int_ty = TypeExpr::Scalar(ValueType::Int);
    let mut plugins = PluginRegistry::new();
    plugins
        .register_node_decl(
            NodeDecl::new(HOST_BRIDGE_ID)
                .metadata(HOST_BRIDGE_META_KEY, Value::Bool(true))
                .input(PortDecl::new("out", INT_KEY).schema(int_ty.clone()))
                .output(PortDecl::new("in", INT_KEY).schema(int_ty.clone())),
        )
        .unwrap();
    plugins
        .register_node_decl(
            NodeDecl::new("inc")
                .input(PortDecl::new("in", INT_KEY).schema(int_ty.clone()))
                .output(PortDecl::new("out", INT_KEY).schema(int_ty)),
        )
        .unwrap();
    let mut host = NodeInstance::new(HOST_BRIDGE_ID)
        .with_label("host")
        .with_inputs(["out"])
        .with_outputs(["in"]);
    host.metadata
        .insert(HOST_BRIDGE_META_KEY.to_string(), Value::Bool(true));
    let graph = Graph {
        nodes: vec![
            host,
            NodeInstance::new("inc")
                .with_inputs(["in"])
                .with_outputs(["out"]),
        ],
        edges: vec![Edge::new(0, "in", 1, "in"), Edge::new(1, "out", 0, "out")],
        metadata: Default::default(),
    };
    Engine::new(EngineConfig::default())
        .unwrap()
        .compile_host_graph_plugin_registry(
            &plugins,
            graph,
            Increment,
            HostBridgeManager::new(),
            "host",
        )
        .unwrap()
}

/// A non-blocking pipe: (read end, write end).
fn pipe() -> (OwnedFd, OwnedFd) {
    let mut fds = [0 as RawFd; 2];
    // SAFETY: `fds` has room for the two descriptors `pipe2` writes.
    let result = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) };
    assert_eq!(result, 0, "pipe2: {}", io::Error::last_os_error());
    // SAFETY: both descriptors are fresh and owned by nobody else.
    unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
}

/// Read every complete frame number queued on the camera pipe.
fn read_frames(camera: &OwnedFd, mut frame: impl FnMut(i64)) {
    let mut buf = [0_u8; 8];
    // SAFETY: reads at most 8 bytes into `buf` from an open, non-blocking descriptor.
    while unsafe { libc::read(camera.as_raw_fd(), buf.as_mut_ptr().cast(), 8) } == 8 {
        frame(i64::from_ne_bytes(buf));
    }
}

#[test]
fn one_poll_drives_a_camera_fd_and_the_graph_inbound_fd() {
    const FRAMES: i64 = 300;
    let mut graph = increment_graph();
    graph.set_latest_input("in").expect("latest-only frames");
    let inbound = graph.inbound_fd().expect("inbound eventfd");
    let stop = graph.stop_handle();
    let (camera, camera_tx) = pipe();
    let camera_thread = thread::spawn(move || {
        for frame in 1..=FRAMES {
            let bytes = frame.to_ne_bytes();
            // SAFETY: writes 8 bytes from `bytes` to the open pipe write end.
            let written = unsafe { libc::write(camera_tx.as_raw_fd(), bytes.as_ptr().cast(), 8) };
            assert_eq!(written, 8);
            thread::sleep(Duration::from_micros(200));
        }
    });

    let mut fds = [camera.as_raw_fd(), inbound.as_raw_fd()].map(|fd| libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    });
    let (mut last, mut wakeups, mut idle_wakeups) = (0, 0, 0);
    let mut stopper = None;
    loop {
        // SAFETY: `fds` is a valid array of two pollfds.
        let ready = unsafe { libc::poll(fds.as_mut_ptr(), 2, 5_000) };
        assert!(ready > 0, "poll timed out: lost wakeup after output {last}");
        if fds[0].revents & libc::POLLIN != 0 {
            read_frames(&camera, |frame| {
                graph.push_as("in", INT_KEY, Value::Int(frame));
            });
        } else if fds[0].revents & libc::POLLHUP != 0 {
            fds[0].fd = -1; // camera gone: `poll` ignores negative descriptors
        }
        if fds[1].revents & libc::POLLIN != 0 {
            wakeups += 1;
            if stop.is_stopped() {
                break;
            }
            if graph.tick_ready().expect("tick").is_none() {
                idle_wakeups += 1;
            }
            for out in graph.drain_owned::<Value>("out").expect("outputs") {
                let Value::Int(out) = out else {
                    panic!("non-int output")
                };
                assert!(out > last, "outputs arrive in frame order");
                last = out;
            }
        }
        if last == FRAMES + 1 && stopper.is_none() {
            // Idle: the inbound fd stays quiet, so the loop sleeps in `poll` until stopped.
            let mut quiet = [fds[1]];
            // SAFETY: one valid pollfd.
            assert_eq!(unsafe { libc::poll(quiet.as_mut_ptr(), 1, 20) }, 0);
            let stop = stop.clone();
            stopper = Some(thread::spawn(move || stop.stop()));
        }
    }
    camera_thread.join().expect("camera");
    stopper.expect("stopped").join().expect("stopper");
    assert_eq!(last, FRAMES + 1, "the last frame was processed");
    assert!(
        idle_wakeups <= wakeups / 2 + 1,
        "{idle_wakeups} of {wakeups} inbound wakeups found nothing to tick"
    );
}
