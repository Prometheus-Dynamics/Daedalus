//! A reload racing the last releases of the old build's plugin-local objects. Each round makes a
//! `Local` on the current build, hands clones of its payload to 8 threads, reloads, and lets the
//! threads drop them all at once, while another thread ticks a graph of the plugin without pause
//! (every node call is a sweep point, so it closes retired builds whose count reached 0). A last
//! release still returns through its build's code after its decrement, so a build that output a
//! `Local` must never be unmapped under it: the test must not crash, and every old build is
//! still let go (unloaded) once its objects are gone. Meant to be run in a release build too
//! (`cargo test --release --test dylib_reload_race`), where the window is shortest.
#![cfg(not(target_env = "musl"))]

#[path = "support/cdylib.rs"]
mod cdylib;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Barrier};

use daedalus::PluginLibrary;
use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::dylib::{HostGraphReloadExt, InstallPath};
use daedalus::engine::{Engine, EngineConfig, HostGraph};
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::plugins::PluginRegistry;
use daedalus::transport::Payload;

const THREADS: usize = 8;
const ROUNDS: u64 = 100;

/// `local_make` alone: one `Local` per tick (the same one while the build lasts) to `obj`.
fn graph(registry: &PluginRegistry) -> HostGraph<HandlerRegistry> {
    let builder = registry
        .graph_builder()
        .unwrap()
        .input_as("id", TypeExpr::Scalar(ValueType::Int))
        .node_id("stable_abi:local_make", "make")
        .connect("id", "make.id")
        .connect("make.obj", "obj");
    Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(registry, builder.build())
        .unwrap()
}

fn object(host: &mut HostGraph<HandlerRegistry>) -> Payload {
    host.push("id", 1_i64);
    host.tick().unwrap();
    host.take_payload("obj").expect("obj")
}

#[test]
fn old_builds_go_while_their_last_objects_drop_on_many_threads() {
    let dir = std::env::temp_dir().join(format!("daedalus-race-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path: PathBuf = dir.join(format!("libfixture{}", std::env::consts::DLL_SUFFIX));
    std::fs::copy(cdylib::stable_abi_cdylib(), &path).unwrap();

    // Safety: a trusted test plugin.
    let mut library = unsafe { PluginLibrary::load_reloadable(&path) }.unwrap();
    let mut registry = PluginRegistry::new();
    library
        .install_into_as(&mut registry, InstallPath::Stable)
        .unwrap();
    let mut host = graph(&registry);
    let mut sweeper = graph(&registry);
    let stop = AtomicBool::new(false);
    let barrier = Arc::new(Barrier::new(THREADS + 1));

    std::thread::scope(|scope| {
        // Ticks without pause: each node call sweeps, and its own objects come and go too.
        scope.spawn(|| {
            while !stop.load(Ordering::Relaxed) {
                drop(object(&mut sweeper));
            }
        });
        let senders: Vec<mpsc::Sender<Payload>> = (0..THREADS)
            .map(|_| {
                let (send, receive) = mpsc::channel::<Payload>();
                let barrier = barrier.clone();
                scope.spawn(move || {
                    for payload in receive {
                        barrier.wait();
                        drop(payload);
                    }
                });
                send
            })
            .collect();

        for round in 0..ROUNDS {
            let old = object(&mut host);
            for send in &senders {
                send.send(old.clone()).unwrap();
            }
            // Safety: as above (the same file, opened again as a new build).
            let generation = unsafe { host.reload_plugin(&mut library) }.unwrap();
            assert_eq!(generation, round + 2);
            // The node moves to the new build, releasing the old one's state: only the
            // threads' clones (and `old`) hold the old object now.
            drop(object(&mut host));
            drop(old);
            barrier.wait();
        }
        drop(senders);
        stop.store(true, Ordering::Relaxed);
    });

    // Every old build is let go once its objects and instances are: the graphs' instances
    // move to the current build (the sweeper may have stopped before the last reload).
    drop(object(&mut host));
    drop(object(&mut sweeper));
    let current = library.generation();
    assert_eq!(library.live_generations(), vec![current]);

    drop((host, sweeper, registry));
    let _ = std::fs::remove_dir_all(&dir);
}
