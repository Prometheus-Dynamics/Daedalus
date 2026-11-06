//! What a tick through a stable-ABI plugin allocates, on both sides of the boundary, against
//! the same graph with the plugin installed statically. The host's allocations are this
//! binary's; the plugin's are its own allocator's, counted in `examples/plugins/stable_abi_counted`
//! (the plugin's `cdylib` with a counting allocator) and read through its exported
//! `stable_abi_allocations`.
//!
//! After warm-up the stable path adds nothing on the host side, and on the plugin side only the
//! handler's own output payload is allocated (as when installed statically): the arenas, the
//! port lists and the decoded inputs' payloads are reused call after call, and `SharedBytes`
//! and frames cross as handles both ways. Each tick hands its telemetry back
//! (`recycle_telemetry`), as a frame loop does.
//!
//! A frame (a handle edge) allocates nothing on either install, host or plugin: a payload holds
//! its foreign handle in place. Neither does a plugin-local object (`Local`) on the host side of
//! the stable path: the plugin counts its live objects, so the host holds nothing per object to
//! keep the build (the plugin allocates its output payload, as when installed statically).
// musl executables are static and cannot dlopen (see `dylib::library`): no loader there. The
// plugin's counter is read through `dlopen(RTLD_NOLOAD)`, so Unix only.
#![cfg(all(unix, not(target_env = "musl")))]

#[path = "support/cdylib.rs"]
mod cdylib;
#[path = "support/frames.rs"]
mod frames;

use std::alloc::{GlobalAlloc, Layout, System};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::dylib::InstallPath;
use daedalus::engine::{Engine, EngineConfig, HostGraph};
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus::{PluginLibrary, SharedBytes};
use daedalus_plugins_stable_abi::StableAbiPlugin;
use frames::{COUNTED_KEY, Counted, FramesHost};

struct Counting;

static ALLOCS: AtomicU64 = AtomicU64::new(0);

// Safety: forwards to the system allocator, counting allocations.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        // Safety: forwarded.
        unsafe { System.alloc(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        // Safety: forwarded.
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALLOCS.fetch_add(1, Relaxed);
        // Safety: forwarded.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const RUNS: u64 = 500;

type Counter = extern "C" fn() -> u64;

/// The stable-ABI plugin loaded through the stable path, and its allocation counter.
fn stable_registry() -> (PluginRegistry, Counter) {
    let path = cdylib::build_cdylib(
        "daedalus-plugins-stable-abi-counted",
        "daedalus_plugins_stable_abi_counted",
        "",
    );
    // Safety: a trusted test plugin, kept loaded for the process.
    let library = Box::leak(Box::new(unsafe { PluginLibrary::load(&path) }.unwrap()));
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&FramesHost::new()).unwrap();
    assert_eq!(
        library.install_into(&mut registry).unwrap(),
        InstallPath::Stable
    );
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    // Safety: the library is loaded (and stays loaded); this only looks up its symbol.
    let counter = unsafe {
        let handle = libc::dlopen(path.as_ptr(), libc::RTLD_NOW | libc::RTLD_NOLOAD);
        assert!(!handle.is_null(), "the plugin is loaded");
        let symbol = libc::dlsym(handle, c"stable_abi_allocations".as_ptr());
        assert!(!symbol.is_null(), "the plugin counts its allocations");
        std::mem::transmute::<*mut libc::c_void, Counter>(symbol)
    };
    (registry, counter)
}

fn static_registry() -> PluginRegistry {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&FramesHost::new()).unwrap();
    registry.install_plugin(&StableAbiPlugin::new()).unwrap();
    registry
}

/// `node` with each of `inputs` fed from a host input of the same name and `output` to `out`.
fn graph(
    registry: &PluginRegistry,
    node: &str,
    inputs: &[(&str, TypeExpr)],
    output: &str,
) -> HostGraph<HandlerRegistry> {
    let mut builder = registry
        .graph_builder()
        .unwrap()
        .node_id(&format!("stable_abi:{node}"), node);
    for (port, ty) in inputs {
        builder = builder
            .input_as(*port, ty.clone())
            .connect(*port, format!("{node}.{port}").as_str());
    }
    let builder = builder.connect(format!("{node}.{output}").as_str(), "out");
    Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(registry, builder.build())
        .unwrap()
}

/// Mean allocations per run of `once` (this binary's, and the plugin's), after a warm-up.
fn per_run(plugin: Option<Counter>, mut once: impl FnMut()) -> (f64, f64) {
    (0..50).for_each(|_| once());
    let plugin_count = || plugin.map_or(0, |count| count());
    let (host, plugin_before) = (ALLOCS.load(Relaxed), plugin_count());
    (0..RUNS).for_each(|_| once());
    let host = (ALLOCS.load(Relaxed) - host) as f64 / RUNS as f64;
    let plugin = (plugin_count() - plugin_before) as f64 / RUNS as f64;
    (host, plugin)
}

/// Allocations per tick of each case: (name, host, plugin).
fn cases(registry: &PluginRegistry, plugin: Option<Counter>) -> Vec<(&'static str, f64, f64)> {
    let mut results = Vec::new();
    let mut host = graph(
        registry,
        "scale",
        &[
            ("value", TypeExpr::Scalar(ValueType::I32)),
            ("factor", TypeExpr::Scalar(ValueType::Float)),
        ],
        "out",
    );
    let (h, p) = per_run(plugin, || {
        host.push("value", 7_i32);
        host.push("factor", 1.5_f64);
        let telemetry = host.tick().unwrap();
        host.recycle_telemetry(telemetry);
        assert_eq!(host.take::<f64>("out"), Some(10.5));
    });
    results.push(("scalars", h, p));

    let bytes = SharedBytes::new(vec![7; 4096]);
    let mut host = graph(
        registry,
        "bytes_pass",
        &[("bytes", TypeExpr::opaque(daedalus::dylib::SHARED_BYTES_KEY))],
        "bytes",
    );
    let (h, p) = per_run(plugin, || {
        host.push("bytes", bytes.clone());
        let telemetry = host.tick().unwrap();
        host.recycle_telemetry(telemetry);
        let out = host.take::<SharedBytes>("out").unwrap();
        assert_eq!(
            out.as_ptr(),
            bytes.as_ptr(),
            "the host's buffer, not a copy"
        );
    });
    results.push(("SharedBytes", h, p));

    let (frame, _) = Counted::nv12(64, 32, 1);
    let mut host = graph(
        registry,
        "frame_pass",
        &[("frame", TypeExpr::opaque(COUNTED_KEY))],
        "frame",
    );
    let (h, p) = per_run(plugin, || {
        host.push_payload("frame", frame.payload());
        let telemetry = host.tick().unwrap();
        host.recycle_telemetry(telemetry);
        assert!(host.take_payload("out").is_some());
    });
    results.push(("SharedFrame", h, p));

    // A plugin-local object: the node keeps one `Local` and outputs it every tick.
    let mut host = graph(
        registry,
        "local_make",
        &[("id", TypeExpr::Scalar(ValueType::Int))],
        "obj",
    );
    let (h, p) = per_run(plugin, || {
        host.push("id", 3_i64);
        let telemetry = host.tick().unwrap();
        host.recycle_telemetry(telemetry);
        assert!(host.take_payload("out").is_some());
    });
    results.push(("Local", h, p));
    results
}

#[test]
#[allow(clippy::print_stderr)]
fn a_stable_plugin_tick_allocates_no_more_than_the_static_install() {
    let (stable, counter) = stable_registry();
    let baseline = cases(&static_registry(), None);
    let measured = cases(&stable, Some(counter));
    for ((name, base, _), (_, host, plugin)) in baseline.iter().zip(&measured) {
        eprintln!(
            "{name}: static {base:.2}, stable host {host:.2}, plugin {plugin:.2} allocs/tick"
        );
    }
    let case = |results: &[(&str, f64, f64)], case: &str| {
        let (_, host, plugin) = results.iter().find(|(name, ..)| *name == case).unwrap();
        (*host, *plugin)
    };
    assert_eq!(
        case(&baseline, "SharedFrame").0,
        0.0,
        "a static frame tick allocates nothing"
    );
    assert_eq!(
        case(&measured, "SharedFrame"),
        (0.0, 0.0),
        "a frame through the stable path allocates nothing, host or plugin"
    );
    assert_eq!(
        case(&measured, "Local").0,
        0.0,
        "a plugin-local object through the stable path allocates nothing on the host: its \
         build's count keeps the build loaded, not a keepalive per payload"
    );
    for ((name, base, _), (_, host, plugin)) in baseline.iter().zip(&measured) {
        assert!(
            host <= base,
            "{name}: the stable path adds host allocations: {host:.2} > {base:.2} per tick"
        );
        // The node pushes one output; its payload is the handler's (statically too).
        assert!(
            *plugin <= 1.0,
            "{name}: the plugin allocates {plugin:.2} per tick, beyond its output payload"
        );
    }
}
