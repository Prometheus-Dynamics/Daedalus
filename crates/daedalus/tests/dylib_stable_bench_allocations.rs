//! What each benchmark case allocates per tick on the host and in the plugin, on the stable handler
//! path, against the static install (the host's allocations are this binary's; the plugin's are
//! its own allocator's, counted in `examples/plugins/stable_bench_counted`). The companion of
//! `dylib_stable_bench`: the same cases, a counting allocator in this binary, so it is a separate
//! test binary and its counter does not touch the timings.
//!
//! `cargo test -p daedalus-rs --features engine,dylib-plugins --release --test
//! dylib_stable_bench_allocations -- --ignored --nocapture`.
// musl executables are static and cannot dlopen (see `dylib::library`): no loader there.
#![cfg(all(unix, not(target_env = "musl")))]

#[path = "support/stable_bench.rs"]
mod bench;
#[path = "support/cdylib.rs"]
mod cdylib;
#[path = "support/frames.rs"]
mod frames;

use std::alloc::{GlobalAlloc, Layout, System};
use std::ffi::CString;
use std::os::unix::ffi::OsStrExt;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

use daedalus::PluginLibrary;
use daedalus::runtime::plugins::PluginRegistry;

use bench::{Host, Step, stable_registry, static_registry};

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

const RUNS: u64 = 300;

type Counter = extern "C" fn() -> u64;

/// Mean allocations per tick of `step` (this binary's, and the plugin's), after a warm-up.
fn per_tick(plugin: Option<Counter>, host: &mut Host, step: &mut Step) -> (f64, f64) {
    (0..50).for_each(|_| {
        step(host);
    });
    let plugin_count = || plugin.map_or(0, |count| count());
    let (before, plugin_before) = (ALLOCS.load(Relaxed), plugin_count());
    (0..RUNS).for_each(|_| {
        std::hint::black_box(step(host));
    });
    let host_allocs = (ALLOCS.load(Relaxed) - before) as f64 / RUNS as f64;
    let plugin_allocs = (plugin_count() - plugin_before) as f64 / RUNS as f64;
    (host_allocs, plugin_allocs)
}

/// The plugin's exported allocation counter, from its loaded `cdylib`.
fn counter(path: &std::path::Path) -> Counter {
    let path = CString::new(path.as_os_str().as_bytes()).unwrap();
    // Safety: the library is loaded (and stays loaded); this only looks up its symbol.
    unsafe {
        let handle = libc::dlopen(path.as_ptr(), libc::RTLD_NOW | libc::RTLD_NOLOAD);
        assert!(!handle.is_null(), "the plugin is loaded");
        let symbol = libc::dlsym(handle, c"stable_bench_allocations".as_ptr());
        assert!(!symbol.is_null(), "the plugin counts its allocations");
        std::mem::transmute::<*mut libc::c_void, Counter>(symbol)
    }
}

#[test]
#[ignore = "timing; run manually"]
#[allow(clippy::print_stdout)]
fn stable_bench_allocations() {
    let path = cdylib::build_cdylib(
        "daedalus-plugins-stable-bench-counted",
        "daedalus_plugins_stable_bench_counted",
        "",
    );
    // Safety: a trusted test plugin, kept loaded for the process.
    let library: &'static PluginLibrary =
        Box::leak(Box::new(unsafe { PluginLibrary::load(&path) }.unwrap()));
    let count = counter(&path);
    let (statics, stables): (PluginRegistry, PluginRegistry) =
        (static_registry(), stable_registry(library));

    // (group, case, builder)
    type Build = Box<dyn Fn(&PluginRegistry) -> (Host, Step)>;
    let mut cases: Vec<(&str, String, Build)> = Vec::new();
    cases.push((
        "constants",
        "scalars n=0".into(),
        Box::new(|registry: &PluginRegistry| bench::config_case(registry, false, 0)),
    ));
    for (mixed, variant) in [(false, "scalars"), (true, "quarter_strings")] {
        for n in [8, 49] {
            cases.push((
                "constants",
                format!("{variant} n={n}"),
                Box::new(move |registry: &PluginRegistry| bench::config_case(registry, mixed, n)),
            ));
        }
    }
    for tags in [0_i64, 4, 16] {
        cases.push((
            "detections_value",
            format!("Detections through Value, tags={tags}"),
            Box::new(move |registry: &PluginRegistry| {
                bench::detections_case(registry, false, tags)
            }),
        ));
        cases.push((
            "detections_handle",
            format!("Detections as a plugin-local handle, tags={tags}"),
            Box::new(move |registry: &PluginRegistry| bench::detections_case(registry, true, tags)),
        ));
        cases.push((
            "detections_pod",
            format!("Detections as StablePod structs, tags={tags}"),
            Box::new(move |registry: &PluginRegistry| bench::pod_case(registry, tags)),
        ));
    }
    for constants in [false, true] {
        cases.push((
            "staged",
            format!(
                "12 stages, handle in/out + frame, {} consts",
                if constants { 10 } else { 0 }
            ),
            Box::new(move |registry: &PluginRegistry| bench::staged_case(registry, constants)),
        ));
    }

    for (group, name, build) in &cases {
        let (mut static_host, mut static_step) = build(&statics);
        let (host_static, _) = per_tick(None, &mut static_host, &mut static_step);
        let (mut stable_host, mut stable_step) = build(&stables);
        let (host_stable, plugin_stable) =
            per_tick(Some(count), &mut stable_host, &mut stable_step);
        println!(
            "ALLOCS group={group} case=\"{name}\" static_host_per_tick={host_static:.2} \
             stable_host_per_tick={host_stable:.2} stable_plugin_per_tick={plugin_stable:.2} runs={RUNS}"
        );
    }
}
