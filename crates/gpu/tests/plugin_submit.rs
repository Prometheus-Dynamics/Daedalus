//! A GPU plugin and its host submitting on one context. The plugin is a `cdylib` with its own copy
//! of this crate (`crates/gpu/tests/plugin`), as Eidos's plugin is next to HeliOS's engine:
//! its statics are its own, so the queue's submit lock must reach it through the context handle.
//! Checked: the plugin sees the host's `SubmitLock` (and not another context's), its
//! `GpuContextHandle::submit` waits while the host holds that lock, and the host's raw
//! `vkQueueSubmit`s (`RawSubmitter`) run concurrently with the plugin's wgpu uploads, submits and
//! readbacks without a wrong byte. Under the validation layer (`scripts/ci.sh vvl`) the concurrent
//! run must also log no message. Skips without a Vulkan adapter.
#![cfg(target_os = "linux")]
#![allow(clippy::print_stderr)]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use daedalus_gpu::raw_vulkan::{RawSubmitter, ash::vk};
use daedalus_gpu::{
    GpuBackendKind, GpuContextHandle, GpuOptions, GpuStorage, SubmitLock, select_backend,
};

const ITERATIONS: usize = 200;

type SameLock = fn(&GpuContextHandle, &SubmitLock) -> bool;
type SubmitEmpty = fn(&GpuContextHandle) -> Result<(), String>;
type RoundTrips = fn(&GpuContextHandle, usize) -> Result<(), String>;

/// The plugin's entry points.
struct Plugin {
    same_lock: SameLock,
    submit_empty: SubmitEmpty,
    round_trips: RoundTrips,
}

/// `daedalus-gpu`'s enabled features besides the default `gpu-noop` (which the plugin always
/// has): the plugin is built with exactly these.
fn features() -> String {
    [
        ("gpu-mock", cfg!(feature = "gpu-mock")),
        ("gpu-wgpu", cfg!(feature = "gpu-wgpu")),
        ("gpu-gles", cfg!(feature = "gpu-gles")),
        ("gpu-image", cfg!(feature = "gpu-image")),
        ("gpu-async", cfg!(feature = "gpu-async")),
        ("gpu-dmabuf", cfg!(feature = "gpu-dmabuf")),
        ("gpu-vulkan-raw", cfg!(feature = "gpu-vulkan-raw")),
    ]
    .into_iter()
    .filter_map(|(name, on)| on.then_some(name))
    .collect::<Vec<_>>()
    .join(",")
}

/// Build the plugin `cdylib` in its own cargo invocation (as the facade's dynamic plugin tests
/// do, `crates/daedalus/tests/support/cdylib.rs`) and return its path.
/// `DAEDALUS_CDYLIB_DAEDALUS_PLUGINS_GPU_SUBMIT` names a prebuilt one instead.
fn build_plugin() -> PathBuf {
    const LIB_NAME: &str = "daedalus_plugins_gpu_submit";
    if let Some(path) = std::env::var_os(format!("DAEDALUS_CDYLIB_{}", LIB_NAME.to_uppercase())) {
        return PathBuf::from(path);
    }
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_string());
    let mut command = Command::new(cargo);
    command
        .args(["build", "-p", "daedalus-plugins-gpu-submit", "--lib"])
        .args(["--features", &features()])
        .arg("--message-format=json")
        .current_dir(root)
        .stderr(Stdio::inherit());
    if !cfg!(debug_assertions) {
        command.arg("--release");
    }
    let output = command.output().expect("cargo build of the plugin");
    assert!(output.status.success(), "the plugin build failed");
    String::from_utf8(output.stdout)
        .expect("cargo output is UTF-8")
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|msg| msg["reason"] == "compiler-artifact" && msg["target"]["name"] == LIB_NAME)
        .flat_map(|msg| msg["filenames"].as_array().cloned().unwrap_or_default())
        .filter_map(|path| path.as_str().map(PathBuf::from))
        .find(|path| path.extension().is_some_and(|ext| ext == "so"))
        .expect("cargo reported no cdylib for the plugin")
}

fn plugin() -> &'static Plugin {
    static PLUGIN: OnceLock<(libloading::Library, Plugin)> = OnceLock::new();
    &PLUGIN
        .get_or_init(|| {
            // SAFETY: the workspace's own fixture, built above by this toolchain with this crate's
            // features, so the Rust-ABI signatures below are the ones it exports.
            unsafe {
                let library = libloading::Library::new(build_plugin()).expect("load the plugin");
                let plugin = Plugin {
                    same_lock: *library.get(b"gpu_submit_plugin_same_lock").unwrap(),
                    submit_empty: *library.get(b"gpu_submit_plugin_submit_empty").unwrap(),
                    round_trips: *library.get(b"gpu_submit_plugin_round_trips").unwrap(),
                };
                (library, plugin)
            }
        })
        .1
}

fn vulkan_ctx() -> Option<GpuContextHandle> {
    if !cfg!(feature = "gpu-noop") {
        eprintln!("built without gpu-noop, which the plugin always has");
        return None;
    }
    let ctx = select_backend(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        ..Default::default()
    })
    .ok()
    .filter(|ctx| ctx.backend_kind() == GpuBackendKind::Wgpu)?;
    // SAFETY: only checks for the handles.
    unsafe { ctx.raw_vulkan() }.is_some().then_some(ctx)
}

#[test]
fn the_plugin_takes_the_host_submit_lock() {
    let Some(ctx) = vulkan_ctx() else {
        return eprintln!("skipping: no Vulkan adapter (or no gpu-noop)");
    };
    let plugin = plugin();
    let lock = ctx.submit_lock().expect("a wgpu context has a submit lock");
    assert!(
        (plugin.same_lock)(&ctx, &lock),
        "the plugin reaches another lock"
    );
    let other = vulkan_ctx().expect("a second context");
    let other_lock = other.submit_lock().unwrap();
    assert!(
        !(plugin.same_lock)(&ctx, &other_lock),
        "another context's lock matched"
    );

    // The plugin's submit waits for the host's guard.
    let submitted = AtomicBool::new(false);
    std::thread::scope(|scope| {
        let guard = lock.lock();
        let worker = scope.spawn(|| {
            (plugin.submit_empty)(&ctx).unwrap();
            submitted.store(true, Ordering::SeqCst);
        });
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            !submitted.load(Ordering::SeqCst),
            "the plugin submitted while the host held the submit lock"
        );
        drop(guard);
        worker.join().unwrap();
    });
    assert!(submitted.load(Ordering::SeqCst));
}

#[test]
fn host_raw_submits_and_plugin_wgpu_submits_overlap_safely() {
    let Some(ctx) = vulkan_ctx() else {
        return eprintln!("skipping: no Vulkan adapter (or no gpu-noop)");
    };
    let plugin = plugin();
    let submitter = RawSubmitter::new(&ctx).unwrap();
    let dst = GpuStorage::new_host_cached(&ctx, 64 << 10, Some("host dst")).unwrap();
    // SAFETY: a buffer of this device; each fill is waited for before the CPU reads it.
    let buffer = unsafe { dst.raw_vk_buffer() }.unwrap();
    std::thread::scope(|scope| {
        let plugin_side = scope.spawn(|| (plugin.round_trips)(&ctx, ITERATIONS));
        let mut out = vec![0u8; buffer.size as usize];
        for i in 0..ITERATIONS {
            let value = (i as u32).wrapping_mul(0x0101_0101) ^ 0x5a5a_5a5a;
            let fill = |d: &daedalus_gpu::raw_vulkan::ash::Device, cmd: vk::CommandBuffer| {
                // SAFETY: a fill of the storage, valid on its device.
                unsafe { d.cmd_fill_buffer(cmd, buffer.buffer, buffer.offset, buffer.size, value) }
            };
            // SAFETY: as above.
            unsafe { submitter.submit(fill) }.unwrap().wait();
            dst.read_mapped(0, &mut out).unwrap();
            assert!(
                out.as_chunks::<4>()
                    .0
                    .iter()
                    .all(|word| u32::from_ne_bytes(*word) == value),
                "host fill {i} read back wrong"
            );
        }
        plugin_side
            .join()
            .unwrap()
            .expect("the plugin's round trips");
    });
}
