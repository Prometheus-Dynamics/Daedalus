//! Owned frames (`SharedFrame`) through the stable plugin path: `examples/plugins/stable_abi`'s
//! frame nodes, loaded as a `cdylib` built "by another toolchain" (so installed through the
//! stable path) and, for comparison, installed statically (the handlers a Rust-ABI install
//! registers). Every case counts the host frame's drops: its owner must drop exactly once, and
//! only after the last reference on either side is gone. Pixels are never copied: pointers seen
//! by the plugin and in returned frames are the host's.

#[path = "support/cdylib.rs"]
mod cdylib;
#[path = "support/frames.rs"]
mod frames;

use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::sync::atomic::Ordering::SeqCst;

use daedalus::PluginLibrary;
use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::dylib::{HostGraphReloadExt, InstallPath};
use daedalus::engine::{Engine, EngineConfig, HostGraph};
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::plugins::{PluginRegistry, RegistryPluginExt};
use daedalus::transport::{ColorRange, FrameColorimetry, SharedFrame};
use daedalus_plugins_stable_abi::StableAbiPlugin;
use frames::{COUNTED_KEY, Counted, FramesHost};

fn stable_cdylib() -> &'static Path {
    static PATH: OnceLock<PathBuf> = OnceLock::new();
    PATH.get_or_init(|| {
        cdylib::build_cdylib(
            "daedalus-plugins-stable-abi",
            "daedalus_plugins_stable_abi",
            "dylib",
        )
    })
}

fn library() -> &'static PluginLibrary {
    static LIBRARY: OnceLock<PluginLibrary> = OnceLock::new();
    // Safety: a trusted test plugin.
    LIBRARY.get_or_init(|| unsafe { PluginLibrary::load(stable_cdylib()) }.unwrap())
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Install {
    Static,
    Stable,
}

fn registry(install: Install) -> PluginRegistry {
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&FramesHost::new()).unwrap();
    match install {
        Install::Static => registry.install_plugin(&StableAbiPlugin::new()).unwrap(),
        Install::Stable => library()
            .install_into_as(&mut registry, InstallPath::Stable)
            .unwrap(),
    }
    registry
}

/// One plugin node `node` fed from host input `frame` (plus string input `sensor` when the node
/// takes one), each output `port` wired to a graph output of the same name.
fn graph(registry: &PluginRegistry, node: &str, outputs: &[&str]) -> HostGraph<HandlerRegistry> {
    let mut builder = registry
        .graph_builder()
        .unwrap()
        .input_as("frame", TypeExpr::opaque(COUNTED_KEY))
        .node_id(&format!("stable_abi:{node}"), node)
        .connect("frame", format!("{node}.frame").as_str());
    if node == "frame_tag" {
        builder = builder
            .input_as("sensor", TypeExpr::Scalar(ValueType::String))
            .connect("sensor", format!("{node}.sensor").as_str());
    }
    for port in outputs {
        builder = builder.connect(format!("{node}.{port}").as_str(), *port);
    }
    Engine::new(EngineConfig::default())
        .unwrap()
        .compile_registry(registry, builder.build())
        .unwrap()
}

fn take_frame(host: &HostGraph<HandlerRegistry>, port: &str) -> Option<SharedFrame> {
    let payload = host.take_payload(port)?;
    Some(SharedFrame::from_payload(&payload).unwrap().unwrap())
}

fn both(test: impl Fn(Install)) {
    for install in [Install::Static, Install::Stable] {
        test(install);
    }
}

#[test]
fn a_frame_handed_back_is_the_hosts_own() {
    both(|install| {
        let (frame, drops) = Counted::nv12(4, 2, 1);
        let mut host = graph(&registry(install), "frame_pass", &["frame"]);
        host.push_payload("frame", frame.payload());
        host.tick().unwrap();
        let out = take_frame(&host, "frame").expect("frame_pass output");
        assert_eq!(
            out.handle().data(),
            frame.data_ptr(),
            "{install:?}: same frame"
        );
        let luma = out.as_view().plane_bytes(0).unwrap();
        assert_eq!(
            luma.as_ptr(),
            frame.luma_ptr(),
            "{install:?}: pixels not copied"
        );
        drop(luma);
        drop(host);
        drop(frame);
        assert_eq!(
            drops.load(SeqCst),
            0,
            "{install:?}: the returned frame keeps it"
        );
        drop(out);
        assert_eq!(drops.load(SeqCst), 1, "{install:?}");
    });
}

#[test]
fn a_frame_dropped_during_the_call_is_released_once() {
    both(|install| {
        let (frame, drops) = Counted::r8(3, 2, vec![0, 10, 20, 30, 40, 255]);
        let mut host = graph(&registry(install), "frame_invert", &["inverted"]);
        host.push_payload("frame", frame.payload());
        host.tick().unwrap();
        let inverted = take_frame(&host, "inverted").expect("frame_invert output");
        drop(frame);
        assert_eq!(drops.load(SeqCst), 1, "{install:?}: the input was released");
        drop(host);
        // A frame the plugin made outlives the graph (its library stays loaded).
        let view = inverted.as_view();
        assert_eq!((view.width(), view.height(), view.sequence()), (3, 2, 1));
        assert_eq!(
            &*view.plane_bytes(0).unwrap(),
            &[255, 245, 235, 225, 215, 0]
        );
        assert_eq!(view.colorimetry(), FrameColorimetry::SYCC);
        assert_eq!(
            inverted.is_local(),
            install == Install::Static,
            "{install:?}"
        );
        assert_eq!(drops.load(SeqCst), 1);
    });
}

#[test]
fn a_wrapped_frame_changes_metadata_not_pixels() {
    both(|install| {
        let (frame, drops) = Counted::nv12(4, 2, 5);
        let mut host = graph(&registry(install), "frame_tag", &["frame"]);
        host.push_payload("frame", frame.payload());
        host.push("sensor", String::from("cam1"));
        host.tick().unwrap();
        let tagged = take_frame(&host, "frame").expect("frame_tag output");
        drop((host, frame));
        let view = tagged.as_view();
        assert_eq!(view.sensor_id(), Some("cam1"), "{install:?}");
        assert_eq!(view.exposure_ns(), Some(Counted::EXPOSURE_NS), "delegated");
        assert_eq!(view.colorimetry().color_range(), ColorRange::Full);
        assert_eq!(view.sequence(), 5);
        assert_eq!(view.plane_count(), 2);
        assert_eq!(view.plane_bytes(1).unwrap().len(), 4);
        assert_eq!(
            drops.load(SeqCst),
            0,
            "{install:?}: the wrapper holds the frame"
        );
        drop(tagged);
        assert_eq!(drops.load(SeqCst), 1, "{install:?}");
    });
}

#[test]
fn the_plugin_reads_v3_metadata_and_pixels_in_place() {
    both(|install| {
        let (frame, drops) = Counted::nv12(4, 2, 1);
        let outputs = ["exposure_ns", "sensor", "range", "fd", "ptr"];
        let mut host = graph(&registry(install), "frame_info", &outputs);
        host.push_payload("frame", frame.payload());
        host.tick().unwrap();
        assert_eq!(
            host.take::<i64>("exposure_ns"),
            Some(Counted::EXPOSURE_NS as i64)
        );
        assert_eq!(host.take::<String>("sensor").as_deref(), Some("imx477"));
        assert_eq!(host.take::<i64>("range"), Some(ColorRange::Limited as i64));
        assert_eq!(host.take::<i64>("fd"), Some(-1), "host memory, no fd");
        assert_eq!(host.take::<i64>("ptr"), Some(frame.luma_ptr() as i64));
        drop((host, frame));
        assert_eq!(drops.load(SeqCst), 1, "{install:?}");
    });
}

#[test]
fn a_held_frame_is_released_on_a_later_tick_and_with_the_node() {
    both(|install| {
        let (first, first_drops) = Counted::nv12(4, 2, 1);
        let (second, second_drops) = Counted::nv12(4, 2, 2);
        let mut host = graph(&registry(install), "frame_hold", &["previous"]);
        host.push_payload("frame", first.payload());
        host.tick().unwrap();
        assert!(take_frame(&host, "previous").is_none());
        drop(first);
        assert_eq!(
            first_drops.load(SeqCst),
            0,
            "{install:?}: held in node state"
        );

        host.push_payload("frame", second.payload());
        host.tick().unwrap();
        let previous = take_frame(&host, "previous").expect("the held frame");
        assert_eq!(previous.as_view().sequence(), 1);
        assert_eq!(first_drops.load(SeqCst), 0, "returned, still referenced");
        drop(previous);
        assert_eq!(first_drops.load(SeqCst), 1, "{install:?}");

        drop(second);
        assert_eq!(
            second_drops.load(SeqCst),
            0,
            "{install:?}: held in node state"
        );
        drop(host);
        assert_eq!(
            second_drops.load(SeqCst),
            1,
            "{install:?}: released with the node"
        );
        assert_eq!(first_drops.load(SeqCst), 1);
    });
}

#[test]
fn a_panic_while_holding_a_frame_releases_it() {
    both(|install| {
        let (unlucky, unlucky_drops) = Counted::nv12(4, 2, 13);
        let (fine, fine_drops) = Counted::nv12(4, 2, 3);
        let mut host = graph(&registry(install), "frame_explode", &["sequence"]);
        host.push_payload("frame", unlucky.payload());
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| host.tick()));
        drop(unlucky);
        if install == Install::Static {
            // A statically installed node's panic is the host's own: it unwinds out of `tick`.
            assert!(result.is_err());
            drop(host);
            assert_eq!(unlucky_drops.load(SeqCst), 1, "unwinding released it");
            drop(fine);
            return;
        }
        // Through the stable path the plugin catches it: a node error the host survives.
        assert!(matches!(result, Ok(Err(_))), "{install:?}");
        assert_eq!(
            unlucky_drops.load(SeqCst),
            1,
            "{install:?}: unwinding released it"
        );
        host.push_payload("frame", fine.payload());
        host.tick().unwrap();
        assert_eq!(host.take::<i64>("sequence"), Some(3));
        drop((host, fine));
        assert_eq!(fine_drops.load(SeqCst), 1, "{install:?}");
    });
}

#[test]
fn a_reload_releases_frames_the_old_build_held() {
    let dir = std::env::temp_dir().join(format!("daedalus-frames-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("libframes{}", std::env::consts::DLL_SUFFIX));
    std::fs::copy(stable_cdylib(), &path).unwrap();
    // Safety: a trusted test plugin.
    let mut library = unsafe { PluginLibrary::load_reloadable(&path) }.unwrap();
    let mut registry = PluginRegistry::new();
    registry.install_plugin(&FramesHost::new()).unwrap();
    library
        .install_into_as(&mut registry, InstallPath::Stable)
        .unwrap();
    let mut host = graph(&registry, "frame_hold", &["previous"]);

    let (first, first_drops) = Counted::nv12(4, 2, 1);
    host.push_payload("frame", first.payload());
    host.tick().unwrap();
    drop(first);
    assert_eq!(first_drops.load(SeqCst), 0, "held by generation 1");

    // Safety: as above (the same build, loaded again).
    assert_eq!(unsafe { host.reload_plugin(&mut library) }.unwrap(), 2);
    let (second, second_drops) = Counted::nv12(4, 2, 2);
    host.push_payload("frame", second.payload());
    host.tick().unwrap();
    assert!(take_frame(&host, "previous").is_none(), "node state reset");
    assert_eq!(first_drops.load(SeqCst), 1, "the old instance released it");
    assert_eq!(library.live_generations(), vec![2], "nothing keeps build 1");

    drop(second);
    drop(host);
    assert_eq!(second_drops.load(SeqCst), 1);
    drop(registry);
    drop(library);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A dma-buf-style frame: NV12 in one memfd, planes at offsets, mapped by the host's provider.
#[cfg(target_os = "linux")]
#[test]
fn a_memfd_frame_crosses_with_its_fd_borrowed() {
    both(|install| {
        let (frame, drops) = Counted::memfd_nv12(64, 4, 9);
        let outputs = ["exposure_ns", "sensor", "range", "fd", "ptr"];
        let mut host = graph(&registry(install), "frame_info", &outputs);
        host.push_payload("frame", frame.payload());
        host.tick().unwrap();
        assert_eq!(
            host.take::<i64>("fd"),
            Some(i64::from(frame.fd())),
            "{install:?}"
        );
        assert_eq!(
            host.take::<i64>("ptr"),
            Some(frame.map_ptr() as i64),
            "{install:?}"
        );
        assert_eq!(
            frame.accesses(),
            (1, 1),
            "{install:?}: one CPU access, ended"
        );
        drop(host);
        assert!(frame.fd_is_open());
        drop(frame);
        assert_eq!(drops.load(SeqCst), 1, "{install:?}");
    });
}
