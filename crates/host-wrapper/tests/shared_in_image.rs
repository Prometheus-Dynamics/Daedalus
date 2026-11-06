//! An image surface (`daedalus_surface::export_surface!`, the `examples/surface_image` crate): a
//! surface library whose hosts start with the `stable_abi` nodes installed in-image, through the
//! Rust path. The same comparison as `shared_vs_static` (see `support/compare.rs`) with no plugin
//! installed by path: identical outputs to the static graph over N frames, no allocation in the
//! surface on a warm scalar frame, and no more on a bytes frame than the static graph. The nodes
//! never touch the stable ABI: the image exports no plugin entry point and the host loads no
//! plugin library, which the stable path needs. A separately built plugin still installs on top.
//!
//! Unix only, as `shared_vs_static`; the image and the plugin are built by nested cargo calls.

#![cfg(unix)]

#[path = "support/cdylib.rs"]
mod cdylib;
#[path = "support/compare.rs"]
mod compare;

use daedalus_host::Surface;

/// `daedalus-surface`'s `FEATURE_IMAGE_NODES`.
const FEATURE_IMAGE_NODES: u64 = 1 << 4;

#[test]
fn an_image_surface_runs_its_nodes_in_image() {
    let image = cdylib::build_cdylib(
        "daedalus-surface-image",
        "daedalus_surface_image",
        "alloc-count",
    );
    let surface = Surface::load(&image).unwrap();
    assert!(surface.features().contains(FEATURE_IMAGE_NODES));
    // The image exports the surface and no stable plugin entry point.
    // Safety: the library is already loaded; this only looks up symbols.
    let library = unsafe { libloading::Library::new(&image) }.unwrap();
    for name in [
        &b"daedalus_plugin_descriptor\0"[..],
        b"daedalus_plugin_abi_version\0",
    ] {
        // Safety: a lookup only; the symbol is never called.
        assert!(unsafe { library.get::<extern "C" fn()>(name) }.is_err());
    }
    // Safety: as above.
    assert!(unsafe { library.get::<extern "C" fn()>(b"daedalus_surface_push_batch\0") }.is_ok());
    drop(library);

    // A fresh host has the nodes and no plugin library: they run their Rust handlers. Their
    // schema is listed, from the registry.
    let host = surface.create_host().unwrap();
    let schemas: serde_json::Value = serde_json::from_str(&host.schemas().unwrap()).unwrap();
    let schemas = schemas.as_array().unwrap();
    assert_eq!(schemas.len(), 1, "one in-image plugin: {schemas:?}");
    let nodes = schemas[0]["nodes"].to_string();
    assert!(
        nodes.contains("accumulate") && nodes.contains("shared_sum"),
        "the in-image plugin's nodes are listed: {nodes}"
    );
    compare::compare(&image, None, "shared-in-image");

    // A separately built plugin installs on top, through the stable path.
    let bench = cdylib::build_cdylib(
        "daedalus-plugins-stable-bench",
        "daedalus_plugins_stable_bench",
        "dylib",
    );
    host.install(&bench).unwrap();
    let schemas: serde_json::Value = serde_json::from_str(&host.schemas().unwrap()).unwrap();
    assert_eq!(
        schemas.as_array().unwrap().len(),
        2,
        "the image's plugin, then the library"
    );
    assert!(schemas[1].to_string().contains("stable_bench"));
}
