//! The same graph run two ways over N frames: statically (the core linked into this process, with
//! the `stable_abi` plugin's Rust handlers) and in shared mode (`daedalus-host` loading the surface
//! library, which installs the same plugin through the stable path). See `support/compare.rs` for
//! what is asserted: identical outputs frame by frame, no allocation in the surface on a warm
//! scalar frame, and no more on a bytes frame than the static graph.
//!
//! Unix only: the test reads the surface's allocation counter through `libloading`, and the
//! `alloc-count` surface is built here. The plugin and surface are built by nested cargo calls.

#![cfg(unix)]

#[path = "support/cdylib.rs"]
mod cdylib;
#[path = "support/compare.rs"]
mod compare;

#[test]
fn static_and_shared_modes_agree() {
    let surface = cdylib::build_cdylib("daedalus-surface", "daedalus_surface", "alloc-count");
    let plugin = cdylib::build_cdylib(
        "daedalus-plugins-stable-abi",
        "daedalus_plugins_stable_abi",
        "dylib",
    );
    compare::compare(&surface, Some(&plugin), "shared");
}
