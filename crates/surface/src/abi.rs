//! The version handshake: the surface's version, its feature set, and the layout hash.

use lemnos_abi::{AbiVersion, LAYOUT_HASH, Status, guard};

/// The surface's ABI major: a change that breaks callers.
pub const SURFACE_MAJOR: u16 = 1;
/// The surface's ABI minor: additions only. 1 added port indices
/// (`daedalus_surface_graph_input_index`, `_output_index`), the `*_at` pushes and takes, and
/// `daedalus_surface_push_batch`.
pub const SURFACE_MINOR: u16 = 1;

/// Stable-path plugin install (`daedalus_surface_host_install`).
pub const FEATURE_PLUGINS: u64 = 1 << 0;
/// Graph compile, push, tick and take.
pub const FEATURE_GRAPHS: u64 = 1 << 1;
/// Explain and telemetry JSON.
pub const FEATURE_TELEMETRY: u64 = 1 << 2;
/// Built with the `alloc-count` feature (`daedalus_surface_allocations` is exported).
pub const FEATURE_ALLOC_COUNT: u64 = 1 << 3;
/// An image surface (`export_surface!`): every new host has the image's nodes installed through
/// the Rust path.
pub const FEATURE_IMAGE_NODES: u64 = 1 << 4;

/// The feature set of this build (and, for an image, of the library it is linked into).
pub fn feature_set() -> u64 {
    let mut bits = FEATURE_PLUGINS | FEATURE_GRAPHS | FEATURE_TELEMETRY;
    if cfg!(feature = "alloc-count") {
        bits |= FEATURE_ALLOC_COUNT;
    }
    if crate::image::install().is_some() {
        bits |= FEATURE_IMAGE_NODES;
    }
    bits
}

/// The version this surface serves: [`SURFACE_MAJOR`].[`SURFACE_MINOR`] over this build's
/// [`LAYOUT_HASH`].
pub const SURFACE_VERSION: AbiVersion = AbiVersion::new(SURFACE_MAJOR, SURFACE_MINOR, LAYOUT_HASH);

/// Writes the surface's [`AbiVersion`] to `out`. A caller checks it with
/// [`AbiVersion::accepts`] against the version it was built with.
///
/// # Safety
/// `out` must be valid for a write of an `AbiVersion`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_version(out: *mut AbiVersion) -> Status {
    guard(|| {
        if out.is_null() {
            return Status::INVALID_ARGUMENT;
        }
        // Safety: the caller passes a writable `AbiVersion`.
        unsafe { out.write(SURFACE_VERSION) };
        Status::OK
    })
}

/// Writes the feature set ([`feature_set`]) to `out`: a bit mask of the `FEATURE_*` constants.
/// Separate from the version because an `AbiVersion` has no room for it.
///
/// # Safety
/// `out` must be valid for a write of a `u64`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_features(out: *mut u64) -> Status {
    guard(|| {
        if out.is_null() {
            return Status::INVALID_ARGUMENT;
        }
        // Safety: the caller passes a writable `u64`.
        unsafe { out.write(feature_set()) };
        Status::OK
    })
}
