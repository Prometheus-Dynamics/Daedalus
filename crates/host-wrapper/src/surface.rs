//! Loading the surface library and its symbol table; the handshake; the message sink.

use std::ffi::c_void;
use std::path::Path;

use lemnos_abi::{AbiVersion, Handle, LAYOUT_HASH, MessageSink, Status};
#[cfg(target_os = "linux")]
use lemnos_abi::{BufferDesc, FenceDesc};
use libloading::Library;

use crate::error::HostError;

/// The surface ABI this crate was built against: major 1, minor 0, the `lemnos-abi` layout hash.
/// A surface accepts it when its own version is at least this (same major, layout equal).
pub const SURFACE_ABI: AbiVersion = AbiVersion::new(1, 0, LAYOUT_HASH);

/// A loaded surface library: the symbol table and the handshake result. The library stays open
/// for as long as this value (and every host and graph borrowed from it) lives.
pub struct Surface {
    pub(crate) api: Api,
    version: AbiVersion,
    features: SurfaceFeatures,
    _library: Library,
}

/// The feature bits a surface was built with (`FEATURE_*` in `daedalus-surface`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SurfaceFeatures(u64);

impl SurfaceFeatures {
    /// The raw bit mask.
    pub fn bits(self) -> u64 {
        self.0
    }

    /// Whether every bit of `bit` is set.
    pub fn contains(self, bit: u64) -> bool {
        self.0 & bit == bit
    }
}

/// The surface's C functions, copied out of the library (the `Library` keeps them valid).
pub(crate) struct Api {
    pub version: unsafe extern "C" fn(*mut AbiVersion) -> Status,
    pub features: unsafe extern "C" fn(*mut u64) -> Status,
    pub host_create: unsafe extern "C" fn(*mut Handle) -> Status,
    pub host_release: unsafe extern "C" fn(Handle) -> Status,
    pub host_install: unsafe extern "C" fn(Handle, *const u8, usize, MessageSink) -> Status,
    pub host_schemas: Out1,
    pub graph_compile:
        unsafe extern "C" fn(Handle, *const u8, usize, *mut Handle, MessageSink) -> Status,
    pub graph_release: unsafe extern "C" fn(Handle) -> Status,
    pub graph_ports: Out1,
    pub graph_explain: Out1,
    pub graph_telemetry: Out1,
    pub push_i64: unsafe extern "C" fn(Handle, *const u8, usize, i64) -> Status,
    pub push_f64: unsafe extern "C" fn(Handle, *const u8, usize, f64) -> Status,
    pub push_bool: unsafe extern "C" fn(Handle, *const u8, usize, u8) -> Status,
    pub push_bytes: unsafe extern "C" fn(Handle, *const u8, usize, *const u8, usize) -> Status,
    pub tick: unsafe extern "C" fn(Handle, MessageSink) -> Status,
    pub take_i64: unsafe extern "C" fn(Handle, *const u8, usize, *mut i64) -> Status,
    pub take_f64: unsafe extern "C" fn(Handle, *const u8, usize, *mut f64) -> Status,
    pub take_bool: unsafe extern "C" fn(Handle, *const u8, usize, *mut u8) -> Status,
    pub take_bytes:
        unsafe extern "C" fn(Handle, *const u8, usize, *mut u8, usize, *mut usize) -> Status,
    #[cfg(target_os = "linux")]
    pub push_frame: unsafe extern "C" fn(
        Handle,
        *const u8,
        usize,
        *const BufferDesc,
        *const FenceDesc,
    ) -> Status,
    #[cfg(target_os = "linux")]
    pub take_frame: unsafe extern "C" fn(
        Handle,
        *const u8,
        usize,
        *mut BufferDesc,
        *mut FenceDesc,
        *mut Handle,
    ) -> Status,
    #[cfg(target_os = "linux")]
    pub frame_release: unsafe extern "C" fn(Handle) -> Status,
}

/// A call that writes JSON into a caller buffer: `(handle, buf, cap, needed)`.
pub(crate) type Out1 = unsafe extern "C" fn(Handle, *mut u8, usize, *mut usize) -> Status;
/// Reads a function out of the library. The caller names the signature through `T`.
fn sym<T: Copy>(library: &Library, name: &'static str) -> Result<T, HostError> {
    let cname = [name.as_bytes(), b"\0"].concat();
    // Safety: `T` is the symbol's signature in `daedalus-surface`; the handshake pins the surface
    // version (and its layout hash) this crate was built against.
    let symbol = unsafe { library.get::<T>(cname.as_slice()) }
        .map_err(|_| HostError::MissingSymbol(name))?;
    Ok(*symbol)
}

impl Surface {
    /// Loads the surface library at `path` and checks its handshake: the version it reports must
    /// accept [`SURFACE_ABI`]. The library's code runs in this process; load only a surface built
    /// by the same release.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, HostError> {
        let path = path.as_ref();
        // Safety: opening a library runs its initializers; the caller names the surface.
        let library = unsafe { Library::new(path) }.map_err(|source| HostError::Load {
            path: path.to_path_buf(),
            source,
        })?;
        let api = Api::load(&library)?;
        let mut version = AbiVersion::new(0, 0, 0);
        // Safety: `version` is a valid out-pointer for the call.
        check("daedalus_surface_version", unsafe {
            (api.version)(&mut version)
        })?;
        if version.accepts(&SURFACE_ABI) != Status::OK {
            return Err(HostError::VersionMismatch {
                found: version,
                wanted: SURFACE_ABI,
            });
        }
        let mut bits = 0u64;
        // Safety: `bits` is a valid out-pointer for the call.
        check("daedalus_surface_features", unsafe {
            (api.features)(&mut bits)
        })?;
        Ok(Self {
            api,
            version,
            features: SurfaceFeatures(bits),
            _library: library,
        })
    }

    /// The version the library reported.
    pub fn version(&self) -> AbiVersion {
        self.version
    }

    /// The feature set the library reported.
    pub fn features(&self) -> SurfaceFeatures {
        self.features
    }
}

impl Api {
    fn load(library: &Library) -> Result<Self, HostError> {
        Ok(Self {
            version: sym(library, "daedalus_surface_version")?,
            features: sym(library, "daedalus_surface_features")?,
            host_create: sym(library, "daedalus_surface_host_create")?,
            host_release: sym(library, "daedalus_surface_host_release")?,
            host_install: sym(library, "daedalus_surface_host_install")?,
            host_schemas: sym(library, "daedalus_surface_host_schemas")?,
            graph_compile: sym(library, "daedalus_surface_graph_compile")?,
            graph_release: sym(library, "daedalus_surface_graph_release")?,
            graph_ports: sym(library, "daedalus_surface_graph_ports")?,
            graph_explain: sym(library, "daedalus_surface_graph_explain")?,
            graph_telemetry: sym(library, "daedalus_surface_graph_telemetry")?,
            push_i64: sym(library, "daedalus_surface_push_i64")?,
            push_f64: sym(library, "daedalus_surface_push_f64")?,
            push_bool: sym(library, "daedalus_surface_push_bool")?,
            push_bytes: sym(library, "daedalus_surface_push_bytes")?,
            tick: sym(library, "daedalus_surface_tick")?,
            take_i64: sym(library, "daedalus_surface_take_i64")?,
            take_f64: sym(library, "daedalus_surface_take_f64")?,
            take_bool: sym(library, "daedalus_surface_take_bool")?,
            take_bytes: sym(library, "daedalus_surface_take_bytes")?,
            #[cfg(target_os = "linux")]
            push_frame: sym(library, "daedalus_surface_push_frame")?,
            #[cfg(target_os = "linux")]
            take_frame: sym(library, "daedalus_surface_take_frame")?,
            #[cfg(target_os = "linux")]
            frame_release: sym(library, "daedalus_surface_frame_release")?,
        })
    }
}

/// Turns a surface status into a result, with no message.
pub(crate) fn check(call: &'static str, status: Status) -> Result<(), HostError> {
    if status.is_ok() {
        Ok(())
    } else {
        Err(HostError::Call {
            call,
            status,
            message: String::new(),
        })
    }
}

/// Turns a surface status into a result, with the message the call wrote to `messages`.
pub(crate) fn check_with(
    call: &'static str,
    status: Status,
    messages: Messages,
) -> Result<(), HostError> {
    if status.is_ok() {
        Ok(())
    } else {
        Err(HostError::Call {
            call,
            status,
            message: messages.into_text(),
        })
    }
}

/// The text a call writes to its [`MessageSink`]: collected in memory, so a failing call can
/// report it.
#[derive(Default)]
pub(crate) struct Messages(Vec<u8>);

impl Messages {
    pub fn new() -> Self {
        Self::default()
    }

    /// A sink that appends to this collection. Valid while `self` is borrowed.
    pub fn sink(&mut self) -> MessageSink {
        MessageSink {
            context: (self as *mut Self).cast::<c_void>(),
            write: Some(collect),
        }
    }

    fn into_text(self) -> String {
        String::from_utf8_lossy(&self.0).into_owned()
    }
}

extern "C" fn collect(context: *mut c_void, text: *const u8, len: usize) {
    if context.is_null() || text.is_null() {
        return;
    }
    // Safety: the surface calls back only with the context of the sink it was given, which
    // `Messages::sink` made from a live `Messages`, and `text` is valid for `len` bytes during
    // the callback.
    unsafe {
        let messages = &mut *context.cast::<Messages>();
        messages
            .0
            .extend_from_slice(core::slice::from_raw_parts(text, len));
    }
}
