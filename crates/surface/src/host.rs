//! Host lifecycle, plugin install and the node schema.

use std::path::Path;

use daedalus::dylib::{InstallPath, PluginLibrary, registry_plugin_schema};
use daedalus::engine::{Engine, EngineConfig};
use daedalus::runtime::plugins::PluginRegistry;
use lemnos_abi::{Handle, MessageSink, Status};

use crate::ffi::{self, run};
use crate::state::{Host, lock};

/// Creates a host: a plugin registry and an engine with the default configuration. The registry
/// is empty, or, in an image surface (`export_surface!`), has the image's nodes installed through
/// the Rust path; a failing image install is [`Status::IO`], with no message (see
/// [`daedalus_surface_host_create_with`]).
///
/// # Safety
/// `out` must be valid for a write of a `Handle` (a `u64`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_host_create(out: *mut Handle) -> Status {
    // Safety: forwarded.
    unsafe { daedalus_surface_host_create_with(out, MessageSink::NONE) }
}

/// [`daedalus_surface_host_create`], with the reason a failing image install is [`Status::IO`]
/// written to `sink` (surface ABI 1.2).
///
/// # Safety
/// `out` must be valid for a write of a `Handle` (a `u64`).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_host_create_with(
    out: *mut Handle,
    sink: MessageSink,
) -> Status {
    run(|| {
        if out.is_null() {
            return Err(Status::INVALID_ARGUMENT);
        }
        let engine = Engine::new(EngineConfig::default()).map_err(|error| {
            ffi::report(sink, &error);
            Status::IO
        })?;
        let mut registry = PluginRegistry::new();
        let mut image_plugins = Vec::new();
        if let Some(install) = crate::image::install() {
            let before: Vec<String> = registry.plugin_manifests.keys().cloned().collect();
            install(&mut registry).map_err(|error| {
                ffi::report(sink, format_args!("the image install failed: {error}"));
                Status::IO
            })?;
            image_plugins = registry
                .plugin_manifests
                .keys()
                .filter(|id| !before.contains(id))
                .cloned()
                .collect();
        }
        let handle = lock().hosts.insert(Host {
            registry,
            engine,
            libraries: Vec::new(),
            image_plugins,
            graphs: 0,
        })?;
        // Safety: `out` is writable (checked above).
        unsafe { out.write(handle) };
        Ok(())
    })
}

/// Releases a host. [`Status::BUSY`] while graphs compiled from it are alive: their handlers run
/// code from its plugin libraries.
///
/// # Safety
/// Any handle value is accepted; a stale or null one is [`Status::STALE_HANDLE`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_host_release(host: Handle) -> Status {
    run(|| {
        let mut tables = lock();
        if tables.hosts.get(host)?.graphs > 0 {
            return Err(Status::BUSY);
        }
        tables.hosts.remove(host)?;
        Ok(())
    })
}

/// Loads the plugin library at `path` (UTF-8, not NUL-terminated) and installs it into the host's
/// registry through the stable handler path. A missing file is [`Status::NOT_FOUND`], a library
/// the stable ABI does not accept is [`Status::NOT_SUPPORTED`]; the reason goes to `sink`.
///
/// # Safety
/// `path` must be valid for `path_len` bytes. The library's code runs in this process for as
/// long as the host lives: load only trusted plugins.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_host_install(
    host: Handle,
    path: *const u8,
    path_len: usize,
    sink: MessageSink,
) -> Status {
    run(|| {
        // Safety: forwarded.
        let path = Path::new(unsafe { ffi::text(path, path_len) }?);
        let mut tables = lock();
        let host = tables.hosts.get_mut(host)?;
        if !path.exists() {
            ffi::report(
                sink,
                format_args!("no plugin library at {}", path.display()),
            );
            return Err(Status::NOT_FOUND);
        }
        // Safety: the caller vouches for the library (see the function's safety notes).
        let library = unsafe { PluginLibrary::load(path) }.map_err(|error| {
            ffi::report(sink, &error);
            Status::NOT_SUPPORTED
        })?;
        library
            .install_into_as(&mut host.registry, InstallPath::Stable)
            .map_err(|error| {
                ffi::report(sink, &error);
                Status::NOT_SUPPORTED
            })?;
        host.libraries.push(library);
        Ok(())
    })
}

/// Writes the node schemas of the host's plugins as a JSON array: one `PluginSchema` (the
/// `daedalus.plugin` schema the loader reads) per in-image plugin of an image surface, then one
/// per installed library. The needed-size
/// protocol of `ffi::write_out` applies.
///
/// # Safety
/// `needed` must be valid for a write of a `usize`; `buf` must be valid for `cap` bytes when the
/// output fits.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_surface_host_schemas(
    host: Handle,
    buf: *mut u8,
    cap: usize,
    needed: *mut usize,
) -> Status {
    run(|| {
        let json = {
            let tables = lock();
            let host = tables.hosts.get(host)?;
            // The image's plugins first (their schemas from the registry), then the libraries.
            let mut schemas = Vec::with_capacity(host.image_plugins.len() + host.libraries.len());
            for id in &host.image_plugins {
                schemas.push(registry_plugin_schema(&host.registry, id).map_err(|_| Status::IO)?);
            }
            schemas.extend(
                host.libraries
                    .iter()
                    .map(|library| library.schema().clone()),
            );
            serde_json::to_vec(&schemas).map_err(|_| Status::IO)?
        };
        // Safety: forwarded.
        unsafe { ffi::write_out(&json, buf, cap, needed) }
    })
}
