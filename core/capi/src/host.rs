//! A host: the nodes it knows and the backends that run them, both from plugins.

use std::sync::Arc;

use daedalus_backend::Backend;
use daedalus_loader::{Loader, Plugin, PluginBackend};
use daedalus_registry::{RegError, Registry, TypeRef};

use crate::Handle;
use crate::guard::{copy_out, fail, guard};
use crate::handles::{Table, lock};
use crate::status::Status;

pub(crate) struct HostEntry {
    pub(crate) registry: Registry,
    loader: Loader,
    /// In the order their plugins were installed, which is the planner's order of preference.
    pub(crate) backends: Vec<Arc<dyn Backend>>,
    /// Graphs compiled on this host and not yet released.
    pub(crate) graphs: usize,
}

pub(crate) static HOSTS: Table<HostEntry> = Table::new(1);

/// The surface's version. A caller built against another major, or another layout, stops.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    /// The hash of every shape that crosses a plugin boundary. A plugin built with another is
    /// refused when it is installed.
    pub layout_hash: u64,
}

/// Text from a caller: `len` bytes at `text`, which must be UTF-8.
///
/// # Safety
/// `text` is null or points to `len` readable bytes that stay as they are for the call.
pub(crate) unsafe fn text<'a>(text: *const u8, len: usize) -> Result<&'a str, Status> {
    if text.is_null() {
        return Err(Status::NULL_POINTER);
    }
    // SAFETY: not null, and the caller says `len` bytes are readable for the call.
    let bytes = unsafe { std::slice::from_raw_parts(text, len) };
    std::str::from_utf8(bytes).map_err(|_| Status::BAD_TEXT)
}

/// Writes the surface's version to `out`.
///
/// # Safety
/// `out` points to a `daedalus_version_t`.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_version(out: *mut Version) -> Status {
    guard(|| {
        if out.is_null() {
            return Status::NULL_POINTER;
        }
        let abi = daedalus_abi::DAEDALUS_ABI;
        // SAFETY: not null, and the caller says it points to a version.
        unsafe {
            out.write(Version {
                major: u32::from(abi.major),
                minor: u32::from(abi.minor),
                layout_hash: abi.layout,
            });
        }
        Status::OK
    })
}

/// Makes a host with no nodes, and writes its handle to `out`.
///
/// # Safety
/// `out` points to a `daedalus_handle_t`.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_host_create(out: *mut Handle) -> Status {
    guard(|| {
        if out.is_null() {
            return Status::NULL_POINTER;
        }
        let handle = HOSTS.insert(HostEntry {
            registry: Registry::new(),
            loader: Loader::new(),
            backends: Vec::new(),
            graphs: 0,
        });
        // SAFETY: not null, and the caller says it points to a handle.
        unsafe { out.write(handle) };
        Status::OK
    })
}

/// Releases a host. One that still has graphs is busy, and stays.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub extern "C" fn daedalus_host_release(host: Handle) -> Status {
    guard(|| {
        let busy = |entry: &HostEntry| (entry.graphs > 0).then_some(Status::BUSY);
        match HOSTS.remove(host, busy) {
            Ok(_) => Status::OK,
            Err(Status::BUSY) => fail(Status::BUSY, "the host still has graphs"),
            Err(status) => status,
        }
    })
}

fn install(entry: &mut HostEntry, path: &str) -> Result<(), String> {
    let plugin: Plugin = entry.loader.open(path).map_err(|error| error.to_string())?;
    // A node an earlier generation of this plugin registered is the same node, and its body
    // now runs from the new library.
    for decl in plugin.nodes() {
        match entry.registry.register_owned(decl.clone()) {
            Ok(()) | Err(RegError::DuplicateBody { .. }) => {}
            Err(error) => return Err(format!("{path}: node `{}`: {error}", decl.id)),
        }
    }
    let plugin = Arc::new(plugin);
    for backend in PluginBackend::all(&plugin) {
        let backend: Arc<dyn Backend> = Arc::new(backend);
        entry.backends.retain(|known| known.id() != backend.id());
        entry.backends.push(backend);
    }
    Ok(())
}

/// Loads the plugin at `path` into the host: its nodes join the host's, and its backends run
/// them. A graph compiled before keeps the library it was compiled with.
///
/// # Safety
/// `path` points to `len` bytes of UTF-8.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_host_install(
    host: Handle,
    path: *const u8,
    len: usize,
) -> Status {
    guard(|| {
        // SAFETY: the caller's contract is this function's.
        let path = match unsafe { text(path, len) } {
            Ok(path) => path,
            Err(status) => return status,
        };
        let entry = match HOSTS.get(host) {
            Ok(entry) => entry,
            Err(status) => return status,
        };
        match install(&mut lock(&entry), path) {
            Ok(()) => Status::OK,
            Err(why) => fail(Status::PLUGIN_FAILED, why),
        }
    })
}

fn type_text(ty: &TypeRef) -> String {
    match ty {
        TypeRef::Key(key) => key.as_str().to_string(),
        TypeRef::Expr(expr) => format!("{expr:?}"),
        TypeRef::Body => String::new(),
    }
}

/// The host's nodes as JSON: `[{"id", "inputs": [{"name", "type"}], "outputs": [..],
/// "backends": [..]}]`, sorted by id.
fn schemas(entry: &HostEntry) -> String {
    let mut nodes: Vec<serde_json::Value> = entry
        .registry
        .entries()
        .map(|node| {
            let ports = |ports: &[daedalus_registry::PortDecl]| -> Vec<serde_json::Value> {
                ports
                    .iter()
                    .map(|port| {
                        serde_json::json!({
                            "name": port.name,
                            "type": type_text(&node.port_type(port)),
                        })
                    })
                    .collect()
            };
            serde_json::json!({
                "id": node.decl.id,
                "inputs": ports(node.decl.inputs),
                "outputs": ports(node.decl.outputs),
                "backends": node.bodies.iter().map(|body| body.backend).collect::<Vec<_>>(),
            })
        })
        .collect();
    nodes.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    serde_json::Value::Array(nodes).to_string()
}

/// The host's nodes, as JSON text with no NUL. Call with a null buffer for the size.
///
/// # Safety
/// `buf` is null or points to `cap` writable bytes, and `len` points to a `size_t`.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_host_schemas(
    host: Handle,
    buf: *mut u8,
    cap: usize,
    len: *mut usize,
) -> Status {
    guard(|| {
        let entry = match HOSTS.get(host) {
            Ok(entry) => entry,
            Err(status) => return status,
        };
        let json = schemas(&lock(&entry));
        // SAFETY: the caller's contract is this function's.
        unsafe { copy_out(json.as_bytes(), buf, cap, len) }
    })
}
