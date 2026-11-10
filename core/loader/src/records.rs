//! Plugin nodes as the host keeps them: owned declarations, copied out of the entry at load and
//! joined into a registry through `register_owned`.

use std::path::Path;

use daedalus_abi::{NodeC, PortC, Text};
use daedalus_registry::{OwnedBody, OwnedDecl, OwnedPort, Registry, TypeRef};
use daedalus_types::TypeKey;

use crate::LoadError;

/// Copies one piece of text out of the plugin.
///
/// # Safety
/// The text must be valid UTF-8 while the library it came from is loaded, which the caller holds.
pub(crate) unsafe fn copy_text(
    text: Text,
    path: &Path,
    what: &'static str,
) -> Result<String, LoadError> {
    // SAFETY: the caller holds the library the text came from, and the plugin keeps its text
    // valid for as long as that library is loaded.
    let text = unsafe { text.as_str() };
    if text.is_empty() && what == "node id" {
        return Err(LoadError::BadText {
            path: path.to_path_buf(),
            what,
        });
    }
    Ok(text.to_owned())
}

/// Copies the ports of a node.
///
/// # Safety
/// `ports` must point at `count` valid `PortC`s, and the library they came from must be loaded.
unsafe fn copy_ports(
    ports: *const PortC,
    count: usize,
    path: &Path,
) -> Result<Vec<OwnedPort>, LoadError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    if ports.is_null() {
        return Err(LoadError::BadText {
            path: path.to_path_buf(),
            what: "port list",
        });
    }
    // SAFETY: the entry declares `count` ports at `ports`, valid while the library is loaded.
    let ports = unsafe { std::slice::from_raw_parts(ports, count) };
    ports
        .iter()
        .map(|port| {
            // SAFETY: each port's text belongs to the same loaded library.
            let name = unsafe { copy_text(port.name, path, "port name")? };
            // SAFETY: as above.
            let key = unsafe { copy_text(port.key, path, "port type key")? };
            let key = TypeKey::shared(&key).map_err(|_| LoadError::BadText {
                path: path.to_path_buf(),
                what: "port type key",
            })?;
            Ok(OwnedPort {
                name,
                ty: TypeRef::Key(key),
                optional: port.optional,
            })
        })
        .collect()
}

/// Copies one node of the entry into an owned declaration.
///
/// # Safety
/// `node` must come from an entry whose library is loaded, and stay loaded during this call.
pub(crate) unsafe fn read_node(node: &NodeC, path: &Path) -> Result<OwnedDecl, LoadError> {
    // SAFETY: as this function's contract says.
    let id = unsafe { copy_text(node.id, path, "node id")? };
    // SAFETY: as above.
    let source = unsafe { copy_text(node.source, path, "node source")? };
    // SAFETY: as above.
    let backend = unsafe { copy_text(node.backend, path, "node backend")? };
    // SAFETY: as above.
    let text = unsafe { copy_text(node.text, path, "node body")? };
    // SAFETY: as above.
    let inputs = unsafe { copy_ports(node.inputs, node.input_count, path)? };
    // SAFETY: as above.
    let outputs = unsafe { copy_ports(node.outputs, node.output_count, path)? };
    Ok(OwnedDecl {
        id,
        source,
        inputs,
        outputs,
        body: OwnedBody { backend, text },
    })
}

/// Joins a plugin's declarations into a registry. A record with an id already present must
/// declare the same node; a new body for a known id joins it.
pub(crate) fn join(
    decls: &[OwnedDecl],
    path: &Path,
    registry: &mut Registry,
) -> Result<(), LoadError> {
    for decl in decls {
        registry
            .register_owned(decl.clone())
            .map_err(|error| LoadError::Register {
                path: path.to_path_buf(),
                error,
            })?;
    }
    Ok(())
}
