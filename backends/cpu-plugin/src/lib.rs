//! Ships a crate of `#[node]` functions as a plugin. The crate keeps its nodes as they are, is
//! built as a `cdylib`, and adds one line:
//!
//! ```ignore
//! daedalus_cpu_plugin::export_nodes!("acme-nodes", NODES);
//! ```
//!
//! `NODES` is the array of node records the crate registers when it is linked into a host
//! (`&[&NodeRecord]`). The plugin carries the CPU backend with it, as its own backend under the
//! plugin's name, and builds each node's step itself: nothing about the compiler that built the
//! host has to match, only the shapes every plugin is checked for.
//!
//! What a node of such a plugin reads and writes is its type's crossing form, on every port: a
//! number or other plain data of 16 bytes or less in place, bytes by handle, anything else as
//! its wire form (`#[derive(WireForm)]`). A node with a port of a type that has none cannot be
//! built into a step, and the graph that uses it is refused.

#![forbid(unsafe_code)]

use std::any::Any;

use daedalus_abi::Step;
use daedalus_loader::export::{Ask, MadeNode, PluginDef, PortDef};
use daedalus_registry::{NodeRecord, PortDecl, TypeRef};

#[doc(hidden)]
pub use daedalus_loader as __loader;

/// Keeps a value for the life of the process: a plugin's definition is made once.
fn kept<T>(value: T) -> &'static T {
    Box::leak(Box::new(value))
}

/// A port as a plugin's entry says it: its name, its type's key, and whether it may stay unwired.
/// `None` for a port whose type has no key of its own, which an entry cannot name.
fn port(decl: &PortDecl) -> Option<PortDef> {
    let TypeRef::Key(key) = &decl.ty else {
        return None;
    };
    Some(PortDef {
        name: decl.name,
        key: kept(key.as_str().to_owned()),
        optional: decl.optional,
    })
}

fn ports(decls: &[PortDecl]) -> Option<&'static [PortDef]> {
    let ports: Option<Vec<PortDef>> = decls.iter().map(port).collect();
    Some(kept(ports?).as_slice())
}

/// Builds the step of the node record `data` is, for its place in a region and the constants
/// the graph gives it.
fn make(data: &'static (dyn Any + Send + Sync), ask: &Ask<'_>) -> Result<Step, String> {
    let record = data
        .downcast_ref::<&'static NodeRecord>()
        .ok_or("this node is not a node record")?;
    let mut consts = Vec::with_capacity(ask.consts.len());
    for (port, bytes) in ask.consts {
        let value = daedalus_made::lit_from_bytes(bytes).map_err(|error| {
            format!(
                "the constant for port `{port}` of {} can't be read: {error}",
                record.decl.id
            )
        })?;
        consts.push((*port, value));
    }
    daedalus_cpu::plugin::boundary_step(record, ask.position, &consts)
}

/// What a plugin named `name` declares for `records`: one backend, named as the plugin is, and
/// each record as a node whose step the plugin builds. `None` when a node has a port whose type
/// an entry cannot name.
pub fn definition(
    name: &'static str,
    records: &'static [&'static NodeRecord],
) -> Option<&'static PluginDef> {
    let mut made = Vec::with_capacity(records.len());
    for record in records {
        made.push(MadeNode {
            id: record.decl.id,
            source: name,
            backend: name,
            inputs: ports(record.decl.inputs)?,
            outputs: ports(record.decl.outputs)?,
            make,
            data: kept(*record),
        });
    }
    let backends: &'static [&'static str] = kept([name]);
    Some(kept(PluginDef {
        name,
        backends,
        nodes: &[],
        made: kept(made).as_slice(),
        codecs: &[],
        produce: None,
    }))
}

/// Declares this crate a plugin named `$name` that carries the `#[node]` records `$nodes`.
#[macro_export]
macro_rules! export_nodes {
    ($name:expr, $nodes:expr) => {
        struct __DaedalusNodes;

        impl $crate::__loader::export::PluginSpec for __DaedalusNodes {
            fn def() -> &'static $crate::__loader::export::PluginDef {
                static DEF: ::std::sync::OnceLock<&'static $crate::__loader::export::PluginDef> =
                    ::std::sync::OnceLock::new();
                DEF.get_or_init(|| {
                    $crate::definition($name, &$nodes)
                        .expect("every port of an exported node has a type with a key")
                })
            }
        }

        $crate::__loader::export_plugin_spec!(__DaedalusNodes);
    };
}
