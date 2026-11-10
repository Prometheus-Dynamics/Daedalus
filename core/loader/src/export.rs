//! The plugin side: what a plugin's crate uses to declare its nodes and backends. The
//! [`export_plugin!`](crate::export_plugin) macro writes the entry point. Every node's run is
//! wrapped in a panic guard, so a panic in a node is a fault and never crosses the boundary.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use daedalus_abi::{
    BackendTable, LAYOUT_HASH, NodeC, OwnedStep, PluginEntry, PortC, RegionView, Slots, Status,
    Step, StepCx, StepEnd, Text, UnitOut, guard_or,
};
use daedalus_abi::{CanTakeFn, CompileFn, write_out};

/// The most backends one plugin may declare.
pub const MAX_BACKENDS: usize = 8;

/// A node's body. It reads its inputs from the first cells of `Slots` and writes its outputs
/// after them, in declaration order.
pub type RunFn = fn(&mut Slots, &mut StepCx) -> StepEnd;

/// One port: a name, a type key, and whether it may stay unwired.
pub struct PortDef {
    pub name: &'static str,
    pub key: &'static str,
    pub optional: bool,
}

/// One node a plugin declares.
pub struct NodeDef {
    pub id: &'static str,
    pub source: &'static str,
    /// The backend this node's body is for. It must be one of the plugin's backends.
    pub backend: &'static str,
    pub text: &'static str,
    pub inputs: &'static [PortDef],
    pub outputs: &'static [PortDef],
    pub run: RunFn,
}

/// Everything a plugin declares.
pub struct PluginDef {
    pub name: &'static str,
    pub backends: &'static [&'static str],
    pub nodes: &'static [NodeDef],
}

/// Implemented by [`export_plugin!`](crate::export_plugin) for the plugin's own type.
pub trait PluginSpec: 'static {
    fn def() -> &'static PluginDef;
}

/// A node's step on the plugin side: runs the body under a panic guard that names the node.
pub struct NodeStep {
    run: RunFn,
    node: u32,
}

impl OwnedStep for NodeStep {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let run = self.run;
        match catch_unwind(AssertUnwindSafe(|| run(slots, cx))) {
            Ok(end) => end,
            Err(_) => {
                cx.fault(Some(self.node), "node panicked");
                StepEnd::Fault
            }
        }
    }
}

/// Writes the entry for plugin `P`. Called by the generated `daedalus_plugin_entry`.
///
/// # Safety
/// `out` must point at a `PluginEntry` the caller owns and may write. The loader passes one.
pub unsafe fn entry<P: PluginSpec>(host_layout: u64, out: *mut PluginEntry) -> Status {
    if host_layout != LAYOUT_HASH {
        // SAFETY: `out` is writable, and the layout is its first field at offset zero.
        unsafe { ptr::addr_of_mut!((*out).layout).write(LAYOUT_HASH) };
        return Status::VERSION_MISMATCH;
    }
    let def = P::def();
    if def.backends.len() > MAX_BACKENDS {
        return Status::from_errno(22);
    }
    let nodes: Vec<NodeC> = def.nodes.iter().map(node_c).collect();
    let tables: Vec<BackendTable> = def
        .backends
        .iter()
        .enumerate()
        .map(|(index, id)| BackendTable {
            id: Text::of(id),
            can_take: can_tables::<P>()[index],
            compile: compile_tables::<P>()[index],
        })
        .collect();
    let node_ptr = leak(nodes);
    let table_ptr = leak(tables);
    let entry = PluginEntry {
        layout: LAYOUT_HASH,
        name: Text::of(def.name),
        nodes: node_ptr.0,
        node_count: node_ptr.1,
        backends: table_ptr.0,
        backend_count: table_ptr.1,
    };
    // SAFETY: `out` is writable, as this function's contract says.
    unsafe { out.write(entry) };
    Status::OK
}

fn node_c(node: &NodeDef) -> NodeC {
    let inputs = leak(node.inputs.iter().map(port_c).collect::<Vec<_>>());
    let outputs = leak(node.outputs.iter().map(port_c).collect::<Vec<_>>());
    NodeC {
        id: Text::of(node.id),
        source: Text::of(node.source),
        backend: Text::of(node.backend),
        text: Text::of(node.text),
        inputs: inputs.0,
        input_count: inputs.1,
        outputs: outputs.0,
        output_count: outputs.1,
    }
}

fn port_c(port: &PortDef) -> PortC {
    PortC {
        name: Text::of(port.name),
        key: Text::of(port.key),
        optional: port.optional,
    }
}

/// Leaks a vector, so the loader can read it for as long as the library is loaded. An entry is
/// read once per load, so this is a fixed cost per load.
fn leak<T: 'static>(items: Vec<T>) -> (*const T, usize) {
    let slice: &'static mut [T] = Box::leak(items.into_boxed_slice());
    (slice.as_ptr(), slice.len())
}

/// The index of the node in `def`, and the node, for each node of the region the backend `index`
/// takes.
fn taken<'a>(
    def: &'a PluginDef,
    index: usize,
    view: &RegionView,
) -> Option<Vec<(u32, &'a NodeDef)>> {
    let backend = def.backends.get(index)?;
    // SAFETY: the host passes a view whose node texts are valid for this call.
    let ids = unsafe { region_nodes(view) };
    let mut taken = Vec::new();
    for (position, id) in ids.iter().enumerate() {
        // SAFETY: as above.
        let id = unsafe { id.as_str() };
        let node = def.nodes.iter().find(|node| node.id == id)?;
        if node.backend != *backend {
            return None;
        }
        taken.push((position as u32, node));
    }
    Some(taken)
}

/// # Safety
/// `view.nodes` must be valid for `view.node_count` texts for the duration of the call.
unsafe fn region_nodes<'a>(view: &RegionView) -> &'a [Text] {
    if view.node_count == 0 || view.nodes.is_null() {
        return &[];
    }
    // SAFETY: the host passes `node_count` texts at `nodes`, valid for the call.
    unsafe { std::slice::from_raw_parts(view.nodes, view.node_count) }
}

fn can_tables<P: PluginSpec>() -> [CanTakeFn; MAX_BACKENDS] {
    [
        can_at::<P, 0>,
        can_at::<P, 1>,
        can_at::<P, 2>,
        can_at::<P, 3>,
        can_at::<P, 4>,
        can_at::<P, 5>,
        can_at::<P, 6>,
        can_at::<P, 7>,
    ]
}

fn compile_tables<P: PluginSpec>() -> [CompileFn; MAX_BACKENDS] {
    [
        compile_at::<P, 0>,
        compile_at::<P, 1>,
        compile_at::<P, 2>,
        compile_at::<P, 3>,
        compile_at::<P, 4>,
        compile_at::<P, 5>,
        compile_at::<P, 6>,
        compile_at::<P, 7>,
    ]
}

/// # Safety
/// As [`CanTakeFn`]: `view` is a valid view, `needed` is writable, and `out` is null or valid for
/// `cap` bytes.
unsafe extern "C" fn can_at<P: PluginSpec, const I: usize>(
    view: *const RegionView,
    out: *mut u8,
    cap: usize,
    needed: *mut usize,
) -> Status {
    guard_or(Status::PANIC, || {
        if view.is_null() || needed.is_null() {
            return Status::from_errno(22);
        }
        // SAFETY: the host passes a valid view for this call.
        let view = unsafe { &*view };
        let Some(taken) = taken(P::def(), I, view) else {
            return Status::NOT_SUPPORTED;
        };
        let mut flags = vec![0u8; view.node_count];
        for (position, _) in &taken {
            flags[*position as usize] = 1;
        }
        let slot = if out.is_null() {
            None
        } else {
            // SAFETY: the host gives `cap` writable bytes at `out`, for this call.
            Some(unsafe { std::slice::from_raw_parts_mut(out, cap) })
        };
        let written = write_out(&flags, slot);
        // SAFETY: `needed` is writable, as this function's contract says.
        unsafe { needed.write(written.needed) };
        written.status
    })
}

/// # Safety
/// As [`CompileFn`]: `view` is a valid view, `needed` is writable, and `out` is null or valid for
/// `cap` entries.
unsafe extern "C" fn compile_at<P: PluginSpec, const I: usize>(
    view: *const RegionView,
    out: *mut UnitOut,
    cap: usize,
    needed: *mut usize,
) -> Status {
    guard_or(Status::PANIC, || {
        if view.is_null() || needed.is_null() {
            return Status::from_errno(22);
        }
        // SAFETY: the host passes a valid view for this call.
        let view = unsafe { &*view };
        let Some(taken) = taken(P::def(), I, view) else {
            return Status::NOT_SUPPORTED;
        };
        // SAFETY: `needed` is writable, as this function's contract says.
        unsafe { needed.write(taken.len()) };
        if out.is_null() {
            return Status::OK;
        }
        if cap < taken.len() {
            return Status::BUFFER_TOO_SMALL;
        }
        for (slot, (position, node)) in taken.into_iter().enumerate() {
            let step = Step::boxed(NodeStep {
                run: node.run,
                node: position,
            });
            // SAFETY: `slot` is below `cap`, the writable entries the host gave.
            unsafe {
                out.add(slot).write(UnitOut {
                    step,
                    node: position,
                    reserved: 0,
                })
            };
        }
        Status::OK
    })
}

/// Declares a plugin: writes `daedalus_plugin_entry`, which the loader calls.
///
/// ```ignore
/// static NODES: [daedalus_loader::export::NodeDef; 1] = [/* ... */];
/// daedalus_loader::export_plugin!(daedalus_loader::export::PluginDef {
///     name: "example",
///     backends: &["example.cpu"],
///     nodes: &NODES,
/// });
/// ```
#[macro_export]
macro_rules! export_plugin {
    ($def:expr) => {
        struct __DaedalusPluginSpec;

        impl $crate::export::PluginSpec for __DaedalusPluginSpec {
            fn def() -> &'static $crate::export::PluginDef {
                static DEF: $crate::export::PluginDef = $def;
                &DEF
            }
        }

        /// The entry point the loader calls. It checks the layout hash before anything else.
        ///
        /// # Safety
        /// Called by the loader with a `PluginEntry` it owns.
        #[unsafe(no_mangle)]
        pub unsafe extern "C" fn daedalus_plugin_entry(
            host_layout: u64,
            out: *mut $crate::abi::PluginEntry,
        ) -> $crate::abi::Status {
            // SAFETY: the loader passes a `PluginEntry` it owns, as `entry` requires.
            unsafe { $crate::export::entry::<__DaedalusPluginSpec>(host_layout, out) }
        }
    };
}
