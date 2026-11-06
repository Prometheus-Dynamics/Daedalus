//! A plugin's loaded builds ([`Generation`]) and the [`PluginSlot`] a stable install's
//! handlers call through, which a reload swaps.
//!
//! An old build is closed (its stable runtime unloaded, its library closed) once nothing can run
//! its code: no node instance or buffer holds its [`Generation`], and, for a build that counts
//! them ([`LiveObjectsFn`]), none of its plugin-local objects is alive. The slot keeps the
//! builds a reload retired and checks them at each sweep point: a reload, an install, every call
//! of an installed node, and [`PluginSlot::live_generations`]. No thread polls.
//!
//! A build that handed the host a plugin-local object is unloaded at count 0 but its library is
//! never closed (pinned): the object's last release may run on any host thread, and it still
//! returns through the build's code after its decrement, so unmapping that code could pull it
//! from under a thread still in it. The library's pages stay mapped until the process exits;
//! nothing else of the build is kept, and nothing of it runs.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock, TryLockError};

use super::{LiveObjectsFn, SetConstsFn, SetServicesFn, StableHandlers};
use crate::dylib::PluginSchema;
use crate::dylib::library::LibraryHandle;

/// What the host reads of a build's [`StableExtension`](super::StableExtension), whichever
/// layout it exported: `live_objects` is `None` before version 6.
#[derive(Copy, Clone, Debug)]
pub struct LoadedExtension {
    pub set_consts: SetConstsFn,
    pub set_services: SetServicesFn,
    pub live_objects: Option<LiveObjectsFn>,
}

/// One loaded build of a plugin: its stable entry points and the library they live in. Node
/// instances and buffers the build produced hold it; once none does and its plugin-local
/// objects are gone, the plugin's runtime is unloaded and the library closed (unless pinned).
pub struct Generation {
    pub(super) number: u64,
    pub(super) handlers: StableHandlers,
    /// The schema index, in this build, of each node the slot's installs registered.
    pub(super) nodes: Vec<u32>,
    /// `None` for a descriptor of this process (tests).
    library: Option<Arc<LibraryHandle>>,
    /// The build's stable extension, when it exports one (see [`LoadedExtension`]); without it
    /// consts are sent with every call and no services are delivered.
    pub(super) extension: Option<LoadedExtension>,
    /// The build output a plugin-local object to the host: its library is never closed (see the
    /// module docs).
    lent_locals: AtomicBool,
}

impl Generation {
    pub fn new(
        number: u64,
        handlers: StableHandlers,
        nodes: Vec<u32>,
        library: Option<Arc<LibraryHandle>>,
        extension: Option<LoadedExtension>,
    ) -> Self {
        Self {
            number,
            handlers,
            nodes,
            library,
            extension,
            lent_locals: AtomicBool::new(false),
        }
    }

    /// The build output a plugin-local object, whose last release may run on any host thread:
    /// once retired, the build is unloaded but its library stays mapped (see the module docs).
    pub(super) fn lend_local(&self) {
        if !self.lent_locals.load(Ordering::Relaxed) {
            self.lent_locals.store(true, Ordering::Relaxed);
        }
    }

    /// Never close this build's library: the host keeps something that points into it.
    pub(super) fn pin(&self) {
        if let Some(library) = &self.library {
            library.pin();
        }
    }

    /// Whether the build counts its live plugin-local objects (version 6): the host then
    /// carries one as a plain handle, and the count keeps the build loaded.
    pub(super) fn counts_objects(&self) -> bool {
        self.live_objects_fn().is_some()
    }

    fn live_objects_fn(&self) -> Option<LiveObjectsFn> {
        self.extension?.live_objects
    }

    /// The build's live plugin-local objects; 0 for a build that does not count them (it pins
    /// itself for any object it hands out instead).
    fn live_objects(&self) -> usize {
        // Safety: the build's own entry point (an atomic load); the library is open while
        // `self` is.
        self.live_objects_fn().map_or(0, |live| unsafe { live() })
    }
}

impl Drop for Generation {
    fn drop(&mut self) {
        if self.live_objects() != 0 {
            // Only when the slot itself goes (the host dropped its installs and library) while
            // objects of this build are still held: their releases run this build's code, so it
            // stays loaded as it is.
            self.pin();
            return;
        }
        // Safety: the plugin's entry point; every instance of this generation was released
        // (each holds the generation), and the library is still open (dropped after this).
        unsafe { (self.handlers.unload)() };
        if self.lent_locals.load(Ordering::Relaxed) {
            // The count reached 0, but the release that took it there may still be returning
            // through this build's code on another thread: keep the code mapped.
            self.pin();
        }
    }
}

/// What a stable install's handlers call: a plugin's current [`Generation`], swapped in place by
/// a reload. Node instances bound to an older generation are reset on their next call.
pub struct PluginSlot {
    current: RwLock<Arc<Generation>>,
    number: AtomicU64,
    /// Generations a reload replaced and not yet closed, oldest first (see the module docs).
    retired: Mutex<Vec<Arc<Generation>>>,
    /// `retired.len()`, so a sweep with nothing retired is one atomic load.
    retired_len: AtomicUsize,
    /// Each installed node's id and ports (JSON), which a reload must keep.
    interface: Vec<(String, String)>,
}

impl PluginSlot {
    /// A slot for `schema`'s nodes, running `generation` (whose `nodes` index `schema.nodes`).
    pub fn new(schema: &PluginSchema, generation: Generation) -> Self {
        let number = generation.number;
        Self {
            current: RwLock::new(Arc::new(generation)),
            retired: Mutex::new(Vec::new()),
            retired_len: AtomicUsize::new(0),
            number: AtomicU64::new(number),
            interface: schema.nodes.iter().map(node_interface).collect(),
        }
    }

    pub fn current(&self) -> Arc<Generation> {
        self.current
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .clone()
    }

    /// The current generation's number (one atomic load).
    pub(super) fn number(&self) -> u64 {
        self.number.load(Ordering::Acquire)
    }

    /// The schema index in `schema` of each installed node, or what changed in its interface.
    pub fn node_indices(&self, schema: &PluginSchema) -> Result<Vec<u32>, Vec<String>> {
        let mut changes = Vec::new();
        let indices = self
            .interface
            .iter()
            .filter_map(|(id, ports)| {
                let Some(index) = schema.nodes.iter().position(|node| &node.id == id) else {
                    changes.push(format!("node `{id}` was removed"));
                    return None;
                };
                if node_interface(&schema.nodes[index]).1 != *ports {
                    changes.push(format!("node `{id}` changed its ports"));
                }
                Some(index as u32)
            })
            .collect();
        if changes.is_empty() {
            Ok(indices)
        } else {
            Err(changes)
        }
    }

    /// Make `generation` current: calls already running finish on the old one, and every node
    /// instance's next call resets it onto the new one. The old one is retired, and closed at a
    /// sweep once nothing can run its code.
    pub fn publish(&self, generation: Generation) {
        let number = generation.number;
        let old = std::mem::replace(
            &mut *self.current.write().unwrap_or_else(|err| err.into_inner()),
            Arc::new(generation),
        );
        self.number.store(number, Ordering::Release);
        let mut retired = self.retired.lock().unwrap_or_else(|err| err.into_inner());
        retired.push(old);
        self.sweep_locked(&mut retired);
    }

    /// Close the retired generations nothing can run any more: none of their node instances or
    /// buffers is left (the slot's is the last reference) and none of their plugin-local objects
    /// is alive. Nothing to do is one atomic load; a sweep already running elsewhere is not
    /// waited for.
    pub fn sweep(&self) {
        if self.retired_len.load(Ordering::Acquire) == 0 {
            return;
        }
        let mut retired = match self.retired.try_lock() {
            Ok(retired) => retired,
            Err(TryLockError::Poisoned(err)) => err.into_inner(),
            Err(TryLockError::WouldBlock) => return,
        };
        self.sweep_locked(&mut retired);
    }

    fn sweep_locked(&self, retired: &mut MutexGuard<'_, Vec<Arc<Generation>>>) {
        // A generation only the slot holds runs no code of its own any more, so its count of live
        // objects can only go down: once it reads 0, it stays 0.
        retired.retain(|generation| {
            Arc::strong_count(generation) > 1 || generation.live_objects() != 0
        });
        self.retired_len.store(retired.len(), Ordering::Release);
    }

    /// The numbers of the generations still loaded (current first), after a sweep.
    pub fn live_generations(&self) -> Vec<u64> {
        let mut retired = self.retired.lock().unwrap_or_else(|err| err.into_inner());
        self.sweep_locked(&mut retired);
        let mut live = vec![self.number()];
        live.extend(retired.iter().map(|generation| generation.number));
        live
    }
}

fn node_interface(node: &daedalus_ffi_host::core::NodeSchema) -> (String, String) {
    let ports = serde_json::to_string(&(&node.inputs, &node.outputs)).unwrap_or_default();
    (node.id.clone(), ports)
}
