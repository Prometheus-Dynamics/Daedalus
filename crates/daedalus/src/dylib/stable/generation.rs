//! A plugin's loaded builds ([`Generation`]) and the [`PluginSlot`] a stable install's
//! handlers call through, which a reload swaps.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock, Weak};

use super::{StableExtension, StableHandlers};
use crate::dylib::PluginSchema;
use crate::dylib::library::LibraryHandle;

/// One loaded build of a plugin: its stable entry points and the library they live in. Node
/// instances and buffers the build produced hold it; when the last one drops, the plugin's
/// runtime is unloaded and the library closed (unless pinned).
pub struct Generation {
    pub(super) number: u64,
    pub(super) handlers: StableHandlers,
    /// The schema index, in this build, of each node the slot's installs registered.
    pub(super) nodes: Vec<u32>,
    /// `None` for a descriptor of this process (tests).
    library: Option<Arc<LibraryHandle>>,
    /// The build's stable extension, when it exports one (see [`super::StableExtension`]);
    /// without it consts are sent with every call and no services are delivered.
    pub(super) extension: Option<StableExtension>,
}

impl Generation {
    pub fn new(
        number: u64,
        handlers: StableHandlers,
        nodes: Vec<u32>,
        library: Option<Arc<LibraryHandle>>,
        extension: Option<StableExtension>,
    ) -> Self {
        Self {
            number,
            handlers,
            nodes,
            library,
            extension,
        }
    }

    /// Never close this build's library: the host keeps something that points into it.
    pub(super) fn pin(&self) {
        if let Some(library) = &self.library {
            library.pin();
        }
    }
}

impl Drop for Generation {
    fn drop(&mut self) {
        // Safety: the plugin's entry point; every instance of this generation was released
        // (each holds the generation), and the library is still open (dropped after this).
        unsafe { (self.handlers.unload)() };
    }
}

/// What a stable install's handlers call: a plugin's current [`Generation`], swapped in place by
/// a reload. Node instances bound to an older generation are reset on their next call.
pub struct PluginSlot {
    current: RwLock<Arc<Generation>>,
    number: AtomicU64,
    /// Every generation published, to report the live ones.
    published: Mutex<Vec<Weak<Generation>>>,
    /// Each installed node's id and ports (JSON), which a reload must keep.
    interface: Vec<(String, String)>,
}

impl PluginSlot {
    /// A slot for `schema`'s nodes, running `generation` (whose `nodes` index `schema.nodes`).
    pub fn new(schema: &PluginSchema, generation: Generation) -> Self {
        let number = generation.number;
        let generation = Arc::new(generation);
        Self {
            published: Mutex::new(vec![Arc::downgrade(&generation)]),
            current: RwLock::new(generation),
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
    /// instance's next call resets it onto the new one.
    pub fn publish(&self, generation: Generation) {
        let number = generation.number;
        let generation = Arc::new(generation);
        {
            let mut published = self.published.lock().unwrap_or_else(|err| err.into_inner());
            published.retain(|generation| generation.strong_count() > 0);
            published.push(Arc::downgrade(&generation));
        }
        let old = std::mem::replace(
            &mut *self.current.write().unwrap_or_else(|err| err.into_inner()),
            generation,
        );
        self.number.store(number, Ordering::Release);
        drop(old);
    }

    /// The numbers of the generations still loaded (current first).
    pub fn live_generations(&self) -> Vec<u64> {
        let current = self.number();
        let mut live: Vec<u64> = self
            .published
            .lock()
            .unwrap_or_else(|err| err.into_inner())
            .iter()
            .filter_map(Weak::upgrade)
            .map(|generation| generation.number)
            .filter(|number| *number != current)
            .collect();
        live.insert(0, current);
        live
    }
}

fn node_interface(node: &daedalus_ffi_host::core::NodeSchema) -> (String, String) {
    let ports = serde_json::to_string(&(&node.inputs, &node.outputs)).unwrap_or_default();
    (node.id.clone(), ports)
}
