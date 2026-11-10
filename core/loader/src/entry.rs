//! A loaded plugin: its nodes, its backends, and the loader that opened it.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{Arc, Weak};

use daedalus_abi::{
    BackendTable, BoundaryPort, CanTakeFn, CompileFn, OwnedStep, RegionView, Slots, Status, Step,
    StepCx, StepEnd, Text, UnitOut,
};
use daedalus_backend::{PortRef, StepUnit, UnitInfo};
use daedalus_registry::{OwnedDecl, Registry};
use daedalus_types::{NodeIndex, PortIndex};

use crate::generation::Generation;
use crate::open::{LoadError, open_library};
use crate::records::{join, read_node};

/// The regions a backend is asked about: node ids in order, and boundary ports.
pub struct RegionIn<'a> {
    pub nodes: &'a [&'a str],
    pub inputs: &'a [BoundaryPort],
    pub outputs: &'a [BoundaryPort],
}

/// Opens plugins. Loading a name again makes a new generation of it.
#[derive(Default)]
pub struct Loader {
    numbers: BTreeMap<String, u64>,
}

impl Loader {
    pub fn new() -> Loader {
        Loader::default()
    }

    /// Opens the library at `path`, checks its layout, and copies out what it declares.
    pub fn open(&mut self, path: impl AsRef<Path>) -> Result<Plugin, LoadError> {
        let path: PathBuf = path.as_ref().to_path_buf();
        // SAFETY: the caller names a trusted plugin library, as `open` documents.
        let opened = unsafe { open_library(&path)? };
        let name = {
            // SAFETY: the library is loaded and the entry's name is valid text for it.
            unsafe { opened.entry.name.as_str() }.to_owned()
        };
        let number = self.numbers.get(&name).map_or(1, |last| last + 1);
        self.numbers.insert(name.clone(), number);
        let generation = Arc::new(Generation::new(
            name.clone(),
            number,
            path.clone(),
            opened.library,
        ));

        // SAFETY: the entry's nodes and backends are read while the library is held by
        // `generation`, which the loop below keeps alive through every backend it hands out.
        let (nodes, backends) = unsafe { read_entry(&opened.entry, &path, &generation)? };
        Ok(Plugin {
            name,
            generation,
            nodes,
            backends,
        })
    }
}

/// Reads the entry's nodes and backends. Strings are copied; backends keep the generation.
///
/// # Safety
/// `entry` must come from a library that `generation` holds.
unsafe fn read_entry(
    entry: &daedalus_abi::PluginEntry,
    path: &Path,
    generation: &Arc<Generation>,
) -> Result<(Vec<OwnedDecl>, Vec<Backend>), LoadError> {
    let mut nodes = Vec::with_capacity(entry.node_count);
    if entry.node_count > 0 {
        if entry.nodes.is_null() {
            return Err(LoadError::BadText {
                path: path.to_path_buf(),
                what: "node list",
            });
        }
        // SAFETY: the entry declares `node_count` nodes at `nodes`, valid while the library is held.
        let declared = unsafe { std::slice::from_raw_parts(entry.nodes, entry.node_count) };
        for node in declared {
            // SAFETY: each node comes from the entry this function was given.
            nodes.push(unsafe { read_node(node, path)? });
        }
    }
    let mut backends = Vec::with_capacity(entry.backend_count);
    if entry.backend_count > 0 {
        if entry.backends.is_null() {
            return Err(LoadError::BadText {
                path: path.to_path_buf(),
                what: "backend list",
            });
        }
        // SAFETY: the entry declares `backend_count` tables at `backends`, valid while held.
        let tables = unsafe { std::slice::from_raw_parts(entry.backends, entry.backend_count) };
        for table in tables {
            backends.push(Backend::from_table(table, path, generation)?);
        }
    }
    Ok((nodes, backends))
}

/// A plugin that is loaded. Its generation stays open while this or any step from it lives.
pub struct Plugin {
    name: String,
    generation: Arc<Generation>,
    nodes: Vec<OwnedDecl>,
    backends: Vec<Backend>,
}

impl std::fmt::Debug for Plugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Plugin")
            .field("name", &self.name)
            .field("generation", &self.generation.number())
            .finish()
    }
}

impl Plugin {
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Which load of this plugin this is.
    pub fn generation(&self) -> &Generation {
        &self.generation
    }

    /// A handle that stops upgrading once this generation is retired.
    pub fn generation_probe(&self) -> Weak<Generation> {
        Arc::downgrade(&self.generation)
    }

    pub fn nodes(&self) -> &[OwnedDecl] {
        &self.nodes
    }

    pub fn backends(&self) -> &[Backend] {
        &self.backends
    }

    pub fn backend(&self, id: &str) -> Option<&Backend> {
        self.backends.iter().find(|backend| backend.id == id)
    }

    /// A value of this plugin's generation holding `bytes`. The value keeps the library open.
    pub fn share(&self, bytes: &[u8]) -> crate::SharedBytes {
        crate::SharedBytes::from_plugin(&self.generation, bytes)
    }

    /// Joins this plugin's nodes into `registry`, under the rules for a compiled-in record.
    pub fn join(&self, registry: &mut Registry) -> Result<(), LoadError> {
        join(&self.nodes, self.generation.path(), registry)
    }
}

/// A backend a plugin provides, with its tables.
pub struct Backend {
    id: String,
    can_take: CanTakeFn,
    compile: CompileFn,
    generation: Arc<Generation>,
}

impl Backend {
    fn from_table(
        table: &BackendTable,
        path: &Path,
        generation: &Arc<Generation>,
    ) -> Result<Backend, LoadError> {
        // SAFETY: the table's text belongs to the library `generation` holds.
        let id = unsafe { table.id.as_str() }.to_owned();
        if id.is_empty() {
            return Err(LoadError::BadText {
                path: path.to_path_buf(),
                what: "backend id",
            });
        }
        Ok(Backend {
            id,
            can_take: table.can_take,
            compile: table.compile,
            generation: generation.clone(),
        })
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    /// One flag per region node: whether this backend takes it.
    pub fn can_take(&self, region: &RegionIn<'_>) -> Result<Vec<bool>, LoadError> {
        self.with_view(region, |view| {
            let mut needed = 0usize;
            // SAFETY: `view` and its arrays live for this call, and `needed` is writable. A null
            // output asks only for the size.
            let status = unsafe { (self.can_take)(view, ptr::null_mut(), 0, &mut needed) };
            if status != Status::OK {
                return Err(self.refused(status));
            }
            let mut flags = vec![0u8; needed];
            // SAFETY: `flags` holds `needed` bytes, the size the backend asked for.
            let status =
                unsafe { (self.can_take)(view, flags.as_mut_ptr(), flags.len(), &mut needed) };
            if status != Status::OK {
                return Err(self.refused(status));
            }
            Ok(flags.iter().map(|flag| *flag != 0).collect())
        })
    }

    /// Compiles the region's nodes into steps. Each step keeps this generation alive until it is
    /// released.
    pub fn compile(&self, region: &RegionIn<'_>) -> Result<Vec<StepUnit>, LoadError> {
        self.with_view(region, |view| {
            let mut needed = 0usize;
            // SAFETY: as in `can_take`: a null output asks only for the size.
            let status = unsafe { (self.compile)(view, ptr::null_mut(), 0, &mut needed) };
            if status != Status::OK {
                return Err(self.refused(status));
            }
            if needed > region.nodes.len() {
                return Err(self.refused(Status::BUFFER_TOO_SMALL));
            }
            let mut units: Vec<UnitOut> = (0..needed)
                .map(|_| UnitOut {
                    step: noop_step(),
                    node: 0,
                    reserved: 0,
                })
                .collect();
            // SAFETY: `units` holds `needed` writable entries, which the backend fills in full.
            let status =
                unsafe { (self.compile)(view, units.as_mut_ptr(), units.len(), &mut needed) };
            if status != Status::OK {
                // The backend may have written steps into the entries: free them before refusing.
                for unit in units {
                    unit.step.discard();
                }
                return Err(self.refused(status));
            }
            Ok(units
                .into_iter()
                .map(|unit| self.unit(unit, region))
                .collect())
        })
    }

    fn unit(&self, unit: UnitOut, region: &RegionIn<'_>) -> StepUnit {
        let node = unit.node;
        let reads = region
            .inputs
            .iter()
            .filter(|port| port.node == node)
            .map(|port| PortRef {
                node: NodeIndex(port.node),
                port: PortIndex(port.port),
            })
            .collect();
        let writes = region
            .outputs
            .iter()
            .filter(|port| port.node == node)
            .map(|port| PortRef {
                node: NodeIndex(port.node),
                port: PortIndex(port.port),
            })
            .collect();
        StepUnit {
            step: Step::boxed(Pinned {
                step: unit.step,
                _generation: self.generation.clone(),
            }),
            info: UnitInfo {
                covers: vec![NodeIndex(node)],
                reads,
                writes,
                absorbed: Vec::new(),
                may_pend: false,
            },
        }
    }

    /// Builds the view the backend reads, and runs `body` with it. The node texts are kept here,
    /// for the call only.
    fn with_view<R>(&self, region: &RegionIn<'_>, body: impl FnOnce(&RegionView) -> R) -> R {
        let nodes: Vec<Text> = region
            .nodes
            .iter()
            .map(|id| Text {
                ptr: id.as_ptr(),
                len: id.len(),
            })
            .collect();
        let view = RegionView {
            nodes: nodes.as_ptr(),
            node_count: nodes.len(),
            inputs: region.inputs.as_ptr(),
            input_count: region.inputs.len(),
            outputs: region.outputs.as_ptr(),
            output_count: region.outputs.len(),
        };
        body(&view)
    }

    fn refused(&self, status: Status) -> LoadError {
        LoadError::Compile {
            backend: self.id.clone(),
            status,
        }
    }
}

/// A step the loader holds: forwards to the plugin's step and keeps its generation open.
struct Pinned {
    step: Step,
    _generation: Arc<Generation>,
}

impl OwnedStep for Pinned {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        // SAFETY: the step's `run` and `data` belong together, and `_generation` keeps the code
        // loaded. The runner passes exclusive `slots` and `cx` for this call.
        unsafe { (self.step.run)(self.step.data, slots, cx) }
    }
}

impl Drop for Pinned {
    fn drop(&mut self) {
        if let Some(release) = self.step.release {
            // SAFETY: `release` runs once, on the `data` it was stored with, while the library
            // is still held by `_generation`, which drops after this.
            unsafe { release(self.step.data) }
        }
    }
}

/// A placeholder for a unit the backend has not written yet.
fn noop_step() -> Step {
    Step {
        run: noop_run,
        data: ptr::null_mut(),
        release: None,
    }
}

// SAFETY: this function takes no pointer it reads, and returns a constant.
unsafe extern "C" fn noop_run(_: *mut (), _: *mut Slots, _: *mut StepCx) -> StepEnd {
    StepEnd::Skipped
}
