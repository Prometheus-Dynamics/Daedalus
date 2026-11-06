//! The objects behind the handles: hosts (a plugin registry and its engine) and the graphs
//! compiled from them. One process-wide table, behind one lock; a call holds it for its own
//! work only.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use daedalus::data::model::{TypeExpr, ValueType};
use daedalus::dylib::{PluginLibrary, SHARED_BYTES_KEY, SharedBytes};
use daedalus::engine::{Engine, HostGraph};
use daedalus::runtime::ExecutionTelemetry;
use daedalus::runtime::handler_registry::HandlerRegistry;
use daedalus::runtime::handles::PortId;
use daedalus::runtime::plugins::PluginRegistry;
use daedalus::transport::{FRAME_INTERFACE_KEY, SharedFrame};
use lemnos_abi::{Handle, Slab, Status};

#[cfg(target_os = "linux")]
use crate::frames::FrameObject;

/// A host: the plugin registry graphs compile from, the engine that compiles them, and the
/// plugin libraries installed into the registry (kept loaded while anything may run their code).
pub struct Host {
    pub registry: PluginRegistry,
    pub engine: Engine,
    pub libraries: Vec<PluginLibrary>,
    /// Graphs compiled from this host and not yet released. A host with graphs cannot be
    /// released: their handlers run code from `libraries`.
    pub graphs: usize,
}

/// A compiled graph and the host-side state its calls need.
pub struct Graph {
    pub host: Handle,
    pub graph: HostGraph<HandlerRegistry>,
    pub inputs: Vec<Port>,
    pub outputs: Vec<Port>,
    /// Per input port: the shared vector of the last bytes pushed to it. Reused while the graph
    /// has let go of it, so a steady stream of pushes allocates nothing.
    pub spare: Vec<Option<Arc<Vec<u8>>>>,
    /// Bytes taken from an output but not yet copied out (the caller's buffer was too small),
    /// with their output port index. They come back first on the next take.
    pub stash: Vec<(usize, SharedBytes)>,
    /// Per input port: the frame object of the last dma-buf frame pushed to it. Reused (its
    /// descriptors replaced in place) once the graph has let go of it, so a steady stream of
    /// pushes allocates no frame object.
    #[cfg(target_os = "linux")]
    pub frame_spare: Vec<Option<Arc<FrameObject>>>,
    /// Frames taken from an output but not handed to the caller (a descriptor could not be
    /// made), with their output port index. They come back first on the next take.
    #[cfg(target_os = "linux")]
    pub frame_stash: Vec<(usize, SharedFrame)>,
    /// The last tick's telemetry.
    pub telemetry: Option<ExecutionTelemetry>,
}

/// A host port: its name, the id the engine takes, and the value kind the surface checks.
pub struct Port {
    pub name: String,
    pub id: PortId,
    pub kind: Kind,
}

/// The kinds of value the surface moves. Anything else is listed in the port table but cannot be
/// pushed or taken through this ABI.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Int,
    Float,
    Bool,
    Bytes,
    /// A `daedalus:frame` (v3) port: a dma-buf frame through `push_frame` and `take_frame`.
    Frame,
    Other,
}

impl Kind {
    pub fn of(ty: Option<&TypeExpr>) -> Self {
        match ty {
            Some(TypeExpr::Scalar(ValueType::Int)) => Self::Int,
            Some(TypeExpr::Scalar(ValueType::Float)) => Self::Float,
            Some(TypeExpr::Scalar(ValueType::Bool)) => Self::Bool,
            Some(TypeExpr::Opaque(key)) if key == SHARED_BYTES_KEY => Self::Bytes,
            Some(TypeExpr::Opaque(key)) if key == FRAME_INTERFACE_KEY => Self::Frame,
            _ => Self::Other,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Int => "int",
            Self::Float => "float",
            Self::Bool => "bool",
            Self::Bytes => "bytes",
            Self::Frame => "frame",
            Self::Other => "other",
        }
    }
}

/// Every object the surface holds. `frames` are the leases of taken frames: each holds one
/// reference to its frame until the caller releases it.
pub struct Tables {
    pub hosts: Slab<Host>,
    pub graphs: Slab<Graph>,
    pub frames: Slab<SharedFrame>,
}

static TABLES: Mutex<Tables> = Mutex::new(Tables {
    hosts: Slab::new(),
    graphs: Slab::new(),
    frames: Slab::new(),
});

/// The tables, locked. A panic while locked poisons the mutex; the state is still usable (a
/// panicking entry point reports [`Status::PANIC`] and its handle should be released).
pub fn lock() -> MutexGuard<'static, Tables> {
    TABLES.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Graph {
    /// The index of input port `name`, checked against `kind`.
    pub fn input(&self, name: &str, kind: Kind) -> Result<usize, Status> {
        find(&self.inputs, name, kind)
    }

    /// The index of output port `name`, checked against `kind`.
    pub fn output(&self, name: &str, kind: Kind) -> Result<usize, Status> {
        find(&self.outputs, name, kind)
    }
}

fn find(ports: &[Port], name: &str, kind: Kind) -> Result<usize, Status> {
    let index = ports
        .iter()
        .position(|port| port.name == name)
        .ok_or(Status::NOT_FOUND)?;
    if ports[index].kind == kind {
        Ok(index)
    } else {
        Err(Status::INVALID_ARGUMENT)
    }
}
