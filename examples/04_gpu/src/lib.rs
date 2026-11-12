//! Shader nodes in a graph, three ways. Each example computes its answer on the CPU as well and
//! compares, and each needs a GPU: without one it is an error, not a skip.

#![forbid(unsafe_code)]

#[cfg(target_os = "linux")]
pub mod dmabuf;
pub mod element;
pub mod mixed;
pub mod upload_download;

use std::sync::Arc;
use std::time::Duration;

use daedalus::layers::types;
use daedalus::prelude::*;
use daedalus_wgpu::{HostBytes, MappedBytes, Options, WgpuBackend};

/// What the examples pass around: 32-bit words. On the host it is this struct, and on the
/// device it is a buffer of the same bytes. A shader's output the host reads stays in the
/// buffer the shader wrote, where the host maps it, and is read there.
#[derive(Clone, Debug, Type)]
#[type_key("gpu:words")]
pub struct Words {
    bytes: Bytes,
}

/// The words' bytes: the host's own, or a buffer's where the shader wrote them. Either way the
/// type is the bytes scalar, as `Vec<u8>` is.
#[derive(Clone, Debug)]
enum Bytes {
    Owned(Vec<u8>),
    Mapped(Arc<MappedBytes>),
}

impl types::Typed for Bytes {
    const KEY: types::TypeKey = <Vec<u8> as types::Typed>::KEY;
    const EXPR: types::TypeExpr = <Vec<u8> as types::Typed>::EXPR;
}

impl PartialEq for Words {
    fn eq(&self, other: &Words) -> bool {
        self.bytes() == other.bytes()
    }
}

impl Eq for Words {}

impl Words {
    pub fn new(words: &[u32]) -> Words {
        Words {
            bytes: Bytes::Owned(words.iter().flat_map(|word| word.to_le_bytes()).collect()),
        }
    }

    /// Whether these are a shader's output read where the shader wrote them.
    pub fn is_in_place(&self) -> bool {
        matches!(self.bytes, Bytes::Mapped(_))
    }

    pub fn to_vec(&self) -> Vec<u32> {
        self.bytes()
            .as_chunks::<4>()
            .0
            .iter()
            .map(|word| u32::from_le_bytes(*word))
            .collect()
    }
}

/// This is all the GPU backend needs to move a `Words` between the host and the device.
impl HostBytes for Words {
    fn bytes(&self) -> &[u8] {
        match &self.bytes {
            Bytes::Owned(bytes) => bytes,
            Bytes::Mapped(bytes) => bytes,
        }
    }

    fn from_bytes(bytes: Vec<u8>) -> Words {
        Words {
            bytes: Bytes::Owned(bytes),
        }
    }

    fn from_mapped(bytes: MappedBytes) -> Words {
        Words {
            bytes: Bytes::Mapped(Arc::new(bytes)),
        }
    }
}

/// The GPU backend, told that `Words` is bytes. Opening the device here makes a machine with
/// no GPU an error before anything is planned.
pub fn gpu() -> Result<WgpuBackend, String> {
    let backend = WgpuBackend::open(Options::default()).with_bytes::<Words>();
    // A frame in a dma-buf: imported in place where the device can, uploaded where it can't.
    #[cfg(target_os = "linux")]
    let backend = backend
        .with_bytes::<dmabuf::Frame>()
        .with_dmabuf::<dmabuf::Frame>();
    backend
        .start()
        .map_err(|error| format!("this example needs a GPU: {error}"))?;
    Ok(backend)
}

/// Plans `graph` for the GPU and the CPU.
pub fn planned(
    graph: &str,
    nodes: &[&'static NodeRecord],
    gpu: &WgpuBackend,
) -> Result<Planned, String> {
    let mut registry = Registry::new();
    registry
        .register_all(nodes)
        .map_err(|errors| format!("{errors:?}"))?;
    let graph = GraphDoc::parse(graph)
        .map_err(|error| error.to_string())?
        .graph;
    plan(
        &graph,
        &registry,
        &[gpu, &daedalus_cpu::CpuBackend],
        &Config::default(),
    )
    .map_err(|error| error.to_string())
}

/// Plans `graph` for the GPU and the CPU, and loads it.
pub fn load(
    graph: &str,
    nodes: &[&'static NodeRecord],
    gpu: &WgpuBackend,
) -> Result<LoadedHost, String> {
    let planned = planned(graph, nodes, gpu)?;
    LoadedHost::load(planned, LoadOptions::default()).map_err(|error| error.to_string())
}

/// Waits until the graph settles, then takes the value at the output port `name`. The device works
/// while the host waits: the host ticks until nothing is left on it, and sleeps between ticks only
/// while a fence is the one thing left. The wait gives up after ten seconds.
pub fn when_ready<T>(
    host: &mut Host<'static>,
    name: &str,
    take: impl Fn(&mut Host<'static>, &str) -> Result<Option<T>, String>,
) -> Result<T, String> {
    host.step_until_settled(Duration::from_secs(10))
        .map_err(|error| error.to_string())?;
    take(host, name)?.ok_or_else(|| format!("nothing arrived at `{name}`"))
}

/// [`when_ready`] for a value of the examples' own type.
pub(crate) fn take_when_ready(host: &mut Host<'static>, name: &str) -> Result<Words, String> {
    when_ready(host, name, |host, name| {
        host.take::<Words>(name).map_err(|error| error.to_string())
    })
}
