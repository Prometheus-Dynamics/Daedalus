//! Shader nodes in a graph, three ways. Each example computes its answer on the CPU as well and
//! compares, and each needs a GPU: without one it is an error, not a skip.

#![forbid(unsafe_code)]

pub mod fragment;
pub mod mixed;
pub mod upload_download;

use std::time::Duration;

use daedalus::prelude::*;
use daedalus_wgpu::{HostBytes, Options, WgpuBackend};

/// What the examples pass around: 32-bit words. On the host it is this struct, and on the
/// device it is a buffer of the same bytes.
#[derive(Clone, Debug, PartialEq, Eq, Type)]
#[type_key("gpu:words")]
pub struct Words {
    bytes: Vec<u8>,
}

impl Words {
    pub fn new(words: &[u32]) -> Words {
        Words {
            bytes: words.iter().flat_map(|word| word.to_le_bytes()).collect(),
        }
    }

    pub fn to_vec(&self) -> Vec<u32> {
        self.bytes
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
        &self.bytes
    }

    fn from_bytes(bytes: Vec<u8>) -> Words {
        Words { bytes }
    }
}

/// The GPU backend, told that `Words` is bytes. Opening the device here makes a machine with
/// no GPU an error before anything is planned.
pub fn gpu() -> Result<WgpuBackend, String> {
    let backend = WgpuBackend::open(Options::default()).with_bytes::<Words>();
    backend
        .start()
        .map_err(|error| format!("this example needs a GPU: {error}"))?;
    Ok(backend)
}

/// Plans `graph` for the GPU and the CPU, and loads it.
pub fn load(
    graph: &str,
    nodes: &[&'static NodeRecord],
    gpu: &WgpuBackend,
) -> Result<LoadedHost, String> {
    let mut registry = Registry::new();
    registry
        .register_all(nodes)
        .map_err(|errors| format!("{errors:?}"))?;
    let graph = GraphDoc::parse(graph)
        .map_err(|error| error.to_string())?
        .graph;
    let planned = plan(
        &graph,
        &registry,
        &[gpu, &daedalus_cpu::CpuBackend],
        &Config::default(),
    )
    .map_err(|error| error.to_string())?;
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
pub fn take_when_ready(host: &mut Host<'static>, name: &str) -> Result<Words, String> {
    when_ready(host, name, |host, name| {
        host.take::<Words>(name).map_err(|error| error.to_string())
    })
}
