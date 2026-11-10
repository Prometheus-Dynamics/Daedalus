//! Your own recording code: the lowest of the three levels a GPU node is written at.
//!
//! The body is a function that records into the region's encoder. It can open any passes it
//! likes, with pipelines of its own. It never submits: the region does, once, for every node in
//! it. Daedalus orders the node and gives it its buffers, and nothing more.

use crate::buffer::Storage;

/// What a recording body is given for one run.
pub struct RecordCx<'a> {
    pub(crate) device: &'a wgpu::Device,
    pub(crate) context: &'a std::sync::Arc<crate::context::Context>,
    pub(crate) encoder: &'a mut wgpu::CommandEncoder,
    pub(crate) inputs: &'a [Option<&'a Storage>],
    pub(crate) outputs: &'a [Option<&'a Storage>],
}

impl<'a> RecordCx<'a> {
    /// The device the region is on, for making pipelines and bind groups. A body that makes
    /// them keeps them itself, and makes them once.
    pub fn device(&self) -> &wgpu::Device {
        self.device
    }

    /// The region's encoder. Record into it, and don't finish or submit it.
    pub fn encoder(&mut self) -> &mut wgpu::CommandEncoder {
        self.encoder
    }

    /// The buffer on the node's input port of this index, in the order the node declares them.
    pub fn input(&self, port: usize) -> Option<&'a Storage> {
        self.inputs.get(port).copied().flatten()
    }

    /// The buffer to write for the node's output port of this index.
    pub fn output(&self, port: usize) -> Option<&'a Storage> {
        self.outputs.get(port).copied().flatten()
    }
}

/// A recording body: the function, and how large its outputs are.
#[derive(Clone, Copy, Debug)]
pub struct RecordBody {
    /// Records the node's work. An error faults the region's step, and nothing is submitted.
    pub record: fn(&mut RecordCx<'_>) -> Result<(), String>,
    /// The bytes of output `output`, given the bytes of each input. Called before `record`.
    pub output_bytes: fn(inputs: &[u64], output: usize) -> u64,
}

fn like_the_first_input(inputs: &[u64], _: usize) -> u64 {
    inputs.first().copied().unwrap_or(0)
}

impl RecordBody {
    /// A body whose outputs are each as large as its first input.
    pub const fn new(record: fn(&mut RecordCx<'_>) -> Result<(), String>) -> RecordBody {
        RecordBody {
            record,
            output_bytes: like_the_first_input,
        }
    }

    /// The same body with its own rule for how large each output is.
    pub const fn sized(self, output_bytes: fn(inputs: &[u64], output: usize) -> u64) -> RecordBody {
        RecordBody {
            record: self.record,
            output_bytes,
        }
    }
}
