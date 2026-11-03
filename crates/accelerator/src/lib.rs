//! The accelerator interface: load a program, import or allocate buffers, submit, and wait on a
//! fence. NPUs and custom accelerators implement [`Accelerator`]; graph nodes hold an
//! `Arc<dyn Accelerator>` and never see the driver. This crate is the interface only; no
//! accelerator is implemented here.
//!
//! The shape follows the GPU path (`daedalus-gpu`): handles that release their object when the
//! last clone drops, zero-copy dmabuf import with an acquire fence, and completion as a fence
//! that can be waited on, polled, or exported as a `sync_file` for the next device to wait on
//! without a CPU round trip. See `docs/accelerator-interface.md`.
//!
//! ```
//! use daedalus_accelerator::{Accelerator, Access, Binding, BufferSource, ProgramDesc, Submission};
//!
//! fn run(npu: &dyn Accelerator, model: &[u8], input: &[u8]) -> Result<Vec<u8>, daedalus_accelerator::AcceleratorError> {
//!     let program = npu.load_program(&ProgramDesc::new("rknpu-cmd", model))?;
//!     let info = npu.program_info(&program)?;
//!     let input = npu.buffer(BufferSource::Copy(input))?;
//!     let output = npu.buffer(BufferSource::Allocate { len: info.outputs[0].len })?;
//!     let bindings = [
//!         Binding::new(info.inputs[0].slot, input, Access::Read),
//!         Binding::new(info.outputs[0].slot, output.clone(), Access::Write),
//!     ];
//!     npu.submit(Submission::new(&program, &bindings))?.wait(None)?;
//!     let mut out = vec![0; output.len()];
//!     npu.read(&output, 0, &mut out)?;
//!     Ok(out)
//! }
//! ```

mod error;
mod fence;
mod handle;
mod program;

pub use error::AcceleratorError;
pub use fence::{CpuFence, Fence, FenceHandle, SignaledFence, Wait};
pub use handle::{AcceleratorId, Buffer, BufferHandle, Handle, Program, ProgramHandle, Release};
pub use program::{ProgramDesc, ProgramInfo, SlotInfo, validate_bindings};

/// What an accelerator is and can do; cheap, and must not fail on a machine without the device.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AcceleratorInfo {
    /// The device, as its driver names it (for logs).
    pub name: String,
    /// The family, which keys residency and program formats (for example `rknpu`, `k230-kpu`,
    /// `edgetpu`).
    pub kind: String,
    /// The program formats [`Accelerator::load_program`] takes: our own compiled command streams,
    /// one per device family.
    pub program_formats: Vec<String>,
    /// Whether [`BufferSource::DmaBuf`] imports without a copy.
    pub dmabuf_import: bool,
    /// Whether its fences export a `sync_file` ([`Fence::export_sync_fd`]).
    pub sync_fd_export: bool,
}

/// How a submission uses a bound buffer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Access {
    Read,
    Write,
    ReadWrite,
}

impl Access {
    /// Whether the accelerator reads the buffer.
    pub fn reads(self) -> bool {
        matches!(self, Access::Read | Access::ReadWrite)
    }

    /// Whether the accelerator writes the buffer.
    pub fn writes(self) -> bool {
        matches!(self, Access::Write | Access::ReadWrite)
    }
}

/// Where a buffer's memory comes from.
#[derive(Debug)]
pub enum BufferSource<'a> {
    /// Device memory of `len` bytes, contents unspecified.
    Allocate { len: usize },
    /// Device memory holding a copy of these bytes.
    Copy(&'a [u8]),
    /// An existing dmabuf, used in place. The accelerator takes the fd (and `acquire`, a
    /// `sync_file` the producer signals when the bytes are ready) and closes them when the handle
    /// is released; it waits on `acquire` on the device where it can, else before the first
    /// submission that reads the buffer.
    #[cfg(unix)]
    DmaBuf {
        fd: std::os::fd::OwnedFd,
        offset: u64,
        len: usize,
        acquire: Option<std::os::fd::OwnedFd>,
    },
}

impl BufferSource<'_> {
    /// The buffer's length in bytes.
    pub fn len(&self) -> usize {
        match self {
            BufferSource::Allocate { len } => *len,
            BufferSource::Copy(bytes) => bytes.len(),
            #[cfg(unix)]
            BufferSource::DmaBuf { len, .. } => *len,
        }
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A buffer bound to one of a program's slots for a submission.
#[derive(Clone, Debug)]
pub struct Binding {
    pub slot: u32,
    pub buffer: BufferHandle,
    pub access: Access,
}

impl Binding {
    pub fn new(slot: u32, buffer: BufferHandle, access: Access) -> Self {
        Self {
            slot,
            buffer,
            access,
        }
    }
}

/// One run of a program: its bindings, and what must complete before it starts.
#[derive(Debug)]
pub struct Submission<'a> {
    pub program: &'a ProgramHandle,
    pub bindings: &'a [Binding],
    /// Waited on before the program runs: on the device where the accelerator can import them,
    /// otherwise on the CPU inside `submit`.
    pub waits: Vec<Wait>,
}

impl<'a> Submission<'a> {
    pub fn new(program: &'a ProgramHandle, bindings: &'a [Binding]) -> Self {
        Self {
            program,
            bindings,
            waits: Vec::new(),
        }
    }

    /// Run after `wait` completes.
    pub fn after(mut self, wait: Wait) -> Self {
        self.waits.push(wait);
        self
    }
}

/// An NPU or custom accelerator. Object safe: nodes hold an `Arc<dyn Accelerator>`.
///
/// Handles belong to the accelerator that made them; passing another's returns
/// [`AcceleratorError::ForeignHandle`] (check with [`Handle::check_owner`]).
pub trait Accelerator: Send + Sync {
    /// This instance's identity, stamped on every handle it makes.
    fn id(&self) -> AcceleratorId;

    fn info(&self) -> AcceleratorInfo;

    /// Load (and compile, if the format needs it) a program. Loading is slow; do it once, when
    /// the graph is built, not per tick.
    fn load_program(&self, desc: &ProgramDesc<'_>) -> Result<ProgramHandle, AcceleratorError>;

    /// A loaded program's input and output slots.
    fn program_info(&self, program: &ProgramHandle) -> Result<ProgramInfo, AcceleratorError>;

    /// Allocate, upload or import a buffer.
    fn buffer(&self, source: BufferSource<'_>) -> Result<BufferHandle, AcceleratorError>;

    /// Copy `out.len()` bytes from `buffer` at `offset` to the CPU. Waits for nothing: wait on
    /// the fence of the submission that wrote it first.
    fn read(
        &self,
        buffer: &BufferHandle,
        offset: usize,
        out: &mut [u8],
    ) -> Result<(), AcceleratorError>;

    /// Queue a run of a program and return the fence it signals on completion. Returns once the
    /// work is queued, not when it is done.
    fn submit(&self, submission: Submission<'_>) -> Result<FenceHandle, AcceleratorError>;
}
