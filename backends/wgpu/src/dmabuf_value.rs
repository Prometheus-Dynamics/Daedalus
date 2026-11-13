//! A type whose values are a dma-buf, as far as a device is concerned.
//!
//! A dma-buf the device can import is used in place: nothing is copied, and the device reads the
//! memory the exporter wrote. [`DmaBufBacked`] says that a type's values are one, once, and
//! [`WgpuBackend::with_dmabuf`] lists the move that imports a value's dma-buf. The move is offered
//! only while the open device imports dma-bufs, and the planner takes it over the byte copy the
//! type's [`HostBytes`](crate::HostBytes) would give, which is what a device that can't import
//! gets. Whoever hands a dma-buf over answers for its contents being readable by a device.
//!
//! The value comes from the host's memory, or from the dma-buf space of `daedalus-dmabuf`, which
//! is where a node that declares its output a dma-buf puts it.

use std::marker::PhantomData;
use std::os::fd::{AsFd, OwnedFd};
use std::sync::Arc;

use daedalus_abi::{OwnedStep, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{CompileCx, Refusal, Transfer, TransferCost};
pub use daedalus_dmabuf::DmaBufBacked;
use daedalus_runner::{UNSTAMPED, put_typed, typed_view};

use crate::backend::WgpuBackend;
use crate::buffer::Storage;
use crate::context::Context;
use crate::handoff::on_device;
use crate::memory::SPACE_DEVICE;
use crate::transfer::{HOST_SPACE, intern};

fn fault(cx: &mut StepCx, reason: &str) -> StepEnd {
    cx.fault(None, reason);
    StepEnd::Fault
}

/// Imports the dma-buf of the `T` in its input cell into device memory, and hands the device
/// buffer on. Nothing is copied: the buffer is the dma-buf.
struct ImportStep<T> {
    /// The device, by its slot: 0 for the default, or a named device's place.
    slot: usize,
    of: PhantomData<fn() -> T>,
}

impl<T: DmaBufBacked> OwnedStep for ImportStep<T> {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let Some(input) = slots.cell(0) else {
            return fault(cx, "an import has no input");
        };
        let Some(value) = typed_view::<T>(input) else {
            return fault(cx, "an import was given a value that is not a dma-buf");
        };
        let Some((dmabuf, want)) = value.dmabuf() else {
            return fault(cx, "an import was given a value with no dma-buf");
        };
        let Ok(fd) = dmabuf.try_clone_to_owned() else {
            return fault(cx, "an import couldn't take the value's dma-buf");
        };
        let Ok(context) = crate::devices::context(self.slot) else {
            return fault(cx, "an import has no device to go to");
        };
        let storage = match import_bytes(&context, fd, want) {
            Ok(storage) => storage,
            Err(error) => return fault(cx, &format!("an import was refused: {error}")),
        };
        let Some(out) = slots.cell(1) else {
            return fault(cx, "an import has no output");
        };
        put_typed(out, UNSTAMPED.0, UNSTAMPED.1, storage);
        StepEnd::Done
    }
}

/// The first `want` bytes of the dma-buf `fd` as a device buffer, with nothing copied. Refused when
/// the device can't import a dma-buf, or needs more memory behind a buffer of `want` bytes than the
/// dma-buf has.
fn import_bytes(context: &Arc<Context>, fd: OwnedFd, want: u64) -> Result<Arc<Storage>, String> {
    use daedalus_vk_sys::Error;
    use daedalus_vk_sys::memory::{dma_buf_bytes_needed, import_dma_buf};
    let refused = |error: Error| match error {
        Error::NoDmaBufImport | Error::NotVulkan => {
            "this device can't import a dma-buf".to_string()
        }
        other => other.to_string(),
    };
    let len = daedalus_dma_sys::DmaBufRef::from_fd(fd.as_fd())
        .len()
        .map_err(|error| format!("the dma-buf's size: {error}"))?;
    let need = dma_buf_bytes_needed(&context.device, want).map_err(refused)?;
    if need > len {
        return Err(Error::TooSmall { have: len, need }.to_string());
    }
    let imported = import_dma_buf(&context.device, fd, want, len, None).map_err(refused)?;
    context.moved.import();
    Ok(Arc::new(Storage::imported(
        Arc::clone(context),
        imported,
        want,
    )))
}

type Make = fn(&CompileCx<'_>) -> Result<Step, Refusal>;

fn import<T: DmaBufBacked, const SLOT: usize>(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(ImportStep::<T> {
        slot: SLOT,
        of: PhantomData,
    }))
}

/// The maker of the import of `T`'s dma-buf, for the device in `slot`.
fn make_import<T: DmaBufBacked>(slot: usize) -> Make {
    match slot {
        1 => import::<T, 1>,
        2 => import::<T, 2>,
        3 => import::<T, 3>,
        _ => import::<T, 0>,
    }
}

impl WgpuBackend {
    /// This backend, also importing a `T`'s dma-buf in place when a CPU node or the host hands a
    /// shader a `T`. Listed only while the open device imports dma-bufs; a device that doesn't
    /// takes the upload of the type's bytes instead, so a `T` should be [`HostBytes`](crate::HostBytes)
    /// as well.
    #[must_use]
    pub fn with_dmabuf<T: DmaBufBacked>(self) -> WgpuBackend {
        let device = self.device_name();
        let slot = self.link().named_as().map_or(0, |named| named.slot);
        let base = match device {
            Some(device) => format!("wgpu.{device}.import"),
            None => "wgpu.import".to_string(),
        };
        // From the host, where a CPU node's value is, and from the dma-buf space, where a node
        // that says its output is a dma-buf puts it. Both import the same dma-buf in place.
        let from = |tag: &str, space: &'static str| Transfer {
            id: intern(format!("{base}{tag}:{}", T::KEY.as_str())),
            key: Some(T::KEY),
            from: std::borrow::Cow::Borrowed(space),
            to: on_device(device, SPACE_DEVICE),
            cost: TransferCost::Import,
            make: make_import::<T>(slot),
        };
        let transfers = [
            from("", HOST_SPACE),
            from(".dmabuf", daedalus_dmabuf::SPACE),
        ];
        self.link().offer_dma(transfers);
        self
    }
}

/// A dma-heap buffer holding `bytes`, with `room` bytes past them for the device's padding, as
/// the fd of its dma-buf. The CPU writes it under a write sync, so a device that imports it reads
/// what was written. The buffer's length is the bytes' own: `room` is only the allocation.
pub fn dma_heap_fd(bytes: &[u8], room: u64) -> Result<std::os::fd::OwnedFd, String> {
    use daedalus_dma_sys::{Access, DmaHeap};
    let heap = DmaHeap::open("system").map_err(|error| error.to_string())?;
    let buf = heap
        .allocate(bytes.len() as u64 + room)
        .map_err(|error| error.to_string())?;
    buf.as_ref()
        .begin_cpu_access(Access::Write)
        .map_err(|error| error.to_string())?;
    let written = buf
        .as_ref()
        .map()
        .and_then(|map| map.write(0, bytes))
        .map_err(|error| error.to_string());
    buf.as_ref()
        .end_cpu_access(Access::Write)
        .map_err(|error| error.to_string())?;
    written?;
    Ok(buf.into_fd())
}
