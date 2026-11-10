//! An author's own type across the host boundary.
//!
//! The backend's uploads move bytes, and a value of a Rust type has none to show. A type that
//! is bytes underneath says so with [`HostBytes`], and a backend told about it with
//! [`WgpuBackend::with_bytes`] lists two more moves, for that type alone: host to device, which
//! copies the value's bytes into a buffer, and host-readable buffer to host, which makes the
//! value from the buffer's bytes. The planner prefers a move for exactly the type over one for
//! any type, so a CPU node that returns the type feeds a shader, and a shader's output reaches
//! a CPU node or the host as the type, with nothing written by the author but the two
//! functions here.

use std::sync::Arc;

use daedalus_abi::{OwnedStep, Slot, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{CompileCx, Refusal, Transfer, TransferCost};
use daedalus_runner::{UNSTAMPED, put_typed, take_typed, typed_view};
use daedalus_types::Typed;

use crate::backend::WgpuBackend;
use crate::buffer::Storage;
use crate::handoff::on_device;
use crate::memory::{SPACE_DEVICE, SPACE_HOST_MAPPED};
use crate::transfer::{HOST_SPACE, intern};

/// A type whose values are a run of bytes, as far as a device is concerned.
pub trait HostBytes: Typed + Send + Sync + 'static {
    /// The bytes a shader reads.
    fn bytes(&self) -> &[u8];

    /// A value from the bytes of a buffer a shader wrote. A buffer is a whole number of 4-byte
    /// words, so there can be up to three bytes of zeroes after what the shader meant.
    fn from_bytes(bytes: Vec<u8>) -> Self;
}

/// The bytes of the `T` a cell holds, for the upload step.
pub(crate) fn bytes_of<T: HostBytes>(slot: &Slot) -> Option<&[u8]> {
    typed_view::<T>(slot).map(T::bytes)
}

/// Makes a `T` from a buffer the host can read. The readback before it finished, or the runner
/// would not have run this.
struct TypedMap<T> {
    of: std::marker::PhantomData<fn() -> T>,
}

impl<T: HostBytes> OwnedStep for TypedMap<T> {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let mut fault = |reason: &str| {
            cx.fault(None, reason);
            StepEnd::Fault
        };
        let Some(storage) = slots.cell(0).and_then(take_typed::<Storage>) else {
            return fault("a buffer was expected");
        };
        let mut bytes = vec![0; storage.size as usize];
        if storage.read_into(0, &mut bytes).is_err() {
            return fault("the buffer couldn't be read");
        }
        let Some(out) = slots.cell(1) else {
            return fault("a map has no output");
        };
        put_typed(
            out,
            UNSTAMPED.0,
            UNSTAMPED.1,
            Arc::new(T::from_bytes(bytes)),
        );
        StepEnd::Done
    }
}

fn make_map<T: HostBytes>(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(TypedMap::<T> {
        of: std::marker::PhantomData,
    }))
}

/// The two moves for `T` on `backend`'s device.
fn transfers<T: HostBytes>(backend: &WgpuBackend) -> [Transfer; 2] {
    let device = backend.device_name();
    let slot = backend.link().named_as().map_or(0, |named| named.slot);
    let id = |what: &str| {
        let base = match device {
            Some(device) => format!("wgpu.{device}.{what}"),
            None => format!("wgpu.{what}"),
        };
        intern(format!("{base}:{}", T::KEY.as_str()))
    };
    [
        Transfer {
            id: id("upload"),
            key: Some(T::KEY),
            from: std::borrow::Cow::Borrowed(HOST_SPACE),
            to: on_device(device, SPACE_DEVICE),
            cost: TransferCost::Copy,
            make: crate::upload::make_typed::<T>(slot),
        },
        Transfer {
            id: id("map"),
            key: Some(T::KEY),
            from: on_device(device, SPACE_HOST_MAPPED),
            to: std::borrow::Cow::Borrowed(HOST_SPACE),
            cost: TransferCost::Copy,
            make: make_map::<T>,
        },
    ]
}

impl WgpuBackend {
    /// This backend, also moving values of `T` between the host and its device. A `T` that a
    /// CPU node or the host hands a shader is uploaded as its bytes, and a shader's output read
    /// by a CPU node or the host arrives as a `T`. Both are copies.
    #[must_use]
    pub fn with_bytes<T: HostBytes>(self) -> WgpuBackend {
        let more = transfers::<T>(&self);
        self.link().offer(more);
        self
    }
}
