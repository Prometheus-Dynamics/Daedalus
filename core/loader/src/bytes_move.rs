//! Bytes a node linked into the host made, handed to a plugin's node as they are.
//!
//! A `Vec<u8>` in `cpu.host` is a Rust value in the host's memory, and its cell form, what a
//! plugin's node reads, is its bytes alone. So the move puts a record in the plugin's cell that
//! points at the list's own bytes and holds the list's count: nothing is copied, and the
//! producer gets its list back to refill once the plugin lets go. The record comes from a pool
//! the move keeps, and its release is the host's own code, so it keeps no plugin open.
//!
//! Only bytes move this way: any other type's cell form is not the value's memory, and it
//! reaches a plugin through `cpu.wire`, written once by the node that makes it.

use std::borrow::Cow;
use std::sync::Arc;

use daedalus_abi::{
    BytesC, OWNER_HANDED, OwnedStep, Slot, SlotState, Slots, Step, StepCx, StepEnd,
};
use daedalus_backend::{CompileCx, Refusal, Transfer, TransferCost};
use daedalus_runner::{Carried, put, take_typed};
use daedalus_types::Scalar;

use crate::adapter::PLUGIN_SPACE;
use crate::owned::Holder;

/// The CPU backend's space whose cells hold Rust values in the host's memory.
pub const CPU_HOST: &str = "cpu.host";

pub const BYTES_TO_PLUGIN: &str = "plugin.bytes_from_cpu_host";

/// Records a move keeps for its hand-overs: as many as a plugin's backend keeps.
const RECORDS: usize = 16;

/// The move of bytes from `cpu.host` to the plugin space, for the bytes type alone.
pub(crate) fn transfer() -> Transfer {
    Transfer {
        id: BYTES_TO_PLUGIN,
        key: Some(Scalar::Bytes.key()),
        from: Cow::Borrowed(CPU_HOST),
        to: Cow::Borrowed(PLUGIN_SPACE),
        cost: TransferCost::Handoff,
        make: hand_bytes,
    }
}

fn hand_bytes(_: &CompileCx<'_>) -> Result<Step, Refusal> {
    Ok(Step::boxed(HandBytes {
        holder: Holder::host(RECORDS),
    }))
}

struct HandBytes {
    holder: Arc<Holder>,
}

impl OwnedStep for HandBytes {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let cells = slots.as_mut_slice();
        let [input, out] = cells else {
            cx.fault(None, "a plugin move has one input and one output");
            return StepEnd::Fault;
        };
        if input.state != SlotState::Full as u32 {
            cx.fault(None, "a plugin move was given no value");
            return StepEnd::Fault;
        }
        let Some(bytes) = held_list(input) else {
            // Bytes already held as bytes, by a record or in the cell: handed on as they are.
            daedalus_abi::forward(out, 0);
            return StepEnd::Done;
        };
        let record = self
            .holder
            .take(bytes)
            .unwrap_or_else(|| self.holder.wrap(bytes));
        // SAFETY: the record holds one count, which the cell takes over.
        let fields = unsafe { record.as_ref() };
        put(
            out,
            Slot {
                state: SlotState::Full as u32,
                owner: OWNER_HANDED,
                data: fields.bytes.ptr.cast_mut(),
                len: fields.bytes.len,
                cap: record.as_ptr().expose_provenance(),
                ..Slot::EMPTY
            },
        );
        StepEnd::Done
    }
}

/// The list a typed cell holds, as bytes whose release lets it go: its handle when the producer
/// shares it, or the list itself when the producer left it for one taker. `None` for a cell that
/// holds no list.
fn held_list(input: &mut Slot) -> Option<BytesC> {
    if let Some(list) = take_typed::<Vec<u8>>(input) {
        return Some(BytesC {
            ptr: list.as_ptr(),
            len: list.len(),
            data: Arc::into_raw(list).cast_mut().cast(),
            release: Some(drop_shared),
        });
    }
    let list = take_typed::<Carried<Vec<u8>>>(input)?.take()?;
    let list = Box::new(list);
    Some(BytesC {
        ptr: list.as_ptr(),
        len: list.len(),
        data: Box::into_raw(list).cast(),
        release: Some(drop_owned),
    })
}

/// Lets go of a shared list's count.
///
/// # Safety
/// `data` must be an `Arc<Vec<u8>>` from `Arc::into_raw`, released once.
unsafe extern "C" fn drop_shared(data: *mut ()) {
    // SAFETY: as the caller promises.
    drop(unsafe { Arc::from_raw(data.cast::<Vec<u8>>().cast_const()) });
}

/// Frees a list taken from its carrier.
///
/// # Safety
/// `data` must be a `Box<Vec<u8>>` from `Box::into_raw`, freed once.
unsafe extern "C" fn drop_owned(data: *mut ()) {
    // SAFETY: as the caller promises.
    drop(unsafe { Box::from_raw(data.cast::<Vec<u8>>()) });
}
