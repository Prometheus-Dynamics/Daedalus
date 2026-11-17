//! A run's memory: the descriptor reads a run begins before its nodes run, and the placing of large
//! output values in lent memory. Each SDK calls these around its own node calls, so a value by
//! reference is read and written the same way in every worker.

use daedalus_process_shm::{Reads, View, Writes};
use daedalus_wire::codec::Writer;
use daedalus_wire::{BoundaryValue, Dir, Schema, WireValue, encode_bare, shared};

/// The buffers sent by descriptor that a run's inputs are in, one per place, in input order.
pub fn fd_slots<'a>(values: impl IntoIterator<Item = &'a WireValue>) -> Vec<u32> {
    values
        .into_iter()
        .filter_map(|value| match value {
            WireValue::Placed(shared::Place {
                at: shared::At::Fd(slot),
                ..
            }) => Some(*slot),
            _ => None,
        })
        .collect()
}

/// Runs `body` with the run's inputs readable and room for its outputs. The descriptor reads of
/// `slots` begin first, with the kernel's sync call, and end after `body`; a read that cannot begin
/// is an error, and `body` does not run. `body` gets the reads, the writes, and the threshold below
/// which a value goes in the packet instead.
pub fn with_run<T>(
    view: &mut View<'_>,
    slots: &[u32],
    body: impl FnOnce(&Reads<'_>, &mut Writes<'_>, usize) -> T,
) -> Result<T, String> {
    let mut begun = 0;
    let mut failure = None;
    for slot in slots {
        if let Err(reason) = view.begin_read(*slot) {
            failure = Some(reason);
            break;
        }
        begun += 1;
    }
    let result = match failure {
        Some(reason) => Err(reason),
        None => {
            let threshold = view.threshold();
            let (reads, mut writes) = view.split();
            Ok(body(&reads, &mut writes, threshold))
        }
    };
    for slot in &slots[..begun] {
        view.end_read(*slot);
    }
    result
}

/// Moves each large output value into the run's lent memory, where the host's cell holds it. A value
/// with a form (a bytes value, or one whose encoding is at least `threshold` long) is written
/// there, its items aligned, and the output then carries its place. A value that does not fit, or
/// is small, stays in the packet.
pub fn place_outputs(
    writes: &mut Writes<'_>,
    threshold: usize,
    schema: &Schema,
    outputs: &mut [BoundaryValue],
) {
    for output in outputs {
        let Ok(ty) = schema.lookup(&output.node, Dir::Output, &output.port) else {
            continue;
        };
        if !schema.has_form(ty) {
            continue;
        }
        if let WireValue::Bytes(bytes) | WireValue::Opaque(bytes) = &output.value {
            if bytes.len() >= threshold
                && let Some((place, room)) = writes.room(bytes.len())
            {
                room.copy_from_slice(bytes);
                output.value = WireValue::Placed(place);
            }
            continue;
        }
        let mut bare = Writer::new();
        if encode_bare(ty, &output.value, schema, &mut bare).is_err() || bare.buf.len() < threshold
        {
            continue;
        }
        let items_at = ty.layout().items_at().unwrap_or(0);
        if let Some((place, room)) = writes.room_for(bare.buf.len(), items_at) {
            room.copy_from_slice(&bare.buf);
            output.value = WireValue::Placed(place);
        }
    }
}
