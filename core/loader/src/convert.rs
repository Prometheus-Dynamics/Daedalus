//! A codec's conversion as a step. A plugin's codec joins the registry as four conversion nodes,
//! and the planner puts one in where a graph's types call for it. The plugin's own backend has no
//! step for those: the conversion is the codec's function over bytes, which the host calls here.

use daedalus_abi::{OWNER_HANDED, OwnedStep, SharedBytes, Slot, SlotState, Slots, StepCx, StepEnd};

use crate::codec::{Codec, Direction};

/// The most bytes a cell holds in place. A longer result is handed over behind a count.
const IN_PLACE: usize = 16;
/// `OwnerKind::Inline` in daedalus-value: bytes the runner copies when the step's outputs are
/// published.
const OWNER_INLINE: u32 = 1;

/// Which conversion of `codec` the node `id` is, if it is one of its four.
pub(crate) fn direction_of(codec: &Codec, id: &str) -> Option<Direction> {
    let rest = id.strip_prefix(codec.key())?.strip_prefix('.')?;
    match rest {
        "to_value" => Some(Direction::ToValue),
        "from_value" => Some(Direction::FromValue),
        "to_bytes" => Some(Direction::ToBytes),
        "from_bytes" => Some(Direction::FromBytes),
        _ => None,
    }
}

/// One conversion of one codec: reads the bytes of its input cell, and writes the converted
/// bytes to its output. The codec keeps the plugin's library open while the step lives.
pub(crate) struct Convert {
    codec: Codec,
    direction: Direction,
    /// The node this runs, for a fault to name.
    node: u32,
    /// The converted bytes. A result that fits in a cell is read from here when the outputs
    /// are published, so nothing is allocated for it once this has grown to its size.
    out: Vec<u8>,
}

impl Convert {
    pub(crate) fn new(codec: Codec, direction: Direction, node: u32) -> Convert {
        Convert {
            codec,
            direction,
            node,
            out: Vec::new(),
        }
    }
}

impl OwnedStep for Convert {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        let cells = slots.as_mut_slice();
        let [input, out] = cells else {
            cx.fault(Some(self.node), "a conversion has one input and one output");
            return StepEnd::Fault;
        };
        let Some(bytes) = input.bytes() else {
            // A value that is not bytes was never the codec's to convert: a Rust value in the
            // host's memory has no crossing form.
            cx.fault(
                Some(self.node),
                "a codec converts bytes, and was given none",
            );
            return StepEnd::Fault;
        };
        if self
            .codec
            .convert_into(self.direction, bytes, &mut self.out)
            .is_err()
        {
            cx.fault(Some(self.node), "the plugin's codec refused the value");
            return StepEnd::Fault;
        }
        let filled = Slot {
            state: SlotState::Full as u32,
            ..Slot::EMPTY
        };
        *out = if self.out.len() <= IN_PLACE {
            Slot {
                owner: OWNER_INLINE,
                data: self.out.as_mut_ptr(),
                len: self.out.len(),
                cap: self.out.len(),
                ..filled
            }
        } else {
            // Too long for a cell: the bytes go behind a count, which the last holder frees.
            let held = SharedBytes::from_vec(core::mem::take(&mut self.out));
            let (data, len) = (held.bytes().as_ptr().cast_mut(), held.bytes().len());
            Slot {
                owner: OWNER_HANDED,
                data,
                len,
                cap: held.into_record().as_ptr().expose_provenance(),
                ..filled
            }
        };
        StepEnd::Done
    }
}
