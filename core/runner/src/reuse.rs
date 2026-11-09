//! Output reuse for pure nodes. A reusable step remembers the version of each input at its last
//! successful run and a handle to each output. When every version matches, the outputs are pushed
//! again by handle, with no call and no copy.

use daedalus_abi::{Slot, SlotState};
use daedalus_plan::Span;

use crate::error::{LoadError, RunError};
use crate::exec::{Env, commit_inputs, load_cells};
use crate::owned::{self, InlineCell};
use crate::step::StepMeta;
use crate::{Runner, spanned};

/// A node's reusable marker, as the registry holds it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReuseMarker {
    /// The node has no marker.
    Absent,
    Bool(bool),
    /// The marker holds something other than a boolean.
    Other,
}

/// Why a node may not opt into reuse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReuseReason {
    /// The marker is not a boolean.
    NotBoolean,
    /// The node runs on a device backend, whose outputs live outside this process's memory.
    Device,
    /// The node has a host-bridge port, so its inputs are not the only thing it depends on.
    HostBridge,
}

/// A refusal at load, naming the node and the reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReuseRefused<'a> {
    pub node: &'a str,
    pub reason: ReuseReason,
}

/// Decides whether a node's reuse opt-in holds. Returns true when the node is reusable.
pub fn check_reuse<'a>(
    node: &'a str,
    marker: ReuseMarker,
    device: bool,
    host_bridge: bool,
) -> Result<bool, ReuseRefused<'a>> {
    let opted = match marker {
        ReuseMarker::Absent | ReuseMarker::Bool(false) => return Ok(false),
        ReuseMarker::Bool(true) => true,
        ReuseMarker::Other => {
            return Err(ReuseRefused {
                node,
                reason: ReuseReason::NotBoolean,
            });
        }
    };
    let reason = if device {
        ReuseReason::Device
    } else if host_bridge {
        ReuseReason::HostBridge
    } else {
        return Ok(opted);
    };
    Err(ReuseRefused { node, reason })
}

/// How one slot stamps a reusable step's run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct InputVersion {
    host: bool,
    version: Option<u64>,
}

/// One step's part of the reuse table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReuseEntry {
    armed: bool,
    stamps: Span,
    stored: Span,
}

impl ReuseEntry {
    pub const EMPTY: ReuseEntry = ReuseEntry {
        armed: false,
        stamps: Span { start: 0, len: 0 },
        stored: Span { start: 0, len: 0 },
    };
}

/// The buffers of a reuse table, supplied by the caller. [`Runner::reuse_needs`] gives their sizes.
pub struct ReuseMemory<'a> {
    /// One per step.
    pub entries: &'a mut [ReuseEntry],
    /// One per input of a reusable step.
    pub stamps: &'a mut [u64],
    /// One per output of a reusable step.
    pub stored: &'a mut [Slot],
    /// One per stored output, for inline values.
    pub inline: &'a mut [InlineCell],
    /// One per slot.
    pub inputs: &'a mut [InputVersion],
}

pub(crate) type Table<'a> = ReuseMemory<'a>;

/// The table as raw pointers. A step touches only its own entry, stamps and stored outputs, so
/// parallel lanes never share one.
#[derive(Clone, Copy)]
pub(crate) struct ReuseParts {
    entries: *mut ReuseEntry,
    stamps: *mut u64,
    stored: *mut Slot,
    inline: *mut InlineCell,
    inputs: *const InputVersion,
}

impl ReuseMemory<'_> {
    pub(crate) fn parts(&mut self) -> ReuseParts {
        ReuseParts {
            entries: self.entries.as_mut_ptr(),
            stamps: self.stamps.as_mut_ptr(),
            stored: self.stored.as_mut_ptr(),
            inline: self.inline.as_mut_ptr(),
            inputs: self.inputs.as_ptr(),
        }
    }
}

/// A host input with no declared version compares by identity: the same payload re-fed is the same
/// input.
fn identity(cell: &Slot) -> u64 {
    let address = cell.data.addr() as u64;
    (address.rotate_left(17) ^ cell.len as u64)
        .wrapping_mul(0x9e37_79b9_7f4a_7c15)
        .wrapping_add(u64::from(cell.owner) ^ u64::from(cell.type_index))
}

fn stamp_of(input: InputVersion, cell: &Slot) -> u64 {
    match input {
        InputVersion { host: false, .. } => u64::from(cell.version),
        InputVersion {
            version: Some(version),
            ..
        } => version,
        InputVersion { .. } => identity(cell),
    }
}

/// Compares each input with the last run's stamp. On a match, pushes the stored outputs again and
/// returns true: the body isn't called, and the run counts as a success.
pub(crate) fn replay<'t, E: Env<'t>>(
    env: &mut E,
    index: usize,
    meta: StepMeta,
) -> Result<bool, RunError> {
    let Some(parts) = env.reuse() else {
        return Ok(false);
    };
    env.counters().reuse_checks += 1;
    // SAFETY: `index` is this step's, and no other step touches this entry.
    let entry = unsafe { *parts.entries.add(index) };
    if !entry.armed {
        return Ok(false);
    }
    load_cells(env, meta);
    let table = env.table();
    for (at, &slot) in spanned(table, meta.inputs).iter().enumerate() {
        if slot == daedalus_abi::NO_SLOT {
            continue;
        }
        let slot = slot as usize;
        let cell = env.parts().0[at];
        // SAFETY: `slot` was range-checked at load, and inputs are not written during a tick.
        let input = unsafe { *parts.inputs.add(slot) };
        // SAFETY: `entry.stamps` was sized by `set_reuse` to this step's input count.
        let stamp = unsafe { *parts.stamps.add(entry.stamps.start as usize + at) };
        if stamp != stamp_of(input, &cell) {
            return Ok(false);
        }
    }
    env.counters().reuse_replays += 1;
    env.health(index).on_success();
    commit_inputs(env, meta);
    for (at, &slot) in spanned(table, meta.outputs).iter().enumerate() {
        if slot == daedalus_abi::NO_SLOT {
            continue;
        }
        // SAFETY: `entry.stored` was sized by `set_reuse` to this step's output count.
        let held = unsafe { *parts.stored.add(entry.stored.start as usize + at) };
        if held.state != SlotState::Full as u32 {
            continue;
        }
        // SAFETY: the stored handle is live until the step's next store, and only this step
        // touches it.
        unsafe { owned::retain(&held) };
        // SAFETY: `with_queue` holds what protects this queue, and this step is its producer.
        if !env.with_queue(slot as usize, |queue, slots| unsafe {
            slots.replay(queue, held)
        }) {
            // SAFETY: the reference taken above is still ours, because the push refused it.
            unsafe { owned::release(&held) };
            forget(env, index, meta);
            return Err(RunError::Overflow { slot });
        }
    }
    for &to in spanned(table, meta.wake) {
        env.wake(to);
    }
    Ok(true)
}

/// Releases a step's stored handles and disarms it.
pub(crate) fn forget<'t, E: Env<'t>>(env: &mut E, index: usize, meta: StepMeta) {
    let Some(parts) = env.reuse() else { return };
    let _ = meta;
    // SAFETY: `index` is this step's, and no other step touches this entry.
    let entry = unsafe { &mut *parts.entries.add(index) };
    release_stored(entry, parts);
}

fn release_stored(entry: &mut ReuseEntry, parts: ReuseParts) {
    entry.armed = false;
    for at in 0..entry.stored.len as usize {
        // SAFETY: `entry.stored` was sized by `set_reuse`, and only this step touches it.
        let held = unsafe { parts.stored.add(entry.stored.start as usize + at) };
        // SAFETY: as above.
        let cell = unsafe { *held };
        if cell.state == SlotState::Full as u32 {
            // SAFETY: the stored handle owns one reference, released once and overwritten below.
            unsafe { owned::release(&cell) };
        }
        // SAFETY: as above.
        unsafe { held.write(Slot::EMPTY) };
    }
}

/// Records a successful run: stamps its inputs and keeps a handle to each output. It runs before
/// the outputs are published, while the cells still hold them.
pub(crate) fn store<'t, E: Env<'t>>(env: &mut E, index: usize, meta: StepMeta) {
    let Some(parts) = env.reuse() else { return };
    // SAFETY: `index` is this step's, and no other step touches this entry.
    let entry = unsafe { &mut *parts.entries.add(index) };
    release_stored(entry, parts);
    let table = env.table();
    for (at, &slot) in spanned(table, meta.inputs).iter().enumerate() {
        if slot == daedalus_abi::NO_SLOT {
            continue;
        }
        let slot = slot as usize;
        let cell = env.parts().0[at];
        // SAFETY: `slot` was range-checked at load, and inputs are not written during a tick.
        let input = unsafe { *parts.inputs.add(slot) };
        // SAFETY: `entry.stamps` was sized by `set_reuse` to this step's input count.
        unsafe {
            parts
                .stamps
                .add(entry.stamps.start as usize + at)
                .write(stamp_of(input, &cell))
        };
    }
    for at in 0..meta.outputs.len as usize {
        let cell = env.parts().0[crate::exec::output_at(meta, at)];
        if cell.state != SlotState::Full as u32 {
            continue;
        }
        let spot = entry.stored.start as usize + at;
        // SAFETY: `spot` is inside this step's own inline cell.
        let held = owned::place(cell, unsafe { &mut *parts.inline.add(spot) });
        // SAFETY: the cell is Full and still owns its reference, so a second one can be taken.
        unsafe { owned::retain(&held) };
        // SAFETY: `spot` is inside this step's own stored range.
        unsafe { parts.stored.add(spot).write(held) };
    }
    entry.armed = true;
}

/// Gives each stored handle the version its output got when published, so a replay keeps it and
/// a reusable consumer can stay unchanged.
pub(crate) fn stamp_versions<'t, E: Env<'t>>(env: &mut E, index: usize, meta: StepMeta) {
    let Some(parts) = env.reuse() else { return };
    // SAFETY: `index` is this step's, and no other step touches this entry.
    let entry = unsafe { *parts.entries.add(index) };
    if !entry.armed {
        return;
    }
    for (at, &slot) in spanned(env.table(), meta.outputs).iter().enumerate() {
        if slot == daedalus_abi::NO_SLOT {
            continue;
        }
        let version = env.with_queue(slot as usize, |queue, _| queue.version);
        // SAFETY: `entry.stored` was sized by `set_reuse` to this step's output count.
        unsafe { (*parts.stored.add(entry.stored.start as usize + at)).version = version };
    }
}

impl<'a> Runner<'a> {
    /// How many stamps and stored outputs the plan's reusable steps need.
    pub fn reuse_needs(&self) -> (usize, usize) {
        let reusable = || self.meta.iter().filter(|meta| meta.reuse);
        let stamps = reusable().map(|meta| meta.inputs.len as usize).sum();
        let stored = reusable().map(|meta| meta.outputs.len as usize).sum();
        (stamps, stored)
    }

    /// Attaches the reuse table. `host_inputs` are the slots the host feeds: they stamp by host
    /// version or identity, and every other slot stamps by its version. A plan with no reusable
    /// step has no table, and the run path checks one flag per step.
    pub fn set_reuse(
        &mut self,
        table: ReuseMemory<'a>,
        host_inputs: &[u32],
    ) -> Result<(), LoadError> {
        let (stamps, stored) = self.reuse_needs();
        let steps = self.steps.len();
        if table.entries.len() < steps
            || table.stamps.len() < stamps
            || table.stored.len() < stored
            || table.inline.len() < stored
            || table.inputs.len() < self.slots
        {
            return Err(LoadError::Length);
        }
        table.inputs[..self.slots].fill(InputVersion::default());
        for &slot in host_inputs {
            let input = table.inputs.get_mut(slot as usize);
            input.ok_or(LoadError::SlotRange)?.host = true;
        }
        let (mut stamp, mut store) = (0, 0);
        for (index, meta) in self.meta.iter().enumerate() {
            table.entries[index] = ReuseEntry::EMPTY;
            if meta.reuse {
                table.entries[index].stamps = Span {
                    start: stamp,
                    len: meta.inputs.len,
                };
                table.entries[index].stored = Span {
                    start: store,
                    len: meta.outputs.len,
                };
                stamp += meta.inputs.len;
                store += meta.outputs.len;
            }
        }
        table.stored[..stored].fill(Slot::EMPTY);
        self.reuse = Some(table);
        Ok(())
    }

    /// Forgets every stored output and stamp, releasing the handles. A reload does this.
    pub fn clear_reuse(&mut self) {
        let Some(table) = self.reuse.as_mut() else {
            return;
        };
        let parts = table.parts();
        for entry in table.entries.iter_mut() {
            release_stored(entry, parts);
        }
    }

    /// Declares the version of what the host input `slot` delivers, or clears it so the slot
    /// compares by identity. Without a reuse table this does nothing.
    pub fn set_input_version(&mut self, slot: u32, version: Option<u64>) {
        if let Some(input) = self
            .reuse
            .as_mut()
            .and_then(|table| table.inputs.get_mut(slot as usize))
        {
            input.version = version;
        }
    }
}
