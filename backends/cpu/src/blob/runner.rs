//! Runs a loaded plan: one queue per edge, a fixed block of memory, and nothing allocated.

use daedalus_abi::{Slot, Slots, StepCx, StepEnd};
use daedalus_runner::{InlineCell, StepArena, bytes_of, copy_inline, fill_inline, value_of};
use daedalus_types::Typed;

use super::library::{Library, name_hash};
use super::load::{
    self, ABSENT, CONST_LEN, CONST_LINK, EDGE_LEN, HOST_LEN, LINK_LEN, LINK_SHIFT, Layout,
    LoadError, STEP_LEN, get_u16, get_u32, put_u16,
};
use super::{MAX_PORTS, Overflow};

/// Why a tick or a port call stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RunError {
    /// No such port, or a value of another type.
    Port { port: u16 },
    /// A queue with the error policy was full.
    QueueFull { edge: u16 },
    /// A node failed. `code` is the code of its error.
    Node { node: u16, code: u32 },
}

const CELLS: usize = MAX_PORTS * 2;

/// Runs plans over a [`Library`], in an arena of at most 65535 bytes that holds the steps, tables
/// and queues.
pub struct Interpreter<const ARENA: usize> {
    library: &'static Library,
    layout: Option<Layout>,
    arena: StepArena<ARENA>,
}

impl<const ARENA: usize> Interpreter<ARENA> {
    const SIZE_OK: () = assert!(
        ARENA <= u16::MAX as usize,
        "an arena is at most 65535 bytes"
    );

    pub const fn new(library: &'static Library) -> Self {
        let () = Self::SIZE_OK;
        Interpreter {
            library,
            layout: None,
            arena: StepArena::new(),
        }
    }

    /// The arena bytes `blob` needs.
    pub fn required(library: &Library, blob: &[u8]) -> Result<usize, LoadError> {
        Ok(load::check(library, blob)?.size)
    }

    /// Checks `blob` and swaps it in: queues empty and node states new. Called
    /// between ticks, and if it's refused the running plan carries on.
    pub fn load(&mut self, blob: &[u8]) -> Result<(), LoadError> {
        let layout = load::check(self.library, blob)?;
        if layout.size > ARENA {
            return Err(LoadError::Arena {
                needed: layout.size as u32,
            });
        }
        self.layout = None;
        load::lay_out(self.library, blob, &layout, &mut self.arena)?;
        self.layout = Some(layout);
        Ok(())
    }

    pub fn unload(&mut self) {
        self.layout = None;
    }

    pub fn plan_hash(&self) -> Option<u64> {
        self.layout.map(|layout| layout.plan)
    }

    /// Arena bytes the running plan uses.
    pub fn used(&self) -> usize {
        self.layout.map_or(0, |layout| layout.size)
    }

    fn port(&self, name: &str, at: impl Fn(&Layout) -> (u16, u16)) -> Option<u16> {
        let (count, table) = at(self.layout.as_ref()?);
        let data = self.arena.data_ref();
        let hash = name_hash(name);
        (0..count)
            .find(|port| get_u32(data, usize::from(table) + usize::from(*port) * HOST_LEN) == hash)
    }

    /// The number of host input `name`.
    pub fn input(&self, name: &str) -> Option<u16> {
        self.port(name, |layout| (layout.inputs, layout.inputs_at))
    }

    /// The number of host output `name`.
    pub fn output(&self, name: &str) -> Option<u16> {
        self.port(name, |layout| (layout.outputs, layout.outputs_at))
    }

    fn host<T: Typed + Copy>(&self, port: u16, count: u16, table: u16) -> Result<usize, RunError> {
        let bad = RunError::Port { port };
        if port >= count {
            return Err(bad);
        }
        let record = usize::from(table) + usize::from(port) * HOST_LEN;
        let data = self.arena.data_ref();
        let matches = get_u32(data, record + 4) == name_hash(T::KEY.as_str())
            && usize::from(data[record + 8]) == core::mem::size_of::<T>();
        matches.then_some(record).ok_or(bad)
    }

    /// Pushes a value to every edge host input `port` feeds.
    pub fn push<T: Typed + Copy>(&mut self, port: u16, value: T) -> Result<(), RunError> {
        let layout = self.layout.ok_or(RunError::Port { port })?;
        let record = self.host::<T>(port, layout.inputs, layout.inputs_at)?;
        let mut bytes = [0; 16];
        let len = bytes_of(&value, &mut bytes);
        let data = self.arena.data();
        let count = usize::from(data[record + 9]);
        let first = usize::from(get_u16(data, record + 10));
        for at in 0..count {
            let link = usize::from(layout.links_at) + (first + at) * LINK_LEN;
            let edge = get_u16(data, link);
            ring_push(data, &layout, edge, &bytes[..len])?;
        }
        Ok(())
    }

    /// Takes the oldest value on host output `port`.
    pub fn pop<T: Typed + Copy>(&mut self, port: u16) -> Result<Option<T>, RunError> {
        let layout = self.layout.ok_or(RunError::Port { port })?;
        let record = self.host::<T>(port, layout.outputs, layout.outputs_at)?;
        let data = self.arena.data();
        let edge = get_u16(data, record + 10);
        let mut bytes = [0; 16];
        match ring_pop(data, &layout, edge, &mut bytes) {
            Some(len) => Ok(value_of::<T>(&bytes, len)),
            None => Ok(None),
        }
    }

    /// Runs every node once, in order.
    pub fn tick(&mut self) -> Result<(), RunError> {
        self.tick_at(0)
    }

    /// As [`Interpreter::tick`], giving nodes the time in nanoseconds.
    pub fn tick_at(&mut self, now_ns: u64) -> Result<(), RunError> {
        let Some(layout) = self.layout else {
            return Ok(());
        };
        let mut cx = StepCx::new();
        cx.now_ns = now_ns;
        for step in 0..layout.steps {
            self.run_step(&layout, step, &mut cx)?;
        }
        Ok(())
    }

    fn run_step(&mut self, layout: &Layout, step: u16, cx: &mut StepCx) -> Result<(), RunError> {
        let data = self.arena.data();
        let at = usize::from(layout.steps_at) + usize::from(step) * STEP_LEN;
        let (all, reads, writes, required) = (
            data[at] != 0,
            usize::from(data[at + 1]),
            usize::from(data[at + 2]),
            data[at + 3],
        );
        let links = usize::from(layout.links_at) + usize::from(get_u16(data, at + 4)) * LINK_LEN;
        if all && waiting(data, layout, links, reads, required) {
            return Ok(());
        }
        let mut cells = [Slot::EMPTY; CELLS];
        let mut bufs: [InlineCell; CELLS] = [[0; 16]; CELLS];
        let mut ready = true;
        for port in 0..reads {
            let link = get_u16(data, links + port * LINK_LEN);
            let got = read_input(data, layout, link, !all, &mut bufs[port]);
            match got {
                Some(len) => fill_inline(&mut bufs[port], len, &mut cells[port]),
                None => ready &= required & (1 << port) == 0,
            }
        }
        if !ready {
            return Ok(());
        }
        // Room for any node's outputs. Cells past its last output are simply never touched.
        let mut slots = Slots::of(&mut cells[..reads + MAX_PORTS]);
        let Some(end) = self.arena.run(u32::from(step), &mut slots, cx) else {
            return Ok(());
        };
        match end {
            StepEnd::Done => {}
            StepEnd::Skipped | StepEnd::Pending(_) => return Ok(()),
            StepEnd::Fault => {
                return Err(RunError::Node {
                    node: step,
                    code: cx.code,
                });
            }
        }
        let data = self.arena.data();
        for write in 0..writes {
            let link = get_u16(data, links + (reads + write) * LINK_LEN);
            let (port, edge) = (
                usize::from(link >> LINK_SHIFT),
                link & ((1 << LINK_SHIFT) - 1),
            );
            let mut bytes = [0; 16];
            if let Some(len) = copy_inline(&cells[reads + port], &mut bytes) {
                ring_push(data, layout, edge, &bytes[..len])?;
            }
        }
        Ok(())
    }
}

/// Whether a node that waits for all its inputs is still missing one.
fn waiting(data: &[u8], layout: &Layout, links: usize, reads: usize, required: u8) -> bool {
    (0..reads).any(|port| {
        let link = get_u16(data, links + port * LINK_LEN);
        let is_edge = link != ABSENT && link & CONST_LINK == 0;
        is_edge && required & (1 << port) != 0 && ring_len(data, layout, link) == 0
    })
}

fn read_input(
    data: &mut [u8],
    layout: &Layout,
    link: u16,
    drain: bool,
    out: &mut InlineCell,
) -> Option<usize> {
    if link == ABSENT {
        return None;
    }
    if link & CONST_LINK != 0 {
        let at = usize::from(layout.consts_at) + usize::from(link & !CONST_LINK) * CONST_LEN;
        let size = usize::from(data[at]);
        out[..8].copy_from_slice(&data[at + 1..at + 9]);
        return Some(size);
    }
    let got = ring_pop(data, layout, link, out);
    if drain {
        put_u16(data, edge_at(layout, link) + 2, 0);
    }
    got
}

fn edge_at(layout: &Layout, edge: u16) -> usize {
    usize::from(layout.edges_at) + usize::from(edge) * EDGE_LEN
}

fn ring_len(data: &[u8], layout: &Layout, edge: u16) -> u16 {
    get_u16(data, edge_at(layout, edge) + 2)
}

/// `index` wrapped once. There's no division, for chips that have none.
fn wrap(index: usize, capacity: usize) -> usize {
    if index >= capacity {
        index - capacity
    } else {
        index
    }
}

fn ring_push(data: &mut [u8], layout: &Layout, edge: u16, value: &[u8]) -> Result<(), RunError> {
    let at = edge_at(layout, edge);
    let (mut head, mut len) = (
        usize::from(get_u16(data, at)),
        usize::from(get_u16(data, at + 2)),
    );
    let capacity = usize::from(get_u16(data, at + 4));
    let stride = usize::from(data[at + 6]);
    if len == capacity {
        if data[at + 7] == Overflow::DropNewest as u8 {
            return Ok(());
        }
        if data[at + 7] == Overflow::Error as u8 {
            return Err(RunError::QueueFull { edge });
        }
        head = wrap(head + 1, capacity);
        len -= 1;
    }
    let base = usize::from(get_u16(data, at + 8)) + wrap(head + len, capacity) * stride;
    data[base..base + value.len().min(stride)].copy_from_slice(&value[..value.len().min(stride)]);
    put_u16(data, at, head as u16);
    put_u16(data, at + 2, (len + 1) as u16);
    Ok(())
}

fn ring_pop(data: &mut [u8], layout: &Layout, edge: u16, out: &mut InlineCell) -> Option<usize> {
    let at = edge_at(layout, edge);
    let (head, len) = (usize::from(get_u16(data, at)), get_u16(data, at + 2));
    if len == 0 {
        return None;
    }
    let capacity = usize::from(get_u16(data, at + 4));
    let stride = usize::from(data[at + 6]);
    let base = usize::from(get_u16(data, at + 8)) + head * stride;
    out[..stride].copy_from_slice(&data[base..base + stride]);
    put_u16(data, at, wrap(head + 1, capacity) as u16);
    put_u16(data, at + 2, len - 1);
    Some(stride)
}
