//! Checking a stored plan and laying it out. The check touches nothing, so a plan that's refused
//! leaves the running one alone, and it needs no memory beyond the stack.

use daedalus_abi::NO_SLOT;
use daedalus_plan::{MAGIC, PlanView, StepKind, StepRecord, VERSION, crc32};
use daedalus_registry::{Fire, NodeDecl};
use daedalus_runner::{ArenaBody, StepArena, steps_end};

use super::library::{Library, name_hash, type_hash};
use super::{CONST_BIT, EDGE_MASK, MAX_NODES, MAX_PORTS, Overflow, PORT_SHIFT};
use crate::scalar_kind::ScalarKind;

/// Why a plan wasn't loaded. The running plan, if there is one, stays.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum LoadError {
    /// Not a stored plan, or cut short.
    Malformed,
    /// A format this firmware doesn't read.
    Version {
        found: u16,
    },
    /// Corrupt or cut short in transfer or in flash.
    Checksum {
        expected: u32,
        found: u32,
    },
    /// Made for another library.
    Library {
        expected: u64,
        found: u64,
    },
    /// A node names no usable library entry, or has the wrong number of ports.
    Node {
        node: u16,
    },
    /// A required input has no source, or a constant doesn't fit it.
    Input {
        node: u16,
        input: u8,
    },
    /// An edge has no producer or consumer, more than one, or runs backwards.
    Edge {
        edge: u16,
    },
    /// The ends of an edge carry different types.
    Type {
        edge: u16,
    },
    Param {
        param: u16,
    },
    Port {
        port: u16,
    },
    /// The plan needs `needed` bytes of arena.
    Arena {
        needed: u32,
    },
}

pub(super) const EDGE_LEN: usize = 10;
pub(super) const STEP_LEN: usize = 6;
pub(super) const LINK_LEN: usize = 2;
pub(super) const CONST_LEN: usize = 9;
/// A parameter in the arena: its kind, its constant's index, and its lowest and highest bits.
pub(super) const PARAM_LEN: usize = 19;
pub(super) const HOST_LEN: usize = 12;

/// A read with nothing wired.
pub(super) const ABSENT: u16 = u16::MAX;
/// A read that names a constant.
pub(super) const CONST_LINK: u16 = 0x8000;
/// Bits of a write link holding the edge. The port is above them.
pub(super) const LINK_SHIFT: u16 = 13;
const MAX_EDGES: usize = 1 << LINK_SHIFT;

/// Where a plan's tables are, inside the data after the steps.
#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Layout {
    pub plan: u64,
    pub size: usize,
    pub steps: u16,
    pub steps_at: u16,
    pub links_at: u16,
    pub edges_at: u16,
    pub consts_at: u16,
    pub params: u16,
    pub params_at: u16,
    pub inputs: u16,
    pub inputs_at: u16,
    pub outputs: u16,
    pub outputs_at: u16,
}

pub(super) fn get_u16(data: &[u8], at: usize) -> u16 {
    u16::from_le_bytes([data[at], data[at + 1]])
}

pub(super) fn put_u16(data: &mut [u8], at: usize, value: u16) {
    data[at..at + 2].copy_from_slice(&value.to_le_bytes());
}

pub(super) fn get_u32(data: &[u8], at: usize) -> u32 {
    u32::from_le_bytes([data[at], data[at + 1], data[at + 2], data[at + 3]])
}

fn put_u32(data: &mut [u8], at: usize, value: u32) {
    data[at..at + 4].copy_from_slice(&value.to_le_bytes());
}

/// The parts of a stored plan, found once.
struct Stored<'a> {
    view: PlanView<'a>,
    edges: usize,
    consts: usize,
    consts_at: usize,
    params: usize,
    params_at: usize,
    inputs: usize,
    inputs_at: usize,
    outputs: usize,
    outputs_at: usize,
}

impl Stored<'_> {
    fn word(&self, at: usize) -> Result<u32, LoadError> {
        self.view.extra().nth(at).ok_or(LoadError::Malformed)
    }

    fn pair(&self, at: usize) -> Result<u64, LoadError> {
        Ok(u64::from(self.word(at)?) | (u64::from(self.word(at + 1)?) << 32))
    }

    fn edge(&self, edge: usize) -> Result<(u32, u32, u32), LoadError> {
        let at = 3 + edge * 3;
        Ok((self.word(at)?, self.word(at + 1)?, self.word(at + 2)?))
    }

    fn edge_key(&self, edge: usize) -> Result<u32, LoadError> {
        self.view.types().nth(edge).ok_or(LoadError::Malformed)
    }

    fn constant(&self, index: usize) -> Result<(ScalarKind, u64), LoadError> {
        let at = self.consts_at + index * 3;
        let kind = ScalarKind::from_word(self.word(at)?).ok_or(LoadError::Malformed)?;
        Ok((kind, self.pair(at + 1)?))
    }

    /// Calls `each` with every host input's position in `extra` and its edge list's length.
    fn for_inputs(
        &self,
        mut each: impl FnMut(usize, usize, usize) -> Result<(), LoadError>,
    ) -> Result<(), LoadError> {
        let mut at = self.inputs_at;
        for port in 0..self.inputs {
            let count = self.word(at + 2)? as usize;
            each(port, at, count)?;
            at += 3 + count;
        }
        Ok(())
    }

    fn reads(&self, step: &StepRecord) -> Result<daedalus_plan::Words<'_>, LoadError> {
        self.view
            .slot_list(step.reads)
            .map_err(|_| LoadError::Malformed)
    }

    fn writes(&self, step: &StepRecord) -> Result<daedalus_plan::Words<'_>, LoadError> {
        self.view
            .slot_list(step.writes)
            .map_err(|_| LoadError::Malformed)
    }
}

fn open<'a>(library: &Library, blob: &'a [u8]) -> Result<Stored<'a>, LoadError> {
    // The version first, so another format is refused whatever its checksum says.
    if blob.get(..MAGIC.len()) != Some(&MAGIC[..]) {
        return Err(LoadError::Malformed);
    }
    let found = blob.get(8..10).ok_or(LoadError::Malformed)?;
    let found = u16::from_le_bytes([found[0], found[1]]);
    if found != VERSION {
        return Err(LoadError::Version { found });
    }
    let (body, trailer) = blob.split_last_chunk::<4>().ok_or(LoadError::Malformed)?;
    let (expected, found) = (crc32(body), u32::from_le_bytes(*trailer));
    if expected != found {
        return Err(LoadError::Checksum { expected, found });
    }
    let view = PlanView::parse(body).map_err(|_| LoadError::Malformed)?;
    let mut stored = Stored {
        view,
        edges: 0,
        consts: 0,
        consts_at: 0,
        params: 0,
        params_at: 0,
        inputs: 0,
        inputs_at: 0,
        outputs: 0,
        outputs_at: 0,
    };
    let found = stored.pair(0)?;
    if found != library.hash {
        return Err(LoadError::Library {
            expected: library.hash,
            found,
        });
    }
    stored.edges = stored.word(2)? as usize;
    if stored.edges >= MAX_EDGES || stored.view.types().len() != stored.edges {
        return Err(LoadError::Malformed);
    }
    let mut at = 3 + stored.edges * 3;
    stored.consts = stored.word(at)? as usize;
    stored.consts_at = at + 1;
    at += 1 + stored.consts * 3;
    stored.params = stored.word(at)? as usize;
    stored.params_at = at + 1;
    at += 1 + stored.params * 5;
    stored.inputs = stored.word(at)? as usize;
    stored.inputs_at = at + 1;
    at += 1;
    for _ in 0..stored.inputs {
        at += 3 + stored.word(at + 2)? as usize;
    }
    stored.outputs = stored.word(at)? as usize;
    stored.outputs_at = at + 1;
    Ok(stored)
}

fn entry_of(library: &Library, step: &StepRecord) -> Option<(&'static NodeDecl, ArenaBody)> {
    let (decl, body) = library.entry(step.body as usize)?;
    let fits = decl.inputs.len() <= MAX_PORTS && decl.outputs.len() <= MAX_PORTS;
    fits.then_some((decl, body))
}

/// The type name and value size of input `port`.
fn input_port(decl: &NodeDecl, body: &ArenaBody, port: usize) -> Option<(u32, u16)> {
    if let Some(declared) = decl.inputs.get(port) {
        return Some((type_hash(&declared.ty)?, *body.inputs.get(port)?));
    }
    Some((type_hash(&decl.fanin.first()?.ty)?, body.fan?))
}

fn check_input(
    stored: &Stored<'_>,
    decl: &NodeDecl,
    body: &ArenaBody,
    (node, port): (u16, usize),
    link: u32,
) -> Result<(), LoadError> {
    let bad = LoadError::Input {
        node,
        input: port as u8,
    };
    let (key, size) = input_port(decl, body, port).ok_or(LoadError::Node { node })?;
    if link == NO_SLOT {
        let required = decl.inputs.get(port).is_some_and(|input| !input.optional);
        return if required { Err(bad) } else { Ok(()) };
    }
    if link & CONST_BIT != 0 {
        let index = (link & !CONST_BIT) as usize;
        if index >= stored.consts {
            return Err(bad);
        }
        let (kind, _) = stored.constant(index)?;
        return if name_hash(kind.key()) == key && kind.size() == size {
            Ok(())
        } else {
            Err(bad)
        };
    }
    check_end(stored, link as usize, key, size)
}

fn check_end(stored: &Stored<'_>, edge: usize, key: u32, size: u16) -> Result<(), LoadError> {
    let bad = edge as u16;
    if edge >= stored.edges {
        return Err(LoadError::Edge { edge: bad });
    }
    if stored.edge_key(edge)? != key || stored.edge(edge)?.2 != u32::from(size) {
        return Err(LoadError::Type { edge: bad });
    }
    Ok(())
}

fn check_step(
    library: &Library,
    stored: &Stored<'_>,
    node: u16,
    step: &StepRecord,
) -> Result<(), LoadError> {
    let bad = LoadError::Node { node };
    let (decl, body) = entry_of(library, step).ok_or(bad)?;
    let (fixed, reads) = (decl.inputs.len(), step.reads.len as usize);
    let shape = step.kind == StepKind::Call
        && step.covers.len == 1
        && reads >= fixed
        && reads <= MAX_PORTS
        && (reads == fixed || body.fan.is_some());
    if !shape {
        return Err(bad);
    }
    for (port, link) in stored.reads(step)?.enumerate() {
        check_input(stored, decl, &body, (node, port), link)?;
    }
    for write in stored.writes(step)? {
        let (port, edge) = ((write >> PORT_SHIFT) as usize, (write & EDGE_MASK) as usize);
        let out = decl
            .outputs
            .get(port)
            .ok_or(LoadError::Edge { edge: edge as u16 })?;
        let key = type_hash(&out.ty).ok_or(bad)?;
        check_end(stored, edge, key, *body.outputs.get(port).ok_or(bad)?)?;
    }
    Ok(())
}

/// Where an edge's value comes from and goes, each of which must be exactly one place.
fn check_edge(stored: &Stored<'_>, edge: usize) -> Result<(), LoadError> {
    let bad = LoadError::Edge { edge: edge as u16 };
    let (capacity, overflow, size) = stored.edge(edge)?;
    if capacity == 0
        || capacity > u32::from(u16::MAX)
        || overflow > Overflow::Error as u32
        || size > 16
    {
        return Err(bad);
    }
    let (mut writers, mut writer) = (0, usize::MAX);
    let (mut readers, mut reader) = (0, usize::MAX);
    for (at, step) in stored.view.steps().enumerate() {
        for write in stored.writes(&step)? {
            if (write & EDGE_MASK) as usize == edge {
                writers += 1;
                writer = at;
            }
        }
        for read in stored.reads(&step)? {
            if read as usize == edge {
                readers += 1;
                reader = at;
            }
        }
    }
    stored.for_inputs(|_, at, count| {
        for slot in 0..count {
            if stored.word(at + 3 + slot)? as usize == edge {
                writers += 1;
            }
        }
        Ok(())
    })?;
    for port in 0..stored.outputs {
        if stored.word(stored.outputs_at + port * 3 + 2)? as usize == edge {
            readers += 1;
        }
    }
    let backwards = writer != usize::MAX && reader != usize::MAX && writer >= reader;
    if writers != 1 || readers != 1 || backwards {
        return Err(bad);
    }
    Ok(())
}

fn check_param(stored: &Stored<'_>, param: usize) -> Result<(), LoadError> {
    let bad = LoadError::Param {
        param: param as u16,
    };
    let at = stored.params_at + param * 5;
    let index = stored.word(at)? as usize;
    if index >= stored.consts {
        return Err(bad);
    }
    let (kind, value) = stored.constant(index)?;
    if kind.in_order(stored.pair(at + 1)?, value, stored.pair(at + 3)?) {
        Ok(())
    } else {
        Err(bad)
    }
}

fn check_host(stored: &Stored<'_>) -> Result<(), LoadError> {
    stored.for_inputs(|port, at, count| {
        let bad = LoadError::Port { port: port as u16 };
        if count > MAX_EDGES {
            return Err(bad);
        }
        let key = stored.word(at + 1)?;
        for slot in 0..count {
            let edge = stored.word(at + 3 + slot)? as usize;
            if edge >= stored.edges || stored.edge_key(edge)? != key {
                return Err(bad);
            }
        }
        Ok(())
    })?;
    for port in 0..stored.outputs {
        let bad = LoadError::Port { port: port as u16 };
        let at = stored.outputs_at + port * 3;
        let edge = stored.word(at + 2)? as usize;
        if edge >= stored.edges || stored.edge_key(edge)? != stored.word(at + 1)? {
            return Err(bad);
        }
    }
    Ok(())
}

/// Checks `blob` against `library` and works out its layout.
pub(super) fn check(library: &Library, blob: &[u8]) -> Result<Layout, LoadError> {
    let stored = open(library, blob)?;
    let steps = stored.view.step_count() as usize;
    if steps > MAX_NODES {
        return Err(LoadError::Malformed);
    }
    let mut links = 0;
    for (node, step) in stored.view.steps().enumerate() {
        check_step(library, &stored, node as u16, &step)?;
        links += step.reads.len as usize + step.writes.len as usize;
    }
    for edge in 0..stored.edges {
        check_edge(&stored, edge)?;
    }
    for param in 0..stored.params {
        check_param(&stored, param)?;
    }
    check_host(&stored)?;
    stored.for_inputs(|_, _, count| {
        links += count;
        Ok(())
    })?;
    let mut rings = 0;
    for edge in 0..stored.edges {
        let (capacity, _, size) = stored.edge(edge)?;
        rings += capacity as usize * size as usize;
    }
    let bodies = stored
        .view
        .steps()
        .filter_map(|step| Some(entry_of(library, &step)?.1));
    let start = steps_end(steps, bodies);
    let mut at = 0;
    let mut table = |len: usize| {
        let here = at;
        at += len;
        here
    };
    let edges_at = table(stored.edges * EDGE_LEN);
    let inputs_at = table(stored.inputs * HOST_LEN);
    let outputs_at = table(stored.outputs * HOST_LEN);
    let steps_at = table(steps * STEP_LEN);
    let links_at = table(links * LINK_LEN);
    let consts_at = table(stored.consts * CONST_LEN);
    let params_at = table(stored.params * PARAM_LEN);
    let values = table(rings);
    let size = start + values + rings;
    if size > usize::from(u16::MAX) {
        return Err(LoadError::Arena {
            needed: size as u32,
        });
    }
    Ok(Layout {
        plan: stored.view.hash(),
        size,
        steps: steps as u16,
        steps_at: steps_at as u16,
        links_at: links_at as u16,
        edges_at: edges_at as u16,
        consts_at: consts_at as u16,
        params: stored.params as u16,
        params_at: params_at as u16,
        inputs: stored.inputs as u16,
        inputs_at: inputs_at as u16,
        outputs: stored.outputs as u16,
        outputs_at: outputs_at as u16,
    })
}

fn read_link(link: u32) -> u16 {
    if link == NO_SLOT {
        ABSENT
    } else if link & CONST_BIT != 0 {
        CONST_LINK | (link & !CONST_BIT) as u16
    } else {
        link as u16
    }
}

/// Writes a plan [`check`] accepted into `arena`.
pub(super) fn lay_out<const N: usize>(
    library: &Library,
    blob: &[u8],
    layout: &Layout,
    arena: &mut StepArena<N>,
) -> Result<(), LoadError> {
    let stored = open(library, blob)?;
    let needed = LoadError::Arena {
        needed: layout.size as u32,
    };
    arena.begin(usize::from(layout.steps)).map_err(|_| needed)?;
    for step in stored.view.steps() {
        let (_, body) = entry_of(library, &step).ok_or(LoadError::Malformed)?;
        let node = stored
            .view
            .node_list(step.covers)
            .ok()
            .and_then(|mut nodes| nodes.next())
            .ok_or(LoadError::Malformed)?;
        arena
            .place(&body, node, step.reads.len)
            .map_err(|_| needed)?;
    }
    let data = arena.data();
    let mut value = usize::from(layout.params_at) + stored.params * PARAM_LEN;
    for edge in 0..stored.edges {
        let (capacity, overflow, size) = stored.edge(edge)?;
        let at = usize::from(layout.edges_at) + edge * EDGE_LEN;
        data[at..at + EDGE_LEN].fill(0);
        put_u16(data, at + 4, capacity as u16);
        data[at + 6] = size as u8;
        data[at + 7] = overflow as u8;
        put_u16(data, at + 8, value as u16);
        value += capacity as usize * size as usize;
    }
    for index in 0..stored.consts {
        let (kind, bits) = stored.constant(index)?;
        let at = usize::from(layout.consts_at) + index * CONST_LEN;
        data[at] = kind.size() as u8;
        data[at + 1..at + 9].copy_from_slice(&bits.to_le_bytes());
    }
    for param in 0..stored.params {
        let word = stored.params_at + param * 5;
        let index = stored.word(word)? as usize;
        let (kind, _) = stored.constant(index)?;
        let at = usize::from(layout.params_at) + param * PARAM_LEN;
        data[at] = kind as u8;
        put_u16(data, at + 1, index as u16);
        data[at + 3..at + 11].copy_from_slice(&stored.pair(word + 1)?.to_le_bytes());
        data[at + 11..at + 19].copy_from_slice(&stored.pair(word + 3)?.to_le_bytes());
    }
    let mut link = usize::from(layout.links_at);
    for (index, step) in stored.view.steps().enumerate() {
        let (decl, _) = entry_of(library, &step).ok_or(LoadError::Malformed)?;
        let at = usize::from(layout.steps_at) + index * STEP_LEN;
        let required = decl
            .inputs
            .iter()
            .enumerate()
            .filter(|(_, input)| !input.optional)
            .fold(0u8, |mask, (port, _)| mask | (1 << port));
        data[at] = u8::from(matches!(decl.fire, Fire::All));
        data[at + 1] = step.reads.len as u8;
        data[at + 2] = step.writes.len as u8;
        data[at + 3] = required;
        put_u16(
            data,
            at + 4,
            ((link - usize::from(layout.links_at)) / LINK_LEN) as u16,
        );
        for read in stored.reads(&step)? {
            put_u16(data, link, read_link(read));
            link += LINK_LEN;
        }
        for write in stored.writes(&step)? {
            let port = (write >> PORT_SHIFT) as u16;
            put_u16(
                data,
                link,
                (port << LINK_SHIFT) | (write & EDGE_MASK) as u16,
            );
            link += LINK_LEN;
        }
    }
    let inputs_at = usize::from(layout.inputs_at);
    stored.for_inputs(|port, at, count| {
        let record = inputs_at + port * HOST_LEN;
        put_u32(data, record, stored.word(at)?);
        put_u32(data, record + 4, stored.word(at + 1)?);
        let size = match count {
            0 => 0,
            _ => stored.edge(stored.word(at + 3)? as usize)?.2,
        };
        data[record + 8] = size as u8;
        data[record + 9] = count as u8;
        put_u16(
            data,
            record + 10,
            ((link - usize::from(layout.links_at)) / LINK_LEN) as u16,
        );
        for slot in 0..count {
            put_u16(data, link, stored.word(at + 3 + slot)? as u16);
            link += LINK_LEN;
        }
        Ok(())
    })?;
    for port in 0..stored.outputs {
        let at = stored.outputs_at + port * 3;
        let record = usize::from(layout.outputs_at) + port * HOST_LEN;
        let edge = stored.word(at + 2)?;
        put_u32(data, record, stored.word(at)?);
        put_u32(data, record + 4, stored.word(at + 1)?);
        data[record + 8] = stored.edge(edge as usize)?.2 as u8;
        data[record + 9] = 1;
        put_u16(data, record + 10, edge as u16);
    }
    Ok(())
}
