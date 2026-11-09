//! Checking a stored plan and laying it out. The check touches nothing, so a plan that's refused
//! leaves the running one alone, and it needs no memory beyond the stack.

use daedalus_abi::NO_SLOT;
use daedalus_plan::{MAGIC, PlanView, Span, StepKind, StepRecord, VERSION, Words, crc32};
use daedalus_runner::{ArenaBody, StepArena, steps_end};

use super::library::{Entry, Library, name_hash};
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

/// What a check refuses for. Small enough to come back in registers, unlike a [`LoadError`], and
/// a firmware's loader is mostly the paths these take.
#[derive(Clone, Copy)]
enum Bad {
    Node(u16),
    Input(u16, u8),
    Edge(u16),
    Type(u16),
    Param(u16),
    Port(u16),
}

impl From<Bad> for LoadError {
    fn from(bad: Bad) -> LoadError {
        match bad {
            Bad::Node(node) => LoadError::Node { node },
            Bad::Input(node, input) => LoadError::Input { node, input },
            Bad::Edge(edge) => LoadError::Edge { edge },
            Bad::Type(edge) => LoadError::Type { edge },
            Bad::Param(param) => LoadError::Param { param },
            Bad::Port(port) => LoadError::Port { port },
        }
    }
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

// The arena's tables are written by `lay_out` to fit it, so these are always in range. Not
// panicking keeps the bounds-check paths out of a firmware.
pub(super) fn get_u8(data: &[u8], at: usize) -> u8 {
    data.get(at).copied().unwrap_or(0)
}

pub(super) fn get_u16(data: &[u8], at: usize) -> u16 {
    match data.get(at..at + 2) {
        Some(&[low, high]) => u16::from_le_bytes([low, high]),
        _ => 0,
    }
}

pub(super) fn put_u16(data: &mut [u8], at: usize, value: u16) {
    if let Some(to) = data.get_mut(at..at + 2) {
        to.copy_from_slice(&value.to_le_bytes());
    }
}

pub(super) fn get_u32(data: &[u8], at: usize) -> u32 {
    match data.get(at..at + 4) {
        Some(&[a, b, c, d]) => u32::from_le_bytes([a, b, c, d]),
        _ => 0,
    }
}

/// Writes a table of the arena, one field after another.
struct Table<'a> {
    data: &'a mut [u8],
    at: usize,
}

impl Table<'_> {
    fn bytes(&mut self, value: &[u8]) {
        self.data[self.at..self.at + value.len()].copy_from_slice(value);
        self.at += value.len();
    }

    fn u8(&mut self, value: u8) {
        self.bytes(&[value]);
    }

    fn u16(&mut self, value: u16) {
        self.bytes(&value.to_le_bytes());
    }

    fn u32(&mut self, value: u32) {
        self.bytes(&value.to_le_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_le_bytes());
    }
}

/// The parts of a stored plan, found once.
pub(super) struct Stored<'a> {
    view: PlanView<'a>,
    extra: Words<'a>,
    types: Words<'a>,
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

// `open` proves every table fits, so these find what they look for. Zero is never read.
impl Stored<'_> {
    fn word(&self, at: usize) -> u32 {
        self.extra.at(at).unwrap_or(0)
    }

    fn pair(&self, at: usize) -> u64 {
        u64::from(self.word(at)) | (u64::from(self.word(at + 1)) << 32)
    }

    fn capacity(&self, edge: usize) -> u32 {
        self.word(3 + edge * 3)
    }

    fn overflow(&self, edge: usize) -> u32 {
        self.word(4 + edge * 3)
    }

    fn value_size(&self, edge: usize) -> u32 {
        self.word(5 + edge * 3)
    }

    fn edge_key(&self, edge: usize) -> u32 {
        self.types.at(edge).unwrap_or(0)
    }

    fn kind(&self, index: usize) -> ScalarKind {
        ScalarKind::from_word(self.word(self.consts_at + index * 3)).unwrap_or(ScalarKind::ALL[0])
    }

    fn bits(&self, index: usize) -> u64 {
        self.pair(self.consts_at + index * 3 + 1)
    }

    /// How many edges the host input whose record is at `at` feeds.
    fn fed(&self, at: usize) -> usize {
        self.word(at + 2) as usize
    }

    fn slots(&self, span: Span) -> impl Iterator<Item = u32> + use<'_> {
        self.view.slot_list(span).ok().into_iter().flatten()
    }

    /// A count from the tables, or `Malformed` if no table that long could fit.
    fn count(&self, at: usize) -> Result<usize, LoadError> {
        match self.extra.at(at) {
            Some(count) if count as usize <= self.extra.len() => Ok(count as usize),
            _ => Err(LoadError::Malformed),
        }
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
        extra: view.extra(),
        types: view.types(),
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
    if stored.extra.len() < 3 {
        return Err(LoadError::Malformed);
    }
    let found = stored.pair(0);
    if found != library.hash {
        return Err(LoadError::Library {
            expected: library.hash,
            found,
        });
    }
    stored.edges = stored.count(2)?;
    if stored.edges >= MAX_EDGES || stored.types.len() != stored.edges {
        return Err(LoadError::Malformed);
    }
    let mut at = 3 + stored.edges * 3;
    stored.consts = stored.count(at)?;
    stored.consts_at = at + 1;
    at += 1 + stored.consts * 3;
    stored.params = stored.count(at)?;
    stored.params_at = at + 1;
    at += 1 + stored.params * 5;
    stored.inputs = stored.count(at)?;
    stored.inputs_at = at + 1;
    at += 1;
    for _ in 0..stored.inputs {
        at += 3 + stored.count(at + 2)?;
    }
    stored.outputs = stored.count(at)?;
    stored.outputs_at = at + 1;
    if at + 1 + stored.outputs * 3 != stored.extra.len() {
        return Err(LoadError::Malformed);
    }
    for index in 0..stored.consts {
        if ScalarKind::from_word(stored.word(stored.consts_at + index * 3)).is_none() {
            return Err(LoadError::Malformed);
        }
    }
    Ok(stored)
}

fn check_input(
    stored: &Stored<'_>,
    decl: &Entry,
    body: &ArenaBody,
    (node, port): (u16, usize),
    link: u32,
) -> Result<(), Bad> {
    let bad = Bad::Input(node, port as u8);
    let (key, size) = decl.input(body, port).ok_or(Bad::Node(node))?;
    if link == NO_SLOT {
        let required = port < MAX_PORTS && decl.required & (1 << port) != 0;
        return if required { Err(bad) } else { Ok(()) };
    }
    if link & CONST_BIT != 0 {
        let index = (link & !CONST_BIT) as usize;
        if index >= stored.consts {
            return Err(bad);
        }
        let kind = stored.kind(index);
        return if name_hash(kind.key()) == key && kind.size() == size {
            Ok(())
        } else {
            Err(bad)
        };
    }
    check_end(stored, link as usize, key, size)
}

fn check_end(stored: &Stored<'_>, edge: usize, key: u32, size: u16) -> Result<(), Bad> {
    let bad = edge as u16;
    if edge >= stored.edges {
        return Err(Bad::Edge(bad));
    }
    if stored.edge_key(edge) != key || stored.value_size(edge) != u32::from(size) {
        return Err(Bad::Type(bad));
    }
    Ok(())
}

fn check_step(
    library: &Library,
    stored: &Stored<'_>,
    node: u16,
    step: &StepRecord,
) -> Result<(), Bad> {
    let bad = Bad::Node(node);
    let (decl, body) = library.entry(step.body as usize).ok_or(bad)?;
    let (fixed, reads) = (usize::from(decl.inputs), step.reads.len as usize);
    let shape = step.kind == StepKind::Call
        && step.covers.len == 1
        && reads >= fixed
        && reads <= MAX_PORTS
        && (reads == fixed || body.fan.is_some());
    if !shape {
        return Err(bad);
    }
    for (port, link) in stored.slots(step.reads).enumerate() {
        check_input(stored, decl, &body, (node, port), link)?;
    }
    for write in stored.slots(step.writes) {
        let (port, edge) = ((write >> PORT_SHIFT) as usize, (write & EDGE_MASK) as usize);
        let key = decl.output(port).ok_or(Bad::Edge(edge as u16))?;
        check_end(stored, edge, key, *body.outputs.get(port).ok_or(bad)?)?;
    }
    Ok(())
}

/// Where an edge's value comes from and goes, each of which must be exactly one place.
fn check_edge(stored: &Stored<'_>, edge: usize) -> Result<(), Bad> {
    let bad = Bad::Edge(edge as u16);
    let capacity = stored.capacity(edge);
    if capacity == 0
        || capacity > u32::from(u16::MAX)
        || stored.overflow(edge) > Overflow::Error as u32
        || stored.value_size(edge) > 16
    {
        return Err(bad);
    }
    let (mut writers, mut writer) = (0, usize::MAX);
    let (mut readers, mut reader) = (0, usize::MAX);
    for (at, step) in stored.view.steps().enumerate() {
        for write in stored.slots(step.writes) {
            if (write & EDGE_MASK) as usize == edge {
                writers += 1;
                writer = at;
            }
        }
        for read in stored.slots(step.reads) {
            if read as usize == edge {
                readers += 1;
                reader = at;
            }
        }
    }
    let mut at = stored.inputs_at;
    for _ in 0..stored.inputs {
        let count = stored.fed(at);
        for slot in 0..count {
            if stored.word(at + 3 + slot) as usize == edge {
                writers += 1;
            }
        }
        at += 3 + count;
    }
    for port in 0..stored.outputs {
        if stored.word(stored.outputs_at + port * 3 + 2) as usize == edge {
            readers += 1;
        }
    }
    let backwards = writer != usize::MAX && reader != usize::MAX && writer >= reader;
    if writers != 1 || readers != 1 || backwards {
        return Err(bad);
    }
    Ok(())
}

fn check_param(stored: &Stored<'_>, param: usize) -> Result<(), Bad> {
    let bad = Bad::Param(param as u16);
    let at = stored.params_at + param * 5;
    let index = stored.word(at) as usize;
    if index >= stored.consts {
        return Err(bad);
    }
    let (kind, value) = (stored.kind(index), stored.bits(index));
    if kind.in_order(stored.pair(at + 1), value, stored.pair(at + 3)) {
        Ok(())
    } else {
        Err(bad)
    }
}

/// Every edge of a host port is real and carries the port's type. Returns the links the input
/// ports use.
fn check_host(stored: &Stored<'_>) -> Result<usize, Bad> {
    let (mut links, mut at) = (0, stored.inputs_at);
    for port in 0..stored.inputs {
        let bad = Bad::Port(port as u16);
        let count = stored.fed(at);
        if count > MAX_EDGES {
            return Err(bad);
        }
        let key = stored.word(at + 1);
        for slot in 0..count {
            let edge = stored.word(at + 3 + slot) as usize;
            if edge >= stored.edges || stored.edge_key(edge) != key {
                return Err(bad);
            }
        }
        links += count;
        at += 3 + count;
    }
    for port in 0..stored.outputs {
        let at = stored.outputs_at + port * 3;
        let edge = stored.word(at + 2) as usize;
        if edge >= stored.edges || stored.edge_key(edge) != stored.word(at + 1) {
            return Err(Bad::Port(port as u16));
        }
    }
    Ok(links)
}

/// Checks `blob` against `library` and works out its layout.
pub(super) fn check<'a>(
    library: &Library,
    blob: &'a [u8],
) -> Result<(Stored<'a>, Layout), LoadError> {
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
    links += check_host(&stored)?;
    let mut rings = 0;
    for edge in 0..stored.edges {
        rings += stored.capacity(edge) as usize * stored.value_size(edge) as usize;
    }
    let bodies = stored
        .view
        .steps()
        .filter_map(|step| Some(library.entry(step.body as usize)?.1));
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
    let layout = Layout {
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
    };
    Ok((stored, layout))
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
    stored: &Stored<'_>,
    layout: &Layout,
    arena: &mut StepArena<N>,
) -> Result<(), LoadError> {
    let needed = LoadError::Arena {
        needed: layout.size as u32,
    };
    arena.begin(usize::from(layout.steps)).map_err(|_| needed)?;
    for step in stored.view.steps() {
        let (_, body) = library
            .entry(step.body as usize)
            .ok_or(LoadError::Malformed)?;
        let node = stored
            .view
            .node_list(step.covers)
            .ok()
            .and_then(|nodes| nodes.at(0))
            .ok_or(LoadError::Malformed)?;
        arena
            .place(&body, node, step.reads.len)
            .map_err(|_| needed)?;
    }
    let links_at = usize::from(layout.links_at);
    let mut table = Table {
        data: arena.data(),
        at: usize::from(layout.edges_at),
    };
    let mut value = usize::from(layout.params_at) + stored.params * PARAM_LEN;
    for edge in 0..stored.edges {
        let (capacity, size) = (stored.capacity(edge), stored.value_size(edge));
        table.u32(0);
        table.u16(capacity as u16);
        table.u8(size as u8);
        table.u8(stored.overflow(edge) as u8);
        table.u16(value as u16);
        value += capacity as usize * size as usize;
    }
    table.at = usize::from(layout.consts_at);
    for index in 0..stored.consts {
        table.u8(stored.kind(index).size() as u8);
        table.u64(stored.bits(index));
    }
    for param in 0..stored.params {
        let word = stored.params_at + param * 5;
        let index = stored.word(word) as usize;
        table.u8(stored.kind(index) as u8);
        table.u16(index as u16);
        table.u64(stored.pair(word + 1));
        table.u64(stored.pair(word + 3));
    }
    let mut link = links_at;
    for (index, step) in stored.view.steps().enumerate() {
        let (decl, _) = library
            .entry(step.body as usize)
            .ok_or(LoadError::Malformed)?;
        table.at = usize::from(layout.steps_at) + index * STEP_LEN;
        table.u8(u8::from(decl.fire_all));
        table.u8(step.reads.len as u8);
        table.u8(step.writes.len as u8);
        table.u8(decl.required);
        table.u16(((link - links_at) / LINK_LEN) as u16);
        table.at = link;
        for read in stored.slots(step.reads) {
            table.u16(read_link(read));
        }
        for write in stored.slots(step.writes) {
            let port = (write >> PORT_SHIFT) as u16;
            table.u16((port << LINK_SHIFT) | (write & EDGE_MASK) as u16);
        }
        link = table.at;
    }
    let mut at = stored.inputs_at;
    for port in 0..stored.inputs {
        let count = stored.fed(at);
        table.at = usize::from(layout.inputs_at) + port * HOST_LEN;
        table.u32(stored.word(at));
        table.u32(stored.word(at + 1));
        let size = match count {
            0 => 0,
            _ => stored.value_size(stored.word(at + 3) as usize),
        };
        table.u8(size as u8);
        table.u8(count as u8);
        table.u16(((link - links_at) / LINK_LEN) as u16);
        table.at = link;
        for slot in 0..count {
            table.u16(stored.word(at + 3 + slot) as u16);
        }
        link = table.at;
        at += 3 + count;
    }
    table.at = usize::from(layout.outputs_at);
    for port in 0..stored.outputs {
        let at = stored.outputs_at + port * 3;
        let edge = stored.word(at + 2);
        table.u32(stored.word(at));
        table.u32(stored.word(at + 1));
        table.u8(stored.value_size(edge as usize) as u8);
        table.u8(1);
        table.u16(edge as u16);
    }
    Ok(())
}
