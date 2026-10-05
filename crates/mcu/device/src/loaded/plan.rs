//! Plan blob -> arena layout. One walk over the blob validates it and computes the layout
//! (no arena); a second, identical walk, run only after the first succeeded, writes the tables
//! and initial values into the arena. A rejected blob therefore leaves the running plan as is,
//! and validation needs no memory beyond the stack.
//!
//! Blob, in postcard encoding (see [`crate::wire`]):
//!
//! ```text
//! magic [u8; 4] = "DMCU", version u8, library u64, plan u64,
//! entries: seq<u8>                     library entry of each node, in schedule order
//! host_inputs, host_outputs: seq<{ name: u32, ty: u16 }>
//! nodes: seq<{ wait_all: bool, inputs: seq<Input> }>
//!   Input = Absent | Edge(Edge) | Const(Scalar) | Param { value, min, max: Scalar }
//!   Edge  = { from: Host(u16) | Node(u16, u8), capacity: u16, overflow: u8 }
//! outputs: seq<Edge>                   the edge into each host output
//! ```
//!
//! Edges are numbered in order of appearance (node inputs, then host outputs), parameters
//! likewise. Type ids are [`ScalarKind`](crate::ScalarKind)s, then the library's types. A producer runs before
//! its consumer: an edge from node `m` into node `n` needs `m < n`.

use core::mem::{align_of, size_of};
use core::ptr;

use super::{FORMAT_VERSION, Library, LoadError, MAGIC, MAX_PORTS, NodeEntry, scalar_kind};
use crate::param::ParamSpec;
use crate::queue::{Overflow, Ring};
use crate::wire::Reader;
use crate::{Scalar, ScalarKind};

/// `EdgeRec::from` of an edge leaving host input `port`: `HOST | port`.
pub(crate) const HOST: u16 = 0x8000;
/// A [`Source`] reading constant `index`: `CONST | index`.
pub(crate) const CONST: u16 = 0x8000;
/// A [`Source`] of an unconnected optional input.
pub(crate) const ABSENT: u16 = u16::MAX;
/// `EdgeRec::widen` of an edge that copies values unchanged.
pub(crate) const NO_WIDEN: u8 = u8::MAX;

#[derive(Clone, Copy)]
pub(crate) struct NodeRec {
    pub entry: u16,
    pub state: u16,
    /// First of the node's [`Source`]s.
    pub sources: u16,
    pub wait_all: bool,
}

/// An input source: an edge index, `CONST | constant index` or [`ABSENT`].
#[derive(Clone, Copy)]
pub(crate) struct Source(pub u16);

#[derive(Clone, Copy)]
pub(crate) struct EdgeRec {
    /// Producer node index, or `HOST | host input`.
    pub from: u16,
    pub port: u8,
    /// The producer's [`ScalarKind`](crate::ScalarKind) when values widen at the push, else [`NO_WIDEN`].
    pub widen: u8,
    pub overflow: Overflow,
    /// Consumer type id.
    pub ty: u16,
    pub offset: u16,
    pub capacity: u16,
    pub stride: u16,
    pub ring: Ring,
}

#[derive(Clone, Copy)]
pub(crate) struct ConstRec {
    pub offset: u16,
}

/// A parameter: its constant, and its bounds stored as values of its kind.
#[derive(Clone, Copy)]
pub(crate) struct ParamRec {
    pub kind: ScalarKind,
    pub konst: u16,
    pub min: u16,
    pub max: u16,
}

#[derive(Clone, Copy)]
pub(crate) struct HostRec {
    pub name: u32,
    pub ty: u16,
    /// The edge into a host output.
    pub edge: u16,
}

/// `len` records at arena offset `at`.
#[derive(Clone, Copy, Default)]
pub(crate) struct Table {
    pub at: u16,
    pub len: u16,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Kind {
    Params,
    Inputs,
    Outputs,
    Nodes,
    Sources,
    Edges,
    Consts,
}

#[derive(Clone, Copy, Default)]
pub(crate) struct Layout {
    pub plan: u64,
    pub params: Table,
    pub inputs: Table,
    pub outputs: Table,
    pub nodes: Table,
    pub sources: Table,
    pub edges: Table,
    pub consts: Table,
    /// Start of the value area: queues, node states, constants.
    pub values: u16,
    /// Output slots of the running node, `stride` bytes apart.
    pub scratch: u16,
    pub stride: u16,
    /// Arena bytes used.
    pub size: usize,
}

impl Layout {
    fn table(&mut self, kind: Kind) -> &mut Table {
        match kind {
            Kind::Params => &mut self.params,
            Kind::Inputs => &mut self.inputs,
            Kind::Outputs => &mut self.outputs,
            Kind::Nodes => &mut self.nodes,
            Kind::Sources => &mut self.sources,
            Kind::Edges => &mut self.edges,
            Kind::Consts => &mut self.consts,
        }
    }
}

/// Validate `blob` against `library` and lay it out; with `arena` (the arena and the layout a
/// validating walk returned), write the plan there.
pub(crate) fn walk(
    library: &Library,
    blob: &[u8],
    arena: Option<(*mut u8, Layout)>,
) -> Result<Layout, LoadError> {
    let (base, layout) = arena.unwrap_or((ptr::null_mut(), Layout::default()));
    let mut walk = Walk {
        library,
        base,
        layout,
        counts: Layout::default(),
        values: 0,
        outputs: 0,
        stride: 0,
    };
    walk.blob(Reader::new(blob))?;
    walk.finish()
}

struct Walk<'l> {
    library: &'l Library,
    /// The arena, or null while validating.
    base: *mut u8,
    /// The final layout when writing.
    layout: Layout,
    /// Records so far, in each table's `len`.
    counts: Layout,
    /// Value bytes so far.
    values: usize,
    /// Most outputs of a node.
    outputs: usize,
    /// Largest output value, rounded up to 8 bytes.
    stride: usize,
}

impl Walk<'_> {
    fn blob(&mut self, mut r: Reader<'_>) -> Result<(), LoadError> {
        if r.bytes::<4>()? != MAGIC {
            return Err(LoadError::Malformed);
        }
        let found = r.u8()?;
        if found != FORMAT_VERSION {
            return Err(LoadError::Version { found });
        }
        let found = r.varint()?;
        if found != self.library.hash {
            return Err(LoadError::Library {
                expected: self.library.hash,
                found,
            });
        }
        self.counts.plan = r.varint()?;
        let entries = r.byte_seq()?;
        for (node, &entry) in entries.iter().enumerate() {
            let bad = LoadError::Node { node: node as u16 };
            let entry = self.entry(entry).ok_or(bad)?;
            self.outputs = self.outputs.max(entry.outputs.len());
            for &ty in entry.outputs {
                let size = self.library.ty(ty).ok_or(bad)?.size;
                self.stride = self.stride.max(align_up(size.into(), 8));
            }
        }

        let hosts = r;
        for kind in [Kind::Inputs, Kind::Outputs] {
            for port in 0..r.seq_len()? {
                let (name, ty) = (r.u32()?, r.u16()?);
                let port = port as u16;
                self.library.ty(ty).ok_or(LoadError::Port { port })?;
                self.push(kind, HostRec { name, ty, edge: 0 })?;
            }
        }

        if r.seq_len()? != entries.len() {
            return Err(LoadError::Malformed);
        }
        for (node, &index) in entries.iter().enumerate() {
            let entry = self.entry(index).ok_or(LoadError::Malformed)?;
            let node = node as u16;
            let wait_all = r.bool()?;
            if r.seq_len()? != entry.inputs.len() {
                return Err(LoadError::Node { node });
            }
            let state = self.value(entry.state.size.into(), entry.state.align.into());
            if !self.base.is_null() {
                // SAFETY: an aligned slot of the state's size inside the arena.
                unsafe { (entry.state.init)(self.base.add(state.into())) };
            }
            let rec = NodeRec {
                entry: index.into(),
                state,
                sources: self.counts.sources.len,
                wait_all,
            };
            for (k, input) in entry.inputs.iter().enumerate() {
                let bad = LoadError::Input {
                    node,
                    input: k as u8,
                };
                let source = match r.u8()? {
                    0 if input.optional => ABSENT,
                    0 => return Err(bad),
                    1 => self.edge(&mut r, hosts, entries, node, input.ty)?,
                    2 => {
                        let value = r.scalar()?;
                        if scalar_kind(input.ty) != Some(value.kind()) {
                            return Err(bad);
                        }
                        CONST | self.constant(value)?
                    }
                    3 => CONST | self.param(&mut r, input.ty)?,
                    _ => return Err(LoadError::Malformed),
                };
                self.push(Kind::Sources, Source(source))?;
            }
            self.push(Kind::Nodes, rec)?;
        }

        if r.seq_len()? != usize::from(self.counts.outputs.len) {
            return Err(LoadError::Malformed);
        }
        for port in 0..self.counts.outputs.len {
            let ty = host_type(hosts, false, port).ok_or(LoadError::Malformed)?;
            let edge = self.edge(&mut r, hosts, entries, u16::MAX, ty)?;
            if !self.base.is_null() {
                let at =
                    usize::from(self.layout.outputs.at) + usize::from(port) * size_of::<HostRec>();
                // SAFETY: the host output record written above.
                unsafe { (*self.base.add(at).cast::<HostRec>()).edge = edge };
            }
        }
        Ok(r.finish()?)
    }

    fn entry(&self, entry: u8) -> Option<&'static NodeEntry> {
        let entry = self.library.nodes.get(usize::from(entry))?;
        let fits = entry.inputs.len() <= MAX_PORTS && entry.outputs.len() <= MAX_PORTS;
        (fits && entry.state.align <= 8).then_some(entry)
    }

    /// An edge into `consumer` (a node index, or `u16::MAX` for a host output) of type `ty`.
    fn edge(
        &mut self,
        r: &mut Reader<'_>,
        hosts: Reader<'_>,
        entries: &[u8],
        consumer: u16,
        ty: u16,
    ) -> Result<u16, LoadError> {
        let edge = self.counts.edges.len;
        let bad = LoadError::Edge { edge };
        let (from, port, from_ty) = match r.u8()? {
            0 => {
                let port = r.u16()?;
                let ty = host_type(hosts, true, port).filter(|_| port < HOST);
                (HOST | port, 0, ty.ok_or(bad)?)
            }
            1 => {
                let (node, port) = (r.u16()?, r.u8()?);
                let entry = entries.get(usize::from(node)).filter(|_| node < consumer);
                let entry = entry.and_then(|&e| self.entry(e)).ok_or(bad)?;
                let ty = entry.outputs.get(usize::from(port)).ok_or(bad)?;
                (node, port, *ty)
            }
            _ => return Err(LoadError::Malformed),
        };
        let capacity = r.u16()?;
        let overflow = *Overflow::ALL.get(usize::from(r.u8()?)).ok_or(bad)?;
        let widen = match (scalar_kind(from_ty), scalar_kind(ty)) {
            _ if from_ty == ty => NO_WIDEN,
            (Some(from), Some(to)) if from.widens_to(to) => from as u8,
            _ => return Err(LoadError::Type { edge }),
        };
        let info = self.library.ty(ty).ok_or(bad)?;
        let stride = align_up(info.size.into(), info.align.into());
        if capacity == 0 {
            return Err(bad);
        }
        let offset = self.value(usize::from(capacity) * stride, info.align.into());
        self.push(
            Kind::Edges,
            EdgeRec {
                from,
                port,
                widen,
                overflow,
                ty,
                offset,
                capacity,
                stride: stride as u16,
                ring: Ring::new(),
            },
        )
    }

    /// A parameter of an input of type `ty`; returns its constant.
    fn param(&mut self, r: &mut Reader<'_>, ty: u16) -> Result<u16, LoadError> {
        let bad = LoadError::Param {
            param: self.counts.params.len,
        };
        let (value, min, max) = (r.scalar()?, r.scalar()?, r.scalar()?);
        let kind = scalar_kind(ty).ok_or(bad)?;
        let spec = ParamSpec { kind, min, max };
        let typed = [value, min, max].iter().all(|v| v.kind() == kind);
        if !typed || spec.check(value).is_err() {
            return Err(bad);
        }
        let konst = self.constant(value)?;
        let (min, max) = (self.scalar(min)?, self.scalar(max)?);
        self.push(
            Kind::Params,
            ParamRec {
                kind,
                konst,
                min,
                max,
            },
        )?;
        Ok(konst)
    }

    fn constant(&mut self, value: Scalar) -> Result<u16, LoadError> {
        let offset = self.scalar(value)?;
        self.push(Kind::Consts, ConstRec { offset })
    }

    /// Store a scalar in the value area; returns its offset.
    fn scalar(&mut self, value: Scalar) -> Result<u16, LoadError> {
        let kind = value.kind() as u16;
        let info = self.library.ty(kind).ok_or(LoadError::Malformed)?;
        let offset = self.value(info.size.into(), info.align.into());
        if !self.base.is_null() {
            // SAFETY: a slot of the value's size inside the arena.
            unsafe { value.write(self.base.add(offset.into())) };
        }
        Ok(offset)
    }

    /// Reserve value bytes; returns the arena offset (meaningful when writing).
    fn value(&mut self, size: usize, align: usize) -> u16 {
        self.values = align_up(self.values, align);
        let offset = usize::from(self.layout.values) + self.values;
        self.values += size;
        offset as u16
    }

    /// Append a record to table `kind`; returns its index.
    fn push<T>(&mut self, kind: Kind, record: T) -> Result<u16, LoadError> {
        let table = self.counts.table(kind);
        let index = table.len;
        table.len = index.checked_add(1).ok_or(LoadError::Malformed)?;
        if !self.base.is_null() {
            let at = usize::from(self.layout.table(kind).at) + usize::from(index) * size_of::<T>();
            // SAFETY: the layout reserved `len` aligned records of `T` at `at`.
            unsafe { self.base.add(at).cast::<T>().write(record) };
        }
        Ok(index)
    }

    fn finish(self) -> Result<Layout, LoadError> {
        if !self.base.is_null() {
            return Ok(self.layout);
        }
        let mut layout = self.counts;
        let mut at = 0;
        let mut place = |table: &mut Table, size: usize, align: usize| {
            at = align_up(at, align);
            table.at = at as u16;
            at += usize::from(table.len) * size;
        };
        macro_rules! place {
            ($($table:ident: $ty:ty),*) => {
                $(place(&mut layout.$table, size_of::<$ty>(), align_of::<$ty>());)*
            };
        }
        place!(params: ParamRec, inputs: HostRec, outputs: HostRec, nodes: NodeRec,
            sources: Source, edges: EdgeRec, consts: ConstRec);
        let values = align_up(at, 8);
        let scratch = align_up(values + self.values, 8);
        layout.size = scratch + self.outputs * self.stride;
        if layout.size > usize::from(u16::MAX) {
            return Err(LoadError::Arena {
                needed: layout.size as u32,
            });
        }
        layout.values = values as u16;
        layout.scratch = scratch as u16;
        layout.stride = self.stride as u16;
        Ok(layout)
    }
}

/// The type of host input (or output) `port`, read from the host port sections at `hosts`.
fn host_type(mut hosts: Reader<'_>, input: bool, port: u16) -> Option<u16> {
    let record = |r: &mut Reader<'_>| r.u32().and_then(|_| r.u16()).ok();
    let inputs = hosts.seq_len().ok()?;
    if !input {
        for _ in 0..inputs {
            record(&mut hosts)?;
        }
    }
    let len = if input { inputs } else { hosts.seq_len().ok()? };
    if usize::from(port) >= len {
        return None;
    }
    for _ in 0..port {
        record(&mut hosts)?;
    }
    record(&mut hosts)
}

/// Round `value` up to `align` (a power of two, or 0 for 1); no division (Cortex-M0 has none).
pub(crate) const fn align_up(value: usize, align: usize) -> usize {
    let mask = if align == 0 { 0 } else { align - 1 };
    (value + mask) & !mask
}
