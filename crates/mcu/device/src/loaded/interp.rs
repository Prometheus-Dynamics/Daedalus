//! The loaded-plan interpreter.

use core::mem::MaybeUninit;
use core::{ptr, slice};

use super::plan::{
    self, ABSENT, CONST, ConstRec, EdgeRec, HOST, HostRec, Layout, NodeRec, ParamRec, Source, Table,
};
use super::{Io, Library, LoadError, TypeInfo, scalar_kind};
use crate::param::{ParamError, ParamSpec, Tunable};
use crate::{Clock, Ctx, McuError, McuType, Scalar, name_hash};

#[repr(C, align(8))]
struct Arena<const N: usize>([MaybeUninit<u8>; N]);

/// Runs plans loaded at run time over a node [`Library`], in an `ARENA`-byte RAM arena
/// (at most 65535) holding the plan tables, edge queues, node states and constants.
///
/// ```ignore
/// static mut PLAN: Interpreter<1024> = Interpreter::new(&library::LIBRARY);
/// interp.load(&blob)?;                       // validate, then swap (at a tick boundary)
/// let sample = interp.input("sample").unwrap();
/// interp.push(sample, raw)?;
/// interp.tick()?;
/// ```
pub struct Interpreter<const ARENA: usize> {
    library: &'static Library,
    layout: Option<Layout>,
    tick: u32,
    arena: Arena<ARENA>,
}

impl<const ARENA: usize> Interpreter<ARENA> {
    const ARENA_OK: () = assert!(ARENA <= u16::MAX as usize, "arena of at most 65535 bytes");

    /// An interpreter without a plan (`tick` does nothing until [`Self::load`]).
    pub const fn new(library: &'static Library) -> Self {
        let () = Self::ARENA_OK;
        Self {
            library,
            layout: None,
            tick: 0,
            arena: Arena([MaybeUninit::uninit(); ARENA]),
        }
    }

    pub fn library(&self) -> &'static Library {
        self.library
    }

    /// The arena bytes `blob` needs, after validating it against `library`.
    pub fn required(library: &Library, blob: &[u8]) -> Result<usize, LoadError> {
        Ok(plan::walk(library, blob, None)?.size)
    }

    /// Validate `blob` and replace the running plan with it: queues empty, node states at
    /// their initial value, the tick counter at 0. On error the running plan stays.
    pub fn load(&mut self, blob: &[u8]) -> Result<(), LoadError> {
        let layout = plan::walk(self.library, blob, None)?;
        if layout.size > ARENA {
            return Err(LoadError::Arena {
                needed: layout.size as u32,
            });
        }
        self.layout = None;
        let base = self.base();
        // Repeats the walk that just succeeded, now writing the arena.
        let layout = plan::walk(self.library, blob, Some((base, layout)))?;
        self.layout = Some(layout);
        self.tick = 0;
        Ok(())
    }

    /// Stop running the current plan.
    pub fn unload(&mut self) {
        self.layout = None;
    }

    /// The host plan hash of the running plan.
    pub fn plan_hash(&self) -> Option<u64> {
        self.layout.map(|layout| layout.plan)
    }

    /// Arena bytes the running plan uses.
    pub fn used(&self) -> usize {
        self.layout.map_or(0, |layout| layout.size)
    }

    /// The id of host input `name`.
    pub fn input(&self, name: &str) -> Option<u16> {
        self.port(name, |layout| layout.inputs)
    }

    /// The id of host output `name`.
    pub fn output(&self, name: &str) -> Option<u16> {
        self.port(name, |layout| layout.outputs)
    }

    /// Push a value into host input `port` (`McuError::Port` for an unknown port or another
    /// type than the port's).
    pub fn push<T: McuType + Copy>(&mut self, port: u16, value: T) -> Result<(), McuError> {
        let (_, edges, base) = self.host::<T>(port, |layout| layout.inputs)?;
        // SAFETY: `value` is a `T`, the type of the port's edges (or of their widening).
        unsafe { push_edges(edges, base, HOST | port, 0, ptr::from_ref(&value).cast()) }
    }

    /// Pop the oldest value of host output `port`.
    pub fn pop<T: McuType + Copy>(&mut self, port: u16) -> Result<Option<T>, McuError> {
        let (host, edges, base) = self.host::<T>(port, |layout| layout.outputs)?;
        let edge = &mut edges[usize::from(host.edge)];
        let Some(slot) = edge.ring.pop(edge.capacity.into()) else {
            return Ok(None);
        };
        let at = usize::from(edge.offset) + slot * usize::from(edge.stride);
        // SAFETY: a slot of the edge, which queues `T`s (checked above).
        Ok(Some(unsafe { base.add(at).cast::<T>().read() }))
    }

    /// Run every node once in schedule order (`Ctx::now_micros` is 0).
    pub fn tick(&mut self) -> Result<(), McuError> {
        self.tick_at(0)
    }

    /// [`Self::tick`] with the time read from `clock`.
    pub fn tick_with(&mut self, clock: &impl Clock) -> Result<(), McuError> {
        self.tick_at(clock.now_micros())
    }

    /// [`Self::tick`] at time `now_micros`, with the compiled graphs' readiness rules.
    pub fn tick_at(&mut self, now_micros: u64) -> Result<(), McuError> {
        let Some(layout) = self.layout else {
            return Ok(());
        };
        let ctx = Ctx {
            tick: self.tick,
            now_micros,
        };
        self.tick = self.tick.wrapping_add(1);
        let library = self.library;
        let base = self.base();
        // SAFETY: tables written by `load`, disjoint from each other and from the values.
        let (nodes, sources, consts, edges) = unsafe {
            (
                table::<NodeRec>(base, layout.nodes),
                table::<Source>(base, layout.sources),
                table::<ConstRec>(base, layout.consts),
                table::<EdgeRec>(base, layout.edges),
            )
        };
        for (index, node) in nodes.iter().enumerate() {
            let entry = &library.nodes[usize::from(node.entry)];
            let sources = &sources[usize::from(node.sources)..][..entry.inputs.len()];
            let edge = |source: &Source| (source.0 & CONST == 0).then_some(usize::from(source.0));
            // Fire mode `all`: wait until every connected required edge holds a value.
            if node.wait_all
                && sources.iter().zip(entry.inputs).any(|(source, input)| {
                    !input.optional && edge(source).is_some_and(|e| edges[e].ring.is_empty())
                })
            {
                continue;
            }
            let mut io = Io::new();
            let mut ready = true;
            for (k, (source, input)) in sources.iter().zip(entry.inputs).enumerate() {
                let value = match (source.0, edge(source)) {
                    (ABSENT, _) => ptr::null(),
                    (_, Some(e)) => {
                        let edge = &mut edges[e];
                        let slot = edge.ring.pop(edge.capacity.into());
                        // Fire mode `any` drains the edge and reads its oldest value.
                        if !node.wait_all {
                            edge.ring.clear();
                        }
                        let at = |slot| usize::from(edge.offset) + slot * usize::from(edge.stride);
                        // SAFETY: a slot inside the edge's storage; popped values stay in place
                        // until a later push.
                        slot.map_or(ptr::null(), |slot| unsafe {
                            base.add(at(slot)).cast_const()
                        })
                    }
                    // SAFETY: the constant's slot.
                    (konst, None) => unsafe {
                        base.add(consts[usize::from(konst & !CONST)].offset.into())
                            .cast_const()
                    },
                };
                ready &= input.optional || !value.is_null();
                io.inputs[k] = value;
            }
            if !ready {
                continue;
            }
            for k in 0..entry.outputs.len() {
                let at = usize::from(layout.scratch) + k * usize::from(layout.stride);
                // SAFETY: the scratch area holds the outputs of the node with the most.
                io.outputs[k] = unsafe { base.add(at) };
            }
            // SAFETY: the node's state slot, and inputs and outputs of the entry's types.
            unsafe { (entry.run)(base.add(node.state.into()), &ctx, &mut io) }.map_err(
                |error| McuError::Node {
                    node: index as u16,
                    error,
                },
            )?;
            for k in (0..entry.outputs.len()).filter(|k| io.produced & (1 << k) != 0) {
                // SAFETY: output `k` was written with the entry's output type.
                unsafe { push_edges(edges, base, index as u16, k as u8, io.outputs[k])? };
            }
        }
        Ok(())
    }

    fn base(&mut self) -> *mut u8 {
        self.arena.0.as_mut_ptr().cast()
    }

    fn port(&self, name: &str, which: fn(&Layout) -> Table) -> Option<u16> {
        let layout = self.layout?;
        let base = self.arena.0.as_ptr().cast::<u8>().cast_mut();
        let hash = name_hash(name);
        // SAFETY: a host port table written by `load` (read only here).
        let ports = unsafe { table_ref::<HostRec>(base, which(&layout)) };
        ports
            .iter()
            .position(|port| port.name == hash)
            .map(|p| p as u16)
    }

    /// Host port `port` of `table` when its type is `T`, the edge table and the arena.
    fn host<T: McuType + Copy>(
        &mut self,
        port: u16,
        which: fn(&Layout) -> Table,
    ) -> Result<(HostRec, &mut [EdgeRec], *mut u8), McuError> {
        let bad = McuError::Port { port };
        let layout = self.layout.ok_or(bad)?;
        let base = self.base();
        // SAFETY: tables written by `load`.
        let (ports, edges) = unsafe {
            (
                table_ref::<HostRec>(base, which(&layout)),
                table::<EdgeRec>(base, layout.edges),
            )
        };
        let host = *ports.get(usize::from(port)).ok_or(bad)?;
        if self.library.ty(host.ty) != Some(TypeInfo::of::<T>()) {
            return Err(bad);
        }
        Ok((host, edges, base))
    }

    /// Parameter `id` (type and range) and its constant slot, in the arena at `base`.
    fn param_slot(&self, base: *mut u8, id: u16) -> Option<(ParamSpec, *mut u8)> {
        let layout = self.layout?;
        // SAFETY: tables written by `load` (read only here), and the parameter's bound and
        // constant slots, which hold values of its kind.
        unsafe {
            let param = *table_ref::<ParamRec>(base, layout.params).get(usize::from(id))?;
            let konst = table_ref::<ConstRec>(base, layout.consts)[usize::from(param.konst)];
            let bound = |at: u16| Scalar::read(param.kind, base.add(at.into()));
            let spec = ParamSpec {
                kind: param.kind,
                min: bound(param.min),
                max: bound(param.max),
            };
            Some((spec, base.add(konst.offset.into())))
        }
    }
}

impl<const ARENA: usize> Tunable for Interpreter<ARENA> {
    fn set_param(&mut self, id: u16, value: Scalar) -> Result<(), ParamError> {
        let base = self.base();
        let (spec, slot) = self.param_slot(base, id).ok_or(ParamError::UnknownId)?;
        let value = spec.check(value)?;
        // SAFETY: the constant slot of the parameter's kind; `&mut self` excludes a running tick.
        unsafe { value.write(slot) };
        Ok(())
    }

    fn param(&self, id: u16) -> Option<Scalar> {
        let base = self.arena.0.as_ptr().cast::<u8>().cast_mut();
        let (spec, slot) = self.param_slot(base, id)?;
        // SAFETY: only read; the constant slot holds a value of the parameter's kind.
        Some(unsafe { Scalar::read(spec.kind, slot) })
    }
}

/// Push the value at `value` into every edge leaving `from`'s output `port`, widening where
/// the plan says so.
///
/// # Safety
/// `value` holds a value of the producer's output type.
unsafe fn push_edges(
    edges: &mut [EdgeRec],
    base: *mut u8,
    from: u16,
    port: u8,
    value: *const u8,
) -> Result<(), McuError> {
    for (index, edge) in edges.iter_mut().enumerate() {
        if edge.from != from || edge.port != port {
            continue;
        }
        let slot = edge
            .ring
            .push(edge.capacity.into(), edge.overflow)
            .map_err(|_| McuError::QueueFull { edge: index as u16 })?;
        let Some(slot) = slot else { continue };
        // SAFETY: a slot of the edge inside the arena; the value types were checked at load.
        unsafe {
            let dst = base.add(usize::from(edge.offset) + slot * usize::from(edge.stride));
            match (scalar_kind(edge.widen.into()), scalar_kind(edge.ty)) {
                (Some(from), Some(to)) => {
                    if let Some(widened) = Scalar::read(from, value).coerce(to) {
                        widened.write(dst);
                    }
                }
                _ => ptr::copy_nonoverlapping(value, dst, edge.stride.into()),
            }
        }
    }
    Ok(())
}

/// # Safety
/// `table` holds initialised `T`s inside the arena at `base`, not otherwise borrowed.
unsafe fn table<'a, T>(base: *mut u8, table: Table) -> &'a mut [T] {
    // SAFETY: the caller's contract.
    unsafe { slice::from_raw_parts_mut(base.add(table.at.into()).cast(), table.len.into()) }
}

/// # Safety
/// `table` holds initialised `T`s inside the arena at `base`.
unsafe fn table_ref<'a, T>(base: *const u8, table: Table) -> &'a [T] {
    // SAFETY: the caller's contract.
    unsafe { slice::from_raw_parts(base.add(table.at.into()).cast(), table.len.into()) }
}
