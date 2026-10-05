//! Allocation-free executor for Daedalus graphs on microcontrollers (see docs/mcu.md).
//!
//! Graphs are planned on the host by `daedalus-mcu-build` (the regular planner: type checks,
//! adapter resolution, schedule). Three modes share that planning:
//!
//! - **Compiled**: the plan becomes a Rust module with one `Graph` struct: a typed [`Queue`]
//!   per edge sized from its policy, a state slot per node, `push_*`/`pop_*` methods per host
//!   port and a `tick` that calls the node functions directly in schedule order.
//! - **Compiled + tunable**: the same, plus graph constants marked as parameters, set at run
//!   time through [`Tunable`] (by id, typed setters or [`ParamUpdate`] messages).
//! - **Loaded** (feature `loaded`): plans compiled on the host into a blob and run by the
//!   [`loaded::Interpreter`] over a node library generated from the firmware's nodes.
//!
//! This crate holds what the generated code, the interpreter and the node functions use:
//! queues, the node descriptor types read by the host compiler, [`McuType`] keys,
//! [`NodeState`], [`Ctx`], [`Scalar`] values, the [`wire`] codec and the error enums. It is
//! `#![no_std]` without `alloc`; nothing here allocates.
//!
//! Nodes are plain functions annotated with [`node`]:
//!
//! ```ignore
//! #[daedalus_mcu::node(id = "demo.lowpass", inputs("x", "alpha"), outputs("y"), state(Lowpass))]
//! pub fn lowpass(x: f32, alpha: f32, state: &mut Lowpass) -> f32 { /* ... */ }
//! ```
#![cfg_attr(not(test), no_std)]

#[cfg(feature = "loaded")]
pub mod loaded;
pub mod param;
mod queue;
mod scalar;
pub mod wire;

pub use daedalus_mcu_macros::node;
pub use param::{ParamError, ParamSpec, ParamUpdate, Tunable};
pub use queue::{Overflow, Queue, QueueFull, Ring};
pub(crate) use scalar::for_each_scalar;
pub use scalar::{Scalar, ScalarKind, ScalarType};

/// Per-tick context a node function receives when it takes a `&Ctx` parameter.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Ctx {
    /// Ticks run before this one.
    pub tick: u32,
    /// The time passed to `tick_at` / read from the [`Clock`] by `tick_with` (0 for `tick`).
    pub now_micros: u64,
}

/// A time source for `Graph::tick_with` (a target timer).
pub trait Clock {
    fn now_micros(&self) -> u64;
}

/// An application-defined node failure code (`Result<_, NodeError>` from a node function).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub struct NodeError(pub u16);

/// A failed tick or host push. Indices refer to the generated `NODE_IDS` and edge order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "defmt", derive(defmt::Format))]
pub enum McuError {
    /// An edge with [`Overflow::Error`] (FIFO edges by default) was full.
    QueueFull { edge: u16 },
    /// A node function returned an error; the tick stopped there.
    Node { node: u16, error: NodeError },
    /// A loaded plan has no host port with this id, or it has another type.
    Port { port: u16 },
}

/// 32-bit FNV-1a hash of `name`: host port names and type keys in loaded plans.
pub const fn name_hash(name: &str) -> u32 {
    let bytes = name.as_bytes();
    let mut hash: u32 = 0x811c_9dc5;
    let mut i = 0;
    while i < bytes.len() {
        hash = (hash ^ bytes[i] as u32).wrapping_mul(0x0100_0193);
        i += 1;
    }
    hash
}

/// The Daedalus transport key of a port type: the planner type checks edges by key. Builtin
/// scalars use the keys of the full runtime (`typeexpr:{"Scalar":"F32"}`, ...); give other
/// types a stable key with [`mcu_type!`].
pub trait McuType {
    const KEY: &'static str;
}

/// Give types a transport key: `mcu_type!(Sample => "demo:sample", Pose => "demo:pose");`.
#[macro_export]
macro_rules! mcu_type {
    ($($ty:ty => $key:expr),+ $(,)?) => {
        $(impl $crate::McuType for $ty {
            const KEY: &'static str = $key;
        })+
    };
}

macro_rules! scalar_keys {
    ($($ty:ty => $name:literal),* $(,)?) => {
        $(impl McuType for $ty {
            const KEY: &'static str = concat!("typeexpr:{\"Scalar\":\"", $name, "\"}");
        })*
    };
}

scalar_keys! {
    () => "Unit", bool => "Bool", i8 => "I8", i16 => "I16", i32 => "I32", i64 => "Int",
    isize => "ISize", u8 => "U8", u16 => "U16", u32 => "U32", u64 => "U64", usize => "USize",
    f32 => "F32", f64 => "Float",
}

#[cfg(feature = "alloc")]
extern crate alloc;
#[cfg(feature = "alloc")]
scalar_keys! { alloc::string::String => "String", alloc::vec::Vec<u8> => "Bytes" }

/// A node's state slot (`state(T)` in [`node`]), initialised in a `const fn` so a whole graph
/// can live in a `static`.
pub trait NodeState {
    const INIT: Self;
}

macro_rules! zero_state {
    ($($ty:ty => $zero:expr),* $(,)?) => {
        $(impl NodeState for $ty {
            const INIT: Self = $zero;
        })*
    };
}

zero_state! {
    () => (), bool => false, i8 => 0, i16 => 0, i32 => 0, i64 => 0, isize => 0, u8 => 0,
    u16 => 0, u32 => 0, u64 => 0, usize => 0, f32 => 0.0, f64 => 0.0,
}

impl<T> NodeState for Option<T> {
    const INIT: Self = None;
}

impl<T: NodeState, const N: usize> NodeState for [T; N] {
    const INIT: Self = [const { T::INIT }; N];
}

/// A node function's declaration, generated by [`node`] as `<fn>::NODE` and read on the host by
/// `daedalus-mcu-build`.
#[derive(Clone, Copy, Debug)]
pub struct NodeDesc {
    pub id: &'static str,
    /// Module path of the generated glue (`<crate>::<fn>`): its `run`, `State`, `In<k>`, `Out<k>`.
    pub path: &'static str,
    pub inputs: &'static [PortDesc],
    pub outputs: &'static [PortDesc],
    /// Declared fire mode `all` (`fire = "all"`); graph node metadata may override it.
    pub fire_all: bool,
}

#[derive(Clone, Copy, Debug)]
pub struct PortDesc {
    pub name: &'static str,
    /// [`McuType::KEY`] of the port type.
    pub key: &'static str,
    /// An `Option` input (never blocks the node) or a conditional (`Option`) output.
    pub optional: bool,
}
