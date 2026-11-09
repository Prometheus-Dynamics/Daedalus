//! What the body expansion builds on. The expansion is in `daedalus-cpu-author`, and everything it
//! names lives here.

pub mod ports;

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::marker::PhantomData;
use core::mem::size_of;

use daedalus_backend::ConstOf;
use daedalus_graph::Lit;
use daedalus_runner::{Held, INLINE_BYTES, InlineCell, read_inline, write_inline};
use daedalus_types::{FromLiteral, Literal, NodeIndex};

pub use alloc::string::String as StepString;
pub use daedalus_abi::{FixedError, OwnedStep, Slot, Slots, Step, StepCx, StepEnd};

/// The sum of `counts`, for checks a body expansion makes at compile time.
pub const fn total(counts: &[usize]) -> usize {
    let (mut sum, mut at) = (0, 0);
    while at < counts.len() {
        sum += counts[at];
        at += 1;
    }
    sum
}

pub use crate::body::CpuBody;
pub use daedalus_runner::{ArenaBody, Fresh};

/// What one node in one graph needs from the plan.
pub struct Setup<'a> {
    pub node: NodeIndex,
    pub inputs: u16,
    pub consts: &'a [ConstOf<'a>],
}

impl Setup<'_> {
    fn value(&self, port: &str) -> Option<&Lit> {
        self.consts
            .iter()
            .find(|one| one.node == self.node && one.port == port)
            .map(|one| one.value)
    }

    /// The config's fields as literals: from a `config` struct, and any set one by one.
    pub fn config_fields(&self) -> Vec<(String, Literal)> {
        let mut fields = Vec::new();
        for one in self.consts.iter().filter(|one| one.node == self.node) {
            match (one.port, one.value) {
                ("config", Lit::Struct(all)) => fields.extend(
                    all.iter()
                        .map(|field| (field.name.clone(), literal(&field.value))),
                ),
                (port, value) => fields.push((String::from(port), literal(value))),
            }
        }
        fields
    }
}

/// Finds a field in [`Setup::config_fields`]' result.
pub fn field<'a>(fields: &'a [(String, Literal)], name: &str) -> Option<&'a Literal> {
    fields
        .iter()
        .find(|(field, _)| field == name)
        .map(|(_, value)| value)
}

/// A plan value as the `'static` form a config reads. Text and lists are kept for the life of the
/// process, once per plan.
fn literal(value: &Lit) -> Literal {
    use alloc::boxed::Box;
    match value {
        Lit::Unit => Literal::Unit,
        Lit::Enum { name, payload } => {
            let name: &'static str = Box::leak(name.clone().into_boxed_str());
            let payload: Option<&'static Literal> = payload
                .as_deref()
                .map(|value| &*Box::leak(Box::new(literal(value))));
            Literal::Enum(name, payload)
        }
        Lit::Bool(value) => Literal::Bool(*value),
        Lit::Int(value) => Literal::Int(*value),
        Lit::Float(value) => Literal::Float(*value),
        Lit::Str(value) => Literal::Str(Box::leak(value.clone().into_boxed_str())),
        Lit::Bytes(value) => Literal::Bytes(Box::leak(value.clone().into_boxed_slice())),
        Lit::List(values) => {
            let all: Vec<Literal> = values.iter().map(literal).collect();
            Literal::List(Box::leak(all.into_boxed_slice()))
        }
        Lit::Struct(fields) => {
            let all: Vec<(&'static str, Literal)> = fields
                .iter()
                .map(|field| {
                    let name: &'static str = Box::leak(field.name.clone().into_boxed_str());
                    (name, literal(&field.value))
                })
                .collect();
            Literal::Struct(Box::leak(all.into_boxed_slice()))
        }
    }
}

/// How a parameter's concrete type is stored. The expansion knows the type, so it picks each entry
/// with the probes below and the compiler removes the ones that don't apply.
pub struct Kind<T: 'static> {
    pub read: Option<fn(&Slot) -> Option<T>>,
    pub write: Option<fn(&T, &mut InlineCell, &mut Slot)>,
    pub clone: Option<fn(&T) -> T>,
    /// The type is `Copy`, so taking it leaves the original for any other consumer.
    pub copy: bool,
    pub decode: Option<fn(&Lit) -> Option<T>>,
}

impl<T> Clone for Kind<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Kind<T> {}

/// Whether `T` is a small `Copy` value, held inside its cell's own buffer.
pub struct CopyOf<T>(PhantomData<T>);

impl<T: Copy + 'static> CopyOf<T> {
    pub const IS: bool = true;
    pub const READ: Option<fn(&Slot) -> Option<T>> = if size_of::<T>() <= INLINE_BYTES {
        Some(read_inline::<T>)
    } else {
        None
    };
    pub const WRITE: Option<fn(&T, &mut InlineCell, &mut Slot)> = if size_of::<T>() <= INLINE_BYTES
    {
        Some(write_inline::<T>)
    } else {
        None
    };
}

/// The answer for a `T` that isn't `Copy`.
pub trait NotCopy<T> {
    const IS: bool = false;
    const READ: Option<fn(&Slot) -> Option<T>> = None;
    const WRITE: Option<fn(&T, &mut InlineCell, &mut Slot)> = None;
}

impl<T> NotCopy<T> for CopyOf<T> {}

/// Whether `T` can be cloned.
pub struct CloneOf<T>(PhantomData<T>);

impl<T: Clone + 'static> CloneOf<T> {
    pub const CLONE: Option<fn(&T) -> T> = Some(T::clone);
}

pub trait NotClone<T> {
    const CLONE: Option<fn(&T) -> T> = None;
}

impl<T> NotClone<T> for CloneOf<T> {}

/// Whether `T` can be built from a literal.
pub struct LitOf<T>(PhantomData<T>);

impl<T: FromLiteral + 'static> LitOf<T> {
    pub const DECODE: Option<fn(&Lit) -> Option<T>> =
        Some(|value| T::from_literal(&literal(value)).ok());
}

pub trait NotLit<T> {
    const DECODE: Option<fn(&Lit) -> Option<T>> = None;
}

impl<T> NotLit<T> for LitOf<T> {}

/// Where a step finds an input's constant.
pub trait ConstTable {
    fn slot(&self, at: usize) -> Option<&Slot>;
}

/// One constant per fixed input that has one. Empty when the node has none.
pub type Consts = Vec<Option<Held>>;

impl ConstTable for Consts {
    #[inline]
    fn slot(&self, at: usize) -> Option<&Slot> {
        self.get(at)?.as_ref().map(Held::slot)
    }
}

/// For a step whose constants are written into its input cells before it runs.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoConsts;

impl ConstTable for NoConsts {
    #[inline]
    fn slot(&self, _: usize) -> Option<&Slot> {
        None
    }
}

/// The empty table for `fixed` inputs. It only takes room if this node has a constant.
pub fn consts_for(setup: &Setup<'_>, fixed: usize) -> Consts {
    let any = setup.consts.iter().any(|one| one.node == setup.node);
    let mut consts = Vec::new();
    if any {
        consts.resize_with(fixed, || None);
    }
    consts
}

/// Fixes input `at`'s constant, if the graph gave one.
pub fn store_const<T: Send + Sync + 'static>(
    setup: &Setup<'_>,
    port: &str,
    at: usize,
    kind: Kind<T>,
    consts: &mut Consts,
) -> Result<(), String> {
    let Some(value) = setup.value(port) else {
        return Ok(());
    };
    let decode = kind
        .decode
        .ok_or_else(|| format!("port `{port}` can't be set from a constant"))?;
    let decoded =
        decode(value).ok_or_else(|| format!("the constant for port `{port}` doesn't fit"))?;
    consts[at] = Some(Held::new(|buf, slot| {
        ports::encode(decoded, kind, buf, slot);
    }));
    Ok(())
}
