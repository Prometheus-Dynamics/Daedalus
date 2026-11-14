//! A node made at run time from a plain closure. A host that only knows what a node does once
//! it is running (a threshold read from a file, a handle it was given) can't write a `#[node]`
//! function for it. [`closure_node`] takes the closure and the names of its ports, and gives
//! the declaration to register: the cells, the types and the fault are handled here.
//!
//! ```ignore
//! let limit = read_limit();
//! registry.register_owned(closure_node(
//!     "app.over_limit",
//!     "made at run time",
//!     ["level"],
//!     "over",
//!     move |level: u32| Ok::<bool, Fault>(level > limit),
//! )?)?;
//! ```

#![cfg(feature = "alloc")]

use alloc::string::{String, ToString};
use alloc::sync::Arc;
use alloc::vec::Vec;

use daedalus_abi::{Fault, OwnedStep, Slot, Slots, Step, StepCx, StepEnd};
use daedalus_registry::{OwnedBody, OwnedDecl, OwnedPort, TypeRef};
use daedalus_runner::{
    InlineCell, UNSTAMPED, put_typed, read_inline, share_typed, typed_view, write_inline,
};
use daedalus_types::{TypeExpr, Typed};

use crate::body::CpuStepBody;

/// A value a closure node takes or gives, and how it sits in a cell: the same way a `#[node]`
/// function's does, so the two kinds of node pass values to each other.
pub trait Carried: Send + Sync + Sized + 'static {
    /// The type of the port that carries it.
    const EXPR: TypeExpr;
    /// The value a full cell holds.
    fn get(cell: &Slot) -> Option<Self>;
    /// Stores the value in `cell`, using `buf` if it is small enough to sit there.
    fn put(self, buf: &mut InlineCell, cell: &mut Slot);
}

macro_rules! carried_inline {
    ($($ty:ty),*) => {$(
        impl Carried for $ty {
            const EXPR: TypeExpr = <$ty as Typed>::EXPR;
            fn get(cell: &Slot) -> Option<$ty> {
                read_inline::<$ty>(cell).or_else(|| typed_view::<$ty>(cell).copied())
            }
            fn put(self, buf: &mut InlineCell, cell: &mut Slot) {
                write_inline(&self, buf, cell);
            }
        }
    )*};
}

carried_inline!(bool, u8, u16, u32, u64, i8, i16, i32, i64, f32, f64);

/// Text is copied for the closure, and a new handle is made for what it returns. A closure that
/// should copy nothing takes and gives `Arc<String>`.
impl Carried for String {
    const EXPR: TypeExpr = <String as Typed>::EXPR;
    fn get(cell: &Slot) -> Option<String> {
        typed_view::<String>(cell).cloned()
    }
    fn put(self, _: &mut InlineCell, cell: &mut Slot) {
        put_typed(cell, UNSTAMPED.0, UNSTAMPED.1, Arc::new(self));
    }
}

/// A handle on a value: taking one copies nothing, and giving one puts it in the cell as it
/// is. For a value too large to copy, or one that isn't `Clone`.
impl<T: Typed + Send + Sync + 'static> Carried for Arc<T> {
    const EXPR: TypeExpr = <T as Typed>::EXPR;
    fn get(cell: &Slot) -> Option<Arc<T>> {
        share_typed(cell)?.downcast::<T>().ok()
    }
    fn put(self, _: &mut InlineCell, cell: &mut Slot) {
        put_typed(cell, UNSTAMPED.0, UNSTAMPED.1, self);
    }
}

/// What a closure node gives: one value, or nothing.
pub trait Given: Send + Sync + 'static {
    /// The type of the output port, when there is one.
    const EXPR: Option<TypeExpr>;
    fn put(self, buf: &mut InlineCell, cell: Option<&mut Slot>);
}

impl Given for () {
    const EXPR: Option<TypeExpr> = None;
    fn put(self, _: &mut InlineCell, _: Option<&mut Slot>) {}
}

impl<T: Carried> Given for T {
    const EXPR: Option<TypeExpr> = Some(T::EXPR);
    fn put(self, buf: &mut InlineCell, cell: Option<&mut Slot>) {
        if let Some(cell) = cell {
            Carried::put(self, buf, cell);
        }
    }
}

/// A closure that can be a node's body: up to four inputs, each a [`Carried`] value, and a
/// result of `Result<R, Fault>` where `R` is a [`Given`].
pub trait NodeFn<Args>: Send + Sync + 'static {
    /// The types of the inputs, in order.
    fn inputs() -> Vec<TypeExpr>;
    /// The type of the output, when there is one.
    fn output() -> Option<TypeExpr>;
    /// Reads the inputs from the first cells, calls the closure, and writes its result to the
    /// cell after them. `None` when an input isn't there or isn't its type.
    fn call(&self, slots: &mut Slots, buf: &mut InlineCell) -> Option<Result<(), Fault>>;
}

macro_rules! node_fn {
    ($count:expr; $($arg:ident $at:tt),*) => {
        impl<F, R, $($arg),*> NodeFn<($($arg,)*)> for F
        where
            F: Fn($($arg),*) -> Result<R, Fault> + Send + Sync + 'static,
            R: Given,
            $($arg: Carried,)*
        {
            fn inputs() -> Vec<TypeExpr> {
                alloc::vec![$($arg::EXPR),*]
            }

            fn output() -> Option<TypeExpr> {
                R::EXPR
            }

            #[allow(non_snake_case)]
            fn call(&self, slots: &mut Slots, buf: &mut InlineCell) -> Option<Result<(), Fault>> {
                $(let $arg = $arg::get(slots.cell($at)?)?;)*
                Some(self($($arg),*).map(|given| given.put(buf, slots.cell($count))))
            }
        }
    };
}

node_fn!(0;);
node_fn!(1; A 0);
node_fn!(2; A 0, B 1);
node_fn!(3; A 0, B 1, C 2);
node_fn!(4; A 0, B 1, C 2, D 3);

/// The step of one closure node in one plan.
struct ClosureStep<F, Args> {
    body: Arc<F>,
    buf: InlineCell,
    of: core::marker::PhantomData<fn() -> Args>,
}

impl<Args: 'static, F: NodeFn<Args>> OwnedStep for ClosureStep<F, Args> {
    fn run(&mut self, slots: &mut Slots, cx: &mut StepCx) -> StepEnd {
        match self.body.call(slots, &mut self.buf) {
            Some(Ok(())) => StepEnd::Done,
            Some(Err(fault)) => {
                cx.fail(0, &fault);
                StepEnd::Fault
            }
            None => {
                cx.fault(
                    Some(0),
                    "an input was missing, or wasn't the type its port declares",
                );
                StepEnd::Fault
            }
        }
    }
}

/// Why a closure can't be the node described.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClosureError {
    /// The closure takes a different number of inputs than were named.
    Inputs { named: usize, takes: usize },
    /// The closure gives a value and no output was named, or the other way round.
    Output { named: bool, gives: bool },
}

impl core::fmt::Display for ClosureError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            ClosureError::Inputs { named, takes } => {
                write!(f, "{named} inputs were named and the closure takes {takes}")
            }
            ClosureError::Output { named: true, .. } => {
                f.write_str("an output was named and the closure gives nothing")
            }
            ClosureError::Output { .. } => {
                f.write_str("the closure gives a value and no output was named")
            }
        }
    }
}

/// The declaration of a node whose body is `body`, for `Registry::register_owned`. `inputs` are
/// the names of its input ports, in the order the closure takes them, and `output` the name of
/// its output port: `Some` when the closure gives a value and `None` when it gives `()`. The
/// port types are the closure's own.
///
/// The node runs as a step in this process, on the CPU backend. It isn't merged with its
/// neighbours and can't be written out as source or a blob, and its inputs come from edges: a
/// constant is given to the closure itself, which is what it is for.
pub fn closure_node<Args: 'static, F: NodeFn<Args>>(
    id: &str,
    source: &str,
    inputs: &[&str],
    output: Option<&str>,
    body: F,
) -> Result<OwnedDecl, ClosureError> {
    let types = F::inputs();
    if types.len() != inputs.len() {
        return Err(ClosureError::Inputs {
            named: inputs.len(),
            takes: types.len(),
        });
    }
    let given = F::output();
    if given.is_some() != output.is_some() {
        return Err(ClosureError::Output {
            named: output.is_some(),
            gives: given.is_some(),
        });
    }
    let port = |name: &str, ty: TypeExpr| OwnedPort {
        name: name.to_string(),
        ty: TypeRef::Expr(ty),
        optional: false,
    };
    let body = Arc::new(body);
    Ok(OwnedDecl {
        id: id.to_string(),
        source: source.to_string(),
        inputs: inputs
            .iter()
            .zip(types)
            .map(|(name, ty)| port(name, ty))
            .collect(),
        outputs: output
            .zip(given)
            .map(|(name, ty)| port(name, ty))
            .into_iter()
            .collect(),
        body: OwnedBody::code(
            crate::BACKEND_ID,
            id,
            Arc::new(CpuStepBody::new(move |_| {
                Ok(Step::boxed(ClosureStep::<F, Args> {
                    body: Arc::clone(&body),
                    buf: [0; daedalus_runner::INLINE_BYTES],
                    of: core::marker::PhantomData,
                }))
            })),
        ),
        conversion: None,
    })
}
