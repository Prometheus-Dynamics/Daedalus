//! What the body expansion builds on. The expansion is in `daedalus-cpu-author`, and everything it
//! names lives here.

pub mod keep;
pub mod ports;

pub use keep::{Keep, Keeper, Keeping, Kept, keep_array};

use core::marker::PhantomData;
use core::mem::size_of;

use daedalus_runner::{INLINE_BYTES, InlineCell, read_inline, write_inline};

pub use daedalus_abi::{FixedError, OwnedStep, Slot, Slots, StepCx, StepEnd};

#[cfg(feature = "alloc")]
mod setup;
#[cfg(feature = "alloc")]
pub use daedalus_abi::Step;
#[cfg(feature = "alloc")]
pub use setup::{Consts, Lit, Setup, StepString, consts_for, field, store_const};

/// In a build with no allocator there are no literals, so nothing can be decoded from one.
#[cfg(not(feature = "alloc"))]
pub enum Lit {}

/// Keeps its tokens only in a build with an allocator.
#[cfg(feature = "alloc")]
#[macro_export]
#[doc(hidden)]
macro_rules! __if_alloc {
    ($($item:tt)*) => { $($item)* };
}

#[cfg(not(feature = "alloc"))]
#[macro_export]
#[doc(hidden)]
macro_rules! __if_alloc {
    ($($item:tt)*) => {};
}

/// A `CpuBody`, with `$make` included only where the field exists.
#[cfg(feature = "alloc")]
#[macro_export]
#[doc(hidden)]
macro_rules! __cpu_body {
    ($make:ident, $shape:expr, $arena:expr) => {
        $crate::steps::CpuBody {
            make: $make,
            shape: $shape,
            arena: $arena,
        }
    };
}

#[cfg(not(feature = "alloc"))]
#[macro_export]
#[doc(hidden)]
macro_rules! __cpu_body {
    ($make:ident, $shape:expr, $arena:expr) => {
        $crate::steps::CpuBody { arena: $arena }
    };
}

pub use crate::{__cpu_body, __if_alloc};

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
pub use crate::shape::{BodyShape, OutputShape, ParamShape, Role};
pub use daedalus_runner::{ArenaBody, Fresh};

/// How a parameter's concrete type is stored. The expansion knows the type, so it picks each entry
/// with the probes below and the compiler removes the ones that don't apply.
pub struct Kind<T: 'static> {
    pub read: Option<fn(&Slot) -> Option<T>>,
    pub write: Option<fn(&T, &mut InlineCell, &mut Slot)>,
    pub clone: Option<fn(&T) -> T>,
    /// The type is `Copy`, so taking it leaves the original for any other consumer.
    pub copy: bool,
    pub decode: Option<fn(&Lit) -> Option<T>>,
    /// Set when `T` is `Arc<U>`: the handle itself is what a parameter takes and a result
    /// gives, so a value handed on is neither copied nor put behind a new handle.
    pub handle: Option<Handle<T>>,
    /// Set when `T` can be made from bytes a cell holds, by copying them: a `Vec<u8>` is. A handed
    /// value reaches a parameter of such a type through this, which is the one copy it makes.
    pub from_bytes: Option<FromBytes<T>>,
}

/// How a `T` is copied out of bytes a cell holds.
pub type FromBytes<T> = fn(&[u8]) -> Option<T>;

/// How a handle type goes in and out of a cell.
pub struct Handle<T: 'static> {
    /// Takes the cell's handle out of it.
    pub take: fn(&mut Slot) -> Option<T>,
    /// Another handle to what the cell holds, leaving the cell's own.
    pub share: fn(&Slot) -> Option<T>,
    /// Puts the handle in a cell, as it is.
    pub put: fn(T, &mut Slot),
}

impl<T> Clone for Handle<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T> Copy for Handle<T> {}

/// Whether `T` is a handle, `Arc<U>`.
pub struct HandleOf<T>(PhantomData<T>);

/// Whether `T` is `SharedBytes`, the bytes a plugin handed over: a handle, taken by move.
impl HandleOf<daedalus_abi::SharedBytes> {
    pub const HANDLE: Option<Handle<daedalus_abi::SharedBytes>> = Some(Handle {
        take: ports::take_handed_bytes,
        share: ports::share_handed_bytes,
        put: ports::put_handed_bytes,
    });
}

/// Whether `T` can be copied out of bytes. A `Vec<u8>` can.
pub struct BytesOf<T>(PhantomData<T>);

#[cfg(feature = "alloc")]
impl BytesOf<alloc::vec::Vec<u8>> {
    pub const FROM: Option<FromBytes<alloc::vec::Vec<u8>>> = Some(|bytes| Some(bytes.to_vec()));
}

/// The answer for a `T` that can't be copied out of bytes.
pub trait NotBytes<T: 'static> {
    const FROM: Option<FromBytes<T>> = None;
}

impl<T: 'static> NotBytes<T> for BytesOf<T> {}

#[cfg(feature = "alloc")]
impl<U: Send + Sync + 'static> HandleOf<alloc::sync::Arc<U>> {
    pub const HANDLE: Option<Handle<alloc::sync::Arc<U>>> = Some(Handle {
        take: ports::take_handle::<U>,
        share: ports::share_handle::<U>,
        put: ports::put_handle::<U>,
    });
}

/// The answer for a `T` that isn't a handle.
pub trait NotHandle<T: 'static> {
    const HANDLE: Option<Handle<T>> = None;
}

impl<T: 'static> NotHandle<T> for HandleOf<T> {}

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

pub trait NotLit<T> {
    const DECODE: Option<fn(&Lit) -> Option<T>> = None;
}

impl<T> NotLit<T> for LitOf<T> {}

/// Where a step finds an input's constant.
pub trait ConstTable {
    fn slot(&self, at: usize) -> Option<&Slot>;
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
