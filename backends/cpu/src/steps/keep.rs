//! Keeping a CPU output's memory from one tick to the next.
//!
//! An output that is made new each tick costs one allocation and one free a tick, since the value
//! is put behind a new `Arc`. The planner says how each output is consumed (see
//! `daedalus_backend::Sharing`), and a producer keeps what it needs to avoid that:
//!
//! - `Readers`: the producer keeps the `Arc`s it published in a small pool. Once every consumer has
//!   let go of one, the producer refills it in place. Consumers only read, so nothing is taken.
//! - `Taker`: the value sits in a [`daedalus_runner::Carried`], a shared cell that holds an `Option`. The one
//!   consumer takes the value out of the cell, and leaves the cell in place. Once the consumer has
//!   let go of a cell, the producer puts the next value in it. A value is moved, never copied.
//!
//! The pool holds more than one, because a queue with several readers keeps its last value until
//! the next one replaces it: a single spare would still be held when the next value is made.
//!
//! A panic in a node body leaves nothing behind. A carrier that was taken holds `None`, and
//! dropping it frees the value the consumer did not take. Nothing is freed twice.

use core::marker::PhantomData;

use daedalus_abi::Slot;

/// Most values a pool keeps. A pool that is full drops one of its references, so a consumer that
/// keeps every value cannot make the producer's pool grow without bound.
#[cfg(feature = "alloc")]
const POOL_LIMIT: usize = 8;

/// What a producer does with an output, from the planner's answer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Keep {
    #[default]
    Fresh,
    Readers,
    Taker,
}

/// Picks what an output holds between ticks: nothing for an output written inline, and a
/// [`Spare`] otherwise. Chosen by the output's type while the step is built, so an inline output
/// adds no drop glue to the step.
pub struct Kept<T, const INLINE: bool>(PhantomData<fn() -> T>);

impl<T, const INLINE: bool> Default for Kept<T, INLINE> {
    fn default() -> Self {
        Kept(PhantomData)
    }
}

/// What an output of type `T` holds between ticks. `INLINE` is whether `T` is written inline.
#[cfg(feature = "alloc")]
pub type Keeping<T, const INLINE: bool> = <Kept<T, INLINE> as Holds<T>>::Held;

/// What an output of type `T` holds between ticks. Without an allocator nothing is kept.
#[cfg(not(feature = "alloc"))]
pub type Keeping<T, const INLINE: bool> = Kept<T, INLINE>;

/// The answer for [`Kept`]: what an output holds.
pub trait Holds<T> {
    type Held: Keeper<T>;
}

/// Puts a value in a slot, keeping its memory as `keep` says.
pub trait Keeper<T> {
    fn publish(&mut self, value: T, keep: Keep, slot: &mut Slot)
    where
        T: Send + Sync + 'static;
}

/// Nothing is kept by an inline output, which never reaches `publish`.
#[cfg(feature = "alloc")]
impl<T: 'static> Holds<T> for Kept<T, true> {
    type Held = ();
}

/// An output that is not written inline keeps a [`Spare`].
#[cfg(feature = "alloc")]
impl<T: 'static> Holds<T> for Kept<T, false> {
    type Held = Spare<T>;
}

#[cfg(feature = "alloc")]
impl<T> Keeper<T> for () {
    fn publish(&mut self, value: T, _: Keep, slot: &mut Slot)
    where
        T: Send + Sync + 'static,
    {
        daedalus_runner::put_typed(
            slot,
            daedalus_runner::UNSTAMPED.0,
            daedalus_runner::UNSTAMPED.1,
            alloc::sync::Arc::new(value),
        );
    }
}

#[cfg(not(feature = "alloc"))]
impl<T, const INLINE: bool> Keeper<T> for Kept<T, INLINE> {
    fn publish(&mut self, _: T, _: Keep, _: &mut Slot)
    where
        T: Send + Sync + 'static,
    {
    }
}

#[cfg(feature = "alloc")]
pub use with_alloc::{Spare, take_carried};

#[cfg(feature = "alloc")]
mod with_alloc {
    use alloc::sync::Arc;
    use core::mem::replace;

    use daedalus_abi::Slot;
    use daedalus_runner::{Carried, UNSTAMPED, put_typed, take_typed};

    use super::{Keep, Keeper, POOL_LIMIT};

    /// The memory an output keeps from one tick to the next. Each kind keeps a small pool: a value
    /// a queue still holds is busy, and the queue may hold its last value for several ticks, so
    /// one spare would often be busy when the next value is made.
    pub enum Spare<T> {
        /// Nothing is kept yet.
        Empty,
        /// Values that every consumer reads.
        Readers(Vec<Arc<T>>),
        /// Cells that a single consumer takes its value from.
        Taker(Vec<Arc<Carried<T>>>),
    }

    // A derive would ask for `T: Default`, which a value need not have.
    #[allow(clippy::derivable_impls)]
    impl<T> Default for Spare<T> {
        fn default() -> Self {
            Spare::Empty
        }
    }

    impl<T> Keeper<T> for Spare<T> {
        fn publish(&mut self, value: T, keep: Keep, slot: &mut Slot)
        where
            T: Send + Sync + 'static,
        {
            match keep {
                Keep::Fresh => {
                    *self = Spare::Empty;
                    put_typed(slot, UNSTAMPED.0, UNSTAMPED.1, Arc::new(value));
                }
                Keep::Readers => {
                    let mut pool = match replace(self, Spare::Empty) {
                        Spare::Readers(pool) => pool,
                        _ => Vec::new(),
                    };
                    // A value is refilled only when nobody else holds it: the pool's is the only
                    // reference left.
                    let held = match pool.iter().position(|held| Arc::strong_count(held) == 1) {
                        Some(at) => {
                            let mut held = pool.swap_remove(at);
                            if let Some(current) = Arc::get_mut(&mut held) {
                                *current = value;
                            }
                            held
                        }
                        None => Arc::new(value),
                    };
                    pool.push(held.clone());
                    if pool.len() > POOL_LIMIT {
                        pool.swap_remove(0);
                    }
                    *self = Spare::Readers(pool);
                    put_typed(slot, UNSTAMPED.0, UNSTAMPED.1, held);
                }
                Keep::Taker => {
                    let mut pool = match replace(self, Spare::Empty) {
                        Spare::Taker(pool) => pool,
                        _ => Vec::new(),
                    };
                    // A cell is refilled only when the consumer has let go of it.
                    let cell = match pool.iter().position(|cell| Arc::strong_count(cell) == 1) {
                        Some(at) => {
                            let cell = pool.swap_remove(at);
                            cell.put(value);
                            cell
                        }
                        None => Arc::new(Carried::holding(value)),
                    };
                    pool.push(cell.clone());
                    if pool.len() > POOL_LIMIT {
                        pool.swap_remove(0);
                    }
                    *self = Spare::Taker(pool);
                    put_typed(slot, UNSTAMPED.0, UNSTAMPED.1, cell);
                }
            }
        }
    }

    /// Takes the carrier a cell holds, and the value in it. `None` when the cell holds no carrier
    /// of `T`, and nothing is moved then.
    pub fn take_carried<T: 'static>(cell: &mut Slot) -> Option<Option<T>> {
        take_typed::<Carried<T>>(cell).map(|carrier| carrier.take())
    }
}

#[cfg(not(feature = "alloc"))]
pub fn take_carried<T: 'static>(_: &mut Slot) -> Option<Option<T>> {
    None
}

/// The keep of each output of a node, from the planner's answers. An output the planner did not
/// name is `Fresh`.
pub fn keep_array<const N: usize>(keep: &[Keep]) -> [Keep; N] {
    let mut out = [Keep::Fresh; N];
    for (slot, &one) in out.iter_mut().zip(keep) {
        *slot = one;
    }
    out
}
