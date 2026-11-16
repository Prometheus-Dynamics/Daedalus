//! Handles: a place in a table and the generation that was put there. Closing a handle moves its
//! place to the next generation, so the old number is recognised and refused for as long as the
//! process lives, whatever is put there after.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::guard::fail;
use crate::status::Status;

/// A client or a watch. Zero is never one.
pub type Handle = u64;

struct Place<T> {
    generation: u32,
    value: Option<Arc<T>>,
}

/// The live values of one kind. `kind` is in every handle, so a client's handle is never taken
/// for a watch's.
pub(crate) struct Table<T> {
    kind: u64,
    places: Mutex<Vec<Place<T>>>,
}

/// Bits of a handle: 8 of kind, 24 of generation, 32 of place. A place's first generation is 1,
/// so no handle is zero.
const PLACE_BITS: u32 = 32;
const GENERATION_BITS: u32 = 24;
const GENERATION_MASK: u64 = (1 << GENERATION_BITS) - 1;

pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

impl<T> Table<T> {
    pub(crate) const fn new(kind: u64) -> Table<T> {
        Table {
            kind,
            places: Mutex::new(Vec::new()),
        }
    }

    fn handle(&self, place: usize, generation: u32) -> Handle {
        (self.kind << (PLACE_BITS + GENERATION_BITS))
            | (u64::from(generation) << PLACE_BITS)
            | place as u64
    }

    /// Where a handle points, if it is one of this table's kind.
    fn parts(&self, handle: Handle) -> Option<(usize, u32)> {
        let kind = handle >> (PLACE_BITS + GENERATION_BITS);
        let generation = ((handle >> PLACE_BITS) & GENERATION_MASK) as u32;
        (handle != 0 && kind == self.kind && generation != 0)
            .then_some(((handle & u64::from(u32::MAX)) as usize, generation))
    }

    /// Puts `value` in the table and gives its handle.
    pub(crate) fn insert(&self, value: T) -> Handle {
        let mut places = lock(&self.places);
        let value = Some(Arc::new(value));
        // A place whose generations ran out is never used again.
        let free = places.iter().position(|place| {
            place.value.is_none() && u64::from(place.generation) < GENERATION_MASK
        });
        match free {
            Some(at) => {
                places[at].generation += 1;
                places[at].value = value;
                self.handle(at, places[at].generation)
            }
            None => {
                places.push(Place {
                    generation: 1,
                    value,
                });
                self.handle(places.len() - 1, 1)
            }
        }
    }

    /// The get and remove refusal: a handle whose place has been given on, or closed, is stale;
    /// one that never named a place is bad.
    fn refusal(generation: u32, place: Option<u32>) -> Status {
        match place {
            Some(current) if generation <= current => Status::STALE_HANDLE,
            _ => Status::BAD_HANDLE,
        }
    }

    /// The value a handle names. The table's lock is let go before the value is used.
    pub(crate) fn get(&self, handle: Handle) -> Result<Arc<T>, Status> {
        let Some((at, generation)) = self.parts(handle) else {
            return Err(fail(Status::BAD_HANDLE, "not a handle of this kind"));
        };
        let places = lock(&self.places);
        match places.get(at) {
            Some(Place {
                value: Some(value),
                generation: current,
            }) if *current == generation => Ok(Arc::clone(value)),
            place => {
                let status = Self::refusal(generation, place.map(|p| p.generation));
                Err(fail(
                    status,
                    "the handle was closed, or was never given out",
                ))
            }
        }
    }

    /// Takes the value out. The handle is stale from then.
    pub(crate) fn remove(&self, handle: Handle) -> Result<Arc<T>, Status> {
        let Some((at, generation)) = self.parts(handle) else {
            return Err(fail(Status::BAD_HANDLE, "not a handle of this kind"));
        };
        let mut places = lock(&self.places);
        match places.get_mut(at) {
            Some(Place {
                value: slot,
                generation: current,
            }) if *current == generation => match slot.take() {
                Some(value) => Ok(value),
                None => Err(fail(Status::STALE_HANDLE, "the handle was closed")),
            },
            place => {
                let status = Self::refusal(generation, place.map(|p| p.generation));
                Err(fail(
                    status,
                    "the handle was closed, or was never given out",
                ))
            }
        }
    }

    /// How many values are live.
    pub(crate) fn live(&self) -> usize {
        lock(&self.places)
            .iter()
            .filter(|place| place.value.is_some())
            .count()
    }
}
