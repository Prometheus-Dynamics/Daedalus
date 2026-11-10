//! Handles: a place in a table and the generation that was put there. Releasing a handle
//! moves its place to the next generation, so the old number is recognised and refused for as
//! long as the process lives, whatever is put there after.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::status::Status;

struct Place<T> {
    generation: u32,
    value: Option<Arc<Mutex<T>>>,
}

/// The live values of one kind. `kind` is in every handle, so a host's handle is never taken
/// for a graph's.
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

    fn handle(&self, place: usize, generation: u32) -> u64 {
        (self.kind << (PLACE_BITS + GENERATION_BITS))
            | (u64::from(generation) << PLACE_BITS)
            | place as u64
    }

    /// Where a handle points, if it is one of this table's kind.
    fn parts(&self, handle: u64) -> Option<(usize, u32)> {
        let kind = handle >> (PLACE_BITS + GENERATION_BITS);
        let generation = ((handle >> PLACE_BITS) & GENERATION_MASK) as u32;
        (handle != 0 && kind == self.kind && generation != 0)
            .then_some(((handle & u64::from(u32::MAX)) as usize, generation))
    }

    /// Puts `value` in the table and gives its handle.
    pub(crate) fn insert(&self, value: T) -> u64 {
        let mut places = lock(&self.places);
        let value = Some(Arc::new(Mutex::new(value)));
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

    /// The value a handle names. The table's lock is let go before the value is used, so a
    /// long call on one graph doesn't hold up another.
    pub(crate) fn get(&self, handle: u64) -> Result<Arc<Mutex<T>>, Status> {
        let (at, generation) = self.parts(handle).ok_or(Status::BAD_HANDLE)?;
        let places = lock(&self.places);
        let place = places.get(at).ok_or(Status::BAD_HANDLE)?;
        match &place.value {
            Some(value) if place.generation == generation => Ok(Arc::clone(value)),
            // Released, or the place has since been given to another.
            _ if generation <= place.generation => Err(Status::STALE_HANDLE),
            _ => Err(Status::BAD_HANDLE),
        }
    }

    /// Takes the value out, unless `keep` says it has to stay. The handle is stale from then.
    pub(crate) fn remove(
        &self,
        handle: u64,
        keep: impl FnOnce(&T) -> Option<Status>,
    ) -> Result<Arc<Mutex<T>>, Status> {
        let (at, generation) = self.parts(handle).ok_or(Status::BAD_HANDLE)?;
        let mut places = lock(&self.places);
        let place = places.get_mut(at).ok_or(Status::BAD_HANDLE)?;
        match &place.value {
            Some(value) if place.generation == generation => {
                if let Some(status) = keep(&lock(value)) {
                    return Err(status);
                }
            }
            _ if generation <= place.generation => return Err(Status::STALE_HANDLE),
            _ => return Err(Status::BAD_HANDLE),
        }
        place.value.take().ok_or(Status::STALE_HANDLE)
    }

    /// How many values are live.
    pub(crate) fn live(&self) -> usize {
        lock(&self.places)
            .iter()
            .filter(|place| place.value.is_some())
            .count()
    }
}

#[cfg(test)]
mod tests {
    use super::Table;
    use crate::status::Status;

    #[test]
    fn a_released_handle_is_stale_and_a_made_up_one_is_bad() {
        let table: Table<u32> = Table::new(7);
        let other: Table<u32> = Table::new(8);
        let first = table.insert(1);
        assert_ne!(first, 0);
        assert!(table.get(first).is_ok());
        assert_eq!(table.get(0).err(), Some(Status::BAD_HANDLE));
        assert_eq!(table.get(12345).err(), Some(Status::BAD_HANDLE), "no kind");
        assert_eq!(
            other.get(first).err(),
            Some(Status::BAD_HANDLE),
            "another kind"
        );
        assert_eq!(
            table.get(first + 1).err(),
            Some(Status::BAD_HANDLE),
            "no such place"
        );

        // Kept while asked to be.
        assert_eq!(
            table.remove(first, |_| Some(Status::BUSY)).err(),
            Some(Status::BUSY)
        );
        assert!(table.get(first).is_ok());
        assert!(table.remove(first, |_| None).is_ok());
        assert_eq!(table.get(first).err(), Some(Status::STALE_HANDLE));
        assert_eq!(
            table.remove(first, |_| None).err(),
            Some(Status::STALE_HANDLE)
        );

        // The place is used again, and the old number still isn't it.
        let second = table.insert(2);
        assert_ne!(second, first);
        assert_eq!(second & 0xffff_ffff, first & 0xffff_ffff, "the same place");
        assert_eq!(table.get(first).err(), Some(Status::STALE_HANDLE));
        assert_eq!(*super::lock(&table.get(second).unwrap()), 2);
        assert_eq!(table.live(), 1);
    }
}
