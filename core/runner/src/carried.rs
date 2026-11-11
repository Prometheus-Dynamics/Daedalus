//! A value that one holder takes out of a shared cell. A producer keeps the cell and refills it
//! once the holder lets go, so a value is moved to the holder and never copied.

use spin::Mutex;

/// A cell that holds one value for one taker. The taker moves the value out and leaves the cell
/// empty, so a value is never read by two holders at once.
pub struct Carried<T>(Mutex<Option<T>>);

impl<T> Carried<T> {
    /// A cell holding `value`.
    pub fn holding(value: T) -> Carried<T> {
        Carried(Mutex::new(Some(value)))
    }

    /// Moves the value out. `None` if it was taken already.
    pub fn take(&self) -> Option<T> {
        self.0.lock().take()
    }

    /// Puts the next value in. Whatever the cell held before is dropped.
    pub fn put(&self, value: T) {
        *self.0.lock() = Some(value);
    }
}
