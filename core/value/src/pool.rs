//! Recycling with no heap, for chips where a decoder reuses one value.

#[derive(Debug)]
pub struct FixedPool<T, const N: usize> {
    items: [Option<T>; N],
}

impl<T, const N: usize> FixedPool<T, N> {
    pub const fn new() -> Self {
        FixedPool {
            items: [const { None }; N],
        }
    }

    /// Gives `item` back. A full pool returns it.
    pub fn put(&mut self, item: T) -> Result<(), T> {
        match self.items.iter_mut().find(|cell| cell.is_none()) {
            Some(cell) => {
                *cell = Some(item);
                Ok(())
            }
            None => Err(item),
        }
    }

    pub fn take(&mut self) -> Option<T> {
        self.items.iter_mut().find_map(Option::take)
    }
}

impl<T, const N: usize> Default for FixedPool<T, N> {
    fn default() -> Self {
        Self::new()
    }
}
