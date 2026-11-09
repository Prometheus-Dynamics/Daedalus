//! A slot: a fixed ring of values, in storage sized when the graph loads.

use crate::Value;

/// What happened to a pushed value. C15's pressure rules are what produce `Dropped`.
#[derive(Debug)]
pub enum Push {
    Stored,
    /// The ring made room by giving up this value.
    Dropped(Value),
    /// The ring is full and the value was not taken.
    Full(Value),
}

#[derive(Debug)]
pub struct Ring<'a> {
    cells: &'a mut [Option<Value>],
    head: u32,
    len: u32,
    unconsumed: u32,
    version: u32,
}

impl<'a> Ring<'a> {
    pub fn new(cells: &'a mut [Option<Value>]) -> Ring<'a> {
        Ring {
            cells,
            head: 0,
            len: 0,
            unconsumed: 0,
            version: 0,
        }
    }

    pub fn capacity(&self) -> usize {
        self.cells.len()
    }

    pub fn len(&self) -> usize {
        self.len as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Goes up on every stored value, so a step can tell whether an input changed.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Values pushed and not yet consumed.
    pub fn unconsumed(&self) -> u32 {
        self.unconsumed
    }

    /// Stores `value` behind the others.
    pub fn push(&mut self, value: Value) -> Push {
        if self.len() == self.capacity() {
            return Push::Full(value);
        }
        let at = (self.head as usize + self.len()) % self.capacity();
        self.cells[at] = Some(value);
        self.len += 1;
        self.unconsumed += 1;
        self.version = self.version.wrapping_add(1);
        Push::Stored
    }

    /// The oldest value, left in place.
    pub fn peek(&self) -> Option<&Value> {
        if self.is_empty() {
            return None;
        }
        self.cells[self.head as usize].as_ref()
    }

    /// Takes the oldest value out. This is what a move does.
    pub fn pop(&mut self) -> Option<Value> {
        if self.is_empty() {
            return None;
        }
        let value = self.cells[self.head as usize].take();
        self.head = (self.head + 1) % self.capacity() as u32;
        self.len -= 1;
        self.unconsumed = self.unconsumed.min(self.len);
        value
    }

    /// Marks everything as read and keeps it, as a latest-only edge does.
    pub fn mark_consumed(&mut self) {
        self.unconsumed = 0;
    }
}
