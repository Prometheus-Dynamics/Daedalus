//! Typed takes: the payload's Rust type is checked, and a sole holder gets the value without a copy.

use core::any::Any;
use std::sync::Arc;

use daedalus_runner::{Carried, INLINE_BYTES, value_of};
use daedalus_types::Typed;
use daedalus_value::Owner;

use crate::io::{Host, HostError};
use crate::label::Label;

impl Host<'_> {
    /// Takes the oldest value out of `name` as a `T`. `None` when the port is empty. A value shared
    /// with another holder is cloned; a sole holder gets it back as it was.
    pub fn take<T: Clone + Send + Sync + 'static>(
        &mut self,
        name: &str,
    ) -> Result<Option<T>, HostError> {
        let Some(value) = self.take_value(name)? else {
            return Ok(None);
        };
        let Owner::Typed(payload) = value.owner else {
            return Err(HostError::NotTyped {
                port: Label::new(name),
            });
        };
        let payload: Arc<dyn Any + Send + Sync> = payload;
        // A value in a cell the producer refills is moved out of it: no copy, and nothing is cloned.
        let payload = match payload.downcast::<Carried<T>>() {
            Ok(cell) => return Ok(cell.take()),
            Err(other) => other,
        };
        let typed = payload
            .downcast::<T>()
            .map_err(|_| HostError::WrongRustType {
                port: Label::new(name),
            })?;
        Ok(Some(
            Arc::try_unwrap(typed).unwrap_or_else(|shared| (*shared).clone()),
        ))
    }

    /// Takes the oldest value out of `name` as a plain `Copy` value, however it's held: a CPU node
    /// writes these in place, with no allocation, and a typed take wouldn't find them. `None` when
    /// the port is empty.
    pub fn take_copy<T: Typed + Copy + Send + Sync + 'static>(
        &mut self,
        name: &str,
    ) -> Result<Option<T>, HostError> {
        let wrong = || HostError::WrongRustType {
            port: Label::new(name),
        };
        // A type made of others has no key to compare: the port's type was proved when the
        // graph was planned, and the size is checked below.
        if !T::STRUCTURAL && self.output(name)?.key.as_str() != T::KEY.as_str() {
            return Err(wrong());
        }
        let Some(value) = self.take_value(name)? else {
            return Ok(None);
        };
        if let Owner::Typed(payload) = &value.owner {
            if let Some(cell) = payload.downcast_ref::<Carried<T>>() {
                return Ok(cell.take());
            }
            if let Some(typed) = payload.downcast_ref::<T>() {
                return Ok(Some(*typed));
            }
        }
        let bytes = value.bytes().ok_or_else(wrong)?;
        let mut cell = [0; INLINE_BYTES];
        cell.get_mut(..bytes.len())
            .ok_or_else(wrong)?
            .copy_from_slice(bytes);
        value_of::<T>(&cell, bytes.len())
            .map(Some)
            .ok_or_else(wrong)
    }

    /// Takes every value out of `name` as a `T`, oldest first.
    pub fn drain<T: Clone + Send + Sync + 'static>(
        &mut self,
        name: &str,
    ) -> Result<std::vec::Vec<T>, HostError> {
        let mut out = std::vec::Vec::new();
        while let Some(value) = self.take::<T>(name)? {
            out.push(value);
        }
        Ok(out)
    }
}
