//! Typed takes: the payload's Rust type is checked, and a sole holder gets the value without a copy.

use core::any::Any;
use std::sync::Arc;

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
        let typed = payload
            .downcast::<T>()
            .map_err(|_| HostError::WrongRustType {
                port: Label::new(name),
            })?;
        Ok(Some(
            Arc::try_unwrap(typed).unwrap_or_else(|shared| (*shared).clone()),
        ))
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
