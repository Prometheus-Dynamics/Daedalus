//! Held (sticky) inputs: the slot keeps its value until a feed replaces it or the host clears it.
//! A feed to a held port replaces the value and does not wake consumers by itself, because held
//! values are context and not work.

use daedalus_value::Value;

use crate::io::{FeedOutcome, Host, HostError};

impl Host<'_> {
    /// Drops the value a held input holds, if any.
    pub fn clear_input(&mut self, name: &str) -> Result<(), HostError> {
        let spec = self.input(name)?;
        self.runner.clear_held(spec.slot).map_err(HostError::Run)
    }

    /// True when `name` is a held input.
    pub fn is_held(&self, name: &str) -> Result<bool, HostError> {
        Ok(self.runner.is_held(self.input(name)?.slot))
    }

    /// Replaces a held input's value. Nothing is woken.
    pub(crate) fn replace_held(
        &mut self,
        name: &str,
        slot: u32,
        value: Value,
    ) -> Result<FeedOutcome, HostError> {
        self.runner.clear_held(slot).map_err(HostError::Run)?;
        self.runner
            .feed_value(slot, value)
            .map_err(|error| self.feed_error(name, error))?;
        Ok(FeedOutcome { woke: 0 })
    }
}
