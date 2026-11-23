//! Kept inputs: the slot keeps its value until a feed replaces it or the host clears it. A held or
//! shared input's value is context, not work, so a feed to it wakes nothing. A consumed input whose
//! slot keeps its value for several readers is work like any other.

use crate::io::{Host, HostError};

impl Host<'_> {
    /// Drops the value a held input holds, if any.
    pub fn clear_input(&mut self, name: &str) -> Result<(), HostError> {
        let spec = self.input(name)?;
        self.runner.clear_held(spec.slot).map_err(HostError::Run)
    }

    /// Gives up everything waiting at the input `name`. Returns how many values that was.
    #[cfg(feature = "std")]
    pub(crate) fn discard_queued(&mut self, name: &str) -> Result<u32, HostError> {
        let spec = self.input(name)?;
        let queued = self.runner.queue(spec.slot).map_or(0, |queue| queue.len);
        if self.runner.is_held(spec.slot) {
            self.runner.clear_held(spec.slot).map_err(HostError::Run)?;
        } else {
            for _ in 0..queued {
                self.runner.drop_oldest(spec.slot).map_err(HostError::Run)?;
            }
        }
        Ok(queued)
    }

    /// True when `name` is a held input: its value is context, kept for every tick until the host
    /// replaces it, and a feed to it wakes nothing.
    pub fn is_held(&self, name: &str) -> Result<bool, HostError> {
        Ok(self.is_context(self.input(name)?.slot))
    }

    pub(crate) fn is_context(&self, slot: u32) -> bool {
        self.context.contains(&slot)
    }

    /// Sets which input slots hold context: a plan's held and shared inputs. Every other input is
    /// work, including one whose slot keeps its value for several readers.
    #[cfg(feature = "std")]
    pub(crate) fn set_context(&mut self, slots: std::vec::Vec<u32>) {
        self.context = slots;
        self.ticked = (0..self.ports.len())
            .filter_map(|at| self.ports.at(at))
            .filter(|spec| {
                spec.dir == crate::Dir::Input
                    && self.runner.is_held(spec.slot)
                    && !self.is_context(spec.slot)
            })
            .map(|spec| (spec.slot, 0))
            .collect();
        self.note_ticked();
    }

    /// Wakes the readers of `slot`, unless its value is context.
    pub(crate) fn wake_unless_context(&mut self, slot: u32, wake: &[u32]) -> u32 {
        if self.is_context(slot) {
            0
        } else {
            self.wake_consumers(wake)
        }
    }

    /// Notes the version of each kept slot that is work, as a tick has run.
    pub(crate) fn note_ticked(&mut self) {
        for (slot, seen) in &mut self.ticked {
            if let Some(queue) = self.runner.queue(*slot) {
                *seen = queue.version;
            }
        }
    }

    /// How many values wait at the input on `slot`. A kept value that is work waits until a tick
    /// has run since it came; any other counts while it is there.
    pub(crate) fn waiting(&self, slot: u32) -> u32 {
        let Some(queue) = self.runner.queue(slot) else {
            return 0;
        };
        match self.ticked.iter().find(|(kept, _)| *kept == slot) {
            Some(&(_, seen)) => u32::from(queue.len > 0 && seen != queue.version),
            None => queue.len,
        }
    }
}
