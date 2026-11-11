//! What the runner does with a step that faults, per step, and the host's manual switches.

use crate::failure::{FailurePolicy, StepHealth};
use crate::{RunError, Runner};

/// What happens to one step's tick when its body returns `Fault`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Recovery {
    /// The tick is contained: outputs are dropped, inputs are consumed, consumers are not woken.
    /// With `fail_fast` the run stops with [`RunError::Fault`].
    #[default]
    Fail,
    /// As `Fail`, but the run never stops for this step: the tick is dropped and the run goes on.
    SkipTick,
    /// The body runs again in the same tick, on the same inputs, up to `max` more times. Then it
    /// fails like `Fail`.
    Retry { max: u32 },
    /// The next call carries the restart flag, and the body resets its own state.
    Restart,
}

impl Runner<'_> {
    /// Sets the failure policy used for every step from now on.
    pub fn set_failure_policy(&mut self, policy: FailurePolicy) {
        self.failure = policy;
    }

    /// Sets the host's clock, in nanoseconds. The runner reads it only for a step in backoff.
    pub fn set_clock(&mut self, now_ns: u64) {
        self.cx.now_ns = now_ns;
    }

    /// The failure record of `step`, or `None` past the end.
    pub fn health(&self, step: u32) -> Option<&StepHealth> {
        self.health
            .get(step as usize)
            .filter(|_| (step as usize) < self.steps.len())
    }

    /// Turns `step` off by hand. It is not called, its queued inputs are dropped, and its
    /// consumers see nothing until it is switched on.
    pub fn switch_off(&mut self, step: u32) -> Result<(), RunError> {
        let index = self.step_index(step)?;
        let health = &mut self.health[index];
        health.disabled = true;
        health.auto_off = false;
        Ok(())
    }

    /// Turns `step` back on. This clears its backoff and any automatic turning off, and keeps its
    /// failure count.
    pub fn switch_on(&mut self, step: u32) -> Result<(), RunError> {
        let index = self.step_index(step)?;
        let health = &mut self.health[index];
        health.disabled = false;
        health.auto_off = false;
        health.backing_off = false;
        health.until_ns = 0;
        Ok(())
    }

    fn step_index(&self, step: u32) -> Result<usize, RunError> {
        if (step as usize) < self.steps.len() {
            Ok(step as usize)
        } else {
            Err(RunError::NoStep { step })
        }
    }
}
