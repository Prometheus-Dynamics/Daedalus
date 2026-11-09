//! What the runner does with a step that faults, per step, and the host's manual switches.

use daedalus_abi::RESTART;

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

    /// The failure policy in force.
    pub fn failure_policy(&self) -> FailurePolicy {
        self.failure
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

    /// Records one failure of `index`. Automatic turning off comes first, and only when the
    /// policy sets it. Otherwise a step at or past `backoff_after` backs off.
    pub(crate) fn record_failure(&mut self, index: usize, recovery: Recovery) {
        let policy = self.failure;
        let now = self.cx.now_ns;
        let health = &mut self.health[index];
        health.failures = health.failures.saturating_add(1);
        if policy.auto_off_after() > 0 && health.failures >= policy.auto_off_after() {
            health.disabled = true;
            health.auto_off = true;
            health.backing_off = false;
        } else if health.failures >= policy.backoff_after() {
            health.backing_off = true;
            health.until_ns = now.saturating_add(policy.delay_ns(health.failures));
        }
        if recovery == Recovery::Restart {
            health.restart = true;
        }
    }

    /// Clears a step's failure record after a success.
    pub(crate) fn record_success(&mut self, index: usize) {
        let health = &mut self.health[index];
        health.failures = 0;
        health.backing_off = false;
        health.until_ns = 0;
    }

    /// The flags a call to a step gets: the restart flag while one is pending.
    pub(crate) fn call_flags(&self, index: usize) -> u32 {
        if self.health[index].restart {
            RESTART
        } else {
            0
        }
    }
}
