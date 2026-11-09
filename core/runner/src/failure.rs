//! The failure rule. A step that keeps failing is backed off, never given up on: the delay doubles
//! from `base` after `backoff_after` failures in a row, and stops at `cap`. Automatic turning off
//! happens only when a policy sets `auto_off_after` above zero.

use daedalus_abi::RESTART;

use crate::policy::Recovery;

/// The longest backoff any policy may ask for, in nanoseconds.
pub const MAX_CAP_NS: u64 = 8_000_000_000;

/// Why a failure policy was refused.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PolicyError {
    /// The cap is above [`MAX_CAP_NS`].
    CapTooLong { cap_ns: u64 },
    /// `backoff_after` is zero: a step must fail at least once before it backs off.
    NoBackoffThreshold,
}

/// How a runner treats a step that faults. Copied into each runner, so changing it never changes
/// a plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FailurePolicy {
    base_ns: u64,
    cap_ns: u64,
    backoff_after: u32,
    auto_off_after: u32,
    fail_fast: bool,
}

impl FailurePolicy {
    /// 50 ms base, a 5 s cap, backoff from the third failure, no automatic turning off, and
    /// containment rather than fail-fast.
    pub const DEFAULT: FailurePolicy = FailurePolicy {
        base_ns: 50_000_000,
        cap_ns: 5_000_000_000,
        backoff_after: 3,
        auto_off_after: 0,
        fail_fast: false,
    };

    /// Checks and builds a policy. `auto_off_after == 0` means a step is never turned off.
    pub const fn new(
        base_ns: u64,
        cap_ns: u64,
        backoff_after: u32,
        auto_off_after: u32,
        fail_fast: bool,
    ) -> Result<FailurePolicy, PolicyError> {
        if cap_ns > MAX_CAP_NS {
            return Err(PolicyError::CapTooLong { cap_ns });
        }
        if backoff_after == 0 {
            return Err(PolicyError::NoBackoffThreshold);
        }
        Ok(FailurePolicy {
            base_ns,
            cap_ns,
            backoff_after,
            auto_off_after,
            fail_fast,
        })
    }

    /// The same policy with `fail_fast` set. Tests and CI turn this on.
    pub const fn with_fail_fast(self, fail_fast: bool) -> FailurePolicy {
        FailurePolicy { fail_fast, ..self }
    }

    pub const fn base_ns(&self) -> u64 {
        self.base_ns
    }

    pub const fn cap_ns(&self) -> u64 {
        self.cap_ns
    }

    pub const fn backoff_after(&self) -> u32 {
        self.backoff_after
    }

    pub const fn auto_off_after(&self) -> u32 {
        self.auto_off_after
    }

    pub const fn fail_fast(&self) -> bool {
        self.fail_fast
    }

    /// The backoff after `failures` failures in a row, which must be at least `backoff_after`.
    /// It is `base * 2^(failures - backoff_after)`, never above the cap.
    pub fn delay_ns(&self, failures: u32) -> u64 {
        let doublings = failures.saturating_sub(self.backoff_after).min(63);
        let raw = self
            .base_ns
            .checked_mul(1u64 << doublings)
            .unwrap_or(self.cap_ns);
        raw.min(self.cap_ns)
    }
}

impl Default for FailurePolicy {
    fn default() -> Self {
        FailurePolicy::DEFAULT
    }
}

/// One step's failure record. The runner keeps one per step, in the buffer the caller supplies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StepHealth {
    pub(crate) failures: u32,
    pub(crate) until_ns: u64,
    pub(crate) backing_off: bool,
    pub(crate) disabled: bool,
    pub(crate) auto_off: bool,
    pub(crate) restart: bool,
    pub(crate) calls: u32,
}

impl StepHealth {
    /// Records one failure at clock time `now`. Automatic turning off comes first, and only when
    /// the policy sets it. Otherwise a step at or past `backoff_after` backs off.
    pub(crate) fn on_failure(&mut self, policy: FailurePolicy, now: u64, recovery: Recovery) {
        self.failures = self.failures.saturating_add(1);
        if policy.auto_off_after() > 0 && self.failures >= policy.auto_off_after() {
            self.disabled = true;
            self.auto_off = true;
            self.backing_off = false;
        } else if self.failures >= policy.backoff_after() {
            self.backing_off = true;
            self.until_ns = now.saturating_add(policy.delay_ns(self.failures));
        }
        if recovery == Recovery::Restart {
            self.restart = true;
        }
    }

    /// Clears the failure record after a success.
    pub(crate) fn on_success(&mut self) {
        self.failures = 0;
        self.backing_off = false;
        self.until_ns = 0;
    }

    /// The flags a call to this step gets: the restart flag while one is pending.
    pub(crate) fn call_flags(&self) -> u32 {
        if self.restart { RESTART } else { 0 }
    }

    /// Failures in a row. A success sets it back to zero.
    pub const fn failures(&self) -> u32 {
        self.failures
    }

    /// True while the step waits out a backoff.
    pub const fn backing_off(&self) -> bool {
        self.backing_off
    }

    /// The clock time (from [`crate::Runner::set_clock`]) at which the backoff ends.
    pub const fn until_ns(&self) -> u64 {
        self.until_ns
    }

    /// True when the step is turned off, whether by the host or automatically.
    pub const fn disabled(&self) -> bool {
        self.disabled
    }

    /// True when the step was turned off by an automatic policy rather than by the host.
    pub const fn auto_off(&self) -> bool {
        self.auto_off
    }

    /// How many times the body has been called, counting recovery retries.
    pub const fn calls(&self) -> u32 {
        self.calls
    }

    /// True when the next call must reset the body's state.
    pub const fn restart_pending(&self) -> bool {
        self.restart
    }
}
