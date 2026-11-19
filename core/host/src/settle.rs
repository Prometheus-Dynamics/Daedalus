//! Waiting for the graph to settle. A host with nothing else to do ticks until no step is held on
//! a fence and no step is woken for a later tick. Between two ticks it sleeps only when the only
//! thing left is a fence, and the sleep starts small and never grows past [`MAX_BACKOFF`].

use std::time::{Duration, Instant};

use daedalus_runner::TickReport;

use crate::{Host, HostError, LoadedHost};

/// The first sleep while only a fence is left.
const MIN_BACKOFF: Duration = Duration::from_micros(50);
/// The longest sleep between two ticks.
const MAX_BACKOFF: Duration = Duration::from_millis(2);

/// How a wait ended.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WaitOutcome {
    /// No step is held on a fence and none is woken.
    Settled,
    /// The timeout passed first. Steps may still be held.
    TimedOut,
}

/// What a wait did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Waited {
    pub outcome: WaitOutcome,
    /// Ticks this wait ran, including the first.
    pub ticks: u32,
    /// Steps still held on a fence when the wait ended.
    pub pending: usize,
    /// Step bodies that returned a fault during the wait. A fault is not an error of the wait:
    /// the step's recovery decides what follows, and a graph whose step faulted settles all the
    /// same, with nothing out of that step. This says it happened; the host's fault reports
    /// say which step and why.
    pub faults: u32,
}

/// What a runner has left to do: whether a step is woken for a later tick, and how many steps are
/// held on a fence.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Work {
    pub(crate) woken: bool,
    pub(crate) pending: usize,
}

/// Ticks `target` until it settles or `timeout` passes. Shared by [`Host`] and [`LoadedHost`].
pub(crate) fn settle<T, E>(
    target: &mut T,
    timeout: Duration,
    tick: impl Fn(&mut T) -> Result<TickReport, E>,
    work: impl Fn(&T) -> Work,
) -> Result<Waited, E> {
    let deadline = Instant::now() + timeout;
    let mut backoff = MIN_BACKOFF;
    let mut ticks = 0;
    let mut faults = 0u32;
    loop {
        faults = faults.saturating_add(tick(target)?.faults);
        ticks += 1;
        let left = work(target);
        let now = Instant::now();
        let outcome = if !left.woken && left.pending == 0 {
            Some(WaitOutcome::Settled)
        } else if now >= deadline {
            Some(WaitOutcome::TimedOut)
        } else {
            None
        };
        if let Some(outcome) = outcome {
            return Ok(Waited {
                outcome,
                ticks,
                pending: left.pending,
                faults,
            });
        }
        if left.woken {
            // Work is in progress, so the next tick goes at once.
            backoff = MIN_BACKOFF;
        } else {
            std::thread::sleep(backoff.min(deadline - now));
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }
}

impl Host<'_> {
    /// How many steps are held on a fence and not yet finished.
    pub fn pending(&self) -> usize {
        self.runner.pending()
    }

    /// Whether nothing is held on a fence and no step is woken for a later tick.
    pub fn is_settled(&self) -> bool {
        self.pending() == 0 && !self.runner.has_woken()
    }

    /// Ticks until the host is settled, or `timeout` passes, waiting on fences without spinning.
    /// A step that faults doesn't end the wait: [`Waited::faults`] counts them.
    pub fn step_until_settled(&mut self, timeout: Duration) -> Result<Waited, HostError> {
        settle(
            self,
            timeout,
            |host| host.step(),
            |host| Work {
                woken: host.runner.has_woken(),
                pending: host.pending(),
            },
        )
    }
}

impl LoadedHost {
    /// As [`Host::step_until_settled`], for a loaded graph. Faults are recorded as each tick runs.
    pub fn step_until_settled(&mut self, timeout: Duration) -> Result<Waited, HostError> {
        settle(
            self,
            timeout,
            |loaded| loaded.step(),
            |loaded| Work {
                woken: loaded.host.runner.has_woken(),
                pending: loaded.host.pending(),
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A target whose first tick has a step fault and whose second settles.
    struct Faulting {
        ticks: u32,
    }

    #[test]
    fn a_wait_counts_the_faults_of_its_ticks() {
        let mut target = Faulting { ticks: 0 };
        let waited = settle(
            &mut target,
            Duration::from_secs(5),
            |target| {
                target.ticks += 1;
                Ok::<_, ()>(TickReport {
                    faults: u32::from(target.ticks == 1),
                    ..TickReport::default()
                })
            },
            |target| Work {
                woken: target.ticks < 2,
                pending: 0,
            },
        )
        .unwrap();
        assert_eq!(waited.outcome, WaitOutcome::Settled);
        assert_eq!((waited.ticks, waited.faults), (2, 1));
    }
}
