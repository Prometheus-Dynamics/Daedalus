//! Per-node health across ticks. A node whose handler, flush or publish fails is backed off: its
//! next attempt waits an exponentially growing delay (from the executor's base, capped at its
//! cap), and until then the node is skipped, its frame dropped and its consumers not ready. A
//! success resets it. Nothing sleeps: the tick path reads one atomic per node, and the timestamps
//! and details sit behind a lock that only a failure or a recovery takes.

use crate::portable::{Arc, AtomicUsize};
use crate::prelude::*;
use crate::sync::Mutex;
use core::sync::atomic::Ordering::Relaxed;
use core::time::Duration;
use daedalus_core::platform::{Clock, Instant};

/// The default delay before a failed node's first retry (`with_backoff`).
pub const DEFAULT_BACKOFF_BASE: Duration = Duration::from_millis(50);
/// The default cap on a node's backoff delay (`with_backoff`).
pub const DEFAULT_BACKOFF_CAP: Duration = Duration::from_secs(5);
/// The largest cap `with_backoff` accepts: a node is never left out for longer than this.
pub const MAX_BACKOFF_CAP: Duration = Duration::from_secs(10);

const READY: usize = 0;
const BACKED_OFF: usize = 1;

/// One node's health: the atomic state the tick path reads, and the detail a failure writes.
#[derive(Default)]
pub(crate) struct NodeHealth {
    state: AtomicUsize,
    detail: Mutex<Detail>,
}

#[derive(Default)]
struct Detail {
    failures: u64,
    consecutive: u32,
    /// The node is skipped until this time.
    until: Option<Instant>,
    last_error: Option<String>,
}

/// Every node's health, shared by a parallel run's worker snapshots. `touched` counts failures,
/// so a run with none builds no report.
pub(crate) struct NodeHealthTable {
    nodes: Box<[NodeHealth]>,
    touched: AtomicUsize,
}

impl NodeHealthTable {
    pub(crate) fn new(nodes: usize) -> Arc<Self> {
        Arc::new(Self {
            nodes: (0..nodes).map(|_| NodeHealth::default()).collect(),
            touched: AtomicUsize::new(0),
        })
    }

    /// Whether node `idx` may run this tick: `None` skips it (it is backed off, inside its
    /// delay); `Some(probing)` runs it, `probing` when it was backed off and its delay has passed.
    /// One atomic load while the node is ready; the clock is read only for a backed-off node.
    #[inline]
    pub(crate) fn admit(&self, idx: usize, clock: &Clock) -> Option<bool> {
        match self.nodes.get(idx) {
            Some(health) if health.state.load(Relaxed) != READY => health.admit_cold(clock),
            _ => Some(false),
        }
    }

    /// Record a failure of node `idx`: back it off for the next delay.
    #[cold]
    pub(crate) fn on_failure(
        &self,
        idx: usize,
        error: String,
        clock: &Clock,
        base: Duration,
        cap: Duration,
    ) {
        let Some(health) = self.nodes.get(idx) else {
            return;
        };
        self.touched.fetch_add(1, Relaxed);
        let mut detail = health.detail.lock();
        detail.failures = detail.failures.saturating_add(1);
        detail.consecutive = detail.consecutive.saturating_add(1);
        let delay = backoff_delay(detail.consecutive, base, cap);
        detail.until = Some(clock.now() + delay);
        detail.last_error = Some(error);
        health.state.store(BACKED_OFF, Relaxed);
    }

    /// Record that node `idx` ran and delivered its outputs: a backed-off node is ready again.
    #[inline]
    pub(crate) fn on_success(&self, idx: usize) {
        if let Some(health) = self.nodes.get(idx)
            && health.state.load(Relaxed) != READY
        {
            health.recover();
        }
    }

    /// Reports for every node that has failed, in node order (empty while none has).
    pub(crate) fn reports(&self, node_ids: &[Arc<str>], clock: &Clock) -> Vec<NodeHealthReport> {
        if self.touched.load(Relaxed) == 0 {
            return Vec::new();
        }
        let now = clock.now();
        self.nodes
            .iter()
            .enumerate()
            .filter_map(|(idx, health)| {
                let detail = health.detail.lock();
                if detail.failures == 0 {
                    return None;
                }
                let backed_off = health.state.load(Relaxed) != READY;
                Some(NodeHealthReport {
                    node_id: node_ids
                        .get(idx)
                        .map(|id| id.to_string())
                        .unwrap_or_default(),
                    state: if backed_off { "backed_off" } else { "ready" }.into(),
                    failures: detail.failures,
                    consecutive_failures: detail.consecutive,
                    backoff_remaining: detail
                        .until
                        .filter(|_| backed_off)
                        .map(|until| until.duration_since(now)),
                    last_error: detail.last_error.clone(),
                })
            })
            .collect()
    }
}

impl NodeHealth {
    /// The backed-off node's gate: skipped inside its delay, else it runs again (its state stays
    /// backed off until it succeeds).
    #[cold]
    fn admit_cold(&self, clock: &Clock) -> Option<bool> {
        let detail = self.detail.lock();
        match detail.until {
            Some(until) if clock.now() < until => None,
            _ => Some(true),
        }
    }

    #[cold]
    fn recover(&self) {
        let mut detail = self.detail.lock();
        detail.consecutive = 0;
        detail.until = None;
        self.state.store(READY, Relaxed);
    }
}

/// The delay after the `consecutive`-th failure in a row: `base` doubled per extra failure, at
/// most `cap`.
pub(crate) fn backoff_delay(consecutive: u32, base: Duration, cap: Duration) -> Duration {
    let shift = consecutive.saturating_sub(1).min(31);
    base.saturating_mul(1u32 << shift).min(cap)
}

/// One node's health in a run's telemetry (`ExecutionTelemetry::node_health`). Field names are
/// stable: consumers read them by name.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NodeHealthReport {
    /// The node's instance key, or its id.
    pub node_id: String,
    /// `"ready"` (it runs) or `"backed_off"` (its next attempt waits `backoff_remaining`).
    pub state: String,
    /// Failures recorded for the node over the executor's lifetime.
    pub failures: u64,
    /// Failures in a row since the node last succeeded (drives the next delay).
    pub consecutive_failures: u32,
    /// How long until the node's next attempt, measured from the moment this report was built
    /// (the end of the tick it is in); `None` while the node is ready.
    pub backoff_remaining: Option<Duration>,
    /// The most recent failure's message.
    pub last_error: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_doubles_from_base_and_stops_at_cap() {
        let base = Duration::from_millis(50);
        let cap = Duration::from_millis(400);
        let delays: Vec<_> = (1..=6)
            .map(|n| backoff_delay(n, base, cap).as_millis())
            .collect();
        assert_eq!(delays, vec![50, 100, 200, 400, 400, 400]);
        // A huge streak neither overflows nor exceeds the cap.
        assert_eq!(backoff_delay(u32::MAX, base, cap), cap);
    }
}
