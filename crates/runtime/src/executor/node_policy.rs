//! Per-node execution policies, set graph-wide or per node through executor builders or node
//! metadata: error recovery ([`NodeRecovery`]), parallel scheduling priority
//! ([`NODE_PRIORITY_META_KEY`]) and blocking backpressure on bounded queue edges.
//!
//! Each policy resolves to a per-node (or per-edge) table when it is set, so a run reads one
//! slot per node and allocates nothing. Tables are `None` while every entry is the default,
//! which keeps the default path a single branch.

use super::{Executor, ExecutorConfigTarget, NodeHandler, OwnedExecutor};
use crate::plan::{RuntimeEdge, RuntimeNode};
use crate::portable::Arc;
use crate::prelude::*;
use alloc::collections::BTreeMap;
use core::time::Duration;
use daedalus_data::model::Value;

/// Node metadata selecting the node's [`NodeRecovery`]: `"fail"`, `"skip_frame"`, `"restart"`,
/// `"retry:<max>"` or `"retry:<max>:<backoff_ms>"` (see [`NodeRecovery::parse`]). Executor
/// builders ([`Executor::with_node_recovery`]) override it.
pub const NODE_RECOVERY_META_KEY: &str = "daedalus.node.recovery";

/// Node metadata giving the node's parallel scheduling priority (an integer, default 0): when
/// more segments are ready than workers are free, segments holding higher-priority nodes start
/// first. Executor builders ([`Executor::with_node_priority`]) override it.
pub const NODE_PRIORITY_META_KEY: &str = "daedalus.node.priority";

/// What the executor does when a node fails: its handler returns an error (or, with `std`,
/// panics), its outputs fail to flush, or publishing them fails.
///
/// Set graph-wide with [`Executor::with_recovery`], per node with
/// [`Executor::with_node_recovery`] or [`NODE_RECOVERY_META_KEY`] metadata. Every failure is
/// recorded in [`ExecutionTelemetry::errors`](super::ExecutionTelemetry::errors); the recovery
/// taken is counted in [`ExecutionTelemetry::recovery`](super::ExecutionTelemetry::recovery).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum NodeRecovery {
    /// The error fails the run under fail-fast (the default), or is recorded while the run
    /// continues without fail-fast. Panics are not caught.
    #[default]
    Fail,
    /// Drop the node's outputs for this tick and keep running: downstream nodes that needed
    /// them are not ready this tick. The run succeeds whatever the fail-fast setting.
    SkipFrame,
    /// Run the handler again on the same inputs, up to `max` more times, waiting `backoff`
    /// before each attempt (`std` only; without it attempts follow immediately). An attempt
    /// that succeeds publishes normally; once the attempts are spent the error is handled as
    /// [`Self::Fail`]. Flush and publish errors are not retried (outputs may be partly
    /// delivered): they are handled as [`Self::SkipFrame`].
    Retry { max: u32, backoff: Duration },
    /// Reset the node, then skip the frame: its typed state (`#[node(state(..))]`), managed
    /// resources and custom metrics are dropped, so the next tick starts the node fresh.
    Restart,
}

impl NodeRecovery {
    /// [`Self::Retry`] without backoff.
    pub const fn retry(max: u32) -> Self {
        Self::Retry {
            max,
            backoff: Duration::ZERO,
        }
    }

    /// Parse the [`NODE_RECOVERY_META_KEY`] text form: `fail`, `skip_frame` (or `skip`),
    /// `restart`, `retry` (one retry), `retry:<max>` or `retry:<max>:<backoff_ms>`.
    pub fn parse(text: &str) -> Option<Self> {
        let mut parts = text.trim().split(':');
        let policy = match parts.next()? {
            "fail" => Self::Fail,
            "skip_frame" | "skip" => Self::SkipFrame,
            "restart" => Self::Restart,
            "retry" => {
                let max = parts.next().map_or(Some(1), |max| max.parse().ok())?;
                let backoff = parts
                    .next()
                    .map_or(Some(0), |ms| ms.parse().ok())
                    .map(Duration::from_millis)?;
                Self::Retry { max, backoff }
            }
            _ => return None,
        };
        parts.next().is_none().then_some(policy)
    }

    /// The policy node `metadata` selects ([`NODE_RECOVERY_META_KEY`]), if any.
    pub fn from_metadata(metadata: &BTreeMap<String, Value>) -> Option<Self> {
        let text = metadata.get(NODE_RECOVERY_META_KEY)?.as_str();
        let policy = text.and_then(Self::parse);
        if policy.is_none() {
            crate::trace::warn!(
                target: "daedalus_runtime::executor",
                value = ?metadata.get(NODE_RECOVERY_META_KEY),
                "ignoring unrecognised node recovery metadata"
            );
        }
        policy
    }

    /// Whether a failure under this policy can leave the run successful (so panics are
    /// caught and converted into errors).
    #[inline]
    pub(crate) fn recovers(self) -> bool {
        !matches!(self, Self::Fail)
    }
}

/// Recoveries taken during a run ([`NodeRecovery`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RecoveryTelemetry {
    /// Handler attempts re-run under [`NodeRecovery::Retry`].
    #[serde(default)]
    pub retries: u64,
    /// Retried nodes whose later attempt succeeded.
    #[serde(default)]
    pub recovered_retries: u64,
    /// Node frames dropped under [`NodeRecovery::SkipFrame`] (and retry publish errors).
    #[serde(default)]
    pub skipped_frames: u64,
    /// Nodes reset under [`NodeRecovery::Restart`].
    #[serde(default)]
    pub restarts: u64,
    /// Handler panics caught and handled as errors under a recovering policy.
    #[serde(default)]
    pub caught_panics: u64,
}

impl RecoveryTelemetry {
    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    pub fn merge(&mut self, other: Self) {
        self.retries = self.retries.saturating_add(other.retries);
        self.recovered_retries = self
            .recovered_retries
            .saturating_add(other.recovered_retries);
        self.skipped_frames = self.skipped_frames.saturating_add(other.skipped_frames);
        self.restarts = self.restarts.saturating_add(other.restarts);
        self.caught_panics = self.caught_panics.saturating_add(other.caught_panics);
    }
}

/// A per-node override named a node the plan does not have.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("no node '{node}' in the runtime plan (expected a node id or instance key)")]
pub struct UnknownNodeError {
    pub node: String,
}

/// The resolved per-node policies of an executor's run configuration: one pointer, `None`
/// while every policy is the default. The executor core is copied into each tick's executor
/// view, so its run configuration stays small.
#[derive(Clone, Default)]
pub(crate) struct NodePolicies(Option<Arc<PolicyTables>>);

#[derive(Clone, Default, PartialEq)]
struct PolicyTables {
    recovery_default: NodeRecovery,
    recovery_overrides: Vec<(String, NodeRecovery)>,
    /// Per node, its recovery; `None` while every node uses [`NodeRecovery::Fail`].
    recovery: Option<Box<[NodeRecovery]>>,
    priority_overrides: Vec<(String, i32)>,
    /// Per node, its scheduling priority; `None` while every node has priority 0.
    priorities: Option<Box<[i32]>>,
    /// Per edge, whether a full queue defers its producer; `None` without blocking edges.
    blocking_edges: Option<Box<[bool]>>,
}

impl NodePolicies {
    #[inline]
    pub(crate) fn recovery_of(&self, node_idx: usize) -> NodeRecovery {
        self.tables()
            .and_then(|tables| tables.recovery.as_deref())
            .and_then(|table| table.get(node_idx).copied())
            .unwrap_or_default()
    }

    /// Whether any node recovers from failures (else every node uses [`NodeRecovery::Fail`]).
    #[inline]
    pub(crate) fn any_recovery(&self) -> bool {
        self.tables()
            .is_some_and(|tables| tables.recovery.is_some())
    }

    /// Per node scheduling priorities, if any node has a non-zero one.
    #[cfg_attr(not(feature = "threads"), allow(dead_code))]
    pub(crate) fn priorities(&self) -> Option<&[i32]> {
        self.tables()?.priorities.as_deref()
    }

    /// Per edge, whether a full bounded queue defers its producer, if any edge blocks.
    #[inline]
    pub(crate) fn blocking_edges(&self) -> Option<&[bool]> {
        self.tables()?.blocking_edges.as_deref()
    }

    /// A handle to the tables when some edge blocks (cheap to hold across a run).
    #[inline]
    pub(crate) fn blocking(&self) -> Option<Self> {
        self.blocking_edges().is_some().then(|| self.clone())
    }

    #[inline]
    fn tables(&self) -> Option<&PolicyTables> {
        self.0.as_deref()
    }

    /// Change the tables, dropping them again when everything is back to the default.
    fn update(&mut self, f: impl FnOnce(&mut PolicyTables)) {
        let mut tables = self.tables().cloned().unwrap_or_default();
        f(&mut tables);
        self.0 = (tables != PolicyTables::default()).then(|| Arc::new(tables));
    }

    /// Re-resolve the per-node tables from `nodes`' metadata, the overrides and the default.
    pub(crate) fn resolve(&mut self, nodes: &[RuntimeNode], keys: &[Arc<str>]) {
        self.update(|tables| tables.resolve(nodes, keys));
    }
}

impl PolicyTables {
    fn resolve(&mut self, nodes: &[RuntimeNode], keys: &[Arc<str>]) {
        let recovery: Vec<NodeRecovery> = nodes
            .iter()
            .enumerate()
            .map(|(idx, node)| {
                lookup(&self.recovery_overrides, node, keys.get(idx))
                    .or_else(|| NodeRecovery::from_metadata(&node.metadata))
                    .unwrap_or(self.recovery_default)
            })
            .collect();
        self.recovery = recovery
            .iter()
            .any(|policy| policy.recovers())
            .then(|| recovery.into());
        let priorities: Vec<i32> = nodes
            .iter()
            .enumerate()
            .map(|(idx, node)| {
                lookup(&self.priority_overrides, node, keys.get(idx))
                    .or_else(|| priority_from_metadata(&node.metadata))
                    .unwrap_or(0)
            })
            .collect();
        self.priorities = priorities
            .iter()
            .any(|&priority| priority != 0)
            .then(|| priorities.into());
    }
}

/// The last override naming `node` by instance key or id.
fn lookup<T: Copy>(
    overrides: &[(String, T)],
    node: &RuntimeNode,
    key: Option<&Arc<str>>,
) -> Option<T> {
    overrides
        .iter()
        .rev()
        .find(|(name, _)| key.is_some_and(|key| **key == **name) || *name == node.id)
        .map(|(_, value)| *value)
}

fn priority_from_metadata(metadata: &BTreeMap<String, Value>) -> Option<i32> {
    let value = metadata.get(NODE_PRIORITY_META_KEY)?;
    let priority = match value {
        Value::Int(priority) => i32::try_from(*priority).ok(),
        Value::String(text) => text.trim().parse().ok(),
        _ => None,
    };
    if priority.is_none() {
        crate::trace::warn!(
            target: "daedalus_runtime::executor",
            value = ?value,
            "ignoring unrecognised node priority metadata"
        );
    }
    priority
}

fn knows_node(nodes: &[RuntimeNode], keys: &[Arc<str>], name: &str) -> bool {
    keys.iter().any(|key| **key == *name) || nodes.iter().any(|node| node.id == name)
}

/// Builders shared by [`Executor`] and [`OwnedExecutor`].
pub(crate) trait NodePolicyTarget: ExecutorConfigTarget {
    fn policy_nodes(&self) -> Arc<[RuntimeNode]>;
    fn policy_edges(&self) -> &[RuntimeEdge];

    fn resolve_policies(&mut self) {
        let nodes = self.policy_nodes();
        let core = self.core_mut();
        let keys = core.node_ids.clone();
        core.run_config.policies.resolve(&nodes, &keys);
    }

    fn apply_recovery(&mut self, policy: NodeRecovery) {
        self.core_mut()
            .run_config
            .policies
            .update(|tables| tables.recovery_default = policy);
        self.resolve_policies();
    }

    fn apply_node_recovery(
        &mut self,
        node: &str,
        policy: NodeRecovery,
    ) -> Result<(), UnknownNodeError> {
        self.check_node(node)?;
        self.core_mut()
            .run_config
            .policies
            .update(|tables| tables.recovery_overrides.push((node.into(), policy)));
        self.resolve_policies();
        Ok(())
    }

    fn apply_node_priority(&mut self, node: &str, priority: i32) -> Result<(), UnknownNodeError> {
        self.check_node(node)?;
        self.core_mut()
            .run_config
            .policies
            .update(|tables| tables.priority_overrides.push((node.into(), priority)));
        self.resolve_policies();
        Ok(())
    }

    fn apply_blocking_backpressure(&mut self, enabled: bool) {
        let mask: Option<Box<[bool]>> = enabled
            .then(|| {
                self.policy_edges()
                    .iter()
                    .map(|edge| edge.policy().bounded_capacity().is_some())
                    .collect::<Box<[bool]>>()
            })
            .filter(|mask| mask.iter().any(|&blocking| blocking));
        self.core_mut()
            .run_config
            .policies
            .update(|tables| tables.blocking_edges = mask);
    }

    fn check_node(&mut self, node: &str) -> Result<(), UnknownNodeError> {
        let nodes = self.policy_nodes();
        let keys = self.core_mut().node_ids.clone();
        if knows_node(&nodes, &keys, node) {
            Ok(())
        } else {
            Err(UnknownNodeError { node: node.into() })
        }
    }
}

impl<H: NodeHandler> NodePolicyTarget for Executor<'_, H> {
    fn policy_nodes(&self) -> Arc<[RuntimeNode]> {
        self.nodes.clone()
    }

    fn policy_edges(&self) -> &[RuntimeEdge] {
        self.edges
    }
}

impl<H: NodeHandler> NodePolicyTarget for OwnedExecutor<H> {
    fn policy_nodes(&self) -> Arc<[RuntimeNode]> {
        self.nodes.clone()
    }

    fn policy_edges(&self) -> &[RuntimeEdge] {
        &self.edges
    }
}

macro_rules! node_policy_builders {
    () => {
        /// Recover from node failures with `policy` (default [`NodeRecovery::Fail`]) on every
        /// node without its own policy ([`Self::with_node_recovery`] or
        /// [`NODE_RECOVERY_META_KEY`] metadata).
        pub fn with_recovery(mut self, policy: NodeRecovery) -> Self {
            self.apply_recovery(policy);
            self
        }

        /// Recover from failures of `node` (a node id, or an instance key for one of several
        /// instances) with `policy`.
        ///
        /// # Panics
        ///
        /// Panics when the plan has no such node; [`Self::try_set_node_recovery`] reports it.
        pub fn with_node_recovery(mut self, node: impl AsRef<str>, policy: NodeRecovery) -> Self {
            self.try_set_node_recovery(node, policy)
                .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"));
            self
        }

        pub fn try_set_node_recovery(
            &mut self,
            node: impl AsRef<str>,
            policy: NodeRecovery,
        ) -> Result<(), UnknownNodeError> {
            self.apply_node_recovery(node.as_ref(), policy)
        }

        /// Give `node`'s segment scheduling priority `priority` in parallel runs (higher first;
        /// default 0, or [`NODE_PRIORITY_META_KEY`] metadata). A segment takes the highest
        /// priority of its nodes. Serial runs keep the schedule order.
        ///
        /// # Panics
        ///
        /// Panics when the plan has no such node; [`Self::try_set_node_priority`] reports it.
        pub fn with_node_priority(mut self, node: impl AsRef<str>, priority: i32) -> Self {
            self.try_set_node_priority(node, priority)
                .unwrap_or_else(|err| panic!("daedalus-runtime: {err}"));
            self
        }

        pub fn try_set_node_priority(
            &mut self,
            node: impl AsRef<str>,
            priority: i32,
        ) -> Result<(), UnknownNodeError> {
            self.apply_node_priority(node.as_ref(), priority)
        }

        /// Make bounded queue edges block instead of overflowing: a node whose bounded output
        /// queue is full is deferred to a later tick (its inputs stay queued) until the consumer
        /// drains it. Direct-slot and fused edges never hold a backlog, so they do not block.
        /// See "Blocking backpressure" in `docs/runtime-diagnostics.md`.
        pub fn with_blocking_backpressure(mut self, enabled: bool) -> Self {
            self.apply_blocking_backpressure(enabled);
            self
        }
    };
}

impl<H: NodeHandler> Executor<'_, H> {
    node_policy_builders!();
}

impl<H: NodeHandler> OwnedExecutor<H> {
    node_policy_builders!();
}

/// Highest node priority of each schedule segment, for the parallel ready queue.
#[cfg(feature = "threads")]
pub(crate) fn segment_priority(
    priorities: &[i32],
    segments: &[crate::plan::RuntimeSegment],
    segment: usize,
) -> i32 {
    segments
        .get(segment)
        .and_then(|segment| {
            segment
                .nodes
                .iter()
                .filter_map(|node| priorities.get(node.0).copied())
                .max()
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_recovery_text() {
        assert_eq!(NodeRecovery::parse("fail"), Some(NodeRecovery::Fail));
        assert_eq!(NodeRecovery::parse("skip"), Some(NodeRecovery::SkipFrame));
        assert_eq!(
            NodeRecovery::parse(" skip_frame "),
            Some(NodeRecovery::SkipFrame)
        );
        assert_eq!(NodeRecovery::parse("restart"), Some(NodeRecovery::Restart));
        assert_eq!(NodeRecovery::parse("retry"), Some(NodeRecovery::retry(1)));
        assert_eq!(NodeRecovery::parse("retry:3"), Some(NodeRecovery::retry(3)));
        assert_eq!(
            NodeRecovery::parse("retry:2:15"),
            Some(NodeRecovery::Retry {
                max: 2,
                backoff: Duration::from_millis(15)
            })
        );
        assert_eq!(NodeRecovery::parse("retry:x"), None);
        assert_eq!(NodeRecovery::parse("retry:1:2:3"), None);
        assert_eq!(NodeRecovery::parse("explode"), None);
    }

    #[test]
    fn recovery_round_trips_through_serde() {
        let policy = NodeRecovery::Retry {
            max: 2,
            backoff: Duration::from_millis(5),
        };
        let json = serde_json::to_string(&policy).expect("serialize");
        assert_eq!(
            serde_json::from_str::<NodeRecovery>(&json).expect("deserialize"),
            policy
        );
        assert_eq!(
            serde_json::to_string(&NodeRecovery::SkipFrame).expect("serialize"),
            "\"skip_frame\""
        );
    }
}
