//! Node recovery ([`NodeRecovery`]) and blocking backpressure for serial runs: calling a handler
//! under its node's policy, retrying it, handling a failure, and deferring producers whose
//! blocking output queue is full.

use crate::prelude::*;
use core::time::Duration;

use crate::executor::queue::edge_is_full;
use crate::executor::{
    ExecuteError, ExecutionTelemetry, FusionTables, NodeConstInputs, NodeError, NodeHandler,
    NodeRecovery,
};
use crate::io::{NodeIo, NodePort, port_buffer, recycle_ports};
use crate::plan::RuntimeNode;
use crate::portable::Arc;
use crate::state::ExecutionContext;

use super::{SerialView, edge_is_active, edge_uses_direct_slot, record_failure};

/// Call `handler` for `node`. Under a recovering policy (and with `std`) a panic is caught and
/// returned as a [`NodeError::Handler`]; under [`NodeRecovery::Fail`] it unwinds as before.
#[inline(never)]
pub(super) fn run_handler<H: NodeHandler>(
    handler: &H,
    node: &RuntimeNode,
    ctx: &ExecutionContext,
    io: &mut NodeIo,
    recovery: NodeRecovery,
    telemetry: &mut ExecutionTelemetry,
) -> Result<(), NodeError> {
    #[cfg(feature = "std")]
    if recovery.recovers() {
        return std::panic::catch_unwind(core::panic::AssertUnwindSafe(|| {
            handler.run(node, ctx, io)
        }))
        .unwrap_or_else(|panic| {
            telemetry.recovery_mut().caught_panics += 1;
            Err(NodeError::Handler(format!(
                "handler panicked: {}",
                crate::executor::panic_message(&*panic)
            )))
        });
    }
    let _ = (recovery, &telemetry);
    handler.run(node, ctx, io)
}

/// A copy of `inputs` for later attempts when `recovery` retries (a pooled port buffer whose
/// payloads share the originals); `None` otherwise.
#[inline]
pub(super) fn retry_inputs(recovery: NodeRecovery, inputs: &[NodePort]) -> Option<Vec<NodePort>> {
    matches!(recovery, NodeRecovery::Retry { max, .. } if max > 0).then(|| copy_ports(inputs))
}

fn copy_ports(ports: &[NodePort]) -> Vec<NodePort> {
    let mut copy = port_buffer();
    copy.extend(ports.iter().cloned());
    copy
}

/// Re-run a failed handler on `saved` inputs as `recovery` allows, replacing `io` with each
/// attempt's. Returns the last attempt's result.
#[cold]
#[allow(clippy::too_many_arguments)]
pub(super) fn retry<H: NodeHandler>(
    v: &mut SerialView<'_>,
    handler: &H,
    node_idx: usize,
    node: &RuntimeNode,
    ctx: &ExecutionContext,
    consts: Option<&Arc<NodeConstInputs>>,
    io: &mut NodeIo,
    mut result: Result<(), NodeError>,
    saved: Vec<NodePort>,
    recovery: NodeRecovery,
) -> Result<(), NodeError> {
    let NodeRecovery::Retry { max, backoff } = recovery else {
        recycle_ports(saved);
        return result;
    };
    let mut attempts = 0u32;
    while attempts < max {
        let Err(error) = &result else { break };
        // A dropped input is gone: retrying cannot help.
        if error.is_input_dropped() {
            break;
        }
        attempts += 1;
        crate::trace::warn!(
            target: "daedalus_runtime::executor",
            node_id = %node.id,
            attempt = attempts,
            max,
            error = %error,
            "node failed; retrying"
        );
        let _ = error;
        backoff_wait(backoff);
        let telemetry = &mut v.core.telemetry;
        telemetry.recovery_mut().retries += 1;
        *io = v.core.node_io(node_idx, copy_ports(&saved), consts);
        result = run_handler(handler, node, ctx, io, recovery, &mut v.core.telemetry);
    }
    if attempts > 0 && result.is_ok() {
        let telemetry = &mut v.core.telemetry;
        telemetry.recovery_mut().recovered_retries += 1;
    }
    recycle_ports(saved);
    result
}

fn backoff_wait(backoff: Duration) {
    #[cfg(feature = "std")]
    if !backoff.is_zero() {
        std::thread::sleep(backoff);
    }
    let _ = backoff;
}

/// What a flush or publish error falls back to: those are not retried, since outputs may be
/// partly delivered.
#[inline]
pub(super) fn after_delivery(recovery: NodeRecovery) -> NodeRecovery {
    match recovery {
        NodeRecovery::Retry { .. } => NodeRecovery::SkipFrame,
        other => other,
    }
}

/// Record node `node_idx`'s failure and handle it under `recovery`: `Err` ends the run
/// (fail-fast); otherwise the run continues, with the error kept in `first_error` when the
/// policy does not recover.
#[cold]
pub(super) fn handle_failure(
    v: &mut SerialView<'_>,
    node_idx: usize,
    node_id: &str,
    error: NodeError,
    recovery: NodeRecovery,
    first_error: &mut Option<ExecuteError>,
) -> Result<(), ExecuteError> {
    if error.is_input_dropped() {
        crate::trace::debug!(target: "daedalus_runtime::executor", node_id, error = %error, "node dropped its input; skipping its frame");
        v.core.telemetry.recovery_mut().dropped_inputs += 1;
        return Ok(());
    }
    record_failure(&mut v.core.telemetry, node_idx, node_id, &error);
    match recovery {
        NodeRecovery::SkipFrame => {
            crate::trace::warn!(target: "daedalus_runtime::executor", node_id, error = %error, "node failed; skipping its frame");
            let telemetry = &mut v.core.telemetry;
            telemetry.recovery_mut().skipped_frames += 1;
            Ok(())
        }
        NodeRecovery::Restart => {
            crate::trace::warn!(target: "daedalus_runtime::executor", node_id, error = %error, "node failed; restarting it");
            if let Some(key) = v.core.node_ids.get(node_idx).cloned() {
                v.core.state.reset_node(&key);
            }
            let telemetry = &mut v.core.telemetry;
            telemetry.recovery_mut().restarts += 1;
            Ok(())
        }
        _ => {
            let failed = ExecuteError::HandlerFailed {
                node: node_id.to_string(),
                error,
            };
            if v.core.run_config.fail_fast {
                return Err(failed);
            }
            first_error.get_or_insert(failed);
            Ok(())
        }
    }
}

/// Whether node `node_idx` must wait for a later tick because one of its blocking output
/// queues is full, or (through `fusion`) one of the nodes its fused outputs feed must.
#[inline(never)]
pub(super) fn output_blocked(
    v: &SerialView<'_>,
    blocking: &[bool],
    fusion: Option<&FusionTables>,
    node_idx: usize,
) -> bool {
    let queued_full = v
        .outgoing_edges
        .get(node_idx)
        .into_iter()
        .flatten()
        .any(|&edge_idx| {
            blocking.get(edge_idx).copied().unwrap_or(false)
                && edge_is_active(v, edge_idx)
                && !edge_uses_direct_slot(v, edge_idx)
                && edge_is_full(edge_idx, &v.core.queues)
        });
    queued_full
        || fusion
            .and_then(|fusion| fusion.outputs.get(node_idx))
            .into_iter()
            .flatten()
            .any(|&(_, _, consumer)| output_blocked(v, blocking, fusion, consumer))
}
