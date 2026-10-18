//! Node recovery ([`NodeRecovery`]) and blocking backpressure for serial runs: calling a handler
//! under its node's policy, retrying it, handling a failure, and deferring producers whose
//! blocking output queue is full.

use crate::prelude::*;
use core::time::Duration;

use crate::executor::queue::edge_is_full;
use crate::executor::{
    ExecuteError, ExecutionTelemetry, Executor, FusionTables, NodeConstInputs, NodeError,
    NodeHandler, NodeRecovery,
};
use crate::io::{NodeIo, NodePort, port_buffer, recycle_ports};
use crate::plan::RuntimeNode;
use crate::portable::Arc;
use crate::state::ExecutionContext;

use super::{edge_is_active, edge_uses_direct_slot, record_failure};

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
    exec: &mut Executor<'_, H>,
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
        let telemetry = &mut exec.core.telemetry;
        telemetry.recovery_mut().retries += 1;
        *io = exec.core.node_io(node_idx, copy_ports(&saved), consts);
        result = run_handler(
            &*exec.handler,
            node,
            ctx,
            io,
            recovery,
            &mut exec.core.telemetry,
        );
    }
    if attempts > 0 && result.is_ok() {
        let telemetry = &mut exec.core.telemetry;
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
pub(super) fn handle_failure<H: NodeHandler>(
    exec: &mut Executor<'_, H>,
    node_idx: usize,
    node_id: &str,
    error: NodeError,
    recovery: NodeRecovery,
    first_error: &mut Option<ExecuteError>,
) -> Result<(), ExecuteError> {
    record_failure(&mut exec.core.telemetry, node_idx, node_id, &error);
    match recovery {
        NodeRecovery::SkipFrame => {
            crate::trace::warn!(target: "daedalus_runtime::executor", node_id, error = %error, "node failed; skipping its frame");
            let telemetry = &mut exec.core.telemetry;
            telemetry.recovery_mut().skipped_frames += 1;
            Ok(())
        }
        NodeRecovery::Restart => {
            crate::trace::warn!(target: "daedalus_runtime::executor", node_id, error = %error, "node failed; restarting it");
            if let Some(key) = exec.core.node_ids.get(node_idx).cloned() {
                exec.core.state.reset_node(&key);
            }
            let telemetry = &mut exec.core.telemetry;
            telemetry.recovery_mut().restarts += 1;
            Ok(())
        }
        _ => {
            let failed = ExecuteError::HandlerFailed {
                node: node_id.to_string(),
                error,
            };
            if exec.core.run_config.fail_fast {
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
pub(super) fn output_blocked<H: NodeHandler>(
    exec: &Executor<'_, H>,
    blocking: &[bool],
    fusion: Option<&FusionTables>,
    node_idx: usize,
) -> bool {
    let queued_full = exec
        .outgoing_edges
        .get(node_idx)
        .into_iter()
        .flatten()
        .any(|&edge_idx| {
            blocking.get(edge_idx).copied().unwrap_or(false)
                && edge_is_active(exec, edge_idx)
                && !edge_uses_direct_slot(exec, edge_idx)
                && edge_is_full(edge_idx, &exec.core.queues)
        });
    queued_full
        || fusion
            .and_then(|fusion| fusion.outputs.get(node_idx))
            .into_iter()
            .flatten()
            .any(|&(_, _, consumer)| output_blocked(exec, blocking, fusion, consumer))
}
