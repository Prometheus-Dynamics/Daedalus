use super::*;
use crate::config::RuntimeDebugConfig;
#[cfg(feature = "gpu")]
use crate::{RuntimeEdgePolicy, plan::RuntimeEdge};
use daedalus_data::model::Value;
use daedalus_planner::ComputeAffinity;
use std::sync::Arc;
use std::thread;

fn test_node(id: &str) -> RuntimeNode {
    RuntimeNode {
        id: id.to_string(),
        stable_id: 0,
        bundle: None,
        label: None,
        compute: ComputeAffinity::CpuOnly,
        const_inputs: Vec::new(),
        sync_groups: Vec::new(),
        metadata: std::collections::BTreeMap::<String, Value>::new(),
    }
}

fn no_op_handler(
    _node: &RuntimeNode,
    _ctx: &crate::state::ExecutionContext,
    _io: &mut crate::io::NodeIo,
) -> Result<(), NodeError> {
    Ok(())
}

fn direct_slot_payload(value: u32) -> CorrelatedPayload {
    CorrelatedPayload::from_edge(daedalus_transport::Payload::owned("test:u32", value))
}

#[test]
fn direct_slot_shared_access_serializes_mutation() {
    let slot = Arc::new(DirectSlot::empty());
    let mut handles = Vec::new();

    for value in 0..16_u32 {
        let slot = Arc::clone(&slot);
        handles.push(thread::spawn(move || {
            slot.shared().put(direct_slot_payload(value));
            let payload = slot.shared().take();
            payload.and_then(|payload| payload.inner.get_ref::<u32>().copied())
        }));
    }

    for handle in handles {
        let _ = handle.join().expect("direct slot worker should not panic");
    }

    slot.shared().put(direct_slot_payload(99));
    assert_eq!(
        slot.shared()
            .take()
            .and_then(|payload| payload.inner.get_ref::<u32>().copied()),
        Some(99)
    );
}

#[test]
fn direct_slot_clear_removes_shared_payload() {
    let slot = DirectSlot::empty();

    slot.shared().put(direct_slot_payload(42));
    slot.clear();

    assert!(slot.shared().take().is_none());
}

#[test]
fn direct_slot_access_modes_round_trip_after_clear_boundaries() {
    let slot = DirectSlot::empty();

    slot.access(DirectSlotAccess::Serial)
        .put(direct_slot_payload(1));
    assert_eq!(
        slot.access(DirectSlotAccess::Serial)
            .take()
            .and_then(|payload| payload.inner.get_ref::<u32>().copied()),
        Some(1)
    );

    slot.access(DirectSlotAccess::Serial)
        .put(direct_slot_payload(2));
    slot.clear();
    assert!(slot.access(DirectSlotAccess::Shared).take().is_none());

    slot.access(DirectSlotAccess::Shared)
        .put(direct_slot_payload(3));
    assert_eq!(
        slot.access(DirectSlotAccess::Shared)
            .take()
            .and_then(|payload| payload.inner.get_ref::<u32>().copied()),
        Some(3)
    );

    slot.access(DirectSlotAccess::Shared)
        .put(direct_slot_payload(4));
    slot.clear();
    assert!(slot.access(DirectSlotAccess::Serial).take().is_none());
}

#[test]
fn try_new_reports_stable_id_collision() {
    let mut plan = RuntimePlan::from_execution(&daedalus_planner::ExecutionPlan::new(
        daedalus_planner::Graph::default(),
        Vec::new(),
    ));
    let mut first = test_node("first");
    first.stable_id = 42;
    let mut second = test_node("second");
    second.stable_id = 42;
    plan.nodes = vec![first, second];

    let err = match Executor::try_new(&plan, no_op_handler) {
        Ok(_) => panic!("expected collision error"),
        Err(err) => err,
    };
    assert_eq!(
        err,
        ExecutorBuildError::StableIdCollision {
            previous: "first".to_string(),
            current: "second".to_string(),
            stable_id: 42,
        }
    );
}

#[test]
fn executor_debug_config_can_be_overridden_per_instance() {
    let plan = RuntimePlan::from_execution(&daedalus_planner::ExecutionPlan::new(
        daedalus_planner::Graph::default(),
        Vec::new(),
    ));
    let first = Executor::new(&plan, no_op_handler).with_runtime_debug_config(RuntimeDebugConfig {
        node_cpu_time: true,
        pool_size: Some(2),
        ..Default::default()
    });
    let second =
        Executor::new(&plan, no_op_handler).with_runtime_debug_config(RuntimeDebugConfig {
            node_perf_counters: true,
            pool_size: Some(4),
            ..Default::default()
        });

    assert!(first.core.run_config.debug_config.node_cpu_time);
    assert!(!first.core.run_config.debug_config.node_perf_counters);
    assert_eq!(first.core.run_config.pool_size, Some(2));
    assert!(!second.core.run_config.debug_config.node_cpu_time);
    assert!(second.core.run_config.debug_config.node_perf_counters);
    assert_eq!(second.core.run_config.pool_size, Some(4));

    let first_snapshot = first.snapshot();
    assert_eq!(
        first_snapshot.core.run_config.debug_config,
        first.core.run_config.debug_config
    );
    assert_eq!(
        first_snapshot.core.run_config.pool_size,
        first.core.run_config.pool_size
    );
}

#[test]
fn owned_executor_debug_config_can_be_overridden_per_instance() {
    let plan = Arc::new(RuntimePlan::from_execution(
        &daedalus_planner::ExecutionPlan::new(daedalus_planner::Graph::default(), Vec::new()),
    ));
    let first = OwnedExecutor::new(plan.clone(), no_op_handler).with_runtime_debug_config(
        RuntimeDebugConfig {
            node_cpu_time: true,
            pool_size: Some(2),
            ..Default::default()
        },
    );
    let second =
        OwnedExecutor::new(plan, no_op_handler).with_runtime_debug_config(RuntimeDebugConfig {
            node_perf_counters: true,
            pool_size: Some(4),
            ..Default::default()
        });

    assert!(first.core.run_config.debug_config.node_cpu_time);
    assert!(!first.core.run_config.debug_config.node_perf_counters);
    assert_eq!(first.core.run_config.pool_size, Some(2));
    assert!(!second.core.run_config.debug_config.node_cpu_time);
    assert!(second.core.run_config.debug_config.node_perf_counters);
    assert_eq!(second.core.run_config.pool_size, Some(4));
}

#[test]
fn executor_reports_invalid_active_node_mask_length() {
    let mut plan = RuntimePlan::from_execution(&daedalus_planner::ExecutionPlan::new(
        daedalus_planner::Graph::default(),
        Vec::new(),
    ));
    plan.nodes = vec![test_node("first"), test_node("second")];

    let err = match Executor::new(&plan, no_op_handler).try_with_active_nodes(vec![true]) {
        Ok(_) => panic!("expected invalid node mask length"),
        Err(err) => err,
    };

    assert_eq!(
        err,
        ExecutorMaskError::LengthMismatch {
            mask: "active_nodes",
            expected: 2,
            actual: 1,
        }
    );
}

#[test]
fn owned_executor_reports_invalid_active_node_mask_length() {
    let mut plan = RuntimePlan::from_execution(&daedalus_planner::ExecutionPlan::new(
        daedalus_planner::Graph::default(),
        Vec::new(),
    ));
    plan.nodes = vec![test_node("first"), test_node("second")];
    let plan = Arc::new(plan);

    let err = match OwnedExecutor::new(plan, no_op_handler).try_with_active_nodes(vec![true]) {
        Ok(_) => panic!("expected invalid node mask length"),
        Err(err) => err,
    };

    assert_eq!(
        err,
        ExecutorMaskError::LengthMismatch {
            mask: "active_nodes",
            expected: 2,
            actual: 1,
        }
    );
}

#[test]
fn owned_executor_convenience_mask_api_panics_on_invalid_length() {
    let mut plan = RuntimePlan::from_execution(&daedalus_planner::ExecutionPlan::new(
        daedalus_planner::Graph::default(),
        Vec::new(),
    ));
    plan.nodes = vec![test_node("first"), test_node("second")];
    let plan = Arc::new(plan);

    let result = std::panic::catch_unwind(|| {
        let _ = OwnedExecutor::new(plan, no_op_handler).with_active_nodes(vec![true]);
    });

    assert!(result.is_err());
}

#[cfg(feature = "gpu")]
#[test]
fn collect_data_edges_skips_host_output_edges() {
    let nodes = vec![test_node("cv:test"), test_node("io.host_output")];
    let edges = vec![RuntimeEdge::new(
        NodeRef(0),
        "out".to_string(),
        NodeRef(1),
        "overlay".to_string(),
        RuntimeEdgePolicy::latest_only(),
    )];
    assert!(collect_data_edges(&nodes, &edges).is_empty());
}
