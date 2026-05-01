use super::{ApplyPolicyOwnedArgs, EdgeQueue, EdgeStorage, EdgeStorageMetrics, RingBuf};
#[cfg(feature = "metrics")]
use crate::executor::EdgePressureReason;
use crate::executor::ExecutionTelemetry;
#[cfg(feature = "metrics")]
use crate::executor::MetricsLevel;
use crate::executor::{CorrelatedPayload, RuntimeDataSizeInspectors};
use crate::plan::{BackpressureStrategy, RuntimeEdgePolicy};
use daedalus_transport::{CoalesceStrategy, FreshnessPolicy, OverflowPolicy, PressurePolicy};
use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn payload(value: u8) -> CorrelatedPayload {
    CorrelatedPayload::from_edge(daedalus_transport::Payload::owned(
        daedalus_transport::TypeKey::new("test.bytes"),
        vec![value],
    ))
}

#[cfg(feature = "lockfree-queues")]
fn bounded_policy(capacity: usize, overflow: OverflowPolicy) -> RuntimeEdgePolicy {
    RuntimeEdgePolicy {
        pressure: PressurePolicy::Bounded { capacity, overflow },
        freshness: FreshnessPolicy::PreserveAll,
    }
}

#[cfg(feature = "lockfree-queues")]
fn apply_lockfree(values: &[u8], policy: &RuntimeEdgePolicy) -> (Vec<u8>, ExecutionTelemetry) {
    use crossbeam_queue::ArrayQueue;

    let capacity = policy.bounded_capacity().unwrap_or(1);
    let queues = Arc::new(vec![EdgeStorage::BoundedLf {
        queue: Arc::new(ArrayQueue::new(capacity)),
        metrics: Arc::new(EdgeStorageMetrics::default()),
    }]);
    let warnings_seen = Arc::new(Mutex::new(HashSet::new()));
    let inspectors = RuntimeDataSizeInspectors::default();
    #[cfg(feature = "metrics")]
    let mut telemetry = ExecutionTelemetry::with_level(MetricsLevel::Detailed);
    #[cfg(not(feature = "metrics"))]
    let mut telemetry = ExecutionTelemetry::default();

    for value in values {
        super::apply_policy_owned(ApplyPolicyOwnedArgs {
            edge_idx: 0,
            policy,
            payload: payload(*value),
            queues: &queues,
            warnings_seen: &warnings_seen,
            telem: &mut telemetry,
            warning_label: Some("lockfree_parity".to_string()),
            backpressure: BackpressureStrategy::None,
            data_size_inspectors: &inspectors,
        })
        .expect("lock-free policy application");
    }

    let mut retained = Vec::new();
    while let Some(payload) = super::pop_edge(0, &queues, &inspectors) {
        retained.push(payload.inner.get_ref::<Vec<u8>>().unwrap()[0]);
    }
    (retained, telemetry)
}

#[cfg(feature = "metrics")]
fn apply_locked_pressure_event(
    policy: &RuntimeEdgePolicy,
    strategy: BackpressureStrategy,
) -> ExecutionTelemetry {
    let queues = Arc::new(vec![EdgeStorage::Locked {
        queue: Arc::new(Mutex::new(EdgeQueue::default())),
        metrics: Arc::new(EdgeStorageMetrics::default()),
    }]);
    let warnings_seen = Arc::new(Mutex::new(HashSet::new()));
    let mut telemetry = ExecutionTelemetry::with_level(MetricsLevel::Detailed);
    let inspectors = RuntimeDataSizeInspectors::default();

    for value in [1, 2] {
        let result = super::apply_policy_owned(ApplyPolicyOwnedArgs {
            edge_idx: 0,
            policy,
            payload: payload(value),
            queues: &queues,
            warnings_seen: &warnings_seen,
            telem: &mut telemetry,
            warning_label: Some(format!("pressure_{strategy:?}")),
            backpressure: strategy.clone(),
            data_size_inspectors: &inspectors,
        });
        if matches!(&strategy, BackpressureStrategy::ErrorOnOverflow) && value == 2 {
            assert!(result.is_err());
        } else {
            result.expect("locked pressure event application");
        }
    }

    telemetry
}

fn poison_queue(queue: &Arc<Mutex<EdgeQueue>>) {
    let _ = std::panic::catch_unwind({
        let queue = Arc::clone(queue);
        move || {
            let _guard = queue.lock().expect("queue lock before poison");
            panic!("poison queue lock for regression test");
        }
    });
    assert!(queue.is_poisoned());
}

#[test]
fn ringbuf_clear_preserves_capacity() {
    let mut ring = RingBuf::new(4);
    ring.push_back(payload(1));
    ring.push_back(payload(2));
    ring.clear();

    assert_eq!(ring.cap(), 4);
    assert_eq!(ring.len(), 0);
    assert!(ring.is_empty());
}

#[test]
fn edge_queue_clear_preserves_bounded_policy_capacity() {
    let mut queue = EdgeQueue::default();
    let policy = RuntimeEdgePolicy::bounded(3);
    queue.ensure_policy(&policy);
    queue.push(&policy, payload(1));
    queue.push(&policy, payload(2));

    queue.clear();

    assert_eq!(queue.capacity(), Some(3));
    assert_eq!(queue.len(), 0);
    assert!(queue.is_empty());
}

#[test]
fn bounded_backpressure_does_not_enqueue_when_full() {
    let mut queue = EdgeQueue::default();
    let policy = RuntimeEdgePolicy {
        pressure: PressurePolicy::Bounded {
            capacity: 1,
            overflow: OverflowPolicy::Backpressure,
        },
        freshness: FreshnessPolicy::PreserveAll,
    };

    queue.ensure_policy(&policy);
    assert!(!queue.push(&policy, payload(1)));
    assert!(queue.push(&policy, payload(2)));
    assert_eq!(queue.len(), 1);
    assert_eq!(
        queue.pop_front().unwrap().inner.get_ref::<Vec<u8>>(),
        Some(&vec![1])
    );
}

#[test]
fn locked_queue_recovers_from_poison_when_applying_policy() {
    let queue = Arc::new(Mutex::new(EdgeQueue::default()));
    poison_queue(&queue);
    let queues = Arc::new(vec![EdgeStorage::Locked {
        queue,
        metrics: Arc::new(EdgeStorageMetrics::default()),
    }]);
    let warnings_seen = Arc::new(Mutex::new(HashSet::new()));
    let mut telemetry = ExecutionTelemetry::default();
    let inspectors = RuntimeDataSizeInspectors::default();

    super::apply_policy_owned(ApplyPolicyOwnedArgs {
        edge_idx: 0,
        policy: &RuntimeEdgePolicy::fifo(),
        payload: payload(1),
        queues: &queues,
        warnings_seen: &warnings_seen,
        telem: &mut telemetry,
        warning_label: None,
        backpressure: BackpressureStrategy::None,
        data_size_inspectors: &inspectors,
    })
    .expect("policy application should recover poisoned queue");

    let value = super::pop_edge(0, &queues, &inspectors)
        .expect("queued payload")
        .inner
        .try_into_owned::<Vec<u8>>()
        .expect("payload value");
    assert_eq!(value, vec![1]);
}

#[test]
fn locked_queue_recovers_from_poison_when_popping() {
    let queue = Arc::new(Mutex::new(EdgeQueue::default()));
    queue
        .lock()
        .expect("queue lock")
        .push(&RuntimeEdgePolicy::fifo(), payload(2));
    poison_queue(&queue);
    let queues = Arc::new(vec![EdgeStorage::Locked {
        queue,
        metrics: Arc::new(EdgeStorageMetrics::default()),
    }]);
    let inspectors = RuntimeDataSizeInspectors::default();

    let value = super::pop_edge(0, &queues, &inspectors)
        .expect("queued payload")
        .inner
        .try_into_owned::<Vec<u8>>()
        .expect("payload value");

    assert_eq!(value, vec![2]);
}

#[cfg(feature = "lockfree-queues")]
#[test]
fn lockfree_bounded_drop_oldest_matches_policy() {
    let policy = bounded_policy(2, OverflowPolicy::DropOldest);

    let (retained, telemetry) = apply_lockfree(&[1, 2, 3], &policy);

    assert_eq!(retained, vec![2, 3]);
    assert_eq!(telemetry.backpressure_events, 1);
    #[cfg(feature = "metrics")]
    assert_eq!(telemetry.edge_metrics[&0].pressure_events.drop_oldest, 1);
}

#[cfg(feature = "lockfree-queues")]
#[test]
fn lockfree_bounded_drop_incoming_matches_policy() {
    let policy = bounded_policy(2, OverflowPolicy::DropIncoming);

    let (retained, telemetry) = apply_lockfree(&[1, 2, 3], &policy);

    assert_eq!(retained, vec![1, 2]);
    assert_eq!(telemetry.backpressure_events, 1);
    #[cfg(feature = "metrics")]
    assert_eq!(telemetry.edge_metrics[&0].pressure_events.drop_incoming, 1);
}

#[cfg(feature = "lockfree-queues")]
#[test]
fn lockfree_bounded_backpressure_matches_policy() {
    let policy = bounded_policy(2, OverflowPolicy::Backpressure);

    let (retained, telemetry) = apply_lockfree(&[1, 2, 3], &policy);

    assert_eq!(retained, vec![1, 2]);
    assert_eq!(telemetry.backpressure_events, 1);
    #[cfg(feature = "metrics")]
    assert_eq!(telemetry.edge_metrics[&0].pressure_events.backpressure, 1);
}

#[cfg(feature = "lockfree-queues")]
#[test]
fn lockfree_bounded_error_matches_policy() {
    let policy = bounded_policy(2, OverflowPolicy::Error);

    let (retained, telemetry) = apply_lockfree(&[1, 2, 3], &policy);

    assert_eq!(retained, vec![1, 2]);
    assert_eq!(telemetry.backpressure_events, 1);
    #[cfg(feature = "metrics")]
    assert_eq!(telemetry.edge_metrics[&0].pressure_events.error_overflow, 1);
}

#[cfg(feature = "metrics")]
#[test]
fn locked_queue_pressure_metrics_are_policy_aware() {
    let cases = [
        (
            RuntimeEdgePolicy {
                pressure: PressurePolicy::Bounded {
                    capacity: 1,
                    overflow: OverflowPolicy::DropIncoming,
                },
                freshness: FreshnessPolicy::PreserveAll,
            },
            BackpressureStrategy::None,
            EdgePressureReason::DropIncoming,
            1,
        ),
        (
            RuntimeEdgePolicy::bounded(1),
            BackpressureStrategy::None,
            EdgePressureReason::DropOldest,
            1,
        ),
        (
            RuntimeEdgePolicy {
                pressure: PressurePolicy::Bounded {
                    capacity: 1,
                    overflow: OverflowPolicy::Backpressure,
                },
                freshness: FreshnessPolicy::PreserveAll,
            },
            BackpressureStrategy::None,
            EdgePressureReason::Backpressure,
            1,
        ),
        (
            RuntimeEdgePolicy::bounded(1),
            BackpressureStrategy::ErrorOnOverflow,
            EdgePressureReason::ErrorOverflow,
            0,
        ),
    ];

    for (policy, strategy, reason, expected_drops) in cases {
        let telemetry = apply_locked_pressure_event(&policy, strategy);
        let metrics = &telemetry.edge_metrics[&0];
        assert_eq!(metrics.pressure_events.total, 1, "{reason:?}");
        assert_eq!(metrics.drops, expected_drops, "{reason:?}");
        match reason {
            EdgePressureReason::DropIncoming => {
                assert_eq!(metrics.pressure_events.drop_incoming, 1)
            }
            EdgePressureReason::DropOldest => assert_eq!(metrics.pressure_events.drop_oldest, 1),
            EdgePressureReason::Backpressure => assert_eq!(metrics.pressure_events.backpressure, 1),
            EdgePressureReason::ErrorOverflow => {
                assert_eq!(metrics.pressure_events.error_overflow, 1)
            }
            other => panic!("unexpected pressure reason in test: {other:?}"),
        }
    }
}

#[test]
fn backpressure_strategies_cover_all_pressure_policies() {
    let policies = [
        (
            "buffer_all",
            RuntimeEdgePolicy {
                pressure: PressurePolicy::BufferAll,
                freshness: FreshnessPolicy::PreserveAll,
            },
            2,
            0,
        ),
        (
            "drop_newest",
            RuntimeEdgePolicy {
                pressure: PressurePolicy::DropNewest,
                freshness: FreshnessPolicy::LatestBySequence,
            },
            1,
            1,
        ),
        (
            "drop_oldest",
            RuntimeEdgePolicy {
                pressure: PressurePolicy::DropOldest,
                freshness: FreshnessPolicy::LatestBySequence,
            },
            1,
            1,
        ),
        ("latest_only", RuntimeEdgePolicy::latest_only(), 1, 1),
        ("bounded_drop_oldest", RuntimeEdgePolicy::bounded(1), 1, 1),
        (
            "bounded_drop_incoming",
            RuntimeEdgePolicy {
                pressure: PressurePolicy::Bounded {
                    capacity: 1,
                    overflow: OverflowPolicy::DropIncoming,
                },
                freshness: FreshnessPolicy::PreserveAll,
            },
            1,
            1,
        ),
        (
            "bounded_backpressure",
            RuntimeEdgePolicy {
                pressure: PressurePolicy::Bounded {
                    capacity: 1,
                    overflow: OverflowPolicy::Backpressure,
                },
                freshness: FreshnessPolicy::PreserveAll,
            },
            1,
            1,
        ),
        (
            "bounded_error",
            RuntimeEdgePolicy {
                pressure: PressurePolicy::Bounded {
                    capacity: 1,
                    overflow: OverflowPolicy::Error,
                },
                freshness: FreshnessPolicy::PreserveAll,
            },
            1,
            1,
        ),
        (
            "coalesce",
            RuntimeEdgePolicy {
                pressure: PressurePolicy::Coalesce {
                    window: Duration::from_millis(1),
                    strategy: CoalesceStrategy::KeepNewest,
                },
                freshness: FreshnessPolicy::LatestBySequence,
            },
            1,
            1,
        ),
        (
            "error_on_full",
            RuntimeEdgePolicy {
                pressure: PressurePolicy::ErrorOnFull,
                freshness: FreshnessPolicy::LatestBySequence,
            },
            1,
            1,
        ),
    ];
    let strategies = [
        BackpressureStrategy::None,
        BackpressureStrategy::BoundedQueues,
        BackpressureStrategy::ErrorOnOverflow,
    ];

    for strategy in strategies {
        for (name, policy, expected_len, expected_events) in policies.clone() {
            let queues = Arc::new(vec![EdgeStorage::Locked {
                queue: Arc::new(Mutex::new(EdgeQueue::default())),
                metrics: Arc::new(EdgeStorageMetrics::default()),
            }]);
            let warnings_seen = Arc::new(Mutex::new(HashSet::new()));
            let mut telemetry = ExecutionTelemetry::default();
            let inspectors = RuntimeDataSizeInspectors::default();

            for value in [1, 2] {
                let result = super::apply_policy_owned(ApplyPolicyOwnedArgs {
                    edge_idx: 0,
                    policy: &policy,
                    payload: payload(value),
                    queues: &queues,
                    warnings_seen: &warnings_seen,
                    telem: &mut telemetry,
                    warning_label: Some(format!("{name}_{strategy:?}")),
                    backpressure: strategy.clone(),
                    data_size_inspectors: &inspectors,
                });
                if matches!(&strategy, BackpressureStrategy::ErrorOnOverflow)
                    && policy.bounded_capacity().is_some()
                    && value == 2
                {
                    assert!(result.is_err(), "{name} with {strategy:?}");
                } else {
                    result.expect("policy application");
                }
            }

            let queue = match queues.first().unwrap() {
                EdgeStorage::Locked { queue, .. } => queue,
                #[cfg(feature = "lockfree-queues")]
                EdgeStorage::BoundedLf { .. } => unreachable!("test queue uses locked storage"),
            };
            assert_eq!(
                queue.lock().unwrap().len(),
                expected_len,
                "{name} with {strategy:?}"
            );
            assert_eq!(
                telemetry.backpressure_events, expected_events,
                "{name} with {strategy:?}"
            );
        }
    }
}
