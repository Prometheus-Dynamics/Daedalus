use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use daedalus_runtime::RuntimeEdgePolicy;
use daedalus_runtime::handles::{HostAlias, PortId};
use daedalus_runtime::host_bridge::{HostBridgeConfig, HostBridgeManager};
use daedalus_transport::{
    DropReason, FeedOutcome, FreshnessPolicy, OverflowPolicy, Payload, PayloadLineage,
    PressurePolicy,
};

#[test]
fn host_bridge_latest_only_replaces_inbound_payloads() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    handle
        .set_input_policy(
            "input",
            PressurePolicy::LatestOnly,
            FreshnessPolicy::LatestBySequence,
        )
        .unwrap();

    let first =
        Payload::owned("demo:u32", 1u32).with_lineage(PayloadLineage::new().with_sequence(1));
    let second =
        Payload::owned("demo:u32", 2u32).with_lineage(PayloadLineage::new().with_sequence(2));
    let first_id = first.correlation_id();
    let second_id = second.correlation_id();

    assert_eq!(
        handle.feed_payload("input", first),
        FeedOutcome::Accepted {
            correlation_id: first_id
        }
    );
    assert_eq!(
        handle.feed_payload("input", second),
        FeedOutcome::Replaced {
            old: first_id,
            new: second_id
        }
    );

    let inbound = manager.take_inbound("host");
    assert_eq!(inbound.len(), 1);
    assert_eq!(inbound[0].payload.get_ref::<u32>(), Some(&2));
    assert_eq!(handle.events().len(), 2);
    assert_eq!(handle.stats().inbound_replaced, 1);
    assert_eq!(
        handle
            .stats()
            .inbound_drop_reasons
            .count(DropReason::LatestOnlyReplace),
        1
    );
    assert_eq!(
        handle
            .events()
            .last()
            .and_then(|event| event.reason.clone()),
        Some(DropReason::LatestOnlyReplace)
    );
}

#[test]
fn host_bridge_accepts_typed_aliases_and_ports() {
    let manager = HostBridgeManager::new();
    let alias = HostAlias::new("typed-host");
    let input = PortId::new("typed-input");
    let handle = manager.ensure_handle(alias.clone());

    let payload = Payload::owned("demo:u32", 7u32);
    assert!(matches!(
        handle.feed_payload(input.clone(), payload),
        FeedOutcome::Accepted { .. }
    ));

    let inbound = manager.take_inbound(alias.as_str());
    assert_eq!(inbound.len(), 1);
    assert_eq!(inbound[0].port, input);
    assert_eq!(inbound[0].payload.get_ref::<u32>(), Some(&7));
}

#[test]
fn host_bridge_event_limit_retains_recent_events() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    handle.set_event_limit(Some(2));

    let mut ids = Vec::new();
    for value in 0..4u32 {
        let payload = Payload::owned("demo:u32", value);
        ids.push(payload.correlation_id());
        handle.feed_payload("input", payload);
    }

    let events = handle.events();
    assert_eq!(events.len(), 2);
    assert_eq!(
        events
            .iter()
            .map(|event| event.correlation_id)
            .collect::<Vec<_>>(),
        ids.into_iter().skip(2).collect::<Vec<_>>()
    );
}

#[test]
fn host_bridge_event_recording_can_be_disabled_or_unbounded() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");

    handle.set_event_recording(false);
    handle.feed_payload("input", Payload::owned("demo:u32", 1u32));
    assert!(handle.events().is_empty());

    handle.set_event_recording(true);
    handle.set_event_limit(None);
    for value in 0..3u32 {
        handle.feed_payload("input", Payload::owned("demo:u32", value));
    }
    assert_eq!(handle.events().len(), 3);

    manager.set_event_limit(Some(1));
    assert_eq!(handle.events().len(), 1);

    let later = manager.ensure_handle("later");
    for value in 0..3u32 {
        later.feed_payload("input", Payload::owned("demo:u32", value));
    }
    assert_eq!(later.events().len(), 1);

    manager.set_event_recording(false);
    let disabled = manager.ensure_handle("disabled");
    disabled.feed_payload("input", Payload::owned("demo:u32", 1u32));
    assert!(disabled.events().is_empty());
}

#[test]
fn disabled_host_bridge_event_recording_keeps_queued_payload_unique() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    handle.set_event_recording(false);

    let payload = Payload::shared("demo:bytes", Arc::new(vec![1u8, 2, 3]));
    assert!(payload.is_storage_unique());
    assert_eq!(payload.typed_strong_count::<Vec<u8>>(), Some(1));

    handle.feed_payload("input", payload);
    assert!(handle.events().is_empty());

    let mut inbound = manager.take_inbound("host");
    assert_eq!(inbound.len(), 1);
    let queued = inbound.pop().unwrap().payload;
    assert!(queued.is_storage_unique());
    assert_eq!(queued.typed_strong_count::<Vec<u8>>(), Some(1));
}

#[test]
fn host_bridge_config_updates_existing_and_future_handles() {
    let manager = HostBridgeManager::new();
    let existing = manager.ensure_handle("host");
    let policy = RuntimeEdgePolicy {
        pressure: PressurePolicy::Bounded {
            capacity: 2,
            overflow: OverflowPolicy::DropIncoming,
        },
        freshness: FreshnessPolicy::PreserveAll,
    };

    manager
        .apply_config(
            &HostBridgeConfig::default()
                .with_default_input_policy(policy)
                .with_event_recording(false)
                .with_event_limit(Some(1)),
        )
        .unwrap();

    for value in 0..3u32 {
        existing.feed_payload("input", Payload::owned("demo:u32", value));
    }
    assert_eq!(existing.pending_inbound(), 2);
    assert_eq!(
        existing
            .stats()
            .inbound_drop_reasons
            .count(DropReason::DropNewest),
        1
    );
    assert!(existing.events().is_empty());

    manager.set_event_recording(true);
    let future = manager.ensure_handle("future");
    for value in 0..3u32 {
        future.feed_payload("input", Payload::owned("demo:u32", value));
    }
    assert_eq!(future.pending_inbound(), 2);
    assert_eq!(future.events().len(), 1);
}

#[test]
fn host_bridge_default_bounds_hold_under_long_running_pressure() {
    let manager = HostBridgeManager::new();
    manager.set_event_limit(Some(32));
    let handle = manager.ensure_handle("host");

    for value in 0..10_000u32 {
        handle.feed_payload("input", Payload::owned("demo:u32", value));
        manager.push_outbound("host", "output", Payload::owned("demo:u32", value));
    }

    assert_eq!(handle.pending_inbound(), 1);
    assert_eq!(handle.pending_outbound(), 1);
    assert_eq!(handle.events().len(), 32);
}

#[test]
fn host_bridge_multi_producer_input_stress_stays_bounded() {
    const PRODUCERS: usize = 8;
    const PER_PRODUCER: usize = 500;

    let manager = HostBridgeManager::new();
    manager.set_event_limit(Some(64));
    let handle = manager.ensure_handle("host");
    let start = Arc::new(Barrier::new(PRODUCERS));

    let mut producers = Vec::with_capacity(PRODUCERS);
    for producer in 0..PRODUCERS {
        let handle = handle.clone();
        let start = Arc::clone(&start);
        producers.push(thread::spawn(move || {
            start.wait();
            for seq in 0..PER_PRODUCER {
                let value = ((producer as u64) << 32) | seq as u64;
                handle.feed_payload("input", Payload::owned("demo:u64", value));
            }
        }));
    }

    for producer in producers {
        producer.join().expect("producer thread panicked");
    }

    let expected_total = (PRODUCERS * PER_PRODUCER) as u64;
    let stats = handle.stats();
    assert_eq!(stats.inbound_accepted, expected_total);
    assert_eq!(stats.inbound_replaced, expected_total - 1);
    assert_eq!(
        stats.inbound_drop_reasons.count(DropReason::DropOldest),
        expected_total - 1
    );
    assert_eq!(handle.pending_inbound(), 1);
    assert_eq!(handle.events().len(), 64);
    assert_eq!(manager.take_inbound("host").len(), 1);
}

#[test]
fn host_bridge_outbound_push_wakes_waiting_receiver() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    let receiver = handle.clone();

    let waiter = thread::spawn(move || {
        receiver
            .recv_payload_timeout("output", Duration::from_secs(1))
            .and_then(|payload| payload.get_ref::<u32>().copied())
    });

    thread::sleep(Duration::from_millis(20));
    manager.push_outbound("host", "output", Payload::owned("demo:u32", 99_u32));

    assert_eq!(
        waiter.join().expect("receiver thread should not panic"),
        Some(99)
    );
}

#[test]
fn host_bridge_default_input_policy_is_bounded_and_overridable() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");

    assert!(matches!(
        handle.feed_payload("input", Payload::owned("demo:u32", 1u32)),
        FeedOutcome::Accepted { .. }
    ));
    assert!(matches!(
        handle.feed_payload("input", Payload::owned("demo:u32", 2u32)),
        FeedOutcome::Replaced { .. }
    ));
    assert_eq!(handle.pending_inbound(), 1);

    handle
        .set_input_policy(
            "fifo",
            PressurePolicy::BufferAll,
            FreshnessPolicy::PreserveAll,
        )
        .unwrap();
    handle.feed_payload("fifo", Payload::owned("demo:u32", 1u32));
    handle.feed_payload("fifo", Payload::owned("demo:u32", 2u32));
    assert_eq!(handle.pending_inbound(), 3);
}

#[test]
fn host_bridge_manager_default_policies_apply_to_existing_and_future_handles() {
    let manager = HostBridgeManager::new();
    let first = manager.ensure_handle("first");
    manager
        .set_default_input_policy(PressurePolicy::BufferAll, FreshnessPolicy::PreserveAll)
        .unwrap();
    let second = manager.ensure_handle("second");

    for handle in [first, second] {
        handle.feed_payload("input", Payload::owned("demo:u32", 1u32));
        handle.feed_payload("input", Payload::owned("demo:u32", 2u32));
        assert_eq!(handle.pending_inbound(), 2);
    }
}

#[test]
fn host_bridge_default_output_policy_is_bounded_and_overridable() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");

    manager.push_outbound("host", "out", Payload::owned("demo:u32", 1u32));
    manager.push_outbound("host", "out", Payload::owned("demo:u32", 2u32));
    assert_eq!(handle.pending_outbound(), 1);
    assert_eq!(handle.stats().outbound_replaced, 1);
    assert_eq!(
        handle
            .stats()
            .outbound_drop_reasons
            .count(DropReason::DropOldest),
        1
    );

    manager
        .set_default_output_policy(PressurePolicy::BufferAll, FreshnessPolicy::PreserveAll)
        .unwrap();
    manager.push_outbound("host", "stream", Payload::owned("demo:u32", 1u32));
    manager.push_outbound("host", "stream", Payload::owned("demo:u32", 2u32));
    assert_eq!(handle.drain_payloads("stream").len(), 2);
}

#[test]
fn host_bridge_bounded_drop_newest_reports_drop() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    handle
        .set_input_policy(
            "input",
            PressurePolicy::Bounded {
                capacity: 1,
                overflow: OverflowPolicy::DropIncoming,
            },
            FreshnessPolicy::PreserveAll,
        )
        .unwrap();

    let first = Payload::owned("demo:u32", 1u32);
    let second = Payload::owned("demo:u32", 2u32);
    let second_id = second.correlation_id();

    assert!(matches!(
        handle.feed_payload("input", first),
        FeedOutcome::Accepted { .. }
    ));
    assert_eq!(
        handle.feed_payload("input", second),
        FeedOutcome::Dropped {
            correlation_id: second_id,
            reason: DropReason::DropNewest,
        }
    );
    assert_eq!(handle.stats().inbound_dropped, 1);
    assert_eq!(
        handle
            .stats()
            .inbound_drop_reasons
            .count(DropReason::DropNewest),
        1
    );
    assert_eq!(
        handle
            .events()
            .last()
            .and_then(|event| event.reason.clone()),
        Some(DropReason::DropNewest)
    );
}

#[test]
fn host_bridge_bounded_backpressure_reports_full_queue() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    handle
        .set_input_policy(
            "input",
            PressurePolicy::Bounded {
                capacity: 1,
                overflow: OverflowPolicy::Backpressure,
            },
            FreshnessPolicy::PreserveAll,
        )
        .unwrap();

    assert!(matches!(
        handle.feed_payload("input", Payload::owned("demo:u32", 1u32)),
        FeedOutcome::Accepted { .. }
    ));
    assert_eq!(
        handle.feed_payload("input", Payload::owned("demo:u32", 2u32)),
        FeedOutcome::Backpressured
    );
    assert_eq!(handle.pending_inbound(), 1);
    assert_eq!(handle.stats().inbound_dropped, 1);
    assert_eq!(
        handle
            .stats()
            .inbound_drop_reasons
            .count(DropReason::Backpressure),
        1
    );
    assert_eq!(
        handle
            .events()
            .last()
            .and_then(|event| event.outcome.clone()),
        Some(FeedOutcome::Backpressured)
    );
}

#[test]
fn host_bridge_drop_oldest_replaces_queued_payload() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    handle
        .set_input_policy(
            "input",
            PressurePolicy::DropOldest,
            FreshnessPolicy::LatestBySequence,
        )
        .unwrap();

    let first =
        Payload::owned("demo:u32", 1u32).with_lineage(PayloadLineage::new().with_sequence(1));
    let second =
        Payload::owned("demo:u32", 2u32).with_lineage(PayloadLineage::new().with_sequence(2));
    let first_id = first.correlation_id();
    let second_id = second.correlation_id();

    assert!(matches!(
        handle.feed_payload("input", first),
        FeedOutcome::Accepted { .. }
    ));
    assert_eq!(
        handle.feed_payload("input", second),
        FeedOutcome::Replaced {
            old: first_id,
            new: second_id,
        }
    );
    let inbound = manager.take_inbound("host");
    assert_eq!(inbound.len(), 1);
    assert_eq!(inbound[0].payload.get_ref::<u32>(), Some(&2));
}

#[test]
fn host_bridge_max_lag_rejects_old_sequence() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    handle
        .set_input_policy(
            "input",
            PressurePolicy::Bounded {
                capacity: 4,
                overflow: OverflowPolicy::DropOldest,
            },
            FreshnessPolicy::MaxLag { frames: 0 },
        )
        .unwrap();

    assert!(matches!(
        handle.feed_payload(
            "input",
            Payload::owned("demo:u32", 2u32).with_lineage(PayloadLineage::new().with_sequence(2)),
        ),
        FeedOutcome::Accepted { .. }
    ));
    let old = Payload::owned("demo:u32", 1u32).with_lineage(PayloadLineage::new().with_sequence(1));
    let old_id = old.correlation_id();

    assert_eq!(
        handle.feed_payload("input", old),
        FeedOutcome::Dropped {
            correlation_id: old_id,
            reason: DropReason::MaxLag,
        }
    );
}

#[test]
fn host_bridge_error_on_full_reports_drop() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    handle
        .set_input_policy(
            "input",
            PressurePolicy::ErrorOnFull,
            FreshnessPolicy::LatestBySequence,
        )
        .unwrap();

    assert!(matches!(
        handle.feed_payload(
            "input",
            Payload::owned("demo:u32", 1u32).with_lineage(PayloadLineage::new().with_sequence(1)),
        ),
        FeedOutcome::Accepted { .. }
    ));
    let rejected =
        Payload::owned("demo:u32", 2u32).with_lineage(PayloadLineage::new().with_sequence(2));
    let rejected_id = rejected.correlation_id();

    assert_eq!(
        handle.feed_payload("input", rejected),
        FeedOutcome::Dropped {
            correlation_id: rejected_id,
            reason: DropReason::ErrorOnFull,
        }
    );
}

#[test]
fn host_bridge_max_age_rejects_stale_payload() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    handle
        .set_input_policy(
            "input",
            PressurePolicy::Bounded {
                capacity: 4,
                overflow: OverflowPolicy::DropOldest,
            },
            FreshnessPolicy::MaxAge(Duration::ZERO),
        )
        .unwrap();

    let payload = Payload::owned("demo:u32", 1u32);
    let id = payload.correlation_id();

    assert_eq!(
        handle.feed_payload("input", payload),
        FeedOutcome::Dropped {
            correlation_id: id,
            reason: DropReason::MaxAge,
        }
    );
}

#[test]
fn host_bridge_output_policy_drops_when_full() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    handle
        .set_output_policy(
            "out",
            PressurePolicy::ErrorOnFull,
            FreshnessPolicy::LatestBySequence,
        )
        .unwrap();

    manager.push_outbound(
        "host",
        "out",
        Payload::owned("demo:u32", 1u32).with_lineage(PayloadLineage::new().with_sequence(1)),
    );
    manager.push_outbound(
        "host",
        "out",
        Payload::owned("demo:u32", 2u32).with_lineage(PayloadLineage::new().with_sequence(2)),
    );

    assert_eq!(handle.pending_outbound(), 1);
    assert_eq!(handle.stats().outbound_dropped, 1);
    let delivered = handle.try_pop_payload("out").unwrap();
    assert_eq!(delivered.get_ref::<u32>(), Some(&1));
}

#[test]
fn host_bridge_arc_payloads_stay_zero_copy() {
    let manager = HostBridgeManager::new();
    let handle = manager.ensure_handle("host");
    let data = Arc::new(vec![1u8, 2, 3]);
    handle.feed_payload("input", Payload::shared("demo:bytes", data.clone()));
    let inbound = manager.take_inbound("host");
    let extracted = inbound[0].payload.get_arc::<Vec<u8>>().unwrap();
    assert!(Arc::ptr_eq(&data, &extracted));
}
