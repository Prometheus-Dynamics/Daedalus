use daedalus_runtime::{BackpressureStrategy, EdgePolicyKind, SchedulerConfig};

#[test]
fn edge_policy_variants_exist() {
    // Sanity check that core policy variants remain exposed.
    let fifo = EdgePolicyKind::Fifo;
    let bounded = EdgePolicyKind::Bounded { cap: 8 };
    let newest = EdgePolicyKind::NewestWins;
    let broadcast = EdgePolicyKind::Broadcast;

    assert!(matches!(fifo, EdgePolicyKind::Fifo));
    assert!(matches!(bounded, EdgePolicyKind::Bounded { cap: 8 }));
    assert!(matches!(newest, EdgePolicyKind::NewestWins));
    assert!(matches!(broadcast, EdgePolicyKind::Broadcast));
}

#[test]
fn scheduler_config_applies_policy() {
    let cfg = SchedulerConfig {
        default_policy: EdgePolicyKind::NewestWins,
        backpressure: BackpressureStrategy::None,
        lockfree_queues: false,
    };
    assert!(matches!(cfg.default_policy, EdgePolicyKind::NewestWins));
    assert!(matches!(cfg.backpressure, BackpressureStrategy::None));
}
