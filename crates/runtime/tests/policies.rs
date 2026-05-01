use daedalus_runtime::{BackpressureStrategy, RuntimeEdgePolicy, SchedulerConfig};
use daedalus_transport::{FreshnessPolicy, PressurePolicy};

#[test]
fn edge_policy_variants_exist() {
    // Sanity check that core policy variants remain exposed.
    let fifo = RuntimeEdgePolicy::default();
    let bounded = RuntimeEdgePolicy::bounded(8);
    let newest = RuntimeEdgePolicy::latest_only();
    let broadcast = RuntimeEdgePolicy::default();

    assert_eq!(fifo, RuntimeEdgePolicy::default());
    assert_eq!(bounded, RuntimeEdgePolicy::bounded(8));
    assert_eq!(newest, RuntimeEdgePolicy::latest_only());
    assert_eq!(broadcast, RuntimeEdgePolicy::default());
}

#[test]
fn scheduler_config_applies_policy() {
    let cfg = SchedulerConfig {
        default_policy: RuntimeEdgePolicy::latest_only(),
        backpressure: BackpressureStrategy::None,
    };
    assert_eq!(cfg.default_policy, RuntimeEdgePolicy::latest_only());
    assert!(matches!(cfg.backpressure, BackpressureStrategy::None));
}

#[test]
fn release_internal_edge_defaults_are_unbounded_fifo_by_policy() {
    let policy = RuntimeEdgePolicy::default();
    assert!(matches!(policy.pressure, PressurePolicy::BufferAll));
    assert!(matches!(policy.freshness, FreshnessPolicy::PreserveAll));
    assert_eq!(policy.bounded_capacity(), None);

    let cfg = SchedulerConfig::default();
    assert_eq!(cfg.default_policy, policy);
    assert!(matches!(cfg.backpressure, BackpressureStrategy::None));
}
