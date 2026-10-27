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

/// `src` pushes 1 then 2 on one tick into a `Coalesce` edge; `sink` records what arrives.
struct TwoPushes(std::sync::Arc<daedalus_runtime::sync::Mutex<Vec<i32>>>);

impl daedalus_runtime::NodeHandler for TwoPushes {
    fn run(
        &self,
        node: &daedalus_runtime::RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), daedalus_runtime::NodeError> {
        match node.id.as_str() {
            "src" => {
                io.push_payload("out", daedalus_transport::Payload::owned("test:i32", 1_i32));
                io.push_payload("out", daedalus_transport::Payload::owned("test:i32", 2_i32));
            }
            "sink" => {
                if let Some(value) = io.get_typed::<i32>("in") {
                    self.0.lock().push(value);
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn coalesce_sink_sees(strategy: daedalus_transport::CoalesceStrategy) -> Vec<i32> {
    let mut graph = daedalus_planner::Graph::default();
    graph.nodes.push(daedalus_planner::NodeInstance::new("src"));
    graph
        .nodes
        .push(daedalus_planner::NodeInstance::new("sink"));
    graph
        .edges
        .push(daedalus_planner::Edge::new(0, "out", 1, "in"));
    let plan = daedalus_planner::ExecutionPlan::new(graph, vec![]);
    let config = SchedulerConfig {
        default_policy: RuntimeEdgePolicy {
            pressure: PressurePolicy::Coalesce {
                window: core::time::Duration::from_secs(60),
                strategy,
            },
            freshness: FreshnessPolicy::PreserveAll,
        },
        backpressure: BackpressureStrategy::None,
    };
    let rt = daedalus_runtime::build_runtime(&plan, &config);
    let seen = std::sync::Arc::default();
    daedalus_runtime::Executor::new(&rt, TwoPushes(std::sync::Arc::clone(&seen)))
        .run()
        .expect("run");
    seen.lock().clone()
}

#[test]
fn coalesce_keep_oldest_edges_keep_the_value_that_opened_the_window() {
    assert_eq!(
        coalesce_sink_sees(daedalus_transport::CoalesceStrategy::KeepOldest),
        vec![1]
    );
}

#[test]
fn coalesce_keep_newest_edges_keep_the_newest_value() {
    assert_eq!(
        coalesce_sink_sees(daedalus_transport::CoalesceStrategy::KeepNewest),
        vec![2]
    );
}
