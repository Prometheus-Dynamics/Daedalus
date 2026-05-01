use daedalus_engine::{
    Engine, EngineConfig, EngineConfigError, GpuBackend, RuntimeMode, RuntimeSection,
};
use daedalus_planner::{ComputeAffinity, ExecutionPlan, Graph, NodeInstance};
use daedalus_runtime::{
    RuntimeEdgePolicy, RuntimeNode, SchedulerConfig, build_runtime,
    config::RuntimeDebugConfig,
    executor::{NodeError, NodeHandler},
};
use std::sync::{
    Arc, Mutex, OnceLock,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

#[cfg(feature = "config-env")]
fn env_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[test]
fn pool_size_zero_is_rejected() {
    let cfg = EngineConfig {
        runtime: RuntimeSection {
            pool_size: Some(0),
            ..RuntimeSection::default()
        },
        ..EngineConfig::default()
    };
    assert!(matches!(
        cfg.validate(),
        Err(EngineConfigError::PoolSizeZero)
    ));
    let err = Engine::new(cfg).err().expect("expected config error");
    let msg = format!("{}", err);
    assert!(msg.contains("pool_size must be > 0"), "{msg}");
}

#[cfg(feature = "config-env")]
#[test]
fn runtime_pool_size_env_is_parsed() {
    let _guard = env_lock().lock().expect("env lock");
    unsafe {
        std::env::set_var("DAEDALUS_RUNTIME_POOL_SIZE", "3");
    }
    let cfg = EngineConfig::from_env().expect("env config");
    unsafe {
        std::env::remove_var("DAEDALUS_RUNTIME_POOL_SIZE");
    }

    assert_eq!(cfg.runtime.pool_size, Some(3));
    assert_eq!(cfg.runtime.debug_config.pool_size, Some(3));
}

#[test]
fn runtime_debug_config_builder_sets_runtime_override() {
    let cfg = EngineConfig::default().with_runtime_debug_config(RuntimeDebugConfig {
        node_perf_counters: true,
        node_cpu_time: true,
        pool_size: Some(2),
    });

    assert!(cfg.runtime.debug_config.node_perf_counters);
    assert!(cfg.runtime.debug_config.node_cpu_time);
    assert_eq!(cfg.runtime.pool_size, Some(2));
}

#[cfg(feature = "config-env")]
#[test]
fn invalid_runtime_pool_size_env_is_rejected() {
    let _guard = env_lock().lock().expect("env lock");
    unsafe {
        std::env::set_var("DAEDALUS_RUNTIME_POOL_SIZE", "nope");
    }
    let err = EngineConfig::from_env().expect_err("invalid pool size");
    unsafe {
        std::env::remove_var("DAEDALUS_RUNTIME_POOL_SIZE");
    }

    assert_eq!(
        err,
        EngineConfigError::InvalidEnvValue {
            var: "DAEDALUS_RUNTIME_POOL_SIZE",
            value: "nope".into()
        }
    );
}

#[test]
fn zero_cache_limits_are_rejected() {
    let cfg = EngineConfig {
        cache: daedalus_engine::CacheSection {
            planner_max_entries: 0,
            ..Default::default()
        },
        ..EngineConfig::default()
    };
    assert!(matches!(
        cfg.validate(),
        Err(EngineConfigError::PlannerCacheLimitZero)
    ));
    let err = Engine::new(cfg).err().expect("expected config error");
    assert!(err.to_string().contains("cache.planner_max_entries"));
}

#[test]
fn gpu_requires_feature() {
    let cfg = EngineConfig {
        gpu: GpuBackend::Device,
        ..EngineConfig::default()
    };
    let res = Engine::new(cfg);
    #[cfg(not(feature = "gpu"))]
    {
        let err = res.err().expect("expected config error");
        let msg = format!("{}", err);
        assert!(msg.contains("feature 'gpu' is disabled"));
    }
    #[cfg(feature = "gpu")]
    {
        assert!(
            res.is_ok(),
            "expected GPU config to succeed when feature `gpu` is enabled"
        );
    }
}

#[test]
fn host_queue_defaults_are_runtime_configurable() {
    let config = EngineConfig::default()
        .with_default_host_input_policy(RuntimeEdgePolicy::bounded(4))
        .with_default_host_output_policy(RuntimeEdgePolicy::bounded(8));

    assert_eq!(
        config.runtime.default_host_input_policy.bounded_capacity(),
        Some(4)
    );
    assert_eq!(
        config.runtime.default_host_output_policy.bounded_capacity(),
        Some(8)
    );
    let host_config = config.runtime.host_bridge_config();
    assert_eq!(host_config.default_input_policy.bounded_capacity(), Some(4));
    assert_eq!(
        host_config.default_output_policy.bounded_capacity(),
        Some(8)
    );
}

#[test]
fn host_event_retention_is_runtime_configurable() {
    let config = EngineConfig::default()
        .with_host_event_recording(false)
        .with_host_event_limit(Some(4));

    assert!(!config.runtime.host_event_recording);
    assert_eq!(config.runtime.host_event_limit, Some(4));
}

#[test]
fn stream_idle_sleep_is_runtime_configurable() {
    let config = EngineConfig::default().with_stream_idle_sleep(Duration::from_micros(250));

    assert_eq!(
        config.runtime.stream_worker_config().idle_sleep,
        Duration::from_micros(250)
    );
}

#[test]
fn engine_planner_cache_limits_are_configurable() {
    let engine = Engine::new(EngineConfig::default().with_cache_limits(1, 1)).unwrap();

    let first = engine.prepare_plan(graph_with_id("first")).unwrap();
    assert!(!first.is_cached());

    let second = engine.prepare_plan(graph_with_id("second")).unwrap();
    assert!(!second.is_cached());

    let metrics = engine.cache_metrics();
    assert_eq!(metrics.planner.entries, 1);
    assert_eq!(metrics.planner.max_entries, 1);
    assert_eq!(metrics.planner.evictions, 1);
    assert_eq!(metrics.runtime_plan.max_entries, 1);

    let metrics = engine.clear_caches();
    assert_eq!(metrics.planner.entries, 0);
    assert_eq!(metrics.runtime_plan.entries, 0);
    assert_eq!(metrics.planner.invalidations, 1);
    assert_eq!(metrics.runtime_plan.invalidations, 0);
}

#[test]
fn direct_and_compiled_parallel_execution_apply_same_runtime_config() {
    let config = EngineConfig {
        runtime: RuntimeSection {
            mode: RuntimeMode::Parallel,
            pool_size: Some(2),
            ..RuntimeSection::default()
        },
        ..EngineConfig::default()
    };
    let engine = Engine::new(config).unwrap();
    let runtime_plan = independent_runtime_plan(6);

    let direct_log = Arc::new(Mutex::new(Vec::new()));
    let direct = engine
        .execute(
            runtime_plan.clone(),
            LogHandler {
                log: direct_log.clone(),
            },
        )
        .unwrap();

    let compiled_log = Arc::new(Mutex::new(Vec::new()));
    let mut compiled = engine
        .compile_runtime_plan(
            runtime_plan,
            LogHandler {
                log: compiled_log.clone(),
            },
        )
        .unwrap();
    let compiled = compiled.run_telemetry().unwrap();

    let mut direct_nodes = direct_log.lock().unwrap().clone();
    let mut compiled_nodes = compiled_log.lock().unwrap().clone();
    direct_nodes.sort();
    compiled_nodes.sort();

    assert_eq!(direct_nodes, compiled_nodes);
    assert_eq!(direct.nodes_executed, compiled.nodes_executed);
    assert_eq!(direct.cpu_segments, compiled.cpu_segments);
}

#[test]
fn adaptive_mode_uses_parallel_for_independent_segments() {
    let runtime_plan = independent_runtime_plan(4);
    assert!(
        runtime_plan.segments.len() >= 4,
        "test plan should expose independent CPU segments"
    );

    let serial = run_concurrency_probe(RuntimeMode::Serial, runtime_plan.clone());
    let adaptive = run_concurrency_probe(RuntimeMode::Adaptive, runtime_plan);

    assert_eq!(serial, 1, "serial execution should not overlap handlers");
    assert!(
        adaptive > 1,
        "adaptive execution should use parallelism when independent segments are available"
    );
}

fn graph_with_id(id: &str) -> Graph {
    let mut graph = Graph::default();
    graph.metadata.insert(
        "cache_test_id".into(),
        daedalus_data::model::Value::String(id.to_owned().into()),
    );
    graph
}

struct LogHandler {
    log: Arc<Mutex<Vec<String>>>,
}

impl NodeHandler for LogHandler {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        self.log.lock().unwrap().push(node.id.clone());
        Ok(())
    }
}

struct ConcurrencyProbeHandler {
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
    sleep: Duration,
}

impl NodeHandler for ConcurrencyProbeHandler {
    fn run(
        &self,
        _node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        _io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_active.fetch_max(active, Ordering::AcqRel);
        std::thread::sleep(self.sleep);
        self.active.fetch_sub(1, Ordering::AcqRel);
        Ok(())
    }
}

fn run_concurrency_probe(mode: RuntimeMode, runtime_plan: daedalus_runtime::RuntimePlan) -> usize {
    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let engine = Engine::new(
        EngineConfig::default()
            .with_runtime_mode(mode)
            .with_pool_size(4),
    )
    .unwrap();
    engine
        .execute(
            runtime_plan,
            ConcurrencyProbeHandler {
                active,
                max_active: max_active.clone(),
                sleep: Duration::from_millis(25),
            },
        )
        .unwrap();
    max_active.load(Ordering::Acquire)
}

fn independent_runtime_plan(count: usize) -> daedalus_runtime::RuntimePlan {
    let mut graph = Graph::default();
    for idx in 0..count {
        graph.nodes.push(NodeInstance {
            id: daedalus_registry::ids::NodeId::new(format!("n{idx}")),
            bundle: None,
            label: None,
            inputs: vec![],
            outputs: vec![],
            compute: ComputeAffinity::CpuOnly,
            const_inputs: vec![],
            sync_groups: vec![],
            metadata: Default::default(),
        });
    }
    build_runtime(
        &ExecutionPlan::new(graph, vec![]),
        &SchedulerConfig::default(),
    )
}
