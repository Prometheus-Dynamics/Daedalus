use daedalus_engine::{Engine, EngineConfig, EngineConfigError, GpuBackend, RuntimeSection};
use daedalus_planner::Graph;
use daedalus_runtime::{RuntimeEdgePolicy, config::RuntimeDebugConfig};
use std::sync::{Mutex, OnceLock};
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

fn graph_with_id(id: &str) -> Graph {
    let mut graph = Graph::default();
    graph.metadata.insert(
        "cache_test_id".into(),
        daedalus_data::model::Value::String(id.to_owned().into()),
    );
    graph
}
