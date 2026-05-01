use daedalus::{
    engine::{Engine, EngineConfig, MetricsLevel, RuntimeMode},
    runtime::RuntimeEdgePolicy,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = EngineConfig::default()
        .with_runtime_mode(RuntimeMode::Parallel)
        .with_pool_size(4)
        .with_metrics_level(MetricsLevel::Basic)
        .with_default_host_input_policy(RuntimeEdgePolicy::bounded(2))
        .with_default_host_output_policy(RuntimeEdgePolicy::bounded(2))
        .with_host_event_limit(Some(128))
        .with_cache_limits(256, 256);

    let engine = Engine::new(config)?;
    println!("engine config: {:?}", engine.config().runtime.mode);
    Ok(())
}
