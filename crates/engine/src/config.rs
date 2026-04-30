#[cfg(feature = "config-env")]
use std::env;
use std::time::Duration;

#[cfg(feature = "config-env")]
use serde::{Deserialize, Serialize};

use daedalus_runtime::{
    BackpressureStrategy, ENV_RUNTIME_POOL_SIZE, HostBridgeConfig, MetricsLevel,
    RuntimeDebugConfig, RuntimeEdgePolicy, StreamWorkerConfig,
};
use daedalus_runtime::{DEFAULT_HOST_BRIDGE_EVENT_LIMIT, DEFAULT_STREAM_IDLE_SLEEP, RuntimeSink};
use thiserror::Error;

pub const DEFAULT_CACHE_ENTRIES: usize = 128;

#[derive(Clone, Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum EngineConfigError {
    #[error("pool_size must be > 0 when provided")]
    PoolSizeZero,
    #[error("planner GPU is enabled but gpu backend is set to cpu")]
    PlannerGpuWithCpuBackend,
    #[error("cache.planner_max_entries must be > 0")]
    PlannerCacheLimitZero,
    #[error("cache.runtime_plan_max_entries must be > 0")]
    RuntimePlanCacheLimitZero,
    #[error("unknown {var} '{value}'")]
    UnknownEnvValue { var: &'static str, value: String },
    #[error("invalid {var} '{value}'")]
    InvalidEnvValue { var: &'static str, value: String },
    #[error("error reading {var}: {error}")]
    EnvRead { var: &'static str, error: String },
}

/// GPU backend selection; device requires the `gpu` feature.
///
/// ```
/// use daedalus_engine::GpuBackend;
/// let backend = GpuBackend::Cpu;
/// assert_eq!(backend, GpuBackend::Cpu);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "config-env", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "config-env", serde(rename_all = "snake_case"))]
pub enum GpuBackend {
    #[default]
    Cpu,
    Mock,
    Device,
}

/// Runtime execution mode.
///
/// ```
/// use daedalus_engine::RuntimeMode;
/// let mode = RuntimeMode::Parallel;
/// assert_eq!(mode, RuntimeMode::Parallel);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Default)]
#[cfg_attr(feature = "config-env", derive(Serialize, Deserialize))]
#[cfg_attr(feature = "config-env", serde(rename_all = "snake_case"))]
pub enum RuntimeMode {
    #[default]
    Serial,
    Parallel,
    Adaptive,
}

/// Planner knobs.
///
/// ```ignore
/// use daedalus_engine::config::PlannerSection;
/// let planner = PlannerSection::default();
/// assert!(!planner.enable_gpu);
/// ```
#[derive(Clone, Debug, PartialEq)]
#[cfg_attr(feature = "config-env", derive(Serialize, Deserialize))]
pub struct PlannerSection {
    #[cfg_attr(feature = "config-env", serde(default))]
    pub enable_gpu: bool,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub enable_lints: bool,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub active_features: Vec<String>,
}

impl Default for PlannerSection {
    fn default() -> Self {
        Self {
            enable_gpu: false,
            enable_lints: true,
            active_features: Vec::new(),
        }
    }
}

/// Runtime scheduler/backpressure options.
///
/// ```ignore
/// use daedalus_engine::config::RuntimeSection;
/// let runtime = RuntimeSection::default();
/// assert_eq!(runtime.pool_size, None);
/// ```
#[derive(Clone, Debug)]
#[cfg_attr(feature = "config-env", derive(Serialize, Deserialize))]
pub struct RuntimeSection {
    #[cfg_attr(feature = "config-env", serde(default))]
    pub default_policy: RuntimeEdgePolicy,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub backpressure: BackpressureStrategy,
    #[cfg_attr(feature = "config-env", serde(default = "default_host_queue_policy"))]
    pub default_host_input_policy: RuntimeEdgePolicy,
    #[cfg_attr(feature = "config-env", serde(default = "default_host_queue_policy"))]
    pub default_host_output_policy: RuntimeEdgePolicy,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub mode: RuntimeMode,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub metrics_level: MetricsLevel,
    #[cfg_attr(
        feature = "config-env",
        serde(default = "default_host_event_recording")
    )]
    pub host_event_recording: bool,
    #[cfg_attr(feature = "config-env", serde(default = "default_host_event_limit"))]
    pub host_event_limit: Option<usize>,
    /// When true, abort the run on the first node error. When false, keep running and
    /// collect failures in `ExecutionTelemetry.errors`.
    #[cfg_attr(feature = "config-env", serde(default = "default_fail_fast"))]
    pub fail_fast: bool,
    /// Demand-driven execution: compute only the subgraph needed for the selected sinks.
    ///
    /// This is useful when a slow preview branch would otherwise drag down unrelated outputs.
    #[cfg_attr(feature = "config-env", serde(default))]
    pub demand_driven: bool,
    /// Active sinks to compute when `demand_driven` is enabled.
    ///
    /// When empty, the engine falls back to full-graph execution.
    #[cfg_attr(
        feature = "config-env",
        serde(default, skip_serializing_if = "Vec::is_empty")
    )]
    pub demand_sinks: Vec<RuntimeSink>,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub pool_size: Option<usize>,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub debug_config: RuntimeDebugConfig,
    #[cfg_attr(feature = "config-env", serde(default = "default_stream_idle_sleep"))]
    pub stream_idle_sleep: Duration,
}

fn default_fail_fast() -> bool {
    true
}

fn default_host_event_recording() -> bool {
    true
}

fn default_host_event_limit() -> Option<usize> {
    Some(DEFAULT_HOST_BRIDGE_EVENT_LIMIT)
}

fn default_host_queue_policy() -> RuntimeEdgePolicy {
    RuntimeEdgePolicy::bounded(1)
}

fn default_stream_idle_sleep() -> Duration {
    DEFAULT_STREAM_IDLE_SLEEP
}

impl Default for RuntimeSection {
    fn default() -> Self {
        Self {
            default_policy: RuntimeEdgePolicy::default(),
            backpressure: BackpressureStrategy::None,
            default_host_input_policy: default_host_queue_policy(),
            default_host_output_policy: default_host_queue_policy(),
            mode: RuntimeMode::Serial,
            metrics_level: MetricsLevel::default(),
            host_event_recording: default_host_event_recording(),
            host_event_limit: default_host_event_limit(),
            fail_fast: default_fail_fast(),
            demand_driven: true,
            demand_sinks: Vec::new(),
            pool_size: None,
            debug_config: RuntimeDebugConfig::default(),
            stream_idle_sleep: default_stream_idle_sleep(),
        }
    }
}

impl RuntimeSection {
    pub fn host_bridge_config(&self) -> HostBridgeConfig {
        HostBridgeConfig {
            default_input_policy: self.default_host_input_policy.clone(),
            default_output_policy: self.default_host_output_policy.clone(),
            event_recording: self.host_event_recording,
            event_limit: self.host_event_limit,
        }
    }

    pub fn stream_worker_config(&self) -> StreamWorkerConfig {
        StreamWorkerConfig {
            idle_sleep: self.stream_idle_sleep,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "config-env", derive(Serialize, Deserialize))]
pub struct CacheSection {
    #[cfg_attr(feature = "config-env", serde(default = "default_cache_entries"))]
    pub planner_max_entries: usize,
    #[cfg_attr(feature = "config-env", serde(default = "default_cache_entries"))]
    pub runtime_plan_max_entries: usize,
}

fn default_cache_entries() -> usize {
    DEFAULT_CACHE_ENTRIES
}

impl Default for CacheSection {
    fn default() -> Self {
        Self {
            planner_max_entries: default_cache_entries(),
            runtime_plan_max_entries: default_cache_entries(),
        }
    }
}

/// Top-level engine configuration.
///
/// ```ignore
/// use daedalus_engine::EngineConfig;
/// let cfg = EngineConfig::default();
/// assert!(cfg.validate().is_ok());
/// ```
#[derive(Clone, Debug)]
#[cfg_attr(feature = "config-env", derive(Serialize, Deserialize))]
pub struct EngineConfig {
    #[cfg_attr(feature = "config-env", serde(default))]
    pub gpu: GpuBackend,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub planner: PlannerSection,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub runtime: RuntimeSection,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub cache: CacheSection,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            gpu: GpuBackend::Cpu,
            planner: PlannerSection::default(),
            runtime: RuntimeSection::default(),
            cache: CacheSection::default(),
        }
    }
}

impl From<GpuBackend> for EngineConfig {
    fn from(gpu: GpuBackend) -> Self {
        Self {
            gpu,
            ..Self::default()
        }
    }
}

impl EngineConfig {
    pub fn cpu() -> Self {
        GpuBackend::Cpu.into()
    }

    pub fn gpu(gpu: GpuBackend) -> Self {
        gpu.into()
    }

    pub fn with_gpu(mut self, gpu: GpuBackend) -> Self {
        self.gpu = gpu;
        self
    }

    pub fn with_runtime_mode(mut self, mode: RuntimeMode) -> Self {
        self.runtime.mode = mode;
        self
    }

    pub fn with_metrics_level(mut self, metrics_level: MetricsLevel) -> Self {
        self.runtime.metrics_level = metrics_level;
        self
    }

    pub fn with_host_event_recording(mut self, enabled: bool) -> Self {
        self.runtime.host_event_recording = enabled;
        self
    }

    pub fn with_host_event_limit(mut self, limit: Option<usize>) -> Self {
        self.runtime.host_event_limit = limit;
        self
    }

    pub fn with_backpressure(mut self, backpressure: BackpressureStrategy) -> Self {
        self.runtime.backpressure = backpressure;
        self
    }

    pub fn with_default_host_input_policy(mut self, policy: RuntimeEdgePolicy) -> Self {
        self.runtime.default_host_input_policy = policy;
        self
    }

    pub fn with_default_host_output_policy(mut self, policy: RuntimeEdgePolicy) -> Self {
        self.runtime.default_host_output_policy = policy;
        self
    }

    pub fn with_stream_idle_sleep(mut self, idle_sleep: Duration) -> Self {
        self.runtime.stream_idle_sleep = idle_sleep;
        self
    }

    pub fn with_fail_fast(mut self, fail_fast: bool) -> Self {
        self.runtime.fail_fast = fail_fast;
        self
    }

    pub fn with_demand_sinks(mut self, sinks: impl IntoIterator<Item = RuntimeSink>) -> Self {
        self.runtime.demand_sinks = sinks.into_iter().collect();
        self.runtime.demand_driven = !self.runtime.demand_sinks.is_empty();
        self
    }

    pub fn with_pool_size(mut self, pool_size: usize) -> Self {
        self.runtime.pool_size = Some(pool_size);
        self
    }

    pub fn with_runtime_debug_config(mut self, debug_config: RuntimeDebugConfig) -> Self {
        self.runtime.debug_config = debug_config;
        self.runtime.pool_size = debug_config.pool_size;
        self
    }

    pub fn with_cache_limits(
        mut self,
        planner_max_entries: usize,
        runtime_plan_max_entries: usize,
    ) -> Self {
        self.cache.planner_max_entries = planner_max_entries.max(1);
        self.cache.runtime_plan_max_entries = runtime_plan_max_entries.max(1);
        self
    }

    pub fn with_planner_gpu(mut self, enable_gpu: bool) -> Self {
        self.planner.enable_gpu = enable_gpu;
        self
    }

    /// Lightweight validation: ensures non-zero pool size when provided.
    ///
    /// ```ignore
    /// use daedalus_engine::EngineConfig;
    /// let cfg = EngineConfig::default();
    /// assert!(cfg.validate().is_ok());
    /// ```
    pub fn validate(&self) -> Result<(), EngineConfigError> {
        if let Some(sz) = self.runtime.pool_size
            && sz == 0
        {
            return Err(EngineConfigError::PoolSizeZero);
        }
        if self.planner.enable_gpu && matches!(self.gpu, GpuBackend::Cpu) {
            return Err(EngineConfigError::PlannerGpuWithCpuBackend);
        }
        if self.cache.planner_max_entries == 0 {
            return Err(EngineConfigError::PlannerCacheLimitZero);
        }
        if self.cache.runtime_plan_max_entries == 0 {
            return Err(EngineConfigError::RuntimePlanCacheLimitZero);
        }
        Ok(())
    }

    /// Construct config from environment variables. Only compiled when `config-env` is enabled.
    ///
    /// Example (doc-test guarded by the feature flag):
    /// ```ignore
    /// # #[cfg(feature = "config-env")] {
    /// use daedalus_engine::{EngineConfig, GpuBackend};
    /// unsafe { std::env::set_var("DAEDALUS_GPU", "mock"); }
    /// let cfg = EngineConfig::from_env().unwrap();
    /// assert_eq!(cfg.gpu, GpuBackend::Mock);
    /// unsafe { std::env::remove_var("DAEDALUS_GPU"); }
    /// # }
    /// ```
    ///
    /// Environment variables:
    /// - `DAEDALUS_METRICS_LEVEL=off|basic|detailed|profile`
    #[cfg(feature = "config-env")]
    pub fn from_env() -> Result<Self, EngineConfigError> {
        let mut cfg = EngineConfig::default();

        if let Ok(raw) = env::var("DAEDALUS_GPU") {
            cfg.gpu = match raw.to_ascii_lowercase().as_str() {
                "cpu" => GpuBackend::Cpu,
                "mock" | "gpu-mock" => GpuBackend::Mock,
                "gpu" | "device" => GpuBackend::Device,
                other => {
                    return Err(EngineConfigError::UnknownEnvValue {
                        var: "DAEDALUS_GPU",
                        value: other.into(),
                    });
                }
            };
        }

        cfg.planner.enable_gpu = read_bool("DAEDALUS_PLANNER_GPU", cfg.planner.enable_gpu)?;
        cfg.planner.enable_lints = read_bool("DAEDALUS_PLANNER_LINTS", cfg.planner.enable_lints)?;
        if let Ok(raw) = env::var("DAEDALUS_PLANNER_FEATURES") {
            cfg.planner.active_features = raw
                .split(',')
                .filter(|s| !s.trim().is_empty())
                .map(|s| s.trim().to_string())
                .collect();
        }

        if let Ok(raw) = env::var("DAEDALUS_RUNTIME_POLICY") {
            cfg.runtime.default_policy = match raw.to_ascii_lowercase().as_str() {
                "fifo" => RuntimeEdgePolicy::default(),
                "newest" | "newest_wins" | "latest_only" => RuntimeEdgePolicy::latest_only(),
                "broadcast" => RuntimeEdgePolicy::default(),
                other => {
                    if let Some(rest) = other.strip_prefix("bounded:") {
                        let cap: usize =
                            rest.parse()
                                .map_err(|_| EngineConfigError::InvalidEnvValue {
                                    var: "DAEDALUS_RUNTIME_POLICY",
                                    value: raw.clone(),
                                })?;
                        RuntimeEdgePolicy::bounded(cap)
                    } else {
                        return Err(EngineConfigError::UnknownEnvValue {
                            var: "DAEDALUS_RUNTIME_POLICY",
                            value: other.into(),
                        });
                    }
                }
            };
        }

        if let Ok(raw) = env::var("DAEDALUS_RUNTIME_BACKPRESSURE") {
            cfg.runtime.backpressure = match raw.to_ascii_lowercase().as_str() {
                "none" => BackpressureStrategy::None,
                "bounded" | "drop_when_full" | "nonblocking_bounded" => {
                    BackpressureStrategy::BoundedQueues
                }
                "error" | "error_on_overflow" => BackpressureStrategy::ErrorOnOverflow,
                other => {
                    return Err(EngineConfigError::UnknownEnvValue {
                        var: "DAEDALUS_RUNTIME_BACKPRESSURE",
                        value: other.into(),
                    });
                }
            };
        }

        if let Ok(raw) = env::var("DAEDALUS_RUNTIME_MODE") {
            cfg.runtime.mode = match raw.to_ascii_lowercase().as_str() {
                "serial" => RuntimeMode::Serial,
                "parallel" => RuntimeMode::Parallel,
                "adaptive" | "auto" | "hybrid" => RuntimeMode::Adaptive,
                other => {
                    return Err(EngineConfigError::UnknownEnvValue {
                        var: "DAEDALUS_RUNTIME_MODE",
                        value: other.into(),
                    });
                }
            };
        }
        cfg.runtime.fail_fast = read_bool("DAEDALUS_RUNTIME_FAIL_FAST", cfg.runtime.fail_fast)?;
        cfg.runtime.demand_driven =
            read_bool("DAEDALUS_RUNTIME_DEMAND_DRIVEN", cfg.runtime.demand_driven)?;
        if let Ok(raw) = env::var("DAEDALUS_METRICS_LEVEL") {
            cfg.runtime.metrics_level = match raw.to_ascii_lowercase().as_str() {
                "off" => MetricsLevel::Off,
                "basic" => MetricsLevel::Basic,
                "detailed" => MetricsLevel::Detailed,
                "profile" => MetricsLevel::Profile,
                other => {
                    return Err(EngineConfigError::UnknownEnvValue {
                        var: "DAEDALUS_METRICS_LEVEL",
                        value: other.into(),
                    });
                }
            };
        }
        cfg.runtime.host_event_recording = read_bool(
            "DAEDALUS_HOST_EVENT_RECORDING",
            cfg.runtime.host_event_recording,
        )?;
        if let Ok(raw) = env::var("DAEDALUS_HOST_EVENT_LIMIT") {
            cfg.runtime.host_event_limit = match raw.to_ascii_lowercase().as_str() {
                "none" | "unbounded" => None,
                other => Some(
                    other
                        .parse()
                        .map_err(|_| EngineConfigError::InvalidEnvValue {
                            var: "DAEDALUS_HOST_EVENT_LIMIT",
                            value: raw.clone(),
                        })?,
                ),
            };
        }
        if let Ok(raw) = env::var(ENV_RUNTIME_POOL_SIZE) {
            cfg.runtime.pool_size =
                Some(
                    raw.parse()
                        .map_err(|_| EngineConfigError::InvalidEnvValue {
                            var: ENV_RUNTIME_POOL_SIZE,
                            value: raw.clone(),
                        })?,
                );
        }
        cfg.runtime.debug_config = RuntimeDebugConfig::from_env();
        cfg.validate()?;
        Ok(cfg)
    }
}

#[cfg(feature = "config-env")]
fn read_bool(var: &'static str, default: bool) -> Result<bool, EngineConfigError> {
    match env::var(var) {
        Ok(val) => {
            let v = val.to_ascii_lowercase();
            match v.as_str() {
                "1" | "true" | "yes" | "on" => Ok(true),
                "0" | "false" | "no" | "off" => Ok(false),
                _ => Err(EngineConfigError::InvalidEnvValue { var, value: val }),
            }
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(e) => Err(EngineConfigError::EnvRead {
            var,
            error: e.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_stable() {
        let cfg = EngineConfig::default();
        assert_eq!(cfg.gpu, GpuBackend::Cpu);
        assert_eq!(cfg.runtime.mode, RuntimeMode::Serial);
    }
}
