use std::env;

#[cfg(feature = "config-env")]
use serde::{Deserialize, Serialize};

use daedalus_runtime::{BackpressureStrategy, EdgePolicyKind, MetricsLevel};

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
}

/// Planner knobs.
///
/// ```ignore
/// use daedalus_engine::config::PlannerSection;
/// let planner = PlannerSection::default();
/// assert!(!planner.enable_gpu);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "config-env", derive(Serialize, Deserialize))]
pub struct RuntimeSection {
    #[cfg_attr(feature = "config-env", serde(default))]
    pub default_policy: EdgePolicyKind,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub backpressure: BackpressureStrategy,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub mode: RuntimeMode,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub metrics_level: MetricsLevel,
    /// Prefer lock-free bounded edge queues when available.
    ///
    /// Requires the `lockfree-queues` Cargo feature.
    #[cfg_attr(feature = "config-env", serde(default))]
    pub lockfree_queues: bool,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub pool_size: Option<usize>,
}

impl Default for RuntimeSection {
    fn default() -> Self {
        Self {
            default_policy: EdgePolicyKind::Fifo,
            backpressure: BackpressureStrategy::None,
            mode: RuntimeMode::Serial,
            metrics_level: MetricsLevel::default(),
            lockfree_queues: false,
            pool_size: None,
        }
    }
}

/// Top-level engine configuration.
///
/// ```
/// use daedalus_engine::EngineConfig;
/// let cfg = EngineConfig::default();
/// assert!(cfg.validate().is_ok());
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "config-env", derive(Serialize, Deserialize))]
pub struct EngineConfig {
    #[cfg_attr(feature = "config-env", serde(default))]
    pub gpu: GpuBackend,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub planner: PlannerSection,
    #[cfg_attr(feature = "config-env", serde(default))]
    pub runtime: RuntimeSection,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            gpu: GpuBackend::Cpu,
            planner: PlannerSection::default(),
            runtime: RuntimeSection::default(),
        }
    }
}

impl EngineConfig {
    /// Lightweight validation: ensures non-zero pool size when provided.
    ///
    /// ```
    /// use daedalus_engine::EngineConfig;
    /// let cfg = EngineConfig::default();
    /// assert!(cfg.validate().is_ok());
    /// ```
    pub fn validate(&self) -> Result<(), String> {
        if let Some(sz) = self.runtime.pool_size
            && sz == 0
        {
            return Err("pool_size must be > 0 when provided".into());
        }
        if self.planner.enable_gpu && matches!(self.gpu, GpuBackend::Cpu) {
            return Err("planner GPU is enabled but gpu backend is set to cpu".into());
        }
        Ok(())
    }

    /// Construct config from environment variables. Only compiled when `config-env` is enabled.
    ///
    /// Example (doc-test guarded by the feature flag):
    /// ```
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
    pub fn from_env() -> Result<Self, String> {
        let mut cfg = EngineConfig::default();

        if let Ok(raw) = env::var("DAEDALUS_GPU") {
            cfg.gpu = match raw.to_ascii_lowercase().as_str() {
                "cpu" => GpuBackend::Cpu,
                "mock" | "gpu-mock" => GpuBackend::Mock,
                "gpu" | "device" => GpuBackend::Device,
                other => return Err(format!("unknown DAEDALUS_GPU '{}'", other)),
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
                "fifo" => EdgePolicyKind::Fifo,
                "newest" | "newest_wins" => EdgePolicyKind::NewestWins,
                "broadcast" => EdgePolicyKind::Broadcast,
                other => {
                    if let Some(rest) = other.strip_prefix("bounded:") {
                        let cap: usize = rest.parse().map_err(|_| {
                            format!("invalid bounded cap in DAEDALUS_RUNTIME_POLICY '{}'", raw)
                        })?;
                        EdgePolicyKind::Bounded { cap }
                    } else {
                        return Err(format!("unknown DAEDALUS_RUNTIME_POLICY '{}'", other));
                    }
                }
            };
        }

        if let Ok(raw) = env::var("DAEDALUS_RUNTIME_BACKPRESSURE") {
            cfg.runtime.backpressure = match raw.to_ascii_lowercase().as_str() {
                "none" => BackpressureStrategy::None,
                "bounded" => BackpressureStrategy::BoundedQueues,
                "error" | "error_on_overflow" => BackpressureStrategy::ErrorOnOverflow,
                other => return Err(format!("unknown DAEDALUS_RUNTIME_BACKPRESSURE '{}'", other)),
            };
        }

        if let Ok(raw) = env::var("DAEDALUS_RUNTIME_MODE") {
            cfg.runtime.mode = match raw.to_ascii_lowercase().as_str() {
                "serial" => RuntimeMode::Serial,
                "parallel" => RuntimeMode::Parallel,
                other => return Err(format!("unknown DAEDALUS_RUNTIME_MODE '{}'", other)),
            };
        }
        if let Ok(raw) = env::var("DAEDALUS_METRICS_LEVEL") {
            cfg.runtime.metrics_level = match raw.to_ascii_lowercase().as_str() {
                "off" => MetricsLevel::Off,
                "basic" => MetricsLevel::Basic,
                "detailed" => MetricsLevel::Detailed,
                "profile" => MetricsLevel::Profile,
                other => return Err(format!("unknown DAEDALUS_METRICS_LEVEL '{}'", other)),
            };
        }
        if let Ok(raw) = env::var("DAEDALUS_RUNTIME_POOL_SIZE") {
            cfg.runtime.pool_size = Some(
                raw.parse()
                    .map_err(|_| format!("invalid DAEDALUS_RUNTIME_POOL_SIZE '{}'", raw))?,
            );
        }
        cfg.runtime.lockfree_queues =
            read_bool("DAEDALUS_LOCKFREE_QUEUES", cfg.runtime.lockfree_queues)?;

        cfg.validate()?;
        Ok(cfg)
    }
}

#[cfg(feature = "config-env")]
fn read_bool(var: &str, default: bool) -> Result<bool, String> {
    match env::var(var) {
        Ok(val) => {
            let v = val.to_ascii_lowercase();
            match v.as_str() {
                "1" | "true" | "yes" | "on" => Ok(true),
                "0" | "false" | "no" | "off" => Ok(false),
                _ => Err(format!("invalid boolean '{}': {}", var, val)),
            }
        }
        Err(env::VarError::NotPresent) => Ok(default),
        Err(e) => Err(format!("error reading {}: {}", var, e)),
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
