use std::time::Duration;

use super::ExecutionTelemetry;

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    serde::Serialize,
    serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum MetricsLevel {
    Off,
    #[default]
    Basic,
    Timing,
    Detailed,
    Hardware,
    Profile,
    Trace,
}

impl MetricsLevel {
    pub fn is_basic(self) -> bool {
        self >= MetricsLevel::Basic
    }

    pub fn is_detailed(self) -> bool {
        self >= MetricsLevel::Detailed
    }

    pub fn is_hardware(self) -> bool {
        self >= MetricsLevel::Hardware
    }

    pub fn is_profile(self) -> bool {
        self >= MetricsLevel::Profile
    }

    pub fn is_trace(self) -> bool {
        self >= MetricsLevel::Trace
    }
}

pub type ProfileLevel = MetricsLevel;

#[derive(Clone, Debug, Default)]
pub struct Profiler {
    telemetry: ExecutionTelemetry,
}

impl Profiler {
    pub fn new(level: ProfileLevel) -> Self {
        Self {
            telemetry: ExecutionTelemetry::with_level(level),
        }
    }

    pub fn telemetry(&self) -> &ExecutionTelemetry {
        &self.telemetry
    }

    pub fn telemetry_mut(&mut self) -> &mut ExecutionTelemetry {
        &mut self.telemetry
    }

    pub fn snapshot(&self) -> ExecutionTelemetry {
        self.telemetry.clone()
    }

    pub fn export_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self.telemetry)
    }
}

const HIST_BUCKETS: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Histogram {
    pub buckets: [u64; HIST_BUCKETS],
}

impl Default for Histogram {
    fn default() -> Self {
        Self {
            buckets: [0; HIST_BUCKETS],
        }
    }
}

impl Histogram {
    pub fn record_value(&mut self, value: u64) {
        let v = value.max(1);
        let idx = (63 - v.leading_zeros() as usize).min(HIST_BUCKETS - 1);
        self.buckets[idx] = self.buckets[idx].saturating_add(1);
    }

    pub fn record_duration(&mut self, duration: Duration) {
        let micros = duration.as_micros() as u64;
        self.record_value(micros);
    }

    pub fn merge(&mut self, other: &Histogram) {
        for (dst, src) in self.buckets.iter_mut().zip(other.buckets.iter()) {
            *dst = dst.saturating_add(*src);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.buckets.iter().all(|v| *v == 0)
    }
}
