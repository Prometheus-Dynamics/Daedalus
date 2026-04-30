use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;

/// Sink for counters/timers emitted by channel primitives.
pub trait MetricsSink: Send + Sync {
    fn increment(&self, key: &'static str, value: u64);
    fn observe_nanos(&self, key: &'static str, nanos: u64);
}

/// Upcast helper for any metrics sink.
pub fn metrics_sink<T: MetricsSink + 'static>(sink: T) -> Arc<dyn MetricsSink> {
    Arc::new(sink)
}

/// No-op metrics collector (default when the feature is off).
#[derive(Debug, Default)]
pub struct NoopMetrics;

impl MetricsSink for NoopMetrics {
    fn increment(&self, _key: &'static str, _value: u64) {}
    fn observe_nanos(&self, _key: &'static str, _nanos: u64) {}
}

/// Simple in-memory collector useful for tests.
#[derive(Debug, Default)]
pub struct InMemoryMetrics {
    counters: Mutex<HashMap<&'static str, u64>>,
}

impl MetricsSink for InMemoryMetrics {
    fn increment(&self, key: &'static str, value: u64) {
        let mut guard = self.counters.lock().unwrap_or_else(|err| err.into_inner());
        *guard.entry(key).or_insert(0) += value;
    }

    fn observe_nanos(&self, key: &'static str, nanos: u64) {
        let mut guard = self.counters.lock().unwrap_or_else(|err| err.into_inner());
        *guard.entry(key).or_insert(0) += nanos;
    }
}

impl InMemoryMetrics {
    pub fn counter(&self, key: &'static str) -> u64 {
        let guard = self.counters.lock().unwrap_or_else(|err| err.into_inner());
        guard.get(key).copied().unwrap_or(0)
    }

    /// Convenience helper to upcast into a trait object for injection.
    pub fn into_sink(self) -> Arc<dyn MetricsSink> {
        Arc::new(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_counters() {
        let metrics = InMemoryMetrics::default();
        metrics.increment("drops", 1);
        metrics.increment("drops", 2);
        assert_eq!(metrics.counter("drops"), 3);
    }
}
