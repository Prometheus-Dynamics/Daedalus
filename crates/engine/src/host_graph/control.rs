//! Per-node control of a running [`HostGraph`]: switching a node off and on between ticks.

use super::HostGraph;
use crate::error::EngineError;
use crate::prelude::*;
use daedalus_runtime::executor::NodeHandler;

impl<H: NodeHandler> HostGraph<H> {
    /// Switch `node` (an id or instance key) off (`enabled` false) or on, between ticks. An off node
    /// runs no frame: its consumers see no new value (a Latest edge keeps its last value).
    /// Switching it on again clears its failure backoff and keeps its typed state. Telemetry shows
    /// it as `"disabled"` in `ExecutionTelemetry::node_health`.
    pub fn set_node_enabled(
        &mut self,
        node: impl AsRef<str>,
        enabled: bool,
    ) -> Result<(), EngineError> {
        self.runner
            .executor
            .try_set_node_enabled(node, enabled)
            .map_err(|err| EngineError::Config(err.to_string()))
    }
}
