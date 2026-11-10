//! A graph: pushed to, ticked, taken from.

use std::sync::Arc;

use daedalus_capi_sys::{Handle, Port, Pushed, Telemetry, Tick};

use crate::error::HostError;
use crate::host::Held;
use crate::ports::{PortIndex, PortValue, ToPort};

/// A compiled graph. Dropping it releases it, and lets its host go once its last graph has.
pub struct Graph {
    /// Declared before `host`, so the graph is released first.
    handle: Handle,
    host: Arc<Held>,
}

impl std::fmt::Debug for Graph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Graph")
            .field("handle", &self.handle)
            .finish()
    }
}

impl Drop for Graph {
    fn drop(&mut self) {
        let _ = self.host.api.graph_release(self.handle);
    }
}

impl Graph {
    pub(crate) fn new(host: Arc<Held>, handle: Handle) -> Graph {
        Graph { handle, host }
    }

    fn fail(&self, code: i32) -> HostError {
        HostError::from_code(&self.host.api, code)
    }

    /// The graph's host ports, sorted by name. A port's place here is its index.
    pub fn ports(&self) -> Result<Vec<Port>, HostError> {
        let api = &self.host.api;
        let count = api
            .graph_port_count(self.handle)
            .map_err(|code| self.fail(code))?;
        (0..count)
            .map(|index| {
                api.graph_port(self.handle, index)
                    .map_err(|code| self.fail(code))
            })
            .collect()
    }

    /// The index of the host port called `name`, for calls that skip the lookup.
    pub fn port(&self, name: &str) -> Result<PortIndex, HostError> {
        self.host
            .api
            .graph_port_index(self.handle, name)
            .map(PortIndex)
            .map_err(|code| self.fail(code))
    }

    /// How the graph was planned, as JSON.
    pub fn explain(&self) -> Result<String, HostError> {
        self.host
            .api
            .graph_explain(self.handle)
            .map_err(|code| self.fail(code))
    }

    /// What the graph has done since it was compiled.
    pub fn telemetry(&self) -> Result<Telemetry, HostError> {
        self.host
            .api
            .graph_telemetry(self.handle)
            .map_err(|code| self.fail(code))
    }

    /// Runs one tick: every node with new input runs once.
    pub fn tick(&self) -> Result<Tick, HostError> {
        self.host
            .api
            .tick(self.handle)
            .map_err(|code| self.fail(code))
    }

    /// Pushes `value` to the input `port`, given by name or by index.
    pub fn push<V: PortValue>(&self, port: impl ToPort, value: V) -> Result<(), HostError> {
        value
            .push(&self.host.api, self.handle, port.at())
            .map_err(|code| self.fail(code))
    }

    /// Pushes bytes without giving them up: the library copies them.
    pub fn push_bytes(&self, port: impl ToPort, bytes: &[u8]) -> Result<(), HostError> {
        self.host
            .api
            .push_bytes(self.handle, port.at(), bytes)
            .map_err(|code| self.fail(code))
    }

    /// Takes the oldest value of the output `port`. `None` when it has none.
    pub fn take<V: PortValue>(&self, port: impl ToPort) -> Result<Option<V>, HostError> {
        V::take(&self.host.api, self.handle, port.at()).map_err(|code| self.fail(code))
    }

    /// Pushes every value or none: each is checked first. An entry that can't be pushed is
    /// named in the error.
    pub fn push_batch(&self, pushes: &[(PortIndex, Pushed<'_>)]) -> Result<(), HostError> {
        let raw: Vec<(usize, Pushed<'_>)> = pushes
            .iter()
            .map(|(port, value)| (port.0, *value))
            .collect();
        self.host
            .api
            .push_batch(self.handle, &raw)
            .map_err(|(code, entry)| match self.fail(code) {
                HostError::BadBatch { reason, .. } => HostError::BadBatch { entry, reason },
                other => other,
            })
    }
}
