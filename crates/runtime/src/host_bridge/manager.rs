use crate::portable::Arc;
use crate::prelude::*;
use crate::sync::Mutex;

use daedalus_core::platform::Clock;
use daedalus_planner::is_host_bridge_metadata;
use daedalus_transport::{
    FreshnessPolicy, Payload, PolicyValidationError, PressurePolicy, validate_stream_policy,
};

use crate::executor::{CorrelatedPayload, NodeError};
use crate::handles::HostAlias;
use crate::io::NodeIo;

use super::{
    Direction, HostBridgeBuffers, HostBridgeConfig, HostBridgeHandle, HostBridgePayload,
    HostBridgeShared,
};
use crate::plan::RuntimeEdgePolicy;
use crate::type_index::TypeIndex;

#[derive(Clone, Default)]
pub struct HostBridgeManager {
    inner: Arc<Mutex<HashMap<HostAlias, Arc<HostBridgeShared>>>>,
    /// Applied to bridges created from now on.
    defaults: Arc<Mutex<BridgeDefaults>>,
}

#[derive(Default)]
struct BridgeDefaults {
    config: HostBridgeConfig,
    types: TypeIndex,
    clock: Clock,
}

impl HostBridgeManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up an existing bridge without allocating.
    pub fn handle(&self, alias: impl AsRef<str>) -> Option<HostBridgeHandle> {
        let guard = self.inner.lock();
        let (alias, shared) = guard.get_key_value(alias.as_ref())?;
        Some(HostBridgeHandle::new(alias.clone(), shared.clone()))
    }

    /// Get or create a bridge. Existing bridges are looked up without allocating.
    pub fn ensure_handle(&self, alias: impl AsRef<str>) -> HostBridgeHandle {
        let alias = alias.as_ref();
        let mut guard = self.inner.lock();
        if let Some((alias, shared)) = guard.get_key_value(alias) {
            return HostBridgeHandle::new(alias.clone(), shared.clone());
        }
        let alias = HostAlias::new(alias);
        let defaults = self.defaults.lock();
        let mut buffers = HostBridgeBuffers::from_config(&defaults.config);
        buffers.types = defaults.types.clone();
        buffers.clock = defaults.clock.clone();
        drop(defaults);
        let shared = Arc::new(HostBridgeShared::new(buffers));
        guard.insert(alias.clone(), shared.clone());
        HostBridgeHandle::new(alias, shared)
    }

    /// Update the defaults for new bridges with `edit`, then run `apply` on every existing
    /// bridge's buffers.
    fn update(
        &self,
        edit: impl FnOnce(&mut BridgeDefaults),
        apply: impl Fn(&mut HostBridgeBuffers),
    ) {
        edit(&mut self.defaults.lock());
        let bridges = self.inner.lock().values().cloned().collect::<Vec<_>>();
        for shared in bridges {
            apply(&mut shared.buffers.lock());
        }
    }

    /// Resolve typed pushes and check fed payloads through `types` (a registry's
    /// `PluginRegistry::type_index`) on every bridge. Engines set it when compiling a graph
    /// from a registry; without one only builtin types resolve.
    pub fn set_type_index(&self, types: TypeIndex) {
        self.update(
            |defaults| defaults.types = types.clone(),
            |buffers| buffers.types = types.clone(),
        );
    }

    /// The clock of every bridge: it stamps the payloads `push*` builds and event timestamps,
    /// and ages payloads for `FreshnessPolicy::MaxAge`. Engines set their own
    /// (`EngineConfig::with_clock`); the default is the platform clock.
    pub fn set_clock(&self, clock: Clock) {
        self.update(
            |defaults| defaults.clock = clock.clone(),
            |buffers| buffers.clock = clock.clone(),
        );
    }

    pub fn set_event_recording(&self, enabled: bool) {
        self.update(
            |defaults| defaults.config.event_recording = enabled,
            |buffers| buffers.events.set_enabled(enabled),
        );
    }

    pub fn set_event_limit(&self, limit: Option<usize>) {
        self.update(
            |defaults| defaults.config.event_limit = limit,
            |buffers| buffers.events.set_limit(limit),
        );
    }

    pub fn set_default_input_policy(
        &self,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        self.set_default_policy(
            Direction::Inbound,
            RuntimeEdgePolicy {
                pressure,
                freshness,
            },
        )
    }

    pub fn set_default_output_policy(
        &self,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        self.set_default_policy(
            Direction::Outbound,
            RuntimeEdgePolicy {
                pressure,
                freshness,
            },
        )
    }

    fn set_default_policy(
        &self,
        direction: Direction,
        policy: RuntimeEdgePolicy,
    ) -> Result<(), PolicyValidationError> {
        validate_stream_policy(&policy.pressure, &policy.freshness)?;
        self.update(
            |defaults| match direction {
                Direction::Inbound => defaults.config.default_input_policy = policy.clone(),
                Direction::Outbound => defaults.config.default_output_policy = policy.clone(),
            },
            |buffers| buffers.ports_mut(direction).set_default_policy(&policy),
        );
        Ok(())
    }

    pub fn apply_config(&self, config: &HostBridgeConfig) -> Result<(), PolicyValidationError> {
        config.validate()?;
        self.update(
            |defaults| defaults.config = config.clone(),
            |buffers| buffers.apply_config(config),
        );
        Ok(())
    }

    /// Queue a graph output for the host side of `alias`, creating the bridge if needed.
    pub fn push_outbound(&self, alias: &str, port: &str, payload: Payload) {
        self.ensure_handle(alias).push_outbound_ref(port, payload);
    }

    /// Move every queued inbound payload of `alias` into `out`; see
    /// [`HostBridgeHandle::take_inbound_into`].
    pub fn take_inbound_into(&self, alias: &str, out: &mut Vec<HostBridgePayload>) {
        if let Some(handle) = self.handle(alias) {
            handle.take_inbound_into(out);
        }
    }

    pub fn populate_from_plan(&self, plan: &crate::RuntimePlan) {
        for node in &plan.nodes {
            if !is_host_bridge_metadata(&node.metadata) {
                continue;
            }
            self.ensure_handle(node.host_alias());
        }
    }
}

pub fn bridge_handler(
    bridges: HostBridgeManager,
) -> impl FnMut(
    &crate::plan::RuntimeNode,
    &crate::state::ExecutionContext,
    &mut NodeIo,
) -> Result<(), NodeError> {
    let mut inbound = Vec::new();
    move |node, _ctx, io| {
        let handle = bridges.ensure_handle(node.host_alias());
        for (port, payload) in io.inputs() {
            handle.push_outbound_ref(port.as_str(), payload.inner.clone());
        }
        handle.take_inbound_into(&mut inbound);
        for entry in inbound.drain(..) {
            io.push_correlated_payload(entry.port, CorrelatedPayload::from_edge(entry.payload));
        }
        Ok(())
    }
}
