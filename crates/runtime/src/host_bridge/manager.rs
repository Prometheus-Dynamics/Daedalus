use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex};

use daedalus_planner::is_host_bridge_metadata;
use daedalus_transport::{
    FreshnessPolicy, Payload, PolicyValidationError, PressurePolicy, validate_stream_policy,
};

use crate::executor::{CorrelatedPayload, NodeError};
use crate::handles::HostAlias;
use crate::io::NodeIo;

use super::events::EventLog;
use super::ports::PortDirection;
use super::{
    DEFAULT_HOST_BRIDGE_EVENT_LIMIT, DEFAULT_HOST_BRIDGE_EVENT_RECORDING, HostBridgeBuffers,
    HostBridgeConfig, HostBridgeHandle, HostBridgePayload, HostBridgeShared, lock_host_buffers,
    lock_host_defaults, lock_host_map,
};

#[derive(Clone, Default)]
pub struct HostBridgeManager {
    inner: Arc<Mutex<HashMap<HostAlias, Arc<HostBridgeShared>>>>,
    defaults: Arc<Mutex<HostBridgeDefaults>>,
}

#[derive(Clone)]
pub(super) struct HostBridgeDefaults {
    pub(super) input_pressure: PressurePolicy,
    pub(super) input_freshness: FreshnessPolicy,
    pub(super) output_pressure: PressurePolicy,
    pub(super) output_freshness: FreshnessPolicy,
    pub(super) events_enabled: bool,
    pub(super) event_limit: Option<usize>,
}

impl Default for HostBridgeDefaults {
    fn default() -> Self {
        Self {
            input_pressure: PressurePolicy::default(),
            input_freshness: FreshnessPolicy::default(),
            output_pressure: PressurePolicy::default(),
            output_freshness: FreshnessPolicy::default(),
            events_enabled: DEFAULT_HOST_BRIDGE_EVENT_RECORDING,
            event_limit: Some(DEFAULT_HOST_BRIDGE_EVENT_LIMIT),
        }
    }
}

impl HostBridgeManager {
    pub fn new() -> Self {
        Self::default()
    }

    /// Look up an existing bridge without allocating.
    pub fn handle(&self, alias: impl AsRef<str>) -> Option<HostBridgeHandle> {
        let guard = lock_host_map(&self.inner);
        let (alias, shared) = guard.get_key_value(alias.as_ref())?;
        Some(HostBridgeHandle::new(alias.clone(), shared.clone()))
    }

    /// Get or create a bridge. Existing bridges are looked up without allocating.
    pub fn ensure_handle(&self, alias: impl AsRef<str>) -> HostBridgeHandle {
        let alias = alias.as_ref();
        let mut guard = lock_host_map(&self.inner);
        if let Some((alias, shared)) = guard.get_key_value(alias) {
            return HostBridgeHandle::new(alias.clone(), shared.clone());
        }
        let alias = HostAlias::from(alias);
        let defaults = lock_host_defaults(&self.defaults).clone();
        let buffers = HostBridgeBuffers {
            inbound: PortDirection::with_defaults(
                defaults.input_pressure,
                defaults.input_freshness,
            ),
            outbound: PortDirection::with_defaults(
                defaults.output_pressure,
                defaults.output_freshness,
            ),
            events: EventLog::new(defaults.events_enabled, defaults.event_limit),
            ..HostBridgeBuffers::default()
        };
        let shared = Arc::new(HostBridgeShared {
            buffers: Mutex::new(buffers),
            ready: Condvar::new(),
        });
        guard.insert(alias.clone(), shared.clone());
        HostBridgeHandle::new(alias, shared)
    }

    pub fn set_event_recording(&self, enabled: bool) {
        {
            let mut defaults = lock_host_defaults(&self.defaults);
            defaults.events_enabled = enabled;
        }
        let handles = lock_host_map(&self.inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for shared in handles {
            lock_host_buffers(&shared).events.set_enabled(enabled);
        }
    }

    pub fn set_event_limit(&self, limit: Option<usize>) {
        {
            let mut defaults = lock_host_defaults(&self.defaults);
            defaults.event_limit = limit;
        }
        let handles = lock_host_map(&self.inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for shared in handles {
            lock_host_buffers(&shared).events.set_limit(limit);
        }
    }

    pub fn set_default_input_policy(
        &self,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        validate_stream_policy(&pressure, &freshness)?;
        {
            let mut defaults = lock_host_defaults(&self.defaults);
            defaults.input_pressure = pressure.clone();
            defaults.input_freshness = freshness.clone();
        }
        let handles = lock_host_map(&self.inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for shared in handles {
            lock_host_buffers(&shared)
                .inbound
                .set_defaults(pressure.clone(), freshness.clone());
        }
        Ok(())
    }

    pub fn set_default_output_policy(
        &self,
        pressure: PressurePolicy,
        freshness: FreshnessPolicy,
    ) -> Result<(), PolicyValidationError> {
        validate_stream_policy(&pressure, &freshness)?;
        {
            let mut defaults = lock_host_defaults(&self.defaults);
            defaults.output_pressure = pressure.clone();
            defaults.output_freshness = freshness.clone();
        }
        let handles = lock_host_map(&self.inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for shared in handles {
            lock_host_buffers(&shared)
                .outbound
                .set_defaults(pressure.clone(), freshness.clone());
        }
        Ok(())
    }

    pub fn apply_config(&self, config: &HostBridgeConfig) -> Result<(), PolicyValidationError> {
        validate_stream_policy(
            &config.default_input_policy.pressure,
            &config.default_input_policy.freshness,
        )?;
        validate_stream_policy(
            &config.default_output_policy.pressure,
            &config.default_output_policy.freshness,
        )?;

        {
            let mut defaults = lock_host_defaults(&self.defaults);
            defaults.input_pressure = config.default_input_policy.pressure.clone();
            defaults.input_freshness = config.default_input_policy.freshness.clone();
            defaults.output_pressure = config.default_output_policy.pressure.clone();
            defaults.output_freshness = config.default_output_policy.freshness.clone();
            defaults.events_enabled = config.event_recording;
            defaults.event_limit = config.event_limit;
        }

        let handles = lock_host_map(&self.inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for shared in handles {
            HostBridgeHandle::new(HostAlias::new(""), shared).apply_config(config)?;
        }
        Ok(())
    }

    /// Queue a graph output for the host side of `alias`, creating the bridge if needed.
    pub fn push_outbound(&self, alias: &str, port: &str, payload: Payload) {
        self.ensure_handle(alias).push_outbound_ref(port, payload);
    }

    pub fn take_inbound(&self, alias: &str) -> Vec<HostBridgePayload> {
        let Some(handle) = self.handle(alias) else {
            return Vec::new();
        };
        handle.take_inbound_small().into_vec()
    }

    pub fn populate_from_plan(&self, plan: &crate::RuntimePlan) {
        for node in &plan.nodes {
            if !is_host_bridge_metadata(&node.metadata) {
                continue;
            }
            let alias = node.label.as_deref().unwrap_or(&node.id);
            self.ensure_handle(alias);
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
    move |node, _ctx, io| {
        let alias = node.label.as_deref().unwrap_or(&node.id);

        for (port, payload) in io.inputs().iter().cloned() {
            bridges.push_outbound(alias, &port, payload.inner);
        }

        for inbound in bridges.take_inbound(alias) {
            io.push_correlated_payload(inbound.port, CorrelatedPayload::from_edge(inbound.payload));
        }

        Ok(())
    }
}
