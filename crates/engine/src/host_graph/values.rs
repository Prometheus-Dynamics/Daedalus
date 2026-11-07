//! Pushing a `Value` into a host input for [`HostGraph`] (see
//! [`daedalus_runtime::host_bridge::HostBridgeHandle::push_value`]).

use daedalus_data::model::Value;
use daedalus_runtime::handles::PortId;
use daedalus_transport::FeedOutcome;

use super::HostGraph;
use daedalus_runtime::executor::NodeHandler;

impl<H: NodeHandler + Send + Sync + 'static> HostGraph<H> {
    /// Feed a `Value` into a host input, converted to the type the graph declares for the port
    /// through the registry's payload codec or const coercer, so a host needs no Rust type for
    /// it. See [`daedalus_runtime::host_bridge::HostBridgeHandle::push_value`].
    pub fn push_value(&self, port: impl Into<PortId>, value: Value) -> FeedOutcome {
        self.host.push_value(port, value)
    }
}
