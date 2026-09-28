//! Host port introspection and payload inspection for [`HostGraph`].

use daedalus_runtime::HostPortDescriptor;
use daedalus_runtime::executor::NodeHandler;
use daedalus_runtime::handles::PortId;
use daedalus_runtime::host_bridge::{PayloadInspection, ValueSerializerMap, inspect_payload};
use daedalus_transport::Payload;

use super::HostGraph;

impl<H: NodeHandler + Send + Sync + 'static> HostGraph<H> {
    /// Alias of the host bridge this graph is driven through.
    pub fn host_alias(&self) -> &str {
        self.host.alias()
    }

    /// All host ports of this graph's host bridge (inputs first, then outputs, each by name).
    pub fn host_ports(&self) -> Vec<HostPortDescriptor> {
        self.runtime_plan().host_ports_for(self.host.alias())
    }

    /// Ports the host can push into, with their resolved types and graph connections.
    pub fn host_inputs(&self) -> Vec<HostPortDescriptor> {
        self.runtime_plan().host_inputs(self.host.alias())
    }

    /// Ports the host can drain, with their resolved types and graph connections.
    pub fn host_outputs(&self) -> Vec<HostPortDescriptor> {
        self.runtime_plan().host_outputs(self.host.alias())
    }

    /// Serializer map used by [`HostGraph::inspect_payload`].
    pub fn value_serializers(&self) -> &ValueSerializerMap {
        &self.value_serializers
    }

    /// Replace the serializer map used by [`HostGraph::inspect_payload`].
    pub fn set_value_serializers(&mut self, serializers: ValueSerializerMap) {
        self.value_serializers = serializers;
    }

    /// Convert a payload into a `Value` using the registered value serializers, falling back to a
    /// structured summary (type key, rust type, residency, layout, bytes estimate).
    pub fn inspect_payload(&self, payload: &Payload) -> PayloadInspection {
        inspect_payload(payload, &self.value_serializers)
    }

    /// Take every queued host output and inspect it, in [`HostGraph::host_outputs`] port order
    /// (oldest first within a port). Use it for debugging and examples; typed hosts should drain
    /// ports through `take`/`bind_output` instead.
    pub fn inspect_outputs(&self) -> Vec<(PortId, PayloadInspection)> {
        let mut inspected = Vec::new();
        for port in self.host_outputs() {
            for payload in self.host.drain_payloads(port.name()) {
                inspected.push((port.name.clone(), self.inspect_payload(&payload)));
            }
        }
        inspected
    }
}
