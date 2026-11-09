//! One invoke: the request's inputs become payloads, the node's handler runs through the runtime's
//! handler registry, and its outputs go back as wire values.

use std::any::Any;
use std::collections::BTreeMap;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use daedalus::data::model::Value;
use daedalus::runtime::executor::CorrelatedPayload;
use daedalus::runtime::executor::NodeHandler;
use daedalus::runtime::host_bridge::{PayloadInspection, inspect_payload};
use daedalus::runtime::{ExecutionContext, NodeIo, PortId, RuntimeNode};
use daedalus::transport::Payload;
use daedalus_ffi_core::{
    ByteEncoding, BytePayload, FfiContractError, ImagePayload, InvokeContractError, InvokeRequest,
    InvokeResponse, WORKER_PROTOCOL_VERSION, WirePayloadHandle, WirePort, WireValue,
};

use crate::events;
use crate::worker::Worker;

/// A call that failed: the `WorkerError` code the host sees, and its message.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct CallFailure {
    pub(crate) code: &'static str,
    pub(crate) message: String,
}

impl CallFailure {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl From<InvokeContractError> for CallFailure {
    fn from(err: InvokeContractError) -> Self {
        Self::new("bad_request", err.to_string())
    }
}

impl From<FfiContractError> for CallFailure {
    fn from(err: FfiContractError) -> Self {
        Self::new("bad_request", err.to_string())
    }
}

impl Worker {
    pub(crate) fn invoke(
        &mut self,
        request: &InvokeRequest,
    ) -> Result<InvokeResponse, CallFailure> {
        let node = self.nodes.get(&request.node_id).cloned().ok_or_else(|| {
            CallFailure::new(
                "unknown_node",
                format!("node `{}` is not served by this worker", request.node_id),
            )
        })?;
        request.validate_against_node(&node)?;

        // Inputs are the node's edge inputs: `from_inputs` is the only way to hand a handler an
        // input (`push_payload` writes an output).
        let mut inputs = Vec::with_capacity(node.inputs.len());
        for port in &node.inputs {
            if let Some(value) = request.args.get(&port.name) {
                let payload = input_payload(value, port)?;
                inputs.push((
                    PortId::new(port.name.clone()),
                    CorrelatedPayload::from_edge(payload),
                ));
            }
        }
        let mut io = NodeIo::from_inputs(inputs)
            .with_const_coercers(Some(self.registry.const_coercers.clone()))
            .with_type_index(Some(self.types.clone()));

        let runtime_node = RuntimeNode::new(node.id.clone());
        let ctx = ExecutionContext::new(
            self.state.clone(),
            Arc::from(node.id.as_str()),
            Arc::default(),
            Arc::default(),
            Arc::clone(&self.capabilities),
        );
        let codec = self.state_codecs.get(&node.id);
        if let (Some(codec), Some(state)) = (codec, &request.state) {
            let value = state
                .clone()
                .into_value()
                .map_err(|err| CallFailure::new("bad_request", format!("state: {err}")))?;
            if !(codec.import)(&ctx, &value) {
                return Err(CallFailure::new(
                    "bad_request",
                    format!("state of node `{}` does not decode", node.id),
                ));
            }
        }
        events::begin_call();
        let handlers = &self.registry.handlers;
        match catch_unwind(AssertUnwindSafe(|| {
            handlers.run(&runtime_node, &ctx, &mut io)
        })) {
            Ok(Ok(())) => {}
            Ok(Err(err)) => return Err(CallFailure::new("node_error", err.to_string())),
            Err(panic) => {
                return Err(CallFailure::new(
                    "node_panic",
                    panic_message(panic.as_ref()),
                ));
            }
        }

        let mut outputs = BTreeMap::new();
        for (port, correlated) in io.take_outputs() {
            let value = self
                .output_wire(&correlated.inner)
                .map_err(|message| CallFailure::new("bad_output", message))?;
            outputs.insert(port.to_string(), value);
        }
        let state = match codec.and_then(|codec| (codec.export)(&ctx)) {
            Some(value) => Some(
                WireValue::from_value(value)
                    .map_err(|err| CallFailure::new("bad_output", format!("state: {err}")))?,
            ),
            None => None,
        };
        let response = InvokeResponse {
            protocol_version: WORKER_PROTOCOL_VERSION,
            correlation_id: request.correlation_id.clone(),
            outputs,
            state,
            events: events::take_call_events(),
        };
        response
            .validate_against_node(&node)
            .map_err(|err| CallFailure::new("bad_output", err.to_string()))?;
        Ok(response)
    }

    /// An output as a wire value: a value or byte payload directly, otherwise through the
    /// registered value serializers (a scalar `i64` is `Value::Int`).
    fn output_wire(&self, payload: &Payload) -> Result<WireValue, String> {
        if let Ok(value) = WireValue::from_payload(payload) {
            return Ok(value);
        }
        if let Some(image) = payload.get_ref::<ImagePayload>() {
            return Ok(WireValue::Image(image.clone()));
        }
        if let Some(handle) = payload.get_ref::<WirePayloadHandle>() {
            return Ok(WireValue::Handle(handle.clone()));
        }
        if let Some(data) = payload.get_ref::<Vec<u8>>() {
            return Ok(WireValue::Bytes(BytePayload {
                data: data.clone(),
                encoding: ByteEncoding::Base64,
            }));
        }
        match inspect_payload(payload, &self.registry.value_serializers) {
            PayloadInspection::Value { value, .. } => {
                WireValue::from_value(value).map_err(|err| err.to_string())
            }
            PayloadInspection::Opaque(summary) => Err(format!(
                "output payload `{}` has no wire encoding (register a value serializer)",
                summary.type_key
            )),
        }
    }
}

/// An input as a payload under the port's type key. The handler's typed argument is coerced from
/// the value, the same path graph constants take.
fn input_payload(value: &WireValue, port: &WirePort) -> Result<Payload, CallFailure> {
    let type_key = port
        .type_key
        .clone()
        .unwrap_or_else(|| daedalus::registry::typeexpr_transport_key(&port.ty));
    // Bytes become a `Value::Bytes` so a `Vec<u8>` parameter coerces from them like any value; a
    // raw byte payload is only read by `&[u8]`-style parameters, which the macros do not take.
    let payload = match value {
        // Images and handles travel as their own wire structs: the worker reads their metadata and
        // passes them on, it does not resolve a handle's payload itself.
        WireValue::Image(image) => Payload::owned(type_key, image.clone()),
        WireValue::Handle(handle) => Payload::owned(type_key, handle.clone()),
        WireValue::Bytes(bytes) => Payload::owned(
            type_key,
            Value::Bytes(std::borrow::Cow::Owned(bytes.data.clone())),
        ),
        other => other.clone().into_payload(type_key).map_err(|err| {
            CallFailure::new("bad_request", format!("argument `{}`: {err}", port.name))
        })?,
    };
    Ok(payload)
}

fn panic_message(panic: &(dyn Any + Send)) -> String {
    let detail = panic
        .downcast_ref::<&str>()
        .map(|text| (*text).to_owned())
        .or_else(|| panic.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "node panicked".into());
    format!("node panicked: {detail}")
}
