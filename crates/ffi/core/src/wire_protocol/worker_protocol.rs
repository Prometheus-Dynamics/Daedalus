use std::collections::BTreeMap;

use daedalus_transport::{
    AccessMode, BoundaryCapabilities, BoundaryTypeContract, Layout, Residency, TypeKey,
};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::wire_value::WireValue;
use crate::{BackendKind, NodeSchema, WORKER_PROTOCOL_VERSION, WirePort};

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct InvokeRequest {
    #[serde(default = "default_worker_protocol_version")]
    pub protocol_version: u32,
    pub node_id: String,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub args: BTreeMap<String, WireValue>,
    #[serde(default)]
    pub state: Option<WireValue>,
    #[serde(default)]
    pub context: BTreeMap<String, serde_json::Value>,
}

impl InvokeRequest {
    pub fn validate_protocol(&self) -> Result<(), WorkerProtocolError> {
        validate_worker_protocol_version(self.protocol_version)
    }

    pub fn validate_contract(&self) -> Result<(), InvokeContractError> {
        self.validate_protocol()?;
        if self.node_id.trim().is_empty() {
            return Err(InvokeContractError::EmptyField { field: "node_id" });
        }
        if let Some(correlation_id) = &self.correlation_id
            && correlation_id.trim().is_empty()
        {
            return Err(InvokeContractError::EmptyField {
                field: "correlation_id",
            });
        }
        for (name, value) in &self.args {
            if name.trim().is_empty() {
                return Err(InvokeContractError::EmptyField {
                    field: "request.arg",
                });
            }
            value.validate_contract()?;
        }
        if let Some(state) = &self.state {
            state.validate_contract()?;
        }
        Ok(())
    }

    pub fn validate_against_node(&self, node: &NodeSchema) -> Result<(), InvokeContractError> {
        self.validate_contract()?;
        if self.node_id != node.id {
            return Err(InvokeContractError::NodeMismatch {
                expected: node.id.clone(),
                found: self.node_id.clone(),
            });
        }
        validate_named_wire_values(
            "argument",
            &node.inputs,
            &self.args,
            InvokeContractError::MissingArgument,
            InvokeContractError::UnexpectedArgument,
        )
    }

    pub fn validate_against_node_with_boundaries(
        &self,
        node: &NodeSchema,
        boundary_contracts: &[BoundaryTypeContract],
    ) -> Result<(), InvokeContractError> {
        self.validate_against_node(node)?;
        validate_wire_values_against_ports(&node.inputs, &self.args, boundary_contracts)
    }
}

fn default_worker_protocol_version() -> u32 {
    WORKER_PROTOCOL_VERSION
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct InvokeResponse {
    #[serde(default = "default_worker_protocol_version")]
    pub protocol_version: u32,
    #[serde(default)]
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub outputs: BTreeMap<String, WireValue>,
    #[serde(default)]
    pub state: Option<WireValue>,
    #[serde(default)]
    pub events: Vec<InvokeEvent>,
}

impl InvokeResponse {
    pub fn validate_protocol(&self) -> Result<(), WorkerProtocolError> {
        validate_worker_protocol_version(self.protocol_version)
    }

    pub fn validate_contract(&self) -> Result<(), InvokeContractError> {
        self.validate_protocol()?;
        if let Some(correlation_id) = &self.correlation_id
            && correlation_id.trim().is_empty()
        {
            return Err(InvokeContractError::EmptyField {
                field: "correlation_id",
            });
        }
        for (name, value) in &self.outputs {
            if name.trim().is_empty() {
                return Err(InvokeContractError::EmptyField {
                    field: "response.output",
                });
            }
            value.validate_contract()?;
        }
        if let Some(state) = &self.state {
            state.validate_contract()?;
        }
        for event in &self.events {
            event.validate_contract()?;
        }
        Ok(())
    }

    pub fn validate_against_node(&self, node: &NodeSchema) -> Result<(), InvokeContractError> {
        self.validate_contract()?;
        validate_named_wire_values(
            "output",
            &node.outputs,
            &self.outputs,
            InvokeContractError::MissingOutput,
            InvokeContractError::UnexpectedOutput,
        )
    }

    pub fn validate_against_node_with_boundaries(
        &self,
        node: &NodeSchema,
        boundary_contracts: &[BoundaryTypeContract],
    ) -> Result<(), InvokeContractError> {
        self.validate_against_node(node)?;
        validate_wire_values_against_ports(&node.outputs, &self.outputs, boundary_contracts)
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerHello {
    #[serde(default = "default_worker_protocol_version")]
    pub protocol_version: u32,
    #[serde(default)]
    pub min_protocol_version: u32,
    #[serde(default)]
    pub worker_id: Option<String>,
    #[serde(default)]
    pub backend: Option<BackendKind>,
    #[serde(default)]
    pub supported_nodes: Vec<String>,
    #[serde(default)]
    pub capabilities: Vec<String>,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl WorkerHello {
    pub fn validate_protocol(&self) -> Result<(), WorkerProtocolError> {
        validate_worker_protocol_range(self.min_protocol_version, self.protocol_version)
    }

    pub fn negotiated_protocol_version(&self) -> Result<u32, WorkerProtocolError> {
        self.validate_protocol()?;
        Ok(WORKER_PROTOCOL_VERSION.min(self.protocol_version))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerProtocolAck {
    pub protocol_version: u32,
    #[serde(default)]
    pub worker_id: Option<String>,
}

impl WorkerProtocolAck {
    pub fn from_hello(hello: &WorkerHello) -> Result<Self, WorkerProtocolError> {
        Ok(Self {
            protocol_version: hello.negotiated_protocol_version()?,
            worker_id: hello.worker_id.clone(),
        })
    }

    pub fn validate_protocol(&self) -> Result<(), WorkerProtocolError> {
        validate_worker_protocol_version(self.protocol_version)
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct WorkerMessage {
    #[serde(default = "default_worker_protocol_version")]
    pub protocol_version: u32,
    #[serde(default)]
    pub correlation_id: Option<String>,
    pub payload: WorkerMessagePayload,
}

impl WorkerMessage {
    pub fn new(payload: WorkerMessagePayload, correlation_id: Option<String>) -> Self {
        Self {
            protocol_version: WORKER_PROTOCOL_VERSION,
            correlation_id,
            payload,
        }
    }

    pub fn validate_protocol(&self) -> Result<(), WorkerProtocolError> {
        validate_worker_protocol_version(self.protocol_version)?;
        match &self.payload {
            WorkerMessagePayload::Hello(hello) => hello.validate_protocol(),
            WorkerMessagePayload::Ack(ack) => ack.validate_protocol(),
            WorkerMessagePayload::Invoke(request) => request.validate_protocol(),
            WorkerMessagePayload::Response(response) => response.validate_protocol(),
            WorkerMessagePayload::Event(_) | WorkerMessagePayload::Error(_) => Ok(()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum WorkerMessagePayload {
    Hello(WorkerHello),
    Ack(WorkerProtocolAck),
    Invoke(InvokeRequest),
    Response(InvokeResponse),
    Event(InvokeEvent),
    Error(WorkerError),
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct WorkerError {
    pub code: String,
    pub message: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum WorkerProtocolError {
    #[error("worker protocol version {found} is unsupported; supported range is {min}..={max}")]
    UnsupportedVersion { found: u32, min: u32, max: u32 },
    #[error("worker protocol range is invalid: min {min} is greater than max {max}")]
    InvalidRange { min: u32, max: u32 },
}

fn validate_worker_protocol_version(version: u32) -> Result<(), WorkerProtocolError> {
    if version == WORKER_PROTOCOL_VERSION {
        Ok(())
    } else {
        Err(WorkerProtocolError::UnsupportedVersion {
            found: version,
            min: WORKER_PROTOCOL_VERSION,
            max: WORKER_PROTOCOL_VERSION,
        })
    }
}

fn validate_worker_protocol_range(min: u32, max: u32) -> Result<(), WorkerProtocolError> {
    if min > max {
        return Err(WorkerProtocolError::InvalidRange { min, max });
    }
    if min <= WORKER_PROTOCOL_VERSION && WORKER_PROTOCOL_VERSION <= max {
        Ok(())
    } else {
        Err(WorkerProtocolError::UnsupportedVersion {
            found: max,
            min: WORKER_PROTOCOL_VERSION,
            max: WORKER_PROTOCOL_VERSION,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct InvokeEvent {
    pub level: InvokeEventLevel,
    pub message: String,
    #[serde(default)]
    pub metadata: BTreeMap<String, serde_json::Value>,
}

impl InvokeEvent {
    pub fn validate_contract(&self) -> Result<(), InvokeContractError> {
        if self.message.trim().is_empty() {
            return Err(InvokeContractError::EmptyField {
                field: "event.message",
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum InvokeContractError {
    #[error("transport protocol invalid: {0}")]
    Protocol(#[from] WorkerProtocolError),
    #[error("{field} must not be empty")]
    EmptyField { field: &'static str },
    #[error("request node mismatch: expected {expected}, found {found}")]
    NodeMismatch { expected: String, found: String },
    #[error("missing argument `{0}`")]
    MissingArgument(String),
    #[error("unexpected argument `{0}`")]
    UnexpectedArgument(String),
    #[error("missing output `{0}`")]
    MissingOutput(String),
    #[error("unexpected output `{0}`")]
    UnexpectedOutput(String),
    #[error("invalid image payload field `{field}`")]
    InvalidImage { field: &'static str },
    #[error(
        "payload handle `{handle_id}` type mismatch on `{port}`: expected {expected}, found {found}"
    )]
    PayloadTypeMismatch {
        port: String,
        handle_id: String,
        expected: TypeKey,
        found: TypeKey,
    },
    #[error(
        "payload handle `{handle_id}` access mismatch on `{port}`: expected {expected}, found {found}"
    )]
    PayloadAccessMismatch {
        port: String,
        handle_id: String,
        expected: AccessMode,
        found: AccessMode,
    },
    #[error(
        "payload handle `{handle_id}` residency mismatch on `{port}`: expected {expected:?}, found {found:?}"
    )]
    PayloadResidencyMismatch {
        port: String,
        handle_id: String,
        expected: Residency,
        found: Option<Residency>,
    },
    #[error(
        "payload handle `{handle_id}` layout mismatch on `{port}`: expected {expected:?}, found {found:?}"
    )]
    PayloadLayoutMismatch {
        port: String,
        handle_id: String,
        expected: Layout,
        found: Option<Layout>,
    },
    #[error("missing boundary contract for payload type `{type_key}`")]
    MissingBoundaryContract { type_key: TypeKey },
    #[error("payload handle `{handle_id}` boundary capability mismatch for `{type_key}`")]
    PayloadBoundaryCapabilities {
        handle_id: String,
        type_key: TypeKey,
    },
}

fn validate_named_wire_values(
    direction: &'static str,
    ports: &[WirePort],
    values: &BTreeMap<String, WireValue>,
    missing: fn(String) -> InvokeContractError,
    unexpected: fn(String) -> InvokeContractError,
) -> Result<(), InvokeContractError> {
    let expected = ports
        .iter()
        .map(|port| (port.name.as_str(), port.optional))
        .collect::<BTreeMap<_, _>>();
    for (name, optional) in &expected {
        if !optional && !values.contains_key(*name) {
            return Err(missing((*name).to_string()));
        }
    }
    for name in values.keys() {
        if !expected.contains_key(name.as_str()) {
            return Err(unexpected(name.clone()));
        }
        if name.trim().is_empty() {
            return Err(InvokeContractError::EmptyField { field: direction });
        }
    }
    Ok(())
}

fn validate_wire_values_against_ports(
    ports: &[WirePort],
    values: &BTreeMap<String, WireValue>,
    boundary_contracts: &[BoundaryTypeContract],
) -> Result<(), InvokeContractError> {
    let contracts = boundary_contracts
        .iter()
        .map(|contract| (&contract.type_key, contract))
        .collect::<BTreeMap<_, _>>();
    for port in ports {
        let Some(value) = values.get(&port.name) else {
            continue;
        };
        validate_wire_value_against_port(port, value, &contracts)?;
    }
    Ok(())
}

fn validate_wire_value_against_port(
    port: &WirePort,
    value: &WireValue,
    boundary_contracts: &BTreeMap<&TypeKey, &BoundaryTypeContract>,
) -> Result<(), InvokeContractError> {
    let WireValue::Handle(handle) = value else {
        return Ok(());
    };
    if let Some(expected) = &port.type_key
        && &handle.type_key != expected
    {
        return Err(InvokeContractError::PayloadTypeMismatch {
            port: port.name.clone(),
            handle_id: handle.id.clone(),
            expected: expected.clone(),
            found: handle.type_key.clone(),
        });
    }
    if !handle.access.satisfies(port.access) {
        return Err(InvokeContractError::PayloadAccessMismatch {
            port: port.name.clone(),
            handle_id: handle.id.clone(),
            expected: port.access,
            found: handle.access,
        });
    }
    if let Some(expected) = port.residency
        && handle.residency != Some(expected)
    {
        return Err(InvokeContractError::PayloadResidencyMismatch {
            port: port.name.clone(),
            handle_id: handle.id.clone(),
            expected,
            found: handle.residency,
        });
    }
    if let Some(expected) = &port.layout
        && handle.layout.as_ref() != Some(expected)
    {
        return Err(InvokeContractError::PayloadLayoutMismatch {
            port: port.name.clone(),
            handle_id: handle.id.clone(),
            expected: expected.clone(),
            found: handle.layout.clone(),
        });
    }
    let Some(contract) = boundary_contracts.get(&handle.type_key) else {
        return Err(InvokeContractError::MissingBoundaryContract {
            type_key: handle.type_key.clone(),
        });
    };
    if !contract
        .capabilities
        .satisfies(required_boundary_capabilities(port.access))
    {
        return Err(InvokeContractError::PayloadBoundaryCapabilities {
            handle_id: handle.id.clone(),
            type_key: handle.type_key.clone(),
        });
    }
    Ok(())
}

fn required_boundary_capabilities(access: AccessMode) -> BoundaryCapabilities {
    match access {
        AccessMode::Read | AccessMode::View => BoundaryCapabilities {
            borrow_ref: true,
            backing_read: true,
            metadata_read: true,
            ..BoundaryCapabilities::default()
        },
        AccessMode::Move => BoundaryCapabilities::owned(),
        AccessMode::Modify => BoundaryCapabilities {
            borrow_mut: true,
            backing_read: true,
            backing_write: true,
            metadata_read: true,
            metadata_write: true,
            ..BoundaryCapabilities::default()
        },
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum InvokeEventLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}
