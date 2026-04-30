use std::collections::BTreeMap;

use daedalus_data::model::Value;
use daedalus_ffi_core::{
    InvokeEvent, InvokeEventLevel, InvokeResponse, WireValue, WireValueConversionError,
    WorkerProtocolError,
};
use daedalus_transport::{Payload, TypeKey};
use thiserror::Error;

#[derive(Clone, Debug, PartialEq)]
pub struct DecodedInvokeResponse {
    response: InvokeResponse,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ResponseDecodeError {
    #[error("response protocol invalid: {0}")]
    Protocol(#[from] WorkerProtocolError),
    #[error("response correlation mismatch: expected {expected:?}, found {found:?}")]
    CorrelationMismatch {
        expected: Option<String>,
        found: Option<String>,
    },
    #[error("response is missing output `{port}`")]
    MissingOutput { port: String },
    #[error("failed to convert output `{port}`: {source}")]
    OutputConversion {
        port: String,
        source: WireValueConversionError,
    },
}

pub fn decode_response(
    response: InvokeResponse,
    expected_correlation_id: Option<&str>,
) -> Result<DecodedInvokeResponse, ResponseDecodeError> {
    response.validate_protocol()?;
    if let Some(expected) = expected_correlation_id
        && response.correlation_id.as_deref() != Some(expected)
    {
        return Err(ResponseDecodeError::CorrelationMismatch {
            expected: Some(expected.to_string()),
            found: response.correlation_id.clone(),
        });
    }
    Ok(DecodedInvokeResponse { response })
}

impl DecodedInvokeResponse {
    pub fn correlation_id(&self) -> Option<&str> {
        self.response.correlation_id.as_deref()
    }

    pub fn outputs(&self) -> &BTreeMap<String, WireValue> {
        &self.response.outputs
    }

    pub fn state(&self) -> Option<&WireValue> {
        self.response.state.as_ref()
    }

    pub fn events(&self) -> &[InvokeEvent] {
        &self.response.events
    }

    pub fn events_at_level(&self, level: InvokeEventLevel) -> Vec<&InvokeEvent> {
        self.response
            .events
            .iter()
            .filter(|event| event.level == level)
            .collect()
    }

    pub fn wire_output(&self, port: &str) -> Result<&WireValue, ResponseDecodeError> {
        self.response
            .outputs
            .get(port)
            .ok_or_else(|| ResponseDecodeError::MissingOutput { port: port.into() })
    }

    pub fn value_output(&self, port: &str) -> Result<Value, ResponseDecodeError> {
        self.wire_output(port)?
            .clone()
            .into_value()
            .map_err(|source| ResponseDecodeError::OutputConversion {
                port: port.into(),
                source,
            })
    }

    pub fn payload_output(
        &self,
        port: &str,
        type_key: impl Into<TypeKey>,
    ) -> Result<Payload, ResponseDecodeError> {
        self.wire_output(port)?
            .clone()
            .into_payload(type_key)
            .map_err(|source| ResponseDecodeError::OutputConversion {
                port: port.into(),
                source,
            })
    }

    pub fn into_inner(self) -> InvokeResponse {
        self.response
    }
}

#[cfg(test)]
mod tests {
    use daedalus_ffi_core::{ByteEncoding, BytePayload, InvokeEventLevel, WORKER_PROTOCOL_VERSION};

    use super::*;

    fn response() -> InvokeResponse {
        InvokeResponse {
            protocol_version: WORKER_PROTOCOL_VERSION,
            correlation_id: Some("req-1".into()),
            outputs: BTreeMap::from([
                ("value".into(), WireValue::Int(42)),
                (
                    "bytes".into(),
                    WireValue::Bytes(BytePayload {
                        data: vec![1, 2, 3],
                        encoding: ByteEncoding::Raw,
                    }),
                ),
            ]),
            state: Some(WireValue::String("state".into())),
            events: vec![
                InvokeEvent {
                    level: InvokeEventLevel::Info,
                    message: "ok".into(),
                    metadata: BTreeMap::new(),
                },
                InvokeEvent {
                    level: InvokeEventLevel::Error,
                    message: "diagnostic".into(),
                    metadata: BTreeMap::new(),
                },
            ],
        }
    }

    #[test]
    fn decodes_response_outputs_state_and_events() {
        let decoded = decode_response(response(), Some("req-1")).expect("decode");

        assert_eq!(decoded.correlation_id(), Some("req-1"));
        assert_eq!(
            decoded.value_output("value").expect("value"),
            Value::Int(42)
        );
        assert!(matches!(
            decoded.wire_output("bytes").expect("bytes"),
            WireValue::Bytes(_)
        ));
        assert_eq!(decoded.state(), Some(&WireValue::String("state".into())));
        assert_eq!(decoded.events_at_level(InvokeEventLevel::Info).len(), 1);
        assert_eq!(decoded.events_at_level(InvokeEventLevel::Error).len(), 1);
    }

    #[test]
    fn decodes_bytes_output_to_payload() {
        let decoded = decode_response(response(), Some("req-1")).expect("decode");

        let payload = decoded
            .payload_output("bytes", "demo:bytes")
            .expect("payload");

        assert_eq!(payload.type_key().as_str(), "demo:bytes");
        assert_eq!(payload.bytes_estimate(), Some(3));
    }

    #[test]
    fn rejects_correlation_mismatch_and_missing_output() {
        assert!(matches!(
            decode_response(response(), Some("req-2")),
            Err(ResponseDecodeError::CorrelationMismatch { .. })
        ));

        let decoded = decode_response(response(), Some("req-1")).expect("decode");
        assert!(matches!(
            decoded.value_output("missing"),
            Err(ResponseDecodeError::MissingOutput { port }) if port == "missing"
        ));
    }
}
