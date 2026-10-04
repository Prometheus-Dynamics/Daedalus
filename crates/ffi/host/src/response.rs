use std::collections::BTreeMap;

use daedalus_data::model::Value;
use daedalus_ffi_core::{
    InvokeEvent, InvokeEventLevel, InvokeResponse, WirePort, WireValue, WireValueConversionError,
    WorkerProtocolError,
};
use daedalus_registry::typeexpr_transport_key;
use daedalus_transport::Payload;
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
    #[error("output `{port}` does not fit its port type: {message}")]
    OutputType { port: String, message: String },
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

    /// Output `port` as a payload under the port's transport key, after checking its numbers fit
    /// the port's exact scalar widths (workers send every integer as `i64`, every float as `f64`).
    pub fn payload_output(&self, port: &WirePort) -> Result<Payload, ResponseDecodeError> {
        let output = self.wire_output(&port.name)?;
        output
            .check_type(&port.ty)
            .map_err(|message| ResponseDecodeError::OutputType {
                port: port.name.clone(),
                message,
            })?;
        let type_key = port
            .type_key
            .clone()
            .unwrap_or_else(|| typeexpr_transport_key(&port.ty));
        output.clone().into_payload(type_key).map_err(|source| {
            ResponseDecodeError::OutputConversion {
                port: port.name.clone(),
                source,
            }
        })
    }

    pub fn into_inner(self) -> InvokeResponse {
        self.response
    }
}

#[cfg(test)]
mod tests {
    use daedalus_data::model::{TypeExpr, ValueType};
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

        let port = WirePort {
            type_key: Some("demo:bytes".into()),
            ..WirePort::new("bytes", TypeExpr::scalar(ValueType::Bytes))
        };
        let payload = decoded.payload_output(&port).expect("payload");

        assert_eq!(payload.type_key().as_str(), "demo:bytes");
        assert_eq!(payload.bytes_estimate(), Some(3));
    }

    #[test]
    fn checks_outputs_against_exact_scalar_widths() {
        let decode = |value: WireValue, ty: TypeExpr| {
            let response = InvokeResponse {
                outputs: BTreeMap::from([("out".into(), value)]),
                ..response()
            };
            decode_response(response, Some("req-1"))
                .expect("decode")
                .payload_output(&WirePort::new("out", ty))
        };
        let scalar = |ty: ValueType| TypeExpr::scalar(ty);

        let payload = decode(WireValue::Int(42), scalar(ValueType::I32)).expect("i32 output");
        assert_eq!(
            payload.type_key(),
            &typeexpr_transport_key(&scalar(ValueType::I32))
        );
        assert_eq!(payload.get_ref::<Value>(), Some(&Value::Int(42)));
        decode(WireValue::Float(1.5), scalar(ValueType::F32)).expect("f32 output");
        decode(WireValue::Int(3), scalar(ValueType::F32)).expect("exact integer into f32");
        decode(WireValue::Int(u32::MAX.into()), scalar(ValueType::U32)).expect("u32 max");
        decode(
            WireValue::List(vec![WireValue::Int(-128), WireValue::Int(127)]),
            TypeExpr::list(scalar(ValueType::I8)),
        )
        .expect("i8 list");
        decode(WireValue::Unit, TypeExpr::optional(scalar(ValueType::U8))).expect("none");
        let max = decode(WireValue::UInt(u64::MAX), scalar(ValueType::U64)).expect("u64 max");
        assert_eq!(max.get_ref::<u64>(), Some(&u64::MAX));

        for (value, ty) in [
            (WireValue::Int(1 << 40), scalar(ValueType::I32)),
            (WireValue::Int(-1), scalar(ValueType::U32)),
            (WireValue::UInt(u64::MAX), scalar(ValueType::Int)),
            (
                WireValue::Int(256),
                TypeExpr::optional(scalar(ValueType::U8)),
            ),
            (WireValue::Float(1e300), scalar(ValueType::F32)),
            (WireValue::Float(0.5), scalar(ValueType::I16)),
            (WireValue::String("7".into()), scalar(ValueType::U16)),
            (
                WireValue::List(vec![WireValue::Int(1), WireValue::Int(128)]),
                TypeExpr::list(scalar(ValueType::I8)),
            ),
        ] {
            assert!(
                matches!(
                    decode(value.clone(), ty.clone()),
                    Err(ResponseDecodeError::OutputType { ref port, .. }) if port == "out"
                ),
                "{value:?} should not fit {ty:?}"
            );
        }
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
