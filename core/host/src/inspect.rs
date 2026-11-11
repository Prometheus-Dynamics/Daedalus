//! Looking inside a payload: through the serializer map when its type has an entry, and as a
//! summary when it doesn't, which is not an error. `push_value` goes the other way.

use std::string::String;

use daedalus_value::{Owner, Value};

use crate::io::{FeedOutcome, Host, HostError};
use crate::label::Label;
use crate::serializers::ValueSerializers;
use crate::value::HostValue;

/// What is known about a payload whether or not its value can be read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PayloadSummary {
    pub type_key: String,
    pub owner: &'static str,
    pub space: u32,
    pub bytes_estimate: Option<u64>,
}

impl PayloadSummary {
    /// The summary as a plain value, with its fields in a fixed order.
    pub fn to_value(&self) -> HostValue {
        let estimate = match self.bytes_estimate {
            Some(bytes) => HostValue::Int(bytes as i64),
            None => HostValue::Unit,
        };
        HostValue::Struct(std::vec![
            (
                String::from("type_key"),
                HostValue::Text(self.type_key.clone())
            ),
            (
                String::from("owner"),
                HostValue::Text(String::from(self.owner))
            ),
            (String::from("space"), HostValue::Int(i64::from(self.space))),
            (String::from("bytes_estimate"), estimate),
        ])
    }
}

/// A payload as a plain value, when a serializer exists.
#[derive(Clone, Debug, PartialEq)]
pub struct PayloadInspection {
    pub summary: PayloadSummary,
    pub value: Option<HostValue>,
}

impl PayloadInspection {
    /// The value, or the summary for a payload without one.
    pub fn to_display_value(&self) -> HostValue {
        self.value
            .clone()
            .unwrap_or_else(|| self.summary.to_value())
    }
}

fn owner_name(owner: &Owner) -> &'static str {
    match owner {
        Owner::Static(_) => "static",
        Owner::Inline(_) => "inline",
        Owner::Slot(_) => "slot",
        Owner::Host(_) => "host",
        Owner::Shared(_) => "shared",
        Owner::Typed(_) => "typed",
        Owner::Handed(_) => "handed",
    }
}

impl Host<'_> {
    /// Reads `value` through the serializer map. A type with no entry gives a summary with no
    /// value.
    pub fn inspect_payload(
        &self,
        value: &Value,
        serializers: &ValueSerializers,
    ) -> PayloadInspection {
        let key = self
            .types
            .get(value.ty.0 as usize)
            .map(|key| key.as_str())
            .unwrap_or("");
        PayloadInspection {
            summary: PayloadSummary {
                type_key: String::from(key),
                owner: owner_name(&value.owner),
                space: u32::from(value.space.0),
                bytes_estimate: value.bytes().map(|bytes| bytes.len() as u64),
            },
            value: serializers
                .for_key(key)
                .and_then(|entry| (entry.to_value)(value)),
        }
    }

    /// Builds a payload for the input port `name` from `value`, checked against the port's type
    /// key, and feeds it. A value the type's entry refuses is reported with the port and the key.
    pub fn push_value(
        &mut self,
        name: &str,
        value: &HostValue,
        serializers: &ValueSerializers,
    ) -> Result<FeedOutcome, HostError> {
        let spec = self.input(name)?;
        let key = spec.key.as_str();
        let payload = serializers
            .for_key(key)
            .and_then(|entry| entry.from_value.as_ref())
            .and_then(|from_value| from_value(value))
            .ok_or(HostError::Refused {
                port: Label::new(name),
                key: Label::new(key),
            })?;
        self.feed(name, payload)
    }
}
