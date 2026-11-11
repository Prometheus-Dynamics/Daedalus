//! Typed values. A node's ports are Rust types; on the wire each is a [`WireValue`]. The
//! conversions below cover the scalars, strings, byte fields, options, pairs, lists and maps of
//! the canonical fixtures. A value that does not have the port's shape is a `BadInput` fault.

use std::collections::BTreeMap;

use daedalus_wire::WireValue;

use crate::serve::NodeError;

/// A Rust type a node takes or returns.
pub trait Wire: Sized {
    fn to_wire(&self) -> WireValue;
    fn from_wire(value: &WireValue) -> Option<Self>;
}

/// The inputs of a node, looked up by port name.
pub fn input<T: Wire>(inputs: &[(&'static str, WireValue)], name: &str) -> Result<T, NodeError> {
    // A parameter named `r#in` is the port `in`: the keyword needs its raw prefix in the name.
    let name = name.strip_prefix("r#").unwrap_or(name);
    let (_, value) = inputs
        .iter()
        .find(|(port, _)| *port == name)
        .ok_or_else(|| NodeError::bad_input(format!("no input {name}")))?;
    T::from_wire(value)
        .ok_or_else(|| NodeError::bad_input(format!("input {name} has another shape")))
}

/// The outputs a node returns: one value for a single output, and a pair for two outputs. A pair
/// is not itself a [`Wire`] type, so the two never overlap.
pub trait Pack {
    fn pack(self) -> Vec<WireValue>;
}

impl Pack for () {
    fn pack(self) -> Vec<WireValue> {
        Vec::new()
    }
}

impl<T: Wire> Pack for T {
    fn pack(self) -> Vec<WireValue> {
        vec![self.to_wire()]
    }
}

impl<A: Wire, B: Wire> Pack for (A, B) {
    fn pack(self) -> Vec<WireValue> {
        vec![self.0.to_wire(), self.1.to_wire()]
    }
}

impl Wire for i64 {
    fn to_wire(&self) -> WireValue {
        WireValue::I64(*self)
    }
    fn from_wire(value: &WireValue) -> Option<i64> {
        match value {
            WireValue::I64(v) => Some(*v),
            _ => None,
        }
    }
}

impl Wire for u32 {
    fn to_wire(&self) -> WireValue {
        WireValue::U32(*self)
    }
    fn from_wire(value: &WireValue) -> Option<u32> {
        match value {
            WireValue::U32(v) => Some(*v),
            _ => None,
        }
    }
}

impl Wire for bool {
    fn to_wire(&self) -> WireValue {
        WireValue::Bool(*self)
    }
    fn from_wire(value: &WireValue) -> Option<bool> {
        match value {
            WireValue::Bool(v) => Some(*v),
            _ => None,
        }
    }
}

impl Wire for String {
    fn to_wire(&self) -> WireValue {
        WireValue::String(self.clone())
    }
    fn from_wire(value: &WireValue) -> Option<String> {
        match value {
            WireValue::String(v) => Some(v.clone()),
            _ => None,
        }
    }
}

/// Byte fields: `Vec<u8>` is the wire's `bytes`, not a list of `u8`.
impl Wire for Vec<u8> {
    fn to_wire(&self) -> WireValue {
        WireValue::Bytes(self.clone())
    }
    fn from_wire(value: &WireValue) -> Option<Vec<u8>> {
        match value {
            WireValue::Bytes(v) => Some(v.clone()),
            _ => None,
        }
    }
}

impl Wire for Option<i64> {
    fn to_wire(&self) -> WireValue {
        WireValue::Optional(self.map(|v| Box::new(WireValue::I64(v))))
    }
    fn from_wire(value: &WireValue) -> Option<Option<i64>> {
        match value {
            WireValue::Optional(None) => Some(None),
            WireValue::Optional(Some(inner)) => i64::from_wire(inner).map(Some),
            _ => None,
        }
    }
}

impl Wire for Vec<u32> {
    fn to_wire(&self) -> WireValue {
        WireValue::List(self.iter().map(|v| WireValue::U32(*v)).collect())
    }
    fn from_wire(value: &WireValue) -> Option<Vec<u32>> {
        match value {
            WireValue::List(items) => items.iter().map(u32::from_wire).collect(),
            _ => None,
        }
    }
}

/// A tuple-shaped value: a single output that is a pair. Named, so it is not the two-output pack.
#[derive(Clone, Debug, PartialEq)]
pub struct Pair(pub String, pub bool);

impl Wire for Pair {
    fn to_wire(&self) -> WireValue {
        WireValue::Tuple(vec![self.0.to_wire(), self.1.to_wire()])
    }
    fn from_wire(value: &WireValue) -> Option<Pair> {
        match value {
            WireValue::Tuple(items) if items.len() == 2 => Some(Pair(
                String::from_wire(&items[0])?,
                bool::from_wire(&items[1])?,
            )),
            _ => None,
        }
    }
}

impl Wire for BTreeMap<String, i64> {
    fn to_wire(&self) -> WireValue {
        WireValue::Map(
            self.iter()
                .map(|(key, value)| (key.to_wire(), value.to_wire()))
                .collect(),
        )
    }
    fn from_wire(value: &WireValue) -> Option<BTreeMap<String, i64>> {
        match value {
            WireValue::Map(pairs) => pairs
                .iter()
                .map(|(key, value)| Some((String::from_wire(key)?, i64::from_wire(value)?)))
                .collect(),
            _ => None,
        }
    }
}
