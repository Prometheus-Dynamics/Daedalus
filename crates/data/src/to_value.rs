use crate::model::{EnumValue, StructFieldValue, Value};
use std::borrow::Cow;
use std::sync::Arc;

/// Convert a Rust value into a Daedalus `Value` for host-visible/JSON-friendly transport.
///
/// This is intentionally explicit: large binary payloads should not be encoded as `Value` unless
/// the graph author inserts a dedicated encoder node.
pub trait ToValue {
    fn to_value(&self) -> Value;
}

impl ToValue for Value {
    fn to_value(&self) -> Value {
        self.clone()
    }
}

impl ToValue for () {
    fn to_value(&self) -> Value {
        Value::Unit
    }
}

impl ToValue for bool {
    fn to_value(&self) -> Value {
        Value::Bool(*self)
    }
}

macro_rules! int_to_value {
    ($($t:ty),*) => {
        $(impl ToValue for $t {
            fn to_value(&self) -> Value {
                Value::Int(*self as i64)
            }
        })*
    };
}

int_to_value!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);

macro_rules! float_to_value {
    ($($t:ty),*) => {
        $(impl ToValue for $t {
            fn to_value(&self) -> Value {
                Value::Float(*self as f64)
            }
        })*
    };
}

float_to_value!(f32, f64);

impl ToValue for String {
    fn to_value(&self) -> Value {
        Value::String(Cow::Owned(self.clone()))
    }
}

impl ToValue for &str {
    fn to_value(&self) -> Value {
        Value::String(Cow::Owned(self.to_string()))
    }
}

impl<T: ToValue> ToValue for Option<T> {
    fn to_value(&self) -> Value {
        match self {
            Some(v) => v.to_value(),
            None => Value::Unit,
        }
    }
}

impl<T: ToValue> ToValue for Vec<T> {
    fn to_value(&self) -> Value {
        Value::List(self.iter().map(|v| v.to_value()).collect())
    }
}

impl<T: ToValue, const N: usize> ToValue for [T; N] {
    fn to_value(&self) -> Value {
        Value::List(self.iter().map(|v| v.to_value()).collect())
    }
}

impl<T: ToValue> ToValue for Arc<T> {
    fn to_value(&self) -> Value {
        (**self).to_value()
    }
}

impl<T: ToValue> ToValue for Box<T> {
    fn to_value(&self) -> Value {
        (**self).to_value()
    }
}

impl ToValue for EnumValue {
    fn to_value(&self) -> Value {
        Value::Enum(self.clone())
    }
}

impl ToValue for StructFieldValue {
    fn to_value(&self) -> Value {
        Value::Struct(vec![self.clone()])
    }
}
