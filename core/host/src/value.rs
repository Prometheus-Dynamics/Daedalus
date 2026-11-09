//! The value a host reads and writes when it looks inside a payload. It is plain data, so it can
//! be logged or shown.

use std::string::String;
use std::vec::Vec;

/// A plain value, for inspection and for `push_value`.
#[derive(Clone, Debug, PartialEq)]
pub enum HostValue {
    Unit,
    Bool(bool),
    Int(i64),
    Float(f64),
    Text(String),
    List(Vec<HostValue>),
    /// Named fields, in a fixed order.
    Struct(Vec<(String, HostValue)>),
}
