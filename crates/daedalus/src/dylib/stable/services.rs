//! Host services a stable plugin reaches through its instance: custom node metrics (recorded
//! by a plugin node into its own context, forwarded to the host node's telemetry after each
//! call) and the node and graph metadata its context carries. The host delivers them once, right
//! after an instance's create ([`SetServicesFn`]), with the host node's state store as the sink
//! the record callback writes to.

use super::StrSink;
use super::value::StableValue;
use crate::data::model::Value;
use crate::runtime::executor::CustomMetricValue;
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};

/// [`MetricRecord::kind`]: what `scalar` and `text` hold.
pub mod metric {
    /// `scalar`: a `u64`.
    pub const COUNTER: u32 = 0;
    /// `scalar`: an `f64`'s bits.
    pub const GAUGE: u32 = 1;
    /// `scalar`: nanoseconds.
    pub const DURATION: u32 = 2;
    /// `scalar`: a `u64`.
    pub const BYTES: u32 = 3;
    /// `text`: UTF-8.
    pub const TEXT: u32 = 4;
    /// `scalar`: 0 or 1.
    pub const BOOL: u32 = 5;
    /// `text`: a JSON document.
    pub const JSON: u32 = 6;
}

/// One custom metric, with its name and text borrowed for the call.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct MetricRecord {
    pub name_ptr: *const u8,
    pub name_len: usize,
    pub kind: u32,
    pub scalar: u64,
    pub text_ptr: *const u8,
    pub text_len: usize,
}

/// Records one metric into the host node's telemetry; returns whether the host accepted it.
pub type RecordMetricFn =
    unsafe extern "C" fn(sink: *mut c_void, record: *const MetricRecord) -> bool;

/// The services a host delivers to an instance. `sink` belongs to the host and stays valid
/// while the instance exists; `metadata` and `graph_metadata` are `Value::Map`s, borrowed for the
/// call (the plugin copies them).
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct HostServices {
    pub sink: *mut c_void,
    pub record_metric: RecordMetricFn,
    pub metadata: StableValue,
    pub graph_metadata: StableValue,
}

/// Delivers `services` to `instance`, once, right after its create; returns a status code,
/// writing a message to `error` unless it is OK (see [`super::status`]).
pub type SetServicesFn =
    unsafe extern "C" fn(instance: u64, services: *const HostServices, error: StrSink) -> u32;

/// The host's side: the metrics the plugin recorded during its calls, which the host's node run
/// records into the node's telemetry once the call returns. It holds no state store: the instance
/// lives in the node's state, so a handle to the store would be a reference cycle.
#[derive(Default)]
pub struct HostSink {
    pub pending: Vec<(String, CustomMetricValue)>,
}

/// The record callback of a [`HostSink`] (the `sink` of [`HostServices`]).
///
/// # Safety
/// `sink` must be a live [`HostSink`] and `record` a valid [`MetricRecord`], for the call.
pub unsafe extern "C" fn record_metric(sink: *mut c_void, record: *const MetricRecord) -> bool {
    catch_unwind(AssertUnwindSafe(|| {
        // Safety: forwarded from the caller.
        let (sink, record) = unsafe { (sink.cast::<HostSink>().as_mut(), record.as_ref()) };
        let (Some(sink), Some(record)) = (sink, record) else {
            return false;
        };
        let name = unsafe { text(record.name_ptr, record.name_len) };
        let value = match record.kind {
            metric::COUNTER => CustomMetricValue::Counter(record.scalar),
            metric::GAUGE => CustomMetricValue::Gauge(f64::from_bits(record.scalar)),
            metric::DURATION => {
                CustomMetricValue::Duration(std::time::Duration::from_nanos(record.scalar))
            }
            metric::BYTES => CustomMetricValue::Bytes(record.scalar),
            metric::TEXT => {
                CustomMetricValue::Text(unsafe { text(record.text_ptr, record.text_len) })
            }
            metric::BOOL => CustomMetricValue::Bool(record.scalar != 0),
            metric::JSON => {
                match serde_json::from_str(&unsafe { text(record.text_ptr, record.text_len) }) {
                    Ok(json) => CustomMetricValue::Json(json),
                    Err(_) => return false,
                }
            }
            _ => return false,
        };
        sink.pending.push((name, value));
        true
    }))
    .unwrap_or(false)
}

/// The text of `(ptr, len)`, lossily decoded.
///
/// # Safety
/// `ptr` must be valid for `len` bytes (or `len` is 0).
unsafe fn text(ptr: *const u8, len: usize) -> String {
    if len == 0 || ptr.is_null() {
        return String::new();
    }
    // Safety: forwarded from the caller.
    String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(ptr, len) }).into_owned()
}

/// `Value::Map` entries of a metadata map, keyed by name.
pub fn metadata_value(map: &BTreeMap<String, Value>) -> Value {
    Value::Map(
        map.iter()
            .map(|(key, value)| (Value::String(Cow::Owned(key.clone())), value.clone()))
            .collect(),
    )
}

/// The metadata map a `Value::Map` holds (keys are strings; other entries are dropped).
pub fn metadata_map(value: Value) -> BTreeMap<String, Value> {
    match value {
        Value::Map(entries) => entries
            .into_iter()
            .filter_map(|(key, value)| match key {
                Value::String(key) => Some((key.into_owned(), value)),
                _ => None,
            })
            .collect(),
        _ => BTreeMap::new(),
    }
}

/// The plugin's side of a delivered sink: the host's record callback and its sink.
#[derive(Copy, Clone)]
pub struct PluginSink {
    pub sink: *mut c_void,
    pub record: RecordMetricFn,
}

/// Forwards one drained metric to the host.
///
/// # Safety
/// `sink` must still be the host's, for the call (the host keeps it while the instance lives).
pub unsafe fn forward(sink: PluginSink, name: &str, value: &CustomMetricValue) {
    let mut record = MetricRecord {
        name_ptr: name.as_ptr(),
        name_len: name.len(),
        kind: metric::TEXT,
        scalar: 0,
        text_ptr: std::ptr::null(),
        text_len: 0,
    };
    let json;
    match value {
        CustomMetricValue::Counter(value) => {
            record.kind = metric::COUNTER;
            record.scalar = *value;
        }
        CustomMetricValue::Gauge(value) => {
            record.kind = metric::GAUGE;
            record.scalar = value.to_bits();
        }
        CustomMetricValue::Duration(value) => {
            record.kind = metric::DURATION;
            record.scalar = u64::try_from(value.as_nanos()).unwrap_or(u64::MAX);
        }
        CustomMetricValue::Bytes(value) => {
            record.kind = metric::BYTES;
            record.scalar = *value;
        }
        CustomMetricValue::Text(value) => {
            record.text_ptr = value.as_ptr();
            record.text_len = value.len();
        }
        CustomMetricValue::Bool(value) => {
            record.kind = metric::BOOL;
            record.scalar = u64::from(*value);
        }
        CustomMetricValue::Json(value) => {
            json = value.to_string();
            record.kind = metric::JSON;
            record.text_ptr = json.as_ptr();
            record.text_len = json.len();
        }
    }
    // Safety: forwarded from the caller; the record borrows `name`, `value` and `json`.
    unsafe { (sink.record)(sink.sink, &record) };
}
