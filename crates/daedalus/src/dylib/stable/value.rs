//! [`StableValue`]: the borrowed, C-safe value tree the stable entry points exchange, and its
//! conversions from and to [`Value`].

use std::borrow::Cow;
use std::ffi::c_void;

use crate::data::model::{EnumValue, StructFieldValue, Value};
use crate::transport::{ForeignHandle, Payload, Residency};

/// Kind of a [`StableValue`] (`StableValue::tag`). Plain `u32` constants rather than a Rust
/// `enum`, so an unknown tag from another build is an error instead of undefined behavior.
pub mod tag {
    pub const UNIT: u32 = 0;
    /// `scalar` is 0 or 1.
    pub const BOOL: u32 = 1;
    /// `scalar` holds an `i64`.
    pub const INT: u32 = 2;
    /// `scalar` holds a `u64` (a `u64`/`usize` port value above `i64::MAX` fits).
    pub const UINT: u32 = 3;
    /// `scalar` holds an `f64`'s bits.
    pub const FLOAT: u32 = 4;
    /// `ptr`/`len`: UTF-8 bytes.
    pub const STRING: u32 = 5;
    /// `ptr`/`len`: bytes.
    pub const BYTES: u32 = 6;
    /// `ptr`/`len`: `len` [`StableValue`](super::StableValue)s.
    pub const LIST: u32 = 7;
    /// As [`LIST`].
    pub const TUPLE: u32 = 8;
    /// `ptr`/`len`: `len` key/value pairs, `2 * len` [`StableValue`](super::StableValue)s.
    pub const MAP: u32 = 9;
    /// `ptr`/`len`: `len` [`StableField`](super::StableField)s, in declaration order.
    pub const STRUCT: u32 = 10;
    /// `ptr`: one [`StableEnum`](super::StableEnum).
    pub const ENUM: u32 = 11;
    /// `ptr`: a borrowed [`ForeignHandle`](crate::transport::ForeignHandle); `scalar`: the
    /// payload's residency (see `residency_code`).
    pub const HANDLE: u32 = 12;
}

/// A value crossing the stable plugin boundary: scalars inline, strings, bytes and nested values
/// borrowed from the side that built it (valid for the duration of the call), foreign handles by
/// pointer. It mirrors ffi-core's `WireValue` model, plus `Tuple`/`Map` so a [`Value`] crosses
/// unchanged; frames and other host-owned values cross as [`ForeignHandle`]s, never copied.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StableValue {
    pub tag: u32,
    pub scalar: u64,
    pub ptr: *const c_void,
    pub len: usize,
}

/// A struct field: borrowed UTF-8 name and value.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StableField {
    pub name_ptr: *const u8,
    pub name_len: usize,
    pub value: StableValue,
}

/// An enum value: borrowed UTF-8 variant name and an optional (null) payload.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StableEnum {
    pub name_ptr: *const u8,
    pub name_len: usize,
    pub value: *const StableValue,
}

impl StableValue {
    pub const UNIT: Self = Self::scalar(tag::UNIT, 0);

    pub const fn scalar(tag: u32, scalar: u64) -> Self {
        Self {
            tag,
            scalar,
            ptr: std::ptr::null(),
            len: 0,
        }
    }

    pub fn bool(value: bool) -> Self {
        Self::scalar(tag::BOOL, u64::from(value))
    }

    pub fn int(value: i64) -> Self {
        Self::scalar(tag::INT, value as u64)
    }

    pub fn uint(value: u64) -> Self {
        Self::scalar(tag::UINT, value)
    }

    pub fn float(value: f64) -> Self {
        Self::scalar(tag::FLOAT, value.to_bits())
    }

    /// Borrow `bytes` (valid while the caller keeps them alive).
    pub fn bytes(bytes: &[u8]) -> Self {
        Self::slice(tag::BYTES, bytes)
    }

    /// Borrow `text` (valid while the caller keeps it alive).
    pub fn string(text: &str) -> Self {
        Self::slice(tag::STRING, text.as_bytes())
    }

    /// Borrow `handle` (valid while the caller keeps it alive).
    pub fn handle(handle: &ForeignHandle, residency: Residency) -> Self {
        Self {
            tag: tag::HANDLE,
            scalar: residency_code(residency),
            ptr: (handle as *const ForeignHandle).cast(),
            len: 0,
        }
    }

    fn slice(tag: u32, bytes: &[u8]) -> Self {
        Self {
            tag,
            scalar: 0,
            ptr: bytes.as_ptr().cast(),
            len: bytes.len(),
        }
    }

    /// The borrowed bytes of a `STRING` or `BYTES` value.
    ///
    /// # Safety
    /// `ptr`/`len` must be valid for the lifetime `'a`.
    pub unsafe fn as_bytes<'a>(&self) -> &'a [u8] {
        // Safety: forwarded from the caller.
        unsafe { raw_slice(self.ptr.cast::<u8>(), self.len) }
    }

    /// The borrowed text of a `STRING` value.
    ///
    /// # Safety
    /// As [`Self::as_bytes`].
    pub unsafe fn as_str<'a>(&self) -> Result<&'a str, String> {
        // Safety: forwarded from the caller.
        std::str::from_utf8(unsafe { self.as_bytes() }).map_err(|err| err.to_string())
    }

    /// The borrowed foreign handle of a `HANDLE` value.
    ///
    /// # Safety
    /// `ptr` must point to a live [`ForeignHandle`] for `'a`.
    pub unsafe fn as_handle<'a>(&self) -> Option<&'a ForeignHandle> {
        // Safety: forwarded from the caller.
        (self.tag == tag::HANDLE)
            .then(|| unsafe { self.ptr.cast::<ForeignHandle>().as_ref() })
            .flatten()
    }

    /// The residency of a `HANDLE` value.
    pub fn residency(&self) -> Residency {
        match self.scalar {
            1 => Residency::Gpu,
            2 => Residency::CpuAndGpu,
            3 => Residency::External,
            _ => Residency::Cpu,
        }
    }
}

fn residency_code(residency: Residency) -> u64 {
    match residency {
        Residency::Cpu => 0,
        Residency::Gpu => 1,
        Residency::CpuAndGpu => 2,
        Residency::External => 3,
    }
}

/// # Safety
/// `ptr` must be valid for `len` elements for `'a` (or `len` must be 0).
unsafe fn raw_slice<'a, T>(ptr: *const T, len: usize) -> &'a [T] {
    if len == 0 || ptr.is_null() {
        return &[];
    }
    // Safety: forwarded from the caller.
    unsafe { std::slice::from_raw_parts(ptr, len) }
}

/// Owns the nested arrays (and temporary values) a [`StableValue`] tree borrows. Moving the
/// arena does not move what it owns, so pointers into it stay valid until it is dropped.
#[derive(Default)]
// Boxed so their addresses survive the vectors growing.
#[allow(clippy::vec_box)]
pub struct Arena {
    values: Vec<Vec<StableValue>>,
    fields: Vec<Vec<StableField>>,
    enums: Vec<Box<StableEnum>>,
    owned: Vec<Box<Value>>,
    handles: Vec<Box<ForeignHandle>>,
}

impl Arena {
    /// Lend `payload`'s foreign value: its carried handle, or (for an owner payload its
    /// provider retyped) a handle wrapping the owner value, kept alive with the arena.
    pub fn encode_foreign(&mut self, payload: &Payload) -> Option<StableValue> {
        let handle = match payload.foreign_handle() {
            Some(handle) => handle,
            None => {
                self.handles.push(Box::new(payload.to_foreign_handle()?));
                let handle: *const ForeignHandle = &**self.handles.last()?;
                // Safety: the box lives (unmoved) as long as the arena.
                unsafe { &*handle }
            }
        };
        Some(StableValue::handle(handle, payload.residency()))
    }

    /// Keep `value` alive with the arena and encode it.
    pub fn encode_owned(&mut self, value: Value) -> StableValue {
        let value = Box::new(value);
        let ptr: *const Value = &*value;
        self.owned.push(value);
        // Safety: the box lives (unmoved) as long as the arena.
        self.encode(unsafe { &*ptr })
    }

    /// Encode `value`, borrowing its strings and bytes; nested arrays live in the arena.
    pub fn encode(&mut self, value: &Value) -> StableValue {
        match value {
            Value::Unit => StableValue::UNIT,
            Value::Bool(value) => StableValue::bool(*value),
            Value::Int(value) => StableValue::int(*value),
            Value::Float(value) => StableValue::float(*value),
            Value::String(text) => StableValue::string(text),
            Value::Bytes(bytes) => StableValue::bytes(bytes),
            Value::List(items) => self.list(tag::LIST, items.iter()),
            Value::Tuple(items) => self.list(tag::TUPLE, items.iter()),
            Value::Map(entries) => {
                let mut value = self.list(tag::MAP, entries.iter().flat_map(|(k, v)| [k, v]));
                value.len = entries.len();
                value
            }
            Value::Struct(fields) => {
                let fields: Vec<StableField> = fields
                    .iter()
                    .map(|field| StableField {
                        name_ptr: field.name.as_ptr(),
                        name_len: field.name.len(),
                        value: self.encode(&field.value),
                    })
                    .collect();
                let value = StableValue {
                    tag: tag::STRUCT,
                    scalar: 0,
                    ptr: fields.as_ptr().cast(),
                    len: fields.len(),
                };
                self.fields.push(fields);
                value
            }
            Value::Enum(variant) => {
                let payload = match &variant.value {
                    Some(inner) => {
                        let inner = self.encode(inner);
                        let cell = vec![inner];
                        let ptr = cell.as_ptr();
                        self.values.push(cell);
                        ptr
                    }
                    None => std::ptr::null(),
                };
                let stable = Box::new(StableEnum {
                    name_ptr: variant.name.as_ptr(),
                    name_len: variant.name.len(),
                    value: payload,
                });
                let ptr: *const StableEnum = &*stable;
                self.enums.push(stable);
                StableValue {
                    tag: tag::ENUM,
                    scalar: 0,
                    ptr: ptr.cast(),
                    len: 0,
                }
            }
        }
    }

    fn list<'v>(&mut self, tag: u32, items: impl Iterator<Item = &'v Value>) -> StableValue {
        let items: Vec<StableValue> = items.map(|item| self.encode(item)).collect();
        let value = StableValue {
            tag,
            scalar: 0,
            ptr: items.as_ptr().cast(),
            len: items.len(),
        };
        self.values.push(items);
        value
    }
}

/// Build a [`Value`] from a value tree (copying what it borrows). `HANDLE` has no `Value`
/// form; a `UINT` above `i64::MAX` does not fit `Value::Int`.
///
/// # Safety
/// Every pointer in the tree must be valid for the call.
pub unsafe fn to_value(value: &StableValue) -> Result<Value, String> {
    // Safety (throughout): forwarded from the caller.
    Ok(match value.tag {
        tag::UNIT => Value::Unit,
        tag::BOOL => Value::Bool(value.scalar != 0),
        tag::INT => Value::Int(value.scalar as i64),
        tag::UINT => Value::Int(
            i64::try_from(value.scalar)
                .map_err(|_| format!("{} does not fit a Daedalus Int", value.scalar))?,
        ),
        tag::FLOAT => Value::Float(f64::from_bits(value.scalar)),
        tag::STRING => Value::String(Cow::Owned(unsafe { value.as_str() }?.to_owned())),
        tag::BYTES => Value::Bytes(Cow::Owned(unsafe { value.as_bytes() }.to_vec())),
        tag::LIST => Value::List(unsafe { values(value) }?),
        tag::TUPLE => Value::Tuple(unsafe { values(value) }?),
        tag::MAP => {
            let items = unsafe { raw_slice(value.ptr.cast::<StableValue>(), value.len * 2) };
            Value::Map(
                items
                    .chunks_exact(2)
                    .map(|pair| {
                        Ok((unsafe { to_value(&pair[0]) }?, unsafe {
                            to_value(&pair[1])
                        }?))
                    })
                    .collect::<Result<_, String>>()?,
            )
        }
        tag::STRUCT => {
            let fields = unsafe { raw_slice(value.ptr.cast::<StableField>(), value.len) };
            Value::Struct(
                fields
                    .iter()
                    .map(|field| {
                        Ok(StructFieldValue {
                            name: unsafe { text(field.name_ptr, field.name_len) }?,
                            value: unsafe { to_value(&field.value) }?,
                        })
                    })
                    .collect::<Result<_, String>>()?,
            )
        }
        tag::ENUM => {
            let variant = unsafe { value.ptr.cast::<StableEnum>().as_ref() }
                .ok_or("enum value without a variant")?;
            Value::Enum(EnumValue {
                name: unsafe { text(variant.name_ptr, variant.name_len) }?,
                value: match unsafe { variant.value.as_ref() } {
                    Some(inner) => Some(Box::new(unsafe { to_value(inner) }?)),
                    None => None,
                },
            })
        }
        tag::HANDLE => return Err("a foreign handle has no Value form".into()),
        other => return Err(format!("unknown stable value tag {other}")),
    })
}

/// # Safety
/// As [`to_value`].
unsafe fn values(value: &StableValue) -> Result<Vec<Value>, String> {
    // Safety: forwarded from the caller.
    unsafe { raw_slice(value.ptr.cast::<StableValue>(), value.len) }
        .iter()
        .map(|item| unsafe { to_value(item) })
        .collect()
}

/// Borrow `len` UTF-8 bytes at `ptr` as text.
///
/// # Safety
/// `ptr` must be valid for `len` bytes for `'a`.
pub unsafe fn str_at<'a>(ptr: *const u8, len: usize) -> Result<&'a str, String> {
    // Safety: forwarded from the caller.
    std::str::from_utf8(unsafe { raw_slice(ptr, len) }).map_err(|err| err.to_string())
}

/// # Safety
/// `ptr` must be valid for `len` bytes.
unsafe fn text(ptr: *const u8, len: usize) -> Result<String, String> {
    // Safety: forwarded from the caller.
    std::str::from_utf8(unsafe { raw_slice(ptr, len) })
        .map(str::to_owned)
        .map_err(|err| err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_round_trip_through_the_stable_tree() {
        let value = Value::Struct(vec![
            StructFieldValue {
                name: "z".into(),
                value: Value::List(vec![Value::Int(-3), Value::Float(1.5)]),
            },
            StructFieldValue {
                name: "a".into(),
                value: Value::Map(vec![(Value::Int(1), Value::Bytes(Cow::Owned(vec![1, 2])))]),
            },
            StructFieldValue {
                name: "m".into(),
                value: Value::Enum(EnumValue {
                    name: "some".into(),
                    value: Some(Box::new(Value::Tuple(vec![
                        Value::String(Cow::Borrowed("x")),
                        Value::Bool(true),
                        Value::Unit,
                    ]))),
                }),
            },
        ]);
        let mut arena = Arena::default();
        let stable = arena.encode(&value);
        // Field order survives (unlike a `WireValue::Record`).
        assert_eq!(unsafe { to_value(&stable) }.unwrap(), value);
        let owned = arena.encode_owned(Value::Enum(EnumValue {
            name: "none".into(),
            value: None,
        }));
        assert!(
            matches!(unsafe { to_value(&owned) }.unwrap(), Value::Enum(v) if v.value.is_none())
        );
        assert!(unsafe { to_value(&StableValue::uint(u64::MAX)) }.is_err());
        assert!(unsafe { to_value(&StableValue::scalar(99, 0)) }.is_err());
    }
}
