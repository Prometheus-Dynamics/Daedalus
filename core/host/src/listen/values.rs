//! Wire codecs: how a value of one type key crosses the service wire. A port whose type has no
//! entry here cannot be fed or taken through the service, and a request that tries is refused as
//! unsupported.

use std::boxed::Box;
use std::collections::BTreeMap;
use std::string::String;
use std::sync::Arc;
use std::vec::Vec;

use daedalus_runner::{INLINE_BYTES, value_of};
use daedalus_types::{Scalar, TypeKey};
use daedalus_value::{Owner, Value};
use daedalus_wire::{WireType, WireValue};

use crate::{FeedOutcome, Host, HostError};

/// Why a codec did not feed a value.
#[derive(Debug)]
pub enum FeedRefusal {
    /// The wire value is not one this type is built from.
    Shape,
    /// The host refused the payload.
    Host(HostError),
}

type FeedFn = Box<
    dyn Fn(&mut Host<'static>, &str, &WireValue) -> Result<FeedOutcome, FeedRefusal> + Send + Sync,
>;
type ReadFn = Box<dyn Fn(&Value) -> Option<WireValue> + Send + Sync>;

/// One type's codec: the wire type its values travel as, and both directions.
pub struct WireCodec {
    pub(crate) ty: WireType,
    pub(crate) feed: FeedFn,
    pub(crate) read: ReadFn,
}

impl WireCodec {
    /// A codec for a type the graph passes as an `Arc<T>`.
    pub fn typed<T: Send + Sync + 'static>(
        ty: WireType,
        to_wire: impl Fn(&T) -> WireValue + Send + Sync + 'static,
        from_wire: impl Fn(&WireValue) -> Option<T> + Send + Sync + 'static,
    ) -> WireCodec {
        WireCodec {
            ty,
            feed: Box::new(move |host, port, value| {
                let payload = from_wire(value).ok_or(FeedRefusal::Shape)?;
                host.feed_typed(port, Arc::new(payload))
                    .map_err(FeedRefusal::Host)
            }),
            read: Box::new(move |value| value.typed::<T>().map(&to_wire)),
        }
    }

    /// A codec for a plain `Copy` type. A node may write one in place, as bytes, so reading also
    /// accepts that form.
    pub fn copy<T: Copy + Send + Sync + 'static>(
        ty: WireType,
        to_wire: impl Fn(T) -> WireValue + Send + Sync + 'static,
        from_wire: impl Fn(&WireValue) -> Option<T> + Send + Sync + 'static,
    ) -> WireCodec {
        WireCodec {
            ty,
            feed: Box::new(move |host, port, value| {
                let payload = from_wire(value).ok_or(FeedRefusal::Shape)?;
                host.feed_typed(port, Arc::new(payload))
                    .map_err(FeedRefusal::Host)
            }),
            read: Box::new(move |value| read_copy::<T>(value).map(&to_wire)),
        }
    }

    /// The wire type this codec's values travel as.
    pub fn wire_type(&self) -> &WireType {
        &self.ty
    }
}

/// A `Copy` value however it is held: typed, or as its bytes.
fn read_copy<T: Copy + 'static>(value: &Value) -> Option<T> {
    if let Owner::Typed(payload) = &value.owner
        && let Some(typed) = payload.downcast_ref::<T>()
    {
        return Some(*typed);
    }
    let bytes = value.bytes()?;
    let mut cell = [0; INLINE_BYTES];
    cell.get_mut(..bytes.len())?.copy_from_slice(bytes);
    value_of::<T>(&cell, bytes.len())
}

/// A bool however it is held. Its byte form is read as a number, since not every byte is a bool.
fn read_bool(value: &Value) -> Option<bool> {
    if let Some(typed) = value.typed::<bool>() {
        return Some(*typed);
    }
    match value.bytes()? {
        [byte] => Some(*byte != 0),
        _ => None,
    }
}

macro_rules! number {
    ($codecs:ident, $($scalar:ident, $ty:ty, $wire:ident, $back:ty);* $(;)?) => {$(
        $codecs.insert(
            &Scalar::$scalar.key(),
            WireCodec::copy::<$ty>(
                WireType::Scalar(Scalar::$scalar),
                |value| WireValue::$wire(value as $back),
                |value| match value {
                    WireValue::$wire(value) => <$ty>::try_from(*value).ok(),
                    _ => None,
                },
            ),
        );
    )*};
}

/// The codecs of one host, by type key.
#[derive(Default)]
pub struct WireCodecs {
    by_key: BTreeMap<String, WireCodec>,
}

impl WireCodecs {
    /// No codecs at all.
    pub fn new() -> WireCodecs {
        WireCodecs::default()
    }

    /// The codecs of every scalar: the numbers, bool, unit, strings and bytes.
    pub fn scalars() -> WireCodecs {
        let mut codecs = WireCodecs::new();
        number!(codecs,
            I8, i8, I8, i8; I16, i16, I16, i16; I32, i32, I32, i32; Int, i64, I64, i64;
            ISize, isize, ISize, i64; U8, u8, U8, u8; U16, u16, U16, u16; U32, u32, U32, u32;
            U64, u64, U64, u64; USize, usize, USize, u64;
        );
        codecs.insert(
            &Scalar::F32.key(),
            WireCodec::copy::<f32>(WireType::Scalar(Scalar::F32), WireValue::F32, |value| {
                match value {
                    WireValue::F32(value) => Some(*value),
                    _ => None,
                }
            }),
        );
        codecs.insert(
            &Scalar::Float.key(),
            WireCodec::copy::<f64>(WireType::Scalar(Scalar::Float), WireValue::F64, |value| {
                match value {
                    WireValue::F64(value) => Some(*value),
                    _ => None,
                }
            }),
        );
        let mut bool_codec =
            WireCodec::copy::<bool>(WireType::Scalar(Scalar::Bool), WireValue::Bool, |value| {
                match value {
                    WireValue::Bool(value) => Some(*value),
                    _ => None,
                }
            });
        bool_codec.read = Box::new(|value| read_bool(value).map(WireValue::Bool));
        codecs.insert(&Scalar::Bool.key(), bool_codec);
        let mut unit_codec = WireCodec::typed::<()>(
            WireType::Scalar(Scalar::Unit),
            |()| WireValue::Unit,
            |value| matches!(value, WireValue::Unit).then_some(()),
        );
        unit_codec.read = Box::new(|_| Some(WireValue::Unit));
        codecs.insert(&Scalar::Unit.key(), unit_codec);
        codecs.insert(
            &Scalar::String.key(),
            WireCodec::typed::<String>(
                WireType::Scalar(Scalar::String),
                |text| WireValue::String(text.clone()),
                |value| match value {
                    WireValue::String(text) => Some(text.clone()),
                    _ => None,
                },
            ),
        );
        codecs.insert(
            &Scalar::Bytes.key(),
            WireCodec::typed::<Vec<u8>>(
                WireType::Scalar(Scalar::Bytes),
                |bytes| WireValue::Bytes(bytes.clone()),
                |value| match value {
                    WireValue::Bytes(bytes) => Some(bytes.clone()),
                    _ => None,
                },
            ),
        );
        codecs
    }

    /// Registers the codec for `key`. A later entry for the same key replaces it.
    pub fn insert(&mut self, key: &TypeKey, codec: WireCodec) {
        self.by_key.insert(String::from(key.as_str()), codec);
    }

    pub fn get(&self, key: &str) -> Option<&WireCodec> {
        self.by_key.get(key)
    }

    /// The type keys that travel as opaque bytes. A hello lists them, so a client knows which
    /// opaque values this host reads.
    pub fn opaque_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();
        for codec in self.by_key.values() {
            collect_opaque(&codec.ty, &mut keys);
        }
        keys.sort_unstable();
        keys.dedup();
        keys
    }
}

fn collect_opaque(ty: &WireType, keys: &mut Vec<String>) {
    match ty {
        WireType::Scalar(_) => {}
        WireType::Opaque(key) => keys.push(key.clone()),
        WireType::Optional(inner) | WireType::List(inner) => collect_opaque(inner, keys),
        WireType::Map(key, value) => {
            collect_opaque(key, keys);
            collect_opaque(value, keys);
        }
        WireType::Tuple(items) => items.iter().for_each(|item| collect_opaque(item, keys)),
        WireType::Struct(fields) => fields.iter().for_each(|(_, ty)| collect_opaque(ty, keys)),
        WireType::Enum(variants) => variants
            .iter()
            .filter_map(|(_, payload)| payload.as_ref())
            .for_each(|ty| collect_opaque(ty, keys)),
    }
}
