//! Wire codecs: how a value of one type key crosses the service wire. A port whose type has no
//! entry here cannot be fed or taken through the service, and a request that tries is refused as
//! unsupported.

use std::boxed::Box;
use std::collections::BTreeMap;
use std::string::String;
use std::sync::Arc;
use std::vec::Vec;

use daedalus_types::{Scalar, TypeKey};
use daedalus_value::{InlineBytes, Owner, Value};
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

    /// A codec for a type whose values are at most 16 bytes and travel in place, as plain data
    /// does: a number is its little-endian bytes. `to_wire` reads those bytes back.
    pub fn inline(
        ty: WireType,
        to_wire: impl Fn(&[u8]) -> Option<WireValue> + Send + Sync + 'static,
        from_wire: impl Fn(&WireValue) -> Option<InlineBytes<16>> + Send + Sync + 'static,
    ) -> WireCodec {
        WireCodec {
            ty,
            feed: Box::new(move |host, port, value| {
                let bytes = from_wire(value).ok_or(FeedRefusal::Shape)?;
                feed_owner(host, port, Owner::Inline(bytes))
            }),
            read: Box::new(move |value| to_wire(value.bytes()?)),
        }
    }

    /// A codec for a type whose values are a run of bytes that is shared, not copied, as a
    /// backend that reads a cell's bytes expects: a plugin, or a worker.
    pub fn shared(
        ty: WireType,
        to_wire: impl Fn(&[u8]) -> Option<WireValue> + Send + Sync + 'static,
        from_wire: impl Fn(&WireValue) -> Option<Arc<[u8]>> + Send + Sync + 'static,
    ) -> WireCodec {
        WireCodec {
            ty,
            feed: Box::new(move |host, port, value| {
                let bytes = from_wire(value).ok_or(FeedRefusal::Shape)?;
                feed_owner(host, port, Owner::Shared(bytes))
            }),
            read: Box::new(move |value| to_wire(value.bytes()?)),
        }
    }

    /// Also reads a value held the other way: `other` is tried when this codec's own reading
    /// finds nothing. A graph's nodes decide how they hold an output, and a take accepts both.
    fn or_read(
        mut self,
        other: impl Fn(&Value) -> Option<WireValue> + Send + Sync + 'static,
    ) -> WireCodec {
        let own = self.read;
        self.read = Box::new(move |value| own(value).or_else(|| other(value)));
        self
    }

    /// The wire type this codec's values travel as.
    pub fn wire_type(&self) -> &WireType {
        &self.ty
    }
}

/// Feeds a value made of bytes into `port`, with the port's own type and memory space.
fn feed_owner(
    host: &mut Host<'static>,
    port: &str,
    owner: Owner,
) -> Result<FeedOutcome, FeedRefusal> {
    let spec = host.input(port).map_err(FeedRefusal::Host)?;
    let value = Value {
        ty: spec.ty,
        space: spec.space,
        owner,
    };
    host.feed(port, value).map_err(FeedRefusal::Host)
}

macro_rules! number {
    ($codecs:ident, $($scalar:ident, $ty:ty, $wire:ident, $back:ty);* $(;)?) => {$(
        $codecs.insert(
            &Scalar::$scalar.key(),
            WireCodec::inline(
                WireType::Scalar(Scalar::$scalar),
                |bytes| {
                    let value = <$ty>::from_le_bytes(bytes.try_into().ok()?);
                    Some(WireValue::$wire(value as $back))
                },
                |value| match value {
                    WireValue::$wire(value) => {
                        let value = <$ty>::try_from(*value).ok()?;
                        InlineBytes::from_slice(&value.to_le_bytes())
                    }
                    _ => None,
                },
            )
            // A node may also hand a number out as a typed payload.
            .or_read(|value| value.typed::<$ty>().map(|value| WireValue::$wire(*value as $back))),
        );
    )*};
}

fn text(bytes: &[u8]) -> Option<WireValue> {
    core::str::from_utf8(bytes)
        .ok()
        .map(|text| WireValue::String(String::from(text)))
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

    /// The codecs of every scalar. Numbers, bool and unit travel in place, as their bytes, which
    /// every backend reads. Text and bytes are fed as the typed payloads (`String`, `Vec<u8>`)
    /// that nodes written as Rust functions take. A take reads either form.
    pub fn scalars() -> WireCodecs {
        let mut codecs = WireCodecs::plain();
        codecs.insert(
            &Scalar::String.key(),
            WireCodec::typed::<String>(
                WireType::Scalar(Scalar::String),
                |text| WireValue::String(text.clone()),
                |value| match value {
                    WireValue::String(text) => Some(text.clone()),
                    _ => None,
                },
            )
            .or_read(|value| text(value.bytes()?)),
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
            )
            .or_read(|value| Some(WireValue::Bytes(value.bytes()?.to_vec()))),
        );
        codecs
    }

    /// As [`scalars`](WireCodecs::scalars), with text and bytes fed as shared bytes: the form a
    /// backend that reads a cell's bytes takes, as a plugin's and a worker's do.
    pub fn byte_form() -> WireCodecs {
        let mut codecs = WireCodecs::plain();
        codecs.insert(
            &Scalar::String.key(),
            WireCodec::shared(
                WireType::Scalar(Scalar::String),
                text,
                |value| match value {
                    WireValue::String(text) => Some(Arc::from(text.as_bytes())),
                    _ => None,
                },
            )
            .or_read(|value| {
                value
                    .typed::<String>()
                    .map(|text| WireValue::String(text.clone()))
            }),
        );
        codecs.insert(
            &Scalar::Bytes.key(),
            WireCodec::shared(
                WireType::Scalar(Scalar::Bytes),
                |bytes| Some(WireValue::Bytes(bytes.to_vec())),
                |value| match value {
                    WireValue::Bytes(bytes) => Some(Arc::from(bytes.as_slice())),
                    _ => None,
                },
            )
            .or_read(|value| {
                value
                    .typed::<Vec<u8>>()
                    .map(|bytes| WireValue::Bytes(bytes.clone()))
            }),
        );
        codecs
    }

    /// The scalars that are plain data: the numbers, bool and unit.
    fn plain() -> WireCodecs {
        let mut codecs = WireCodecs::new();
        number!(codecs,
            I8, i8, I8, i8; I16, i16, I16, i16; I32, i32, I32, i32; Int, i64, I64, i64;
            ISize, isize, ISize, i64; U8, u8, U8, u8; U16, u16, U16, u16; U32, u32, U32, u32;
            U64, u64, U64, u64; USize, usize, USize, u64; F32, f32, F32, f32;
            Float, f64, F64, f64;
        );
        codecs.insert(
            &Scalar::Bool.key(),
            WireCodec::inline(
                WireType::Scalar(Scalar::Bool),
                // Not every byte is a bool, so the byte is read as a number.
                |bytes| match bytes {
                    [byte] => Some(WireValue::Bool(*byte != 0)),
                    _ => None,
                },
                |value| match value {
                    WireValue::Bool(value) => InlineBytes::from_slice(&[u8::from(*value)]),
                    _ => None,
                },
            )
            .or_read(|value| value.typed::<bool>().map(|value| WireValue::Bool(*value))),
        );
        codecs.insert(
            &Scalar::Unit.key(),
            WireCodec::inline(
                WireType::Scalar(Scalar::Unit),
                |_| Some(WireValue::Unit),
                |value| matches!(value, WireValue::Unit).then(InlineBytes::new),
            )
            .or_read(|_| Some(WireValue::Unit)),
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
