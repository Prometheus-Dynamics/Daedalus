//! Payload codecs: how the payloads of one type key (a plugin's Rust type, a plugin-local
//! [`Local<T>`](daedalus_transport::Local) object, a foreign payload) convert to and from a
//! [`Value`] and bytes, keyed by type key.
//!
//! [`PluginRegistry::register_payload_codec`] makes the `Value` conversions planner adapters
//! (`Opaque(key)` to the codec's schema and back), so a plugin type reaches a host `Value` port
//! and comes back without the host knowing the Rust type, and serializes the key's payloads for
//! the host bridge ([`inspect_payload`](crate::host_bridge::inspect_payload)). The byte
//! conversions are not adapters (bytes of two keys are not interchangeable); hosts call them
//! through [`PluginRegistry::payload_codec`].
//!
//! A plugin registers typed codecs ([`PluginRegistry::register_value_codec`],
//! [`PluginRegistry::register_local_codec`]); installed statically they work as above, and a
//! dynamic plugin installed through its stable entry points exports them as C codecs that the
//! host registers the same way (see `daedalus::dylib`).

use super::*;
use crate::portable::{Arc, arc_dyn};
use daedalus_data::model::Value;
use daedalus_transport::{CopyCost, Local, LocalCodec, LocalPort};

/// A payload of the codec's key to a [`Value`] of its schema.
pub type CodecToValueFn = Arc<dyn Fn(&Payload) -> Result<Value, String> + Send + Sync>;
/// A payload under the given key from a [`Value`] of the codec's schema.
pub type CodecFromValueFn = Arc<dyn Fn(TypeKey, &Value) -> Result<Payload, String> + Send + Sync>;
/// A payload of the codec's key to bytes.
pub type CodecToBytesFn = Arc<dyn Fn(&Payload) -> Result<Vec<u8>, String> + Send + Sync>;
/// A payload under the given key from bytes ([`CodecToBytesFn`]'s).
pub type CodecFromBytesFn = Arc<dyn Fn(TypeKey, &[u8]) -> Result<Payload, String> + Send + Sync>;

/// The conversions of one type key's payloads; each is optional.
#[derive(Clone)]
pub struct PayloadCodec {
    /// The type of the [`Value`]s it converts to and from.
    pub schema: TypeExpr,
    pub to_value: Option<CodecToValueFn>,
    pub from_value: Option<CodecFromValueFn>,
    pub to_bytes: Option<CodecToBytesFn>,
    pub from_bytes: Option<CodecFromBytesFn>,
    /// The key's payloads are `Local<_>`s ([`PluginRegistry::register_local_codec`]): the stable
    /// path reads them back from this build's handles.
    pub local: Option<LocalCodec>,
}

impl PayloadCodec {
    /// A codec of `schema` values with no conversions yet.
    pub fn new(schema: TypeExpr) -> Self {
        Self {
            schema,
            to_value: None,
            from_value: None,
            to_bytes: None,
            from_bytes: None,
            local: None,
        }
    }
}

impl core::fmt::Debug for PayloadCodec {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("PayloadCodec")
            .field("schema", &self.schema)
            .field("to_value", &self.to_value.is_some())
            .field("from_value", &self.from_value.is_some())
            .field("to_bytes", &self.to_bytes.is_some())
            .field("from_bytes", &self.from_bytes.is_some())
            .field("local", &self.local.is_some())
            .finish()
    }
}

/// A `T` to a [`Value`].
pub type ToValueFn<T> = fn(&T) -> Result<Value, String>;
/// A `T` from a [`Value`].
pub type FromValueFn<T> = fn(&Value) -> Result<T, String>;
/// A `T` to bytes.
pub type ToBytesFn<T> = fn(&T) -> Result<Vec<u8>, String>;
/// A `T` from bytes.
pub type FromBytesFn<T> = fn(&[u8]) -> Result<T, String>;

/// Typed conversions of a `T` for [`PluginRegistry::register_value_codec`] and
/// [`PluginRegistry::register_local_codec`], as plain functions.
pub struct ValueCodec<T> {
    pub schema: TypeExpr,
    pub to_value: Option<ToValueFn<T>>,
    pub from_value: Option<FromValueFn<T>>,
    pub to_bytes: Option<ToBytesFn<T>>,
    pub from_bytes: Option<FromBytesFn<T>>,
}

impl<T> Clone for ValueCodec<T> {
    fn clone(&self) -> Self {
        Self {
            schema: self.schema.clone(),
            ..*self
        }
    }
}

impl<T> ValueCodec<T> {
    /// A codec of `schema` values with no conversions yet.
    pub fn new(schema: TypeExpr) -> Self {
        Self {
            schema,
            to_value: None,
            from_value: None,
            to_bytes: None,
            from_bytes: None,
        }
    }

    pub fn to_value(mut self, f: ToValueFn<T>) -> Self {
        self.to_value = Some(f);
        self
    }

    pub fn from_value(mut self, f: FromValueFn<T>) -> Self {
        self.from_value = Some(f);
        self
    }

    pub fn to_bytes(mut self, f: ToBytesFn<T>) -> Self {
        self.to_bytes = Some(f);
        self
    }

    pub fn from_bytes(mut self, f: FromBytesFn<T>) -> Self {
        self.from_bytes = Some(f);
        self
    }
}

/// The value or bytes a `from` adapter reads: a `Value` (bytes as `Value::Bytes`), a `Vec<u8>` or
/// an `Arc<[u8]>`.
fn payload_bytes(payload: &Payload) -> Option<&[u8]> {
    if let Some(bytes) = payload.get_ref::<Vec<u8>>() {
        return Some(bytes);
    }
    if let Some(Value::Bytes(bytes)) = payload.get_ref::<Value>() {
        return Some(bytes);
    }
    payload.get_ref::<Arc<[u8]>>().map(|bytes| &**bytes)
}

/// `payload`'s `T`, or the error a codec of `key` reports.
fn typed<T: Send + Sync + 'static>(payload: &Payload) -> Result<&T, String> {
    payload.get_ref::<T>().ok_or_else(|| {
        format!(
            "codec of `{}` expected `{}`, found `{}`",
            payload.type_key(),
            core::any::type_name::<T>(),
            payload.storage_rust_type_name().unwrap_or("bytes")
        )
    })
}

impl PluginRegistry {
    /// Register `codec` for payloads of `key` holding a `T`: its `Value` conversions become
    /// planner adapters and its `to_value` the host bridge's serializer of `key` (see
    /// [`Self::register_payload_codec`]). A dynamic plugin installed through its stable entry
    /// points exports it to the host.
    pub fn register_value_codec<T: Send + Sync + 'static>(
        &mut self,
        key: impl Into<TypeKey>,
        codec: ValueCodec<T>,
    ) -> PluginResult<()> {
        let mut erased = PayloadCodec::new(codec.schema);
        erased.to_value = codec.to_value.map(|f| -> CodecToValueFn {
            arc_dyn!(move |payload: &Payload| f(typed::<T>(payload)?))
        });
        erased.from_value = codec.from_value.map(|f| -> CodecFromValueFn {
            arc_dyn!(move |key: TypeKey, value: &Value| Ok(Payload::owned(key, f(value)?)))
        });
        erased.to_bytes = codec.to_bytes.map(|f| -> CodecToBytesFn {
            arc_dyn!(move |payload: &Payload| f(typed::<T>(payload)?))
        });
        erased.from_bytes = codec.from_bytes.map(|f| -> CodecFromBytesFn {
            arc_dyn!(move |key: TypeKey, bytes: &[u8]| Ok(Payload::owned(key, f(bytes)?)))
        });
        self.register_payload_codec(key, erased)
    }

    /// [`Self::register_value_codec`] for a plugin-local object: `key`'s payloads are
    /// [`Local<T>`]s, the conversions read and build the `T` inside, and the stable path reads
    /// the objects back from this build's handles.
    pub fn register_local_codec<T: Send + Sync + 'static>(
        &mut self,
        key: impl Into<TypeKey>,
        codec: ValueCodec<T>,
    ) -> PluginResult<()> {
        let mut erased = PayloadCodec::new(codec.schema);
        erased.to_value = codec.to_value.map(|f| -> CodecToValueFn {
            arc_dyn!(move |payload: &Payload| f(typed::<Local<T>>(payload)?))
        });
        erased.from_value = codec.from_value.map(|f| -> CodecFromValueFn {
            arc_dyn!(move |key: TypeKey, value: &Value| {
                Ok(Payload::owned(key, Local::new(f(value)?)))
            })
        });
        erased.to_bytes = codec.to_bytes.map(|f| -> CodecToBytesFn {
            arc_dyn!(move |payload: &Payload| f(typed::<Local<T>>(payload)?))
        });
        erased.from_bytes = codec.from_bytes.map(|f| -> CodecFromBytesFn {
            arc_dyn!(move |key: TypeKey, bytes: &[u8]| {
                Ok(Payload::owned(key, Local::new(f(bytes)?)))
            })
        });
        erased.local = Some(Local::<T>::codec());
        self.register_payload_codec(key, erased)
    }

    /// Register `codec` for payloads of `key` (replacing an earlier one): `to_value` as the
    /// planner adapter `Opaque(key)` to `codec.schema` and as the host bridge's serializer of
    /// `key`, `from_value` as the adapter back. Byte conversions are only looked up
    /// ([`Self::payload_codec`]).
    pub fn register_payload_codec(
        &mut self,
        key: impl Into<TypeKey>,
        codec: PayloadCodec,
    ) -> PluginResult<()> {
        self.ensure_open()?;
        let key = key.into();
        let schema = codec.schema.clone().normalize();
        let value_key = typeexpr_transport_key(&schema);
        let mut cost = AdaptCost::new(AdaptKind::Materialize);
        cost.bytes_copied = CopyCost::Proportional;
        let options = TransportAdapterOptions::default()
            .cost(cost)
            .access(AccessMode::Read);
        if let Some(to_value) = codec.to_value.clone() {
            let target = value_key.clone();
            let adapt = to_value.clone();
            self.register_shared_transport_adapter(
                format!("daedalus.codec:{key}->value"),
                TypeExpr::opaque(key.as_str()),
                schema.clone(),
                options.clone(),
                arc_dyn!(move |payload: Payload, _request: &AdaptRequest| {
                    let value = adapt(&payload).map_err(TransportError::Unsupported)?;
                    Ok(Payload::owned(target.clone(), value)
                        .with_lineage(payload.lineage().clone()))
                }),
            )?;
            self.value_serializers.write().insert_for_key(
                key.clone(),
                Box::new(move |payload: &Payload| to_value(payload).ok()),
            );
        }
        if let Some(from_value) = codec.from_value.clone() {
            let target = key.clone();
            self.register_shared_transport_adapter(
                format!("daedalus.codec:value->{key}"),
                schema.clone(),
                TypeExpr::opaque(key.as_str()),
                options,
                arc_dyn!(move |payload: Payload, _request: &AdaptRequest| {
                    let value = match payload.get_ref::<Value>() {
                        Some(value) => from_value(target.clone(), value),
                        // A builtin scalar's own Rust type, e.g. bytes.
                        None => match payload_bytes(&payload) {
                            Some(bytes) => {
                                from_value(target.clone(), &Value::Bytes(bytes.to_vec().into()))
                            }
                            None => Err(format!(
                                "codec of `{target}` expected a `Value`, found `{}`",
                                payload.storage_rust_type_name().unwrap_or("bytes")
                            )),
                        },
                    };
                    let lineage = payload.lineage().clone();
                    Ok(value
                        .map_err(TransportError::Unsupported)?
                        .with_lineage(lineage))
                }),
            )?;
        }
        self.payload_codecs.insert(key, codec);
        Ok(())
    }

    /// The codec of `key` ([`Self::register_payload_codec`]).
    pub fn payload_codec(&self, key: &TypeKey) -> Option<&PayloadCodec> {
        self.payload_codecs.get(key)
    }

    /// Every registered codec, by key.
    pub fn payload_codecs(&self) -> impl Iterator<Item = (&TypeKey, &PayloadCodec)> {
        self.payload_codecs.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq)]
    struct Pair(i64, i64);

    fn schema() -> TypeExpr {
        TypeExpr::Tuple(vec![
            TypeExpr::Scalar(ValueType::Int),
            TypeExpr::Scalar(ValueType::Int),
        ])
    }

    fn codec() -> ValueCodec<Pair> {
        ValueCodec::new(schema())
            .to_value(|pair: &Pair| Ok(Value::Tuple(vec![Value::Int(pair.0), Value::Int(pair.1)])))
            .from_value(|value| match value {
                Value::Tuple(items) => match items.as_slice() {
                    [Value::Int(a), Value::Int(b)] => Ok(Pair(*a, *b)),
                    _ => Err("two ints".into()),
                },
                _ => Err("a tuple".into()),
            })
            .to_bytes(|pair: &Pair| Ok([pair.0.to_le_bytes(), pair.1.to_le_bytes()].concat()))
    }

    #[test]
    fn a_value_codec_adapts_both_ways_and_serializes_by_key() {
        let mut registry = PluginRegistry::new();
        registry.register_value_codec("test:pair", codec()).unwrap();
        let adapters = registry.runtime_transport.adapters();
        let request = AdaptRequest::new(typeexpr_transport_key(&schema()));
        let value = adapters
            .adapt(
                &AdapterId::new("daedalus.codec:test:pair->value"),
                Payload::owned("test:pair", Pair(2, 3)),
                &request,
            )
            .unwrap();
        let expected = Value::Tuple(vec![Value::Int(2), Value::Int(3)]);
        assert_eq!(value.get_ref::<Value>(), Some(&expected));
        assert_eq!(value.type_key(), &typeexpr_transport_key(&schema()));

        let pair = adapters
            .adapt(
                &AdapterId::new("daedalus.codec:value->test:pair"),
                value,
                &AdaptRequest::new("test:pair"),
            )
            .unwrap();
        assert_eq!(pair.get_ref::<Pair>(), Some(&Pair(2, 3)));

        let serialized = crate::host_bridge::serialize_payload_value(
            &Payload::owned("test:pair", Pair(4, 5)),
            &registry.value_serializers,
        );
        let expected = Value::Tuple(vec![Value::Int(4), Value::Int(5)]);
        assert_eq!(serialized, Some(expected));

        let codec = registry.payload_codec(&TypeKey::new("test:pair")).unwrap();
        let bytes = (codec.to_bytes.as_ref().unwrap())(&pair).unwrap();
        assert_eq!(bytes.len(), 16);
        assert!(codec.from_bytes.is_none());
    }

    #[test]
    fn a_local_codec_reads_the_object_inside() {
        let mut registry = PluginRegistry::new();
        registry
            .register_local_codec("test:local_pair", codec())
            .unwrap();
        let codec = registry
            .payload_codec(&TypeKey::new("test:local_pair"))
            .unwrap();
        assert!(codec.local.is_some());
        let local = Payload::owned("test:local_pair", Local::new(Pair(6, 7)));
        let value = (codec.to_value.as_ref().unwrap())(&local).unwrap();
        let back =
            (codec.from_value.as_ref().unwrap())(TypeKey::new("test:local_pair"), &value).unwrap();
        assert_eq!(
            back.get_ref::<Local<Pair>>().map(|local| &**local),
            Some(&Pair(6, 7))
        );
    }
}
