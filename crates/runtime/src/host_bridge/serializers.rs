use crate::portable::Arc;
use crate::prelude::*;
use crate::sync::RwLock;
use alloc::collections::BTreeMap;
use core::any::{Any, TypeId};
use core::ops::{Deref, DerefMut};

use daedalus_data::model::Value;
use daedalus_transport::{Payload, TypeKey};

pub type ValueSerializer =
    Box<dyn Fn(&(dyn Any + Send + Sync)) -> Option<Value> + Send + Sync + 'static>;
/// Serializes a whole payload (see [`ValueSerializers::insert_for_key`]).
pub type PayloadSerializer = Box<dyn Fn(&Payload) -> Option<Value> + Send + Sync + 'static>;
pub type ValueSerializerMap = Arc<RwLock<ValueSerializers>>;

/// Value serializers: by the payload value's `TypeId` (the map this dereferences to), and by
/// type key for payloads whose Rust type does not tell them apart, such as foreign handles of a
/// stable plugin's codec ([`Self::insert_for_key`]). [`super::serialize_payload_value`] tries
/// the `TypeId` first.
#[derive(Default)]
pub struct ValueSerializers {
    by_type: HashMap<TypeId, ValueSerializer>,
    by_key: BTreeMap<TypeKey, PayloadSerializer>,
}

impl ValueSerializers {
    /// Serialize payloads of type key `key` with `serializer` (replacing an earlier one).
    pub fn insert_for_key(&mut self, key: TypeKey, serializer: PayloadSerializer) {
        self.by_key.insert(key, serializer);
    }

    /// The serializer of type key `key` ([`Self::insert_for_key`]).
    pub fn for_key(&self, key: &TypeKey) -> Option<&PayloadSerializer> {
        self.by_key.get(key)
    }
}

impl Deref for ValueSerializers {
    type Target = HashMap<TypeId, ValueSerializer>;

    fn deref(&self) -> &Self::Target {
        &self.by_type
    }
}

impl DerefMut for ValueSerializers {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.by_type
    }
}

pub fn new_value_serializer_map() -> ValueSerializerMap {
    Arc::new(RwLock::new(ValueSerializers::default()))
}

pub fn register_value_serializer_in<T, F>(map: &ValueSerializerMap, serializer: F)
where
    T: Any + Send + Sync + 'static,
    F: Fn(&T) -> Value + Send + Sync + 'static,
{
    let mut guard = map.write();
    guard.insert(
        TypeId::of::<T>(),
        Box::new(move |any| any.downcast_ref::<T>().map(&serializer)),
    );
}

/// Invoke `$callback!` with the primitive host value types the plugin registry installs as
/// built-ins, as `rust_type => "name", ValueType;` entries.
macro_rules! for_each_builtin_primitive {
    ($callback:ident) => {
        $callback! {
            () => "unit", Unit;
            bool => "bool", Bool;
            i8 => "i8", I8;
            i16 => "i16", I16;
            i32 => "i32", I32;
            i64 => "i64", Int;
            isize => "isize", ISize;
            u8 => "u8", U8;
            u16 => "u16", U16;
            u32 => "u32", U32;
            u64 => "u64", U64;
            usize => "usize", USize;
            f32 => "f32", F32;
            f64 => "f64", Float;
            String => "string", String;
            Vec<u8> => "bytes", Bytes;
        }
    };
}
#[cfg(feature = "plugins")]
pub(crate) use for_each_builtin_primitive;

/// Register `ToValue` serializers for the built-in primitive host value types (`()`, `bool`,
/// integers and floats of every width but 128 bits, `String`, `Vec<u8>`) plus
/// `daedalus_data::model::Value` itself.
pub fn register_primitive_value_serializers_in(map: &ValueSerializerMap) {
    use daedalus_data::to_value::ToValue;

    macro_rules! register {
        ($($ty:ty => $name:literal, $value_type:ident;)*) => {
            $(register_value_serializer_in::<$ty, _>(map, |v| v.to_value());)*
        };
    }
    for_each_builtin_primitive!(register);
    register_value_serializer_in::<Value, _>(map, Value::clone);
}

/// A fresh serializer map pre-populated by [`register_primitive_value_serializers_in`].
pub fn primitive_value_serializer_map() -> ValueSerializerMap {
    let map = new_value_serializer_map();
    register_primitive_value_serializers_in(&map);
    map
}
