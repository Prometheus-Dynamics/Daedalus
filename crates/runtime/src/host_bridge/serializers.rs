use parking_lot::RwLock;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;

use daedalus_data::model::Value;

pub type ValueSerializer =
    Box<dyn Fn(&(dyn Any + Send + Sync)) -> Option<Value> + Send + Sync + 'static>;
pub type ValueSerializerMap = Arc<RwLock<HashMap<TypeId, ValueSerializer>>>;

pub fn new_value_serializer_map() -> ValueSerializerMap {
    Arc::new(RwLock::new(HashMap::new()))
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
            i64 => "i64", Int;
            i32 => "i32", Int;
            u32 => "u32", Int;
            f64 => "f64", Float;
            f32 => "f32", Float;
            String => "string", String;
            Vec<u8> => "bytes", Bytes;
        }
    };
}
pub(crate) use for_each_builtin_primitive;

/// Register `ToValue` serializers for the built-in primitive host value types (`()`, `bool`,
/// `i64`, `i32`, `u32`, `f64`, `f32`, `String`, `Vec<u8>`) plus `daedalus_data::model::Value`
/// itself.
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
