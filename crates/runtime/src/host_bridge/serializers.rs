use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use daedalus_data::model::Value;

pub type ValueSerializer =
    Box<dyn Fn(&(dyn Any + Send + Sync)) -> Option<Value> + Send + Sync + 'static>;
pub type ValueSerializerMap = Arc<RwLock<HashMap<TypeId, ValueSerializer>>>;

static VALUE_SERIALIZERS: OnceLock<ValueSerializerMap> = OnceLock::new();

pub fn new_value_serializer_map() -> ValueSerializerMap {
    Arc::new(RwLock::new(HashMap::new()))
}

pub fn value_serializer_map() -> ValueSerializerMap {
    VALUE_SERIALIZERS
        .get_or_init(new_value_serializer_map)
        .clone()
}

pub fn register_value_serializer_in<T, F>(map: &ValueSerializerMap, serializer: F)
where
    T: Any + Clone + Send + Sync + 'static,
    F: Fn(&T) -> Value + Send + Sync + 'static,
{
    let mut guard = map.write().unwrap_or_else(|poisoned| poisoned.into_inner());
    guard.insert(
        TypeId::of::<T>(),
        Box::new(move |any| any.downcast_ref::<T>().map(&serializer)),
    );
}

/// Register `ToValue` serializers for the primitive types the plugin registry installs as
/// built-ins (`()`, `bool`, `i64`, `i32`, `u32`, `f64`, `f32`, `String`, `Vec<u8>`) plus
/// `daedalus_data::model::Value` itself.
pub fn register_primitive_value_serializers_in(map: &ValueSerializerMap) {
    use daedalus_data::to_value::ToValue;

    register_value_serializer_in::<(), _>(map, |v| v.to_value());
    register_value_serializer_in::<bool, _>(map, |v| v.to_value());
    register_value_serializer_in::<i64, _>(map, |v| v.to_value());
    register_value_serializer_in::<i32, _>(map, |v| v.to_value());
    register_value_serializer_in::<u32, _>(map, |v| v.to_value());
    register_value_serializer_in::<f64, _>(map, |v| v.to_value());
    register_value_serializer_in::<f32, _>(map, |v| v.to_value());
    register_value_serializer_in::<String, _>(map, |v| v.to_value());
    register_value_serializer_in::<Vec<u8>, _>(map, |v| v.to_value());
    register_value_serializer_in::<Value, _>(map, Value::clone);
}

/// A fresh serializer map pre-populated by [`register_primitive_value_serializers_in`].
pub fn primitive_value_serializer_map() -> ValueSerializerMap {
    let map = new_value_serializer_map();
    register_primitive_value_serializers_in(&map);
    map
}
