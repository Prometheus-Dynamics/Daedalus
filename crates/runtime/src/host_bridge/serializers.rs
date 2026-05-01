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
