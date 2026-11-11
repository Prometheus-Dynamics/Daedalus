//! The serializer map, keyed by type key. The registry supplies the entries.

use std::boxed::Box;
use std::collections::BTreeMap;
use std::string::String;

use daedalus_types::TypeKey;
use daedalus_value::Value;

use crate::value::HostValue;

/// Turns a payload of one type into a plain value, or says it can't.
pub type ValueSerializer = Box<dyn Fn(&Value) -> Option<HostValue> + Send + Sync>;

/// Builds a payload from a plain value, or refuses it.
pub type ValueDeserializer = Box<dyn Fn(&HostValue) -> Option<Value> + Send + Sync>;

/// One type's entry.
pub struct SerializerEntry {
    pub(crate) to_value: ValueSerializer,
    pub(crate) from_value: Option<ValueDeserializer>,
}

/// Serializers by type key.
#[derive(Default)]
pub struct ValueSerializers {
    entries: BTreeMap<String, SerializerEntry>,
}

impl ValueSerializers {
    pub fn new() -> ValueSerializers {
        ValueSerializers::default()
    }

    /// Registers a serializer and a deserializer for `key`, so `push_value` can build a payload.
    pub fn insert_both_for_key(
        &mut self,
        key: &TypeKey,
        to_value: ValueSerializer,
        from_value: ValueDeserializer,
    ) {
        self.entries.insert(
            String::from(key.as_str()),
            SerializerEntry {
                to_value,
                from_value: Some(from_value),
            },
        );
    }

    pub(crate) fn for_key(&self, key: &str) -> Option<&SerializerEntry> {
        self.entries.get(key)
    }
}
