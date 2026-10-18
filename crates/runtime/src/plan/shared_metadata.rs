//! Serde for [`RuntimeNode::metadata`](super::RuntimeNode::metadata): the map's own format,
//! behind the shared pointer, so the serialized plan is unchanged.

use crate::portable::Arc;
use crate::prelude::*;
use daedalus_data::model::Value;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

type Map = alloc::collections::BTreeMap<String, Value>;

pub(super) fn is_empty(metadata: &Arc<Map>) -> bool {
    metadata.is_empty()
}

pub(super) fn serialize<S: Serializer>(
    metadata: &Arc<Map>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    metadata.as_ref().serialize(serializer)
}

pub(super) fn deserialize<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Arc<Map>, D::Error> {
    Map::deserialize(deserializer).map(Arc::new)
}
