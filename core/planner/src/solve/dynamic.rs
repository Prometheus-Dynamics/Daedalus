//! Types that belong to one node instance: only the typed `port_types` field sets them.

use daedalus_graph::NodeDoc;
use daedalus_registry::TypeRef;
use daedalus_types::TypeKey;

use super::port_types::generic_text;

/// The type `node` gives its port `name`.
///
/// `Ok(None)` when the node doesn't set one. `Err` when the text isn't a type key.
pub(super) fn instance_type(node: &NodeDoc, name: &str) -> Result<Option<TypeRef>, String> {
    let Some(text) = node.port_types.get(name) else {
        return Ok(None);
    };
    let lowered;
    let text = if generic_text(text) {
        lowered = text.to_ascii_lowercase();
        &lowered
    } else {
        text
    };
    TypeKey::shared(text)
        .map(|key| Some(TypeRef::Key(key)))
        .map_err(|_| text.clone())
}
