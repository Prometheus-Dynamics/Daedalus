//! Types that belong to one node instance: only the typed `port_types` field sets them.

use daedalus_graph::NodeDoc;
use daedalus_registry::TypeRef;

use super::port_types::generic_text;

/// The type `node` gives its port `name`.
///
/// `Ok(None)` when the node doesn't set one. `Err` when the text isn't a type: a key, or a
/// type made of others, such as `list<f32>`.
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
    super::type_text::parse(text)
        .map(Some)
        .ok_or_else(|| text.to_string())
}
