//! The `#[node(reusable)]` attribute declares the reuse metadata key on the node.

use daedalus::{
    macros::{node, plugin},
    runtime::{NODE_REUSABLE_META_KEY, NodeError},
};

#[node(id = "reusable_scale", inputs("x"), outputs("y"), reusable)]
fn reusable_scale(x: i64) -> Result<i64, NodeError> {
    Ok(x * 2)
}

#[node(id = "plain_scale", inputs("x"), outputs("y"))]
fn plain_scale(x: i64) -> Result<i64, NodeError> {
    Ok(x * 2)
}

#[plugin(id = "reuse_attr", nodes(reusable_scale, plain_scale))]
struct ReuseAttrPlugin;

#[test]
fn the_attribute_declares_the_reuse_key_and_only_on_its_node() {
    let reusable = ReusableScaleNode::node_decl().expect("declared");
    assert_eq!(
        reusable
            .metadata_json
            .get(NODE_REUSABLE_META_KEY)
            .map(String::as_str),
        Some(r#"{"type":"Bool","value":true}"#)
    );
    let plain = PlainScaleNode::node_decl().expect("declared");
    assert!(!plain.metadata_json.contains_key(NODE_REUSABLE_META_KEY));
}
