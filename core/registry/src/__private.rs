//! What the macros' generated code names.

pub use daedalus_types::{
    Error, Field, FromLiteral, Literal, Scalar, TypeExpr, TypeKey, Typed, Variant,
};
pub use daedalus_value::Access;

const fn starts_with(text: &[u8], prefix: &[u8]) -> bool {
    if text.len() < prefix.len() {
        return false;
    }
    let mut at = 0;
    while at < prefix.len() {
        if text[at] != prefix[at] {
            return false;
        }
        at += 1;
    }
    true
}

/// The first `::`-separated segment of `path` is the crate.
const fn same_crate(key: &[u8], module: &[u8]) -> bool {
    let mut at = 0;
    while at < module.len() && module[at] != b':' {
        if at >= key.len() || key[at] != module[at] {
            return false;
        }
        at += 1;
    }
    at + 1 < key.len() && key[at] == b':'
}

/// A default key holds the defining crate's path, so it means nothing as an interface to
/// another crate.
pub const fn require_local_key(key: &TypeKey, module: &str) {
    const PREFIX: &[u8] = b"rust:";
    let Some(text) = key.literal_text() else {
        return;
    };
    let text = text.as_bytes();
    if starts_with(text, PREFIX) {
        let (_, rest) = text.split_at(PREFIX.len());
        assert!(
            same_crate(rest, module.as_bytes()),
            "this type has a default key and comes from another crate: give it an explicit #[type_key]"
        );
    }
}
