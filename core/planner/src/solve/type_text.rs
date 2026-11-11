//! A port's type as text in a graph document: a key, or a type made of others, written the
//! way the planner prints one: `list<f32>`, `optional<u32>`, `map<string, i64>`, `(u32, f32)`.

use std::collections::BTreeMap;
use std::sync::{Mutex, PoisonError};

use daedalus_registry::TypeRef;
use daedalus_types::{Scalar, TypeExpr, TypeKey};

const SCALARS: [Scalar; 16] = [
    Scalar::Unit,
    Scalar::Bool,
    Scalar::I8,
    Scalar::I16,
    Scalar::I32,
    Scalar::Int,
    Scalar::ISize,
    Scalar::U8,
    Scalar::U16,
    Scalar::U32,
    Scalar::U64,
    Scalar::USize,
    Scalar::F32,
    Scalar::Float,
    Scalar::String,
    Scalar::Bytes,
];

/// Types made at run time live as long as the process, once for each distinct one: a type
/// expression holds its parts by reference.
static KEPT: Mutex<BTreeMap<String, &'static TypeExpr>> = Mutex::new(BTreeMap::new());

fn keep(text: &str, expr: TypeExpr) -> &'static TypeExpr {
    let mut kept = KEPT.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(found) = kept.get(text) {
        return found;
    }
    let leaked: &'static TypeExpr = Box::leak(Box::new(expr));
    kept.insert(text.to_owned(), leaked);
    leaked
}

/// Splits `text` at the commas that aren't inside brackets.
fn parts(text: &str) -> Vec<&str> {
    let (mut depth, mut from, mut out) = (0usize, 0, Vec::new());
    for (at, byte) in text.bytes().enumerate() {
        match byte {
            b'<' | b'(' => depth += 1,
            b'>' | b')' => depth = depth.saturating_sub(1),
            b',' if depth == 0 => {
                out.push(text[from..at].trim());
                from = at + 1;
            }
            _ => {}
        }
    }
    out.push(text[from..].trim());
    out
}

/// What `text` names, or `None` when it names nothing.
fn expr(text: &str) -> Option<TypeExpr> {
    let text = text.trim();
    let inside = |head: &str| {
        text.strip_prefix(head)
            .and_then(|rest| rest.strip_suffix('>'))
    };
    if let Some(inner) = inside("list<") {
        // A list of bytes is the bytes scalar, as a `Vec<u8>` is.
        return Some(match expr(inner)? {
            TypeExpr::Scalar(Scalar::U8) => TypeExpr::Scalar(Scalar::Bytes),
            item => TypeExpr::List(keep(inner.trim(), item)),
        });
    }
    if let Some(inner) = inside("optional<") {
        return Some(TypeExpr::Optional(keep(inner.trim(), expr(inner)?)));
    }
    if let Some(inner) = inside("map<") {
        let [key, value] = parts(inner)[..] else {
            return None;
        };
        return Some(TypeExpr::Map(
            keep(key, expr(key)?),
            keep(value, expr(value)?),
        ));
    }
    if let Some(inner) = text
        .strip_prefix('(')
        .and_then(|rest| rest.strip_suffix(')'))
    {
        let items: Option<Vec<TypeExpr>> = parts(inner).into_iter().map(expr).collect();
        let items = items.filter(|items| items.len() >= 2)?;
        let whole = format!("({inner})");
        let TypeExpr::Tuple(kept) = keep(&whole, TypeExpr::Tuple(Vec::leak(items))) else {
            return None;
        };
        return Some(TypeExpr::Tuple(kept));
    }
    if let Some(scalar) = SCALARS.iter().find(|scalar| scalar.key().as_str() == text) {
        return Some(TypeExpr::Scalar(*scalar));
    }
    TypeKey::shared(text).ok().map(TypeExpr::Opaque)
}

/// The type `text` names in a graph document. A plain key stays a key.
pub(super) fn parse(text: &str) -> Option<TypeRef> {
    if let Ok(key) = TypeKey::shared(text) {
        return Some(TypeRef::Key(key));
    }
    match expr(text)? {
        TypeExpr::Opaque(key) => Some(TypeRef::Key(key)),
        TypeExpr::Scalar(scalar) => Some(TypeRef::Key(scalar.key())),
        made => Some(TypeRef::Expr(made)),
    }
}

#[cfg(test)]
mod tests {
    use daedalus_registry::{TypeRef, port_type};
    use std::collections::BTreeMap;

    use super::parse;
    use crate::solve::port_types::{display, same};

    #[test]
    fn a_type_written_as_the_planner_prints_it_is_the_rust_types() {
        fn check<T: daedalus_types::Typed>(text: &str) {
            let parsed = parse(text).unwrap_or_else(|| panic!("`{text}` doesn't parse"));
            let declared = port_type::<T>();
            assert!(
                same(&parsed, &declared),
                "`{text}`: {parsed:?} and {declared:?}"
            );
            // And it prints back as it was written.
            assert_eq!(display(&declared), text);
        }
        check::<Vec<f32>>("list<f32>");
        check::<Vec<Vec<u32>>>("list<list<u32>>");
        check::<Option<u32>>("optional<u32>");
        check::<Vec<Option<i64>>>("list<optional<i64>>");
        check::<(u32, f32)>("(u32, f32)");
        check::<(u32, Vec<f64>, bool)>("(u32, list<f64>, bool)");
        check::<BTreeMap<String, u32>>("map<string, u32>");
        check::<Vec<(u32, u32)>>("list<(u32, u32)>");
        // Bytes are bytes, however they are written, and a plain key is a key.
        assert!(same(&parse("list<u8>").unwrap(), &port_type::<Vec<u8>>()));
        assert_eq!(parse("bytes"), Some(port_type::<Vec<u8>>()));
        assert!(matches!(parse("acme:point"), Some(TypeRef::Key(_))));
        // What two lists hold tells them apart.
        assert!(!same(
            &parse("list<f32>").unwrap(),
            &parse("list<u32>").unwrap()
        ));
        for bad in ["list<", "map<u32>", "(u32)", "list<Not A Key>", ""] {
            assert_eq!(parse(bad), None, "{bad}");
        }
    }
}
