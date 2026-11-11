//! Reading, comparing and naming port types. Comparison uses keys and expressions, never text.

use daedalus_registry::TypeRef;
use daedalus_types::TypeExpr;

const MARKER: &str = "generic";

/// Whether this text names the generic marker, in any case: `generic` or `generic:<var>`.
pub(crate) fn generic_text(text: &str) -> bool {
    let head = text.split(':').next().unwrap_or(text);
    head.eq_ignore_ascii_case(MARKER)
}

fn key_text(ty: &TypeRef) -> Option<&str> {
    match ty {
        TypeRef::Key(key) => Some(key.as_str()),
        TypeRef::Expr(TypeExpr::Opaque(key)) => Some(key.as_str()),
        _ => None,
    }
}

/// Whether a type is the generic marker. `Some` holds the variable's name, when it has one.
pub(crate) fn generic_var(ty: &TypeRef) -> Option<Option<&str>> {
    let text = key_text(ty)?;
    if !generic_text(text) {
        return None;
    }
    Some(text.split_once(':').map(|(_, name)| name))
}

/// Whether two concrete types are the same type.
pub fn same(a: &TypeRef, b: &TypeRef) -> bool {
    match (a, b) {
        (TypeRef::Key(a), TypeRef::Key(b)) => a == b,
        (TypeRef::Expr(a), TypeRef::Expr(b)) => a == b,
        (TypeRef::Key(key), TypeRef::Expr(expr)) | (TypeRef::Expr(expr), TypeRef::Key(key)) => {
            match expr {
                TypeExpr::Opaque(other) => key == other,
                TypeExpr::Scalar(scalar) => *key == scalar.key(),
                _ => false,
            }
        }
        (TypeRef::Body, TypeRef::Body) => true,
        _ => false,
    }
}

fn write_expr(expr: &TypeExpr, out: &mut String) {
    let list = |items: &mut dyn Iterator<Item = &TypeExpr>, out: &mut String| {
        for (i, item) in items.enumerate() {
            if i > 0 {
                out.push_str(", ");
            }
            write_expr(item, out);
        }
    };
    match expr {
        TypeExpr::Scalar(scalar) => out.push_str(scalar.key().as_str()),
        TypeExpr::Opaque(key) => out.push_str(key.as_str()),
        TypeExpr::Optional(inner) => {
            out.push_str("optional<");
            write_expr(inner, out);
            out.push('>');
        }
        TypeExpr::List(inner) => {
            out.push_str("list<");
            write_expr(inner, out);
            out.push('>');
        }
        TypeExpr::Map(key, value) => {
            out.push_str("map<");
            write_expr(key, out);
            out.push_str(", ");
            write_expr(value, out);
            out.push('>');
        }
        TypeExpr::Tuple(items) => {
            out.push('(');
            list(&mut items.iter(), out);
            out.push(')');
        }
        TypeExpr::Struct(fields) => {
            out.push_str("struct { ");
            for (i, field) in fields.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(field.name);
                out.push_str(": ");
                write_expr(&field.ty, out);
            }
            out.push_str(" }");
        }
        TypeExpr::Enum(variants) => {
            out.push_str("enum { ");
            for (i, variant) in variants.iter().enumerate() {
                if i > 0 {
                    out.push_str(", ");
                }
                out.push_str(variant.name);
            }
            out.push_str(" }");
        }
    }
}

/// The name people read in an error. Never compare it.
pub fn display(ty: &TypeRef) -> String {
    match ty {
        TypeRef::Key(key) => key.as_str().to_string(),
        TypeRef::Expr(expr) => {
            let mut out = String::new();
            write_expr(expr, &mut out);
            out
        }
        TypeRef::Body => "(from the body)".to_string(),
    }
}
