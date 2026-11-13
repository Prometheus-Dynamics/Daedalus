//! A worker's own declarations, joined into a registry.
//!
//! A worker says what it serves in its hello: each node's id, ports and types. A host that has
//! never seen the worker's nodes doesn't have to write them out again: the hello is enough to
//! register them. A node the registry already has is checked against it, as before.

use std::path::Path;

use daedalus_registry::{OwnedBody, OwnedDecl, OwnedPort, Registry, TypeRef};
use daedalus_types::{Field, TypeExpr, TypeKey, Variant};
use daedalus_wire::{Declaration, Hello, WireType};

use crate::error::ProcessError;
use crate::region::BACKEND_ID;

fn leak<T>(value: T) -> &'static T {
    Box::leak(Box::new(value))
}

fn leak_str(text: &str) -> &'static str {
    Box::leak(text.to_owned().into_boxed_str())
}

fn leak_all<T>(items: Vec<T>) -> &'static [T] {
    Box::leak(items.into_boxed_slice())
}

/// The registry's type expression for a type a worker declared. `Err` names an opaque type whose
/// key isn't a valid one. The expression is kept for the life of the process, as a registry's
/// records are.
pub fn type_expr(ty: &WireType) -> Result<TypeExpr, String> {
    Ok(match ty {
        WireType::Scalar(scalar) => TypeExpr::Scalar(*scalar),
        WireType::Opaque(key) => TypeExpr::Opaque(
            TypeKey::shared(key).map_err(|_| format!("`{key}` is not a valid type key"))?,
        ),
        WireType::Optional(inner) => TypeExpr::Optional(leak(type_expr(inner)?)),
        WireType::List(inner) => TypeExpr::List(leak(type_expr(inner)?)),
        WireType::Map(key, value) => TypeExpr::Map(leak(type_expr(key)?), leak(type_expr(value)?)),
        WireType::Tuple(items) => TypeExpr::Tuple(leak_all(
            items.iter().map(type_expr).collect::<Result<_, _>>()?,
        )),
        WireType::Struct(fields) => TypeExpr::Struct(leak_all(
            fields
                .iter()
                .map(|(name, ty)| {
                    Ok(Field {
                        name: leak_str(name),
                        ty: type_expr(ty)?,
                    })
                })
                .collect::<Result<_, String>>()?,
        )),
        WireType::Enum(variants) => TypeExpr::Enum(leak_all(
            variants
                .iter()
                .map(|(name, payload)| {
                    Ok(Variant {
                        name: leak_str(name),
                        payload: payload.as_ref().map(type_expr).transpose()?,
                    })
                })
                .collect::<Result<_, String>>()?,
        )),
    })
}

/// What a worker declared for one node, as a declaration a registry takes. Its body is the
/// process backend's, so the node is served by a worker.
pub fn owned_decl(source: &str, decl: &Declaration) -> Result<OwnedDecl, String> {
    let port = |port: &daedalus_wire::PortSpec| {
        Ok(OwnedPort {
            name: port.name.clone(),
            ty: TypeRef::Expr(
                type_expr(&port.ty).map_err(|why| format!("port {}: {why}", port.name))?,
            ),
            optional: false,
        })
    };
    Ok(OwnedDecl {
        id: decl.id.clone(),
        source: source.to_owned(),
        inputs: decl
            .inputs
            .iter()
            .map(port)
            .collect::<Result<_, String>>()?,
        outputs: decl
            .outputs
            .iter()
            .map(port)
            .collect::<Result<_, String>>()?,
        body: OwnedBody::text(BACKEND_ID, decl.id.clone()),
        conversion: None,
    })
}

/// Registers every node of `hello` that `registry` doesn't have, and returns their ids. A node
/// it has is left for the check that follows.
pub(crate) fn register_new(
    program: &Path,
    hello: &Hello,
    registry: &mut Registry,
) -> Result<Vec<String>, ProcessError> {
    let source = program.display().to_string();
    let mut added = Vec::new();
    for decl in &hello.nodes {
        if registry.entry(&decl.id).is_some() {
            continue;
        }
        let fail = |reason: String| ProcessError::Declaration {
            program: program.to_path_buf(),
            node: decl.id.clone(),
            reason,
        };
        let owned = owned_decl(&source, decl).map_err(fail)?;
        registry
            .register_owned(owned)
            .map_err(|error| fail(format!("the registry refused it: {error:?}")))?;
        added.push(decl.id.clone());
    }
    Ok(added)
}
