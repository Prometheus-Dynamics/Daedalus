//! Works out each port's constant and checks it against the port's type.

use std::fmt;

use daedalus_graph::{Lit, NodeKindDoc};
use daedalus_registry::TypeRef;
use daedalus_types::{Scalar, TypeExpr, TypeKey};

use super::expand::lit_of;
use super::port_types::{display, generic_var};
use super::{Port, Work};
use crate::pipeline::SolveError;
use crate::solve::ports::{Direction, PortError};

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConstError {
    TypeMismatch {
        node: String,
        port: String,
        expected: String,
        found: String,
    },
}

impl fmt::Display for ConstError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConstError::TypeMismatch {
                node,
                port,
                expected,
                found,
            } => {
                write!(
                    f,
                    "node `{node}` port `{port}` takes `{expected}`, but its const is {found}"
                )
            }
        }
    }
}

impl std::error::Error for ConstError {}

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

pub(crate) fn scalar_of(key: &TypeKey) -> Option<Scalar> {
    SCALARS.into_iter().find(|scalar| scalar.key() == *key)
}

fn int_fits(scalar: Scalar, value: i64) -> bool {
    match scalar {
        Scalar::I8 => i8::try_from(value).is_ok(),
        Scalar::I16 => i16::try_from(value).is_ok(),
        Scalar::I32 => i32::try_from(value).is_ok(),
        Scalar::Int | Scalar::ISize => true,
        Scalar::U8 => u8::try_from(value).is_ok(),
        Scalar::U16 => u16::try_from(value).is_ok(),
        Scalar::U32 => u32::try_from(value).is_ok(),
        Scalar::U64 | Scalar::USize => value >= 0,
        Scalar::F32 | Scalar::Float => true,
        _ => false,
    }
}

fn scalar_accepts(scalar: Scalar, value: &Lit) -> bool {
    match value {
        Lit::Unit => scalar == Scalar::Unit,
        Lit::Bool(_) => scalar == Scalar::Bool,
        Lit::Int(value) => int_fits(scalar, *value),
        Lit::Float(_) => matches!(scalar, Scalar::F32 | Scalar::Float),
        Lit::Str(_) => scalar == Scalar::String,
        Lit::Bytes(_) => scalar == Scalar::Bytes,
        _ => false,
    }
}

fn expr_accepts(expr: &TypeExpr, value: &Lit) -> bool {
    match (expr, value) {
        (TypeExpr::Scalar(scalar), value) => scalar_accepts(*scalar, value),
        (TypeExpr::Opaque(_), _) => true,
        (TypeExpr::Optional(_), Lit::Unit) => true,
        (TypeExpr::Optional(inner), value) => expr_accepts(inner, value),
        (TypeExpr::List(inner), Lit::List(items)) => items.iter().all(|i| expr_accepts(inner, i)),
        (TypeExpr::Tuple(types), Lit::List(items)) => {
            types.len() == items.len()
                && types
                    .iter()
                    .zip(items)
                    .all(|(ty, item)| expr_accepts(ty, item))
        }
        (TypeExpr::Struct(fields), Lit::Struct(values)) => {
            fields.len() == values.len()
                && fields.iter().all(|field| {
                    values
                        .iter()
                        .find(|value| value.name == field.name)
                        .is_some_and(|value| expr_accepts(&field.ty, &value.value))
                })
        }
        (TypeExpr::Enum(variants), Lit::Enum { name, payload }) => variants
            .iter()
            .find(|variant| variant.name == name)
            .is_some_and(|variant| match (&variant.payload, payload) {
                (None, None) => true,
                (Some(ty), Some(value)) => expr_accepts(ty, value),
                _ => false,
            }),
        _ => false,
    }
}

fn accepts(ty: &TypeRef, optional: bool, value: &Lit) -> bool {
    if generic_var(ty).is_some() {
        return true;
    }
    if *value == Lit::Unit && optional {
        return true;
    }
    match ty {
        TypeRef::Key(key) => match scalar_of(key) {
            Some(scalar) => scalar_accepts(scalar, value),
            None => *value != Lit::Unit,
        },
        TypeRef::Expr(expr) => expr_accepts(expr, value),
        TypeRef::Body => true,
    }
}

fn describe(value: &Lit) -> &'static str {
    match value {
        Lit::Unit => "unit",
        Lit::Bool(_) => "a bool",
        Lit::Int(_) => "an int out of range or of the wrong type",
        Lit::Float(_) => "a float",
        Lit::Str(_) => "a string",
        Lit::Bytes(_) => "bytes",
        Lit::List(_) => "a list",
        Lit::Struct(_) => "a struct",
        Lit::Enum { .. } => "an enum",
    }
}

pub fn run(work: &mut Work<'_>) {
    let catalog = work.catalog;
    let mut errors = Vec::new();
    for node in &mut work.graph.nodes {
        if node.doc.kind == NodeKindDoc::HostBridge {
            continue;
        }
        let Some(entry) = catalog.entry(&node.doc.id) else {
            continue;
        };
        for port in &node.inputs {
            if !node.doc.consts.contains_key(&port.name)
                && let Some(default) = &port.default
            {
                node.consts.insert(port.name.clone(), lit_of(default));
            }
        }
        for (name, value) in &node.doc.consts {
            let port: Option<&Port> = node.inputs.iter().find(|port| port.name == *name);
            let config = entry.decl.config.iter().find(|config| config.name == *name);
            let (ty, optional) = match (port, config) {
                (Some(port), _) => (port.ty.clone(), port.optional),
                (None, Some(config)) => (config.ty.clone(), false),
                (None, None) => {
                    errors.push(SolveError::Ports(PortError::PortMissing {
                        node: node.doc.id.clone(),
                        direction: Direction::Input,
                        port: name.clone(),
                    }));
                    continue;
                }
            };
            if !accepts(&ty, optional, value) {
                errors.push(SolveError::Consts(ConstError::TypeMismatch {
                    node: node.doc.id.clone(),
                    port: name.clone(),
                    expected: display(&ty),
                    found: describe(value).to_string(),
                }));
            }
            node.consts.insert(name.clone(), value.clone());
        }
        let consts = &node.consts;
        node.required.retain(|port| !consts.contains_key(port));
    }
    work.errors.extend(errors);
}
