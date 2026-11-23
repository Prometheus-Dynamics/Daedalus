//! Works out each port's constant and checks it against the port's type.

use std::collections::BTreeMap;
use std::fmt;

use daedalus_graph::{Lit, LitField, NodeKindDoc};
use daedalus_registry::TypeRef;
use daedalus_types::{Scalar, TypeExpr, TypeKey};

use super::expand::lit_of;
use super::port_types::{display, generic_var};
use super::{Port, Work};
use crate::pipeline::SolveError;

#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConstError {
    TypeMismatch {
        node: String,
        port: String,
        expected: String,
        found: String,
    },
    /// A const names neither an input port of the node nor a field of its config.
    NoPortOrField {
        node: String,
        name: String,
        /// The fields of the node's config, so a misspelt one is plain.
        fields: Vec<String>,
    },
    /// A config field set both in the packed config and by a const of its own.
    ConfigFieldTwice { node: String, field: String },
    /// The graph gives a const to an input that an edge also feeds.
    ConstOnWired {
        node: String,
        port: String,
        /// The edge's source, as `node.port`.
        from: String,
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
            ConstError::NoPortOrField { node, name, fields } => {
                write!(f, "node `{node}` has no input port `{name}`")?;
                match fields.is_empty() {
                    true => write!(f, " and no config fields"),
                    false => write!(
                        f,
                        ", and its config has no field of that name: its fields are {}",
                        fields.join(", ")
                    ),
                }
            }
            ConstError::ConfigFieldTwice { node, field } => write!(
                f,
                "node `{node}` sets config field `{field}` both in its config and by itself"
            ),
            ConstError::ConstOnWired { node, port, from } => write!(
                f,
                "node `{node}` input `{port}` has a const and is also fed by `{from}`: remove \
                 the const or the edge"
            ),
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

fn mismatch(node: &str, port: &str, ty: &TypeRef, value: &Lit) -> SolveError {
    SolveError::Consts(ConstError::TypeMismatch {
        node: node.to_string(),
        port: port.to_string(),
        expected: display(ty),
        found: describe(value).to_string(),
    })
}

/// Puts a config field's own const into the packed config, so a backend reads one form.
fn fold(
    consts: &mut BTreeMap<String, Lit>,
    config: &str,
    field: &str,
    value: &Lit,
    node: &str,
) -> Result<(), ConstError> {
    let packed = consts
        .entry(config.to_string())
        .or_insert_with(|| Lit::Struct(Vec::new()));
    let Lit::Struct(set) = packed else {
        // A field can't join a packed config that isn't a struct of fields.
        return Err(ConstError::TypeMismatch {
            node: node.to_string(),
            port: config.to_string(),
            expected: "a struct of its fields".to_string(),
            found: describe(packed).to_string(),
        });
    };
    if set.iter().any(|one| one.name == field) {
        return Err(ConstError::ConfigFieldTwice {
            node: node.to_string(),
            field: field.to_string(),
        });
    }
    set.push(LitField {
        name: field.to_string(),
        value: value.clone(),
    });
    Ok(())
}

pub fn run(work: &mut Work<'_>) {
    let catalog = work.catalog;
    let mut errors = Vec::new();
    // Each wired input, and where its edge comes from.
    let mut wired: BTreeMap<(usize, &str), String> = BTreeMap::new();
    for edge in &work.graph.edges {
        let from = work
            .graph
            .nodes
            .get(edge.from.node as usize)
            .map_or("?", |node| node.doc.id.as_str());
        wired
            .entry((edge.to.node as usize, edge.to.port.as_str()))
            .or_insert_with(|| format!("{from}.{}", edge.from.port));
    }
    let wired: BTreeMap<(usize, String), String> = wired
        .into_iter()
        .map(|((node, port), from)| ((node, port.to_string()), from))
        .collect();
    for (at, node) in work.graph.nodes.iter_mut().enumerate() {
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
        let mut fields: Vec<(&'static str, &String, &Lit)> = Vec::new();
        for (name, value) in &node.doc.consts {
            let port: Option<&Port> = node.inputs.iter().find(|port| port.name == *name);
            let config = entry.decl.config.iter().find(|config| config.name == *name);
            let field = || {
                entry.decl.config.iter().find_map(|config| {
                    let field = config.fields.iter().find(|field| field.name == *name)?;
                    Some((config.name, field))
                })
            };
            // A wired input reads its edge. A const the graph writes there would be a second
            // value for one port: a declared default is the only fallback a wired input has.
            if let (Some(_), Some(from)) = (port, wired.get(&(at, name.clone()))) {
                errors.push(SolveError::Consts(ConstError::ConstOnWired {
                    node: node.doc.id.clone(),
                    port: name.clone(),
                    from: from.clone(),
                }));
                continue;
            }
            let (ty, optional) = match (port, config) {
                (Some(port), _) => (port.ty.clone(), port.optional),
                (None, Some(config)) => (config.ty.clone(), false),
                (None, None) => match field() {
                    Some((config, field)) => {
                        if accepts(&field.ty, field.optional, value) {
                            fields.push((config, name, value));
                        } else {
                            errors.push(mismatch(&node.doc.id, name, &field.ty, value));
                        }
                        continue;
                    }
                    None => {
                        errors.push(SolveError::Consts(ConstError::NoPortOrField {
                            node: node.doc.id.clone(),
                            name: name.clone(),
                            fields: entry
                                .decl
                                .config
                                .iter()
                                .flat_map(|config| config.fields)
                                .map(|field| field.name.to_string())
                                .collect(),
                        }));
                        continue;
                    }
                },
            };
            if !accepts(&ty, optional, value) {
                errors.push(mismatch(&node.doc.id, name, &ty, value));
            }
            node.consts.insert(name.clone(), value.clone());
        }
        for (config, name, value) in fields {
            if let Err(error) = fold(&mut node.consts, config, name, value, &node.doc.id) {
                errors.push(SolveError::Consts(error));
            }
        }
        let consts = &node.consts;
        node.required.retain(|port| !consts.contains_key(port));
    }
    work.errors.extend(errors);
}
