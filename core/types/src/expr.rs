//! The structure of a type, as a constant.

use crate::hash::{FNV1A64_OFFSET, fnv1a64_extend};
use crate::{Scalar, TypeKey};

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Field {
    pub name: &'static str,
    pub ty: TypeExpr,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Variant {
    pub name: &'static str,
    pub payload: Option<TypeExpr>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TypeExpr {
    Scalar(Scalar),
    Opaque(TypeKey),
    Optional(&'static TypeExpr),
    List(&'static TypeExpr),
    Map(&'static TypeExpr, &'static TypeExpr),
    Tuple(&'static [TypeExpr]),
    Struct(&'static [Field]),
    Enum(&'static [Variant]),
}

fn write_text(text: &str, out: &mut dyn FnMut(&[u8])) {
    out(&(text.len() as u32).to_le_bytes());
    out(text.as_bytes());
}

fn write_count(count: usize, out: &mut dyn FnMut(&[u8])) {
    out(&(count as u32).to_le_bytes());
}

impl TypeExpr {
    /// The canonical byte stream: a tag per variant, then little-endian lengths and bytes.
    /// Struct field order is part of a type's identity.
    pub fn encode(&self, out: &mut dyn FnMut(&[u8])) {
        match self {
            TypeExpr::Scalar(scalar) => out(&[0, *scalar as u8]),
            TypeExpr::Opaque(key) => {
                out(&[1]);
                write_text(key.as_str(), out);
            }
            TypeExpr::Optional(inner) => {
                out(&[2]);
                inner.encode(out);
            }
            TypeExpr::List(inner) => {
                out(&[3]);
                inner.encode(out);
            }
            TypeExpr::Map(key, value) => {
                out(&[4]);
                key.encode(out);
                value.encode(out);
            }
            TypeExpr::Tuple(items) => {
                out(&[5]);
                write_count(items.len(), out);
                items.iter().for_each(|item| item.encode(out));
            }
            TypeExpr::Struct(fields) => {
                out(&[6]);
                write_count(fields.len(), out);
                for field in *fields {
                    write_text(field.name, out);
                    field.ty.encode(out);
                }
            }
            TypeExpr::Enum(variants) => {
                out(&[7]);
                write_count(variants.len(), out);
                for variant in *variants {
                    write_text(variant.name, out);
                    match &variant.payload {
                        None => out(&[0]),
                        Some(payload) => {
                            out(&[1]);
                            payload.encode(out);
                        }
                    }
                }
            }
        }
    }

    /// FNV-1a over [`TypeExpr::encode`]. Equal expressions hash equal.
    pub fn structural_hash(&self) -> u64 {
        let mut hash = FNV1A64_OFFSET;
        self.encode(&mut |bytes| hash = fnv1a64_extend(hash, bytes));
        hash
    }
}
