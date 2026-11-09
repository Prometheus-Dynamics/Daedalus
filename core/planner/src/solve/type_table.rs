//! The plan's table of types.

use std::collections::HashMap;

use daedalus_registry::TypeRef;
use daedalus_types::{TypeExpr, TypeIndex};

use super::consts::scalar_of;

/// Each distinct type gets the next index, in the order first seen.
#[derive(Clone, Debug, Default)]
pub struct TypeTable {
    exprs: Vec<TypeExpr>,
    index: HashMap<TypeExpr, TypeIndex>,
}

fn expr_of(ty: &TypeRef) -> Option<TypeExpr> {
    match ty {
        TypeRef::Key(key) => Some(match scalar_of(key) {
            Some(scalar) => TypeExpr::Scalar(scalar),
            None => TypeExpr::Opaque(key.clone()),
        }),
        TypeRef::Expr(expr) => Some(expr.clone()),
        TypeRef::Body => None,
    }
}

impl TypeTable {
    /// Adds a type, returning its index. `None` for a type the body was meant to supply.
    pub fn add(&mut self, ty: &TypeRef) -> Option<TypeIndex> {
        let expr = expr_of(ty)?;
        if let Some(index) = self.index.get(&expr) {
            return Some(*index);
        }
        let index = TypeIndex(self.exprs.len() as u32);
        self.exprs.push(expr.clone());
        self.index.insert(expr, index);
        Some(index)
    }

    pub fn get(&self, ty: &TypeRef) -> Option<TypeIndex> {
        self.index.get(&expr_of(ty)?).copied()
    }

    pub fn exprs(&self) -> &[TypeExpr] {
        &self.exprs
    }
}
