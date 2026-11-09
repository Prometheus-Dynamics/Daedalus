//! Named types. One key has one structure.

use std::collections::BTreeMap;

use daedalus_types::{TypeExpr, TypeKey};

use crate::{RegError, SourceAt};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TypeRecord {
    pub key: TypeKey,
    pub expr: TypeExpr,
    pub at: SourceAt,
}

#[derive(Clone, Debug, Default)]
pub struct TypeTable {
    types: BTreeMap<TypeKey, TypeRecord>,
}

impl TypeTable {
    pub fn new() -> TypeTable {
        TypeTable::default()
    }

    pub fn register(&mut self, record: TypeRecord) -> Result<(), RegError> {
        if let Some(known) = self.types.get(&record.key) {
            if known.expr == record.expr {
                return Ok(());
            }
            return Err(RegError::TypeConflict {
                key: record.key,
                first: known.at,
                second: record.at,
            });
        }
        self.types.insert(record.key.clone(), record);
        Ok(())
    }

    pub fn get(&self, key: &TypeKey) -> Option<&TypeRecord> {
        self.types.get(key)
    }

    /// Sorted by key.
    pub fn records(&self) -> impl Iterator<Item = &TypeRecord> {
        self.types.values()
    }
}
