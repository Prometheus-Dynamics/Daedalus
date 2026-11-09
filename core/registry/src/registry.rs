//! Joining records by id.

use std::collections::BTreeMap;

use daedalus_types::TypeKey;

use crate::{BodyRecord, NodeDecl, NodeRecord, PortDecl, RegError, SourceAt, TypeRef};

/// One node: its declaration and every body registered for it.
#[derive(Clone, Debug)]
pub struct Entry {
    pub decl: &'static NodeDecl,
    /// Sorted by backend name.
    pub bodies: Vec<&'static BodyRecord>,
    /// Input and output port types, once known. A `TypeRef::Body` port fills in on first join.
    resolved: BTreeMap<&'static str, TypeRef>,
}

impl Entry {
    /// The port's type, which is `TypeRef::Body` only if no body has supplied it yet.
    pub fn port_type(&self, port: &PortDecl) -> TypeRef {
        self.resolved
            .get(port.name)
            .cloned()
            .unwrap_or_else(|| port.ty.clone())
    }

    pub fn body(&self, backend: &str) -> Option<&'static BodyRecord> {
        self.bodies
            .iter()
            .copied()
            .find(|body| body.backend == backend)
    }
}

/// A conversion, read from a one-in, one-out node marked as one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConversionEntry {
    pub id: &'static str,
    pub from: TypeKey,
    pub to: TypeKey,
    pub cost: u32,
    pub lossy: bool,
}

#[derive(Clone, Debug, Default)]
pub struct Registry {
    entries: BTreeMap<&'static str, Entry>,
}

/// Compares what every body shares, everything but the source location. Port types are compared separately, because a port's type
/// can arrive from a body.
fn first_difference(a: &NodeDecl, b: &NodeDecl) -> Option<Option<&'static str>> {
    fn ports(a: &[PortDecl], b: &[PortDecl]) -> Option<Option<&'static str>> {
        for (x, y) in a.iter().zip(b) {
            let same = x.name == y.name
                && x.access == y.access
                && x.optional == y.optional
                && x.conditional == y.conditional
                && x.default == y.default
                && x.label == y.label
                && x.unit == y.unit
                && x.group == y.group
                && x.advanced == y.advanced;
            if !same {
                return Some(Some(x.name));
            }
        }
        match a.len().cmp(&b.len()) {
            std::cmp::Ordering::Equal => None,
            std::cmp::Ordering::Greater => Some(Some(a[b.len()].name)),
            std::cmp::Ordering::Less => Some(Some(b[a.len()].name)),
        }
    }
    ports(a.inputs, b.inputs)
        .or_else(|| ports(a.outputs, b.outputs))
        .or_else(|| (a.config != b.config).then_some(None))
        .or_else(|| (a.fanin != b.fanin).then_some(None))
        .or_else(|| (a.fire != b.fire || a.reusable != b.reusable).then_some(None))
        .or_else(|| (a.shareable != b.shareable || a.conversion != b.conversion).then_some(None))
        .or_else(|| (a.summary != b.summary || a.group != b.group).then_some(None))
}

fn effective(port: &PortDecl, body: &BodyRecord) -> Option<TypeRef> {
    match &port.ty {
        TypeRef::Body => body
            .ports
            .iter()
            .find(|(name, _)| *name == port.name)
            .map(|(_, ty)| ty.clone()),
        known => Some(known.clone()),
    }
}

fn valid_id(id: &str) -> bool {
    TypeKey::shared(id).is_ok()
}

impl Registry {
    pub fn new() -> Registry {
        Registry::default()
    }

    /// Joins one record. A rejected record changes nothing.
    pub fn register(&mut self, record: &'static NodeRecord) -> Result<(), RegError> {
        let (decl, body) = (record.decl, &record.body);
        let conflict = |first: SourceAt, port| RegError::DeclConflict {
            id: decl.id.to_string(),
            first,
            second: decl.at,
            port,
        };
        if !valid_id(decl.id) {
            return Err(RegError::InvalidId {
                id: decl.id.to_string(),
                at: decl.at,
            });
        }
        if body.backend.is_empty() {
            return Err(RegError::UnknownBackend {
                id: decl.id.to_string(),
                at: body.at,
            });
        }
        let mut updates = Vec::new();
        for port in decl.inputs.iter().chain(decl.outputs) {
            if let Some(ty) = effective(port, body) {
                updates.push((port.name, ty));
            }
        }
        let Some(entry) = self.entries.get_mut(decl.id) else {
            self.entries.insert(
                decl.id,
                Entry {
                    decl,
                    bodies: vec![body],
                    resolved: updates.into_iter().collect(),
                },
            );
            return Ok(());
        };
        if let Some(port) = first_difference(entry.decl, decl) {
            return Err(conflict(entry.decl.at, port));
        }
        if let Some(known) = entry.body(body.backend) {
            return Err(RegError::DuplicateBody {
                id: decl.id.to_string(),
                backend: body.backend,
                first: known.at,
                second: body.at,
            });
        }
        for (name, ty) in &updates {
            if entry.resolved.get(name).is_some_and(|known| known != ty) {
                return Err(conflict(entry.decl.at, Some(name)));
            }
        }
        entry.resolved.extend(updates);
        let at = entry
            .bodies
            .partition_point(|known| known.backend < body.backend);
        entry.bodies.insert(at, body);
        Ok(())
    }

    /// Joins every record, and reports every error rather than the first.
    pub fn register_all(&mut self, records: &[&'static NodeRecord]) -> Result<(), Vec<RegError>> {
        let errors: Vec<RegError> = records
            .iter()
            .filter_map(|record| self.register(record).err())
            .collect();
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    pub fn entry(&self, id: &str) -> Option<&Entry> {
        self.entries.get(id)
    }

    /// Sorted by id.
    pub fn entries(&self) -> impl Iterator<Item = &Entry> {
        self.entries.values()
    }

    /// Every node marked as a conversion whose two types are known.
    pub fn conversions(&self) -> Vec<ConversionEntry> {
        let key = |entry: &Entry, port: &PortDecl| match entry.port_type(port) {
            TypeRef::Key(key) => Some(key),
            _ => None,
        };
        self.entries()
            .filter_map(|entry| {
                let meta = entry.decl.conversion?;
                let ([input], [output]) = (entry.decl.inputs, entry.decl.outputs) else {
                    return None;
                };
                Some(ConversionEntry {
                    id: entry.decl.id,
                    from: key(entry, input)?,
                    to: key(entry, output)?,
                    cost: meta.cost,
                    lossy: meta.lossy,
                })
            })
            .collect()
    }
}
