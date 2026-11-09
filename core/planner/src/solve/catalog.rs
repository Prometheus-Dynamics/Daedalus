//! The registry, as a planning run sees it: lookups and nothing else.

use daedalus_registry::{ConversionEntry, Entry, NodeDecl, Registry};

#[derive(Clone, Copy)]
pub struct Catalog<'a> {
    registry: &'a Registry,
}

impl<'a> Catalog<'a> {
    pub fn new(registry: &'a Registry) -> Catalog<'a> {
        Catalog { registry }
    }

    pub fn entry(&self, id: &str) -> Option<&'a Entry> {
        self.registry.entry(id)
    }

    pub fn decl(&self, id: &str) -> Option<&'static NodeDecl> {
        self.entry(id).map(|entry| entry.decl)
    }

    pub fn conversions(&self) -> Vec<ConversionEntry> {
        self.registry.conversions()
    }

    /// Sorted.
    pub fn ids(&self) -> impl Iterator<Item = &'static str> + 'a {
        self.registry.entries().map(|entry| entry.decl.id)
    }
}
