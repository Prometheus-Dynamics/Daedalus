//! Port tables need `'static` strings and slices, and this crate has no `unsafe` to fake one. Each
//! distinct value is kept once for the life of the process, so reloading a graph adds nothing and
//! memory is bounded by how many different graphs were ever loaded.

#![cfg(feature = "std")]

use std::collections::HashMap;
use std::format;
use std::string::String;
use std::sync::{Mutex, OnceLock, PoisonError};
use std::vec::Vec;

use daedalus_types::TypeKey;

use crate::PortSpec;

struct Table<V: ?Sized + 'static>(OnceLock<Mutex<HashMap<String, &'static V>>>);

impl<V: ?Sized> Table<V> {
    const fn new() -> Table<V> {
        Table(OnceLock::new())
    }

    fn get(&'static self, key: String, make: impl FnOnce() -> &'static V) -> &'static V {
        let map = self.0.get_or_init(Mutex::default);
        let mut map = map.lock().unwrap_or_else(PoisonError::into_inner);
        map.entry(key).or_insert_with(make)
    }
}

static TEXT: Table<str> = Table::new();
static KEY: Table<TypeKey> = Table::new();
static KEYS: Table<[TypeKey]> = Table::new();
static WAKE: Table<[u32]> = Table::new();
static PORTS: Table<[PortSpec<'static>]> = Table::new();

pub(crate) fn intern(text: &str) -> &'static str {
    TEXT.get(String::from(text), || String::from(text).leak())
}

pub(crate) fn intern_key(text: &str) -> &'static TypeKey {
    KEY.get(String::from(text), || {
        std::boxed::Box::leak(std::boxed::Box::new(TypeKey::new(intern(text))))
    })
}

pub(crate) fn intern_keys(texts: &[String]) -> &'static [TypeKey] {
    KEYS.get(texts.join("\n"), || {
        texts
            .iter()
            .map(|text| TypeKey::new(intern(text)))
            .collect::<Vec<_>>()
            .leak()
    })
}

pub(crate) fn intern_wake(steps: &[u32]) -> &'static [u32] {
    WAKE.get(format!("{steps:?}"), || steps.to_vec().leak())
}

pub(crate) fn intern_ports(specs: Vec<PortSpec<'static>>) -> &'static [PortSpec<'static>] {
    PORTS.get(format!("{specs:?}"), || specs.leak())
}
