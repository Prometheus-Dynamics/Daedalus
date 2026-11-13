//! Declarations that arrive at run time, from a worker or a plugin starting up. Records are
//! `'static`, so an owned declaration is promoted once. The same declaration arriving again reuses
//! its record, so the promoted set grows with the distinct declarations and not with reloads.

#![cfg(feature = "std")]

use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use crate::{
    BodyRecord, Conversion, Fire, NoCode, NodeDecl, NodeRecord, PortDecl, RegError, Registry,
    SourceAt, TypeRef,
};

#[derive(Clone, Debug)]
pub struct OwnedPort {
    pub name: String,
    pub ty: TypeRef,
    pub optional: bool,
}

#[derive(Clone)]
pub struct OwnedBody {
    pub backend: String,
    pub text: String,
    /// The backend's own payload, for a body whose code is in this process: what the backend
    /// named in `backend` reads as a body's code. `None` for a body that is only text, as a
    /// worker's is. The registry never looks inside it.
    pub code: Option<Arc<dyn Any + Send + Sync>>,
}

impl OwnedBody {
    /// A body that is only text: what a declaration that arrived as data has.
    pub fn text(backend: impl Into<String>, text: impl Into<String>) -> OwnedBody {
        OwnedBody {
            backend: backend.into(),
            text: text.into(),
            code: None,
        }
    }

    /// A body with a backend's payload, made in this process.
    pub fn code(
        backend: impl Into<String>,
        text: impl Into<String>,
        code: Arc<dyn Any + Send + Sync>,
    ) -> OwnedBody {
        OwnedBody {
            backend: backend.into(),
            text: text.into(),
            code: Some(code),
        }
    }
}

// The payload is named by where it is: two declarations with the same text and different code
// are different declarations.
impl fmt::Debug for OwnedBody {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OwnedBody")
            .field("backend", &self.backend)
            .field("text", &self.text)
            .field(
                "code",
                &self
                    .code
                    .as_ref()
                    .map(|code| Arc::as_ptr(code).cast::<()>()),
            )
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct OwnedDecl {
    pub id: String,
    pub source: String,
    pub inputs: Vec<OwnedPort>,
    pub outputs: Vec<OwnedPort>,
    pub body: OwnedBody,
    /// Set for a one-in, one-out node that is a conversion between its two types.
    pub conversion: Option<Conversion>,
}

fn leak(text: String) -> &'static str {
    Box::leak(text.into_boxed_str())
}

fn ports(ports: Vec<OwnedPort>) -> &'static [PortDecl] {
    Box::leak(
        ports
            .into_iter()
            .map(|port| PortDecl {
                optional: port.optional,
                ..PortDecl::new(leak(port.name), port.ty)
            })
            .collect(),
    )
}

/// The declarations promoted so far, keyed by their full content. A plugin that is loaded again
/// finds its declarations here, so promotion happens once per distinct declaration.
fn promoted() -> &'static Mutex<BTreeMap<String, &'static NodeRecord>> {
    static PROMOTED: OnceLock<Mutex<BTreeMap<String, &'static NodeRecord>>> = OnceLock::new();
    PROMOTED.get_or_init(|| Mutex::new(BTreeMap::new()))
}

/// How many distinct declarations have been promoted into `'static` records. Loading an
/// unchanged plugin again adds none, so this does not grow with reloads.
pub fn promoted_declarations() -> usize {
    promoted()
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .len()
}

/// Promotes a declaration into records that live for the whole process.
fn promote(decl: OwnedDecl) -> &'static NodeRecord {
    let at = SourceAt(leak(decl.source));
    let node: &'static NodeDecl = Box::leak(Box::new(NodeDecl {
        id: leak(decl.id),
        at,
        summary: "",
        description: "",
        inputs: ports(decl.inputs),
        outputs: ports(decl.outputs),
        config: &[],
        fanin: &[],
        fire: Fire::default(),
        reusable: false,
        shareable: false,
        conversion: decl.conversion,
        group: None,
    }));
    Box::leak(Box::new(NodeRecord {
        decl: node,
        body: BodyRecord {
            backend: leak(decl.body.backend),
            at,
            needs: &[],
            state: None,
            ports: &[],
            memory: &[],
            // The record lives for the whole process, so its payload does too.
            code: match decl.body.code {
                Some(code) => &**Box::leak(Box::new(code)),
                None => &NoCode,
            },
            text: leak(decl.body.text),
        },
    }))
}

impl Registry {
    /// Joins a declaration a worker sent, under the same rules as a compiled-in record. The
    /// promoted record is reused when the same declaration arrives again.
    pub fn register_owned(&mut self, decl: OwnedDecl) -> Result<(), RegError> {
        let key = format!("{decl:?}");
        let record = {
            let mut known = promoted().lock().unwrap_or_else(PoisonError::into_inner);
            match known.get(&key) {
                Some(record) => *record,
                None => {
                    let record = promote(decl);
                    known.insert(key, record);
                    record
                }
            }
        };
        self.register(record)
    }
}
