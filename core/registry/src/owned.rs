//! Declarations that arrive at run time, from a worker starting up. Records are `'static`, so an
//! owned declaration is promoted once. A host registers each worker's nodes when the worker
//! starts, so this grows with the number of node kinds and not with time.

use crate::{
    BodyRecord, Fire, NoCode, NodeDecl, NodeRecord, PortDecl, RegError, Registry, SourceAt, TypeRef,
};

#[derive(Clone, Debug)]
pub struct OwnedPort {
    pub name: String,
    pub ty: TypeRef,
    pub optional: bool,
}

#[derive(Clone, Debug)]
pub struct OwnedBody {
    pub backend: String,
    pub text: String,
}

#[derive(Clone, Debug)]
pub struct OwnedDecl {
    pub id: String,
    pub source: String,
    pub inputs: Vec<OwnedPort>,
    pub outputs: Vec<OwnedPort>,
    pub body: OwnedBody,
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

impl Registry {
    /// Joins a declaration a worker sent, under the same rules as a compiled-in record.
    pub fn register_owned(&mut self, decl: OwnedDecl) -> Result<(), RegError> {
        let at = SourceAt(leak(decl.source));
        let node: &'static NodeDecl = Box::leak(Box::new(NodeDecl {
            id: leak(decl.id),
            at,
            summary: "",
            inputs: ports(decl.inputs),
            outputs: ports(decl.outputs),
            config: &[],
            fanin: &[],
            fire: Fire::default(),
            reusable: false,
            shareable: false,
            conversion: None,
            group: None,
        }));
        let record: &'static NodeRecord = Box::leak(Box::new(NodeRecord {
            decl: node,
            body: BodyRecord {
                backend: leak(decl.body.backend),
                at,
                needs: &[],
                state: None,
                ports: &[],
                memory: &[],
                code: &NoCode,
                text: leak(decl.body.text),
            },
        }));
        self.register(record)
    }
}
