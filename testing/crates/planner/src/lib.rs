//! Building registries and graphs for the planner's tests.

#![forbid(unsafe_code)]

use daedalus_graph::{EdgeDoc, EdgePolicyDoc, NodeDoc, PortRefDoc};
use daedalus_registry::{
    BodyRecord, Conversion, Fire, NoCode, NodeDecl, NodeRecord, PortDecl, Registry, SourceAt,
    TypeRef,
};
use daedalus_types::{Literal, Scalar, TypeKey};

fn leak_str(text: impl Into<String>) -> &'static str {
    Box::leak(text.into().into_boxed_str())
}

fn leak<T>(items: Vec<T>) -> &'static [T] {
    Box::leak(items.into_boxed_slice())
}

pub fn key(name: &str) -> TypeRef {
    TypeRef::Key(TypeKey::shared(name).unwrap())
}

pub fn u8_ty() -> TypeRef {
    TypeRef::Key(Scalar::U8.key())
}

pub fn u32_ty() -> TypeRef {
    TypeRef::Key(Scalar::U32.key())
}

pub fn f64_ty() -> TypeRef {
    TypeRef::Key(Scalar::Float.key())
}

/// A declaration under construction, and the backends it has bodies for.
pub struct Decl {
    id: &'static str,
    inputs: Vec<PortDecl>,
    outputs: Vec<PortDecl>,
    fire: Fire,
    reusable: bool,
    shareable: bool,
    conversion: Option<Conversion>,
    group: Option<&'static daedalus_graph::GroupDef>,
    bodies: Vec<(&'static str, &'static str, &'static [&'static str])>,
}

pub fn decl(id: &str) -> Decl {
    Decl {
        id: leak_str(id),
        inputs: Vec::new(),
        outputs: Vec::new(),
        fire: Fire::All,
        reusable: false,
        shareable: false,
        conversion: None,
        group: None,
        bodies: Vec::new(),
    }
}

impl Decl {
    pub fn input(mut self, name: &str, ty: TypeRef) -> Decl {
        self.inputs.push(PortDecl::new(leak_str(name), ty));
        self
    }

    pub fn input_with(mut self, port: PortDecl) -> Decl {
        self.inputs.push(port);
        self
    }

    pub fn optional(mut self, name: &str, ty: TypeRef) -> Decl {
        self.inputs.push(PortDecl {
            optional: true,
            ..PortDecl::new(leak_str(name), ty)
        });
        self
    }

    pub fn defaulted(mut self, name: &str, ty: TypeRef, value: Literal) -> Decl {
        self.inputs.push(PortDecl {
            default: Some(value),
            ..PortDecl::new(leak_str(name), ty)
        });
        self
    }

    pub fn output(mut self, name: &str, ty: TypeRef) -> Decl {
        self.outputs.push(PortDecl::new(leak_str(name), ty));
        self
    }

    pub fn fire(mut self, fire: Fire) -> Decl {
        self.fire = fire;
        self
    }

    pub fn shareable(mut self) -> Decl {
        self.shareable = true;
        self
    }

    pub fn conversion(mut self, cost: u32, lossy: bool) -> Decl {
        self.conversion = Some(Conversion { cost, lossy });
        self
    }

    pub fn group(mut self, group: &'static daedalus_graph::GroupDef) -> Decl {
        self.group = Some(group);
        self
    }

    /// A body whose `text` is the node id, as the sim backend expects.
    pub fn body(mut self, backend: &str) -> Decl {
        self.bodies.push((leak_str(backend), self.id, &[]));
        self
    }

    pub fn body_needing(mut self, backend: &str, needs: &'static [&'static str]) -> Decl {
        self.bodies.push((leak_str(backend), self.id, needs));
        self
    }

    pub fn add(self, registry: &mut Registry) {
        let node: &'static NodeDecl = Box::leak(Box::new(NodeDecl {
            id: self.id,
            at: SourceAt("test"),
            summary: "",
            description: "",
            inputs: leak(self.inputs),
            outputs: leak(self.outputs),
            config: &[],
            fanin: &[],
            fire: self.fire,
            reusable: self.reusable,
            shareable: self.shareable,
            conversion: self.conversion,
            group: self.group,
        }));
        for (backend, text, needs) in self.bodies {
            let record: &'static NodeRecord = Box::leak(Box::new(NodeRecord {
                decl: node,
                body: BodyRecord {
                    backend,
                    at: SourceAt("test"),
                    needs,
                    state: None,
                    ports: &[],
                    memory: &[],
                    code: &NoCode,
                    text,
                },
            }));
            registry.register(record).unwrap();
        }
    }
}

pub fn node(id: &str) -> NodeDoc {
    NodeDoc::new(id)
}

pub fn edge(from: u32, from_port: &str, to: u32, to_port: &str) -> EdgeDoc {
    EdgeDoc {
        from: PortRefDoc {
            node: from,
            port: from_port.to_string(),
        },
        to: PortRefDoc {
            node: to,
            port: to_port.to_string(),
        },
        policy: EdgePolicyDoc::default(),
    }
}
