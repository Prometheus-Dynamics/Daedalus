//! What one side knows about the values it may read and write: each port's type, and the type
//! keys it has codecs for. Built from a hello, so both sides agree on it.

use crate::WireError;
use crate::hello::Hello;
use crate::value::WireType;

/// Which list of a node a port is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dir {
    Input,
    Output,
    Config,
    /// A stateful node's state. Its name is empty.
    State,
}

#[derive(Clone, Debug)]
struct Entry {
    node: String,
    dir: Dir,
    name: String,
    ty: WireType,
}

#[derive(Clone, Debug, Default)]
pub struct Schema {
    ports: Vec<Entry>,
    codecs: Vec<String>,
}

impl Schema {
    pub fn new() -> Schema {
        Schema::default()
    }

    /// Declares a port (or a node's state, with `Dir::State` and an empty name).
    pub fn port(&mut self, node: &str, dir: Dir, name: &str, ty: WireType) {
        self.ports.push(Entry {
            node: node.to_owned(),
            dir,
            name: name.to_owned(),
            ty,
        });
    }

    /// Declares that this side has a codec for `key`.
    pub fn codec(&mut self, key: &str) {
        if !self.has_codec(key) {
            self.codecs.push(key.to_owned());
        }
    }

    pub fn has_codec(&self, key: &str) -> bool {
        self.codecs.iter().any(|known| known == key)
    }

    /// The type of a port. Lookups compare borrowed text, so they allocate nothing.
    pub fn lookup(&self, node: &str, dir: Dir, name: &str) -> Result<&WireType, WireError> {
        self.ports
            .iter()
            .find(|entry| entry.dir == dir && entry.node == node && entry.name == name)
            .map(|entry| &entry.ty)
            .ok_or_else(|| WireError::UnknownPort {
                node: node.to_owned(),
                port: name.to_owned(),
            })
    }

    /// The schema a hello declares. `codecs` lists the type keys this side has codecs for.
    pub fn from_hello(hello: &Hello, codecs: &[&str]) -> Schema {
        let mut schema = Schema::new();
        for codec in codecs {
            schema.codec(codec);
        }
        for node in &hello.nodes {
            for port in &node.inputs {
                schema.port(&node.id, Dir::Input, &port.name, port.ty.clone());
            }
            for port in &node.outputs {
                schema.port(&node.id, Dir::Output, &port.name, port.ty.clone());
            }
            for port in &node.config {
                schema.port(&node.id, Dir::Config, &port.name, port.ty.clone());
            }
            if let Some(state) = &node.state {
                schema.port(&node.id, Dir::State, "", state.clone());
            }
        }
        schema
    }
}
