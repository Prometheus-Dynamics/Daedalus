//! What each operator's ports are, worked out from its bodies.

use std::sync::{Mutex, OnceLock};

use daedalus_graph::{ControlDoc, ControlOp, GraphBody, HostInputPolicy, NodeKindDoc};
use daedalus_registry::{Registry, TypeRef};
use daedalus_types::{Scalar, TypeExpr};
use daedalus_value::Access;

use super::ControlError;
use crate::config::Config;
use crate::pipeline::{SolveError, solve};
use crate::solve::consts::scalar_of;
use crate::solve::port_types::{display, same};
use crate::solve::{Graph, Port, Work};

pub const THEN: &str = "then";
pub const ELSE: &str = "else";
pub const BODY: &str = "body";
pub const MAX_LIMIT: u32 = 65_535;

const COND: &str = "cond";
const LIST: &str = "list";
const ITEM: &str = "item";
const OUT: &str = "out";
const INIT: &str = "init";
const STATE: &str = "state";
const DONE: &str = "done";

pub(super) struct Ports {
    pub inputs: Vec<Port>,
    pub outputs: Vec<Port>,
}

/// The body with every port held, so a node in it that needs its input exclusively gets a copy
/// and never takes the view it was lent.
pub(super) fn closed(body: &GraphBody) -> GraphBody {
    let mut body = body.clone();
    for node in body
        .nodes
        .iter_mut()
        .filter(|node| node.kind == NodeKindDoc::HostBridge)
    {
        let ports: Vec<String> = node.port_types.keys().cloned().collect();
        for port in ports {
            node.host_inputs.insert(port, HostInputPolicy::Held);
        }
    }
    body
}

fn port(name: &str, ty: TypeRef) -> Port {
    Port {
        name: name.to_string(),
        ty,
        access: Access::Read,
        optional: false,
        default: None,
        generic: false,
    }
}

fn bool_type() -> TypeRef {
    TypeRef::Expr(TypeExpr::Scalar(Scalar::Bool))
}

/// One `&'static` expression per distinct list type, however often graphs are planned.
fn list_of(element: &TypeRef) -> Option<TypeRef> {
    static LISTS: OnceLock<Mutex<Vec<&'static TypeExpr>>> = OnceLock::new();
    let inner = match element {
        TypeRef::Key(key) => match scalar_of(key) {
            Some(scalar) => TypeExpr::Scalar(scalar),
            None => TypeExpr::Opaque(key.clone()),
        },
        TypeRef::Expr(expr) => expr.clone(),
        TypeRef::Body => return None,
    };
    let mut known = LISTS
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    let at = match known.iter().position(|expr| **expr == inner) {
        Some(at) => at,
        None => {
            known.push(Box::leak(Box::new(inner)));
            known.len() - 1
        }
    };
    Some(TypeRef::Expr(TypeExpr::List(known[at])))
}

/// A body's ports: what it takes, then what it gives back.
struct Shape {
    inputs: Vec<Port>,
    outputs: Vec<Port>,
}

fn find<'a>(ports: &'a [Port], name: &str) -> Option<&'a Port> {
    ports.iter().find(|port| port.name == name)
}

struct Check<'a> {
    node: &'a str,
    control: &'a ControlDoc,
    registry: &'a Registry,
    config: &'a Config,
}

impl Check<'_> {
    fn shaped(&self, body: &str, detail: String) -> ControlError {
        ControlError::BodyShape {
            node: self.node.to_string(),
            body: body.to_string(),
            detail,
        }
    }

    fn solved(&self, name: &str, body: &GraphBody) -> Result<Graph, ControlError> {
        let count = body.nodes.len() as u32;
        for edge in &body.edges {
            let outside = [&edge.from, &edge.to]
                .into_iter()
                .find(|end| end.node >= count);
            if let Some(end) = outside {
                return Err(ControlError::BodyEdge {
                    node: self.node.to_string(),
                    body: name.to_string(),
                    port: end.port.clone(),
                });
            }
        }
        solve(&closed(body), self.registry, self.config).map_err(|errors: Vec<SolveError>| {
            ControlError::BodyInvalid {
                node: self.node.to_string(),
                body: name.to_string(),
                errors,
            }
        })
    }

    fn shape(&self, name: &str) -> Result<Shape, ControlError> {
        let Some(body) = self.control.bodies.get(name) else {
            return Err(ControlError::BodyMissing {
                node: self.node.to_string(),
                body: name.to_string(),
            });
        };
        let graph = self.solved(name, body)?;
        let mut bridges = graph
            .nodes
            .iter()
            .filter(|n| n.doc.kind == NodeKindDoc::HostBridge);
        let (Some(bridge), None) = (bridges.next(), bridges.next()) else {
            return Err(self.shaped(name, "a body needs exactly one host bridge".to_string()));
        };
        let wired = |port: &&Port| {
            graph.edges.iter().any(|edge| {
                let from = graph.nodes[edge.from.node as usize].doc.kind == NodeKindDoc::HostBridge;
                from && edge.from.port == port.name
            })
        };
        Ok(Shape {
            inputs: bridge.outputs.iter().filter(wired).cloned().collect(),
            outputs: bridge.inputs.clone(),
        })
    }

    fn need<'a>(
        &self,
        body: &str,
        ports: &'a [Port],
        name: &str,
    ) -> Result<&'a Port, ControlError> {
        find(ports, name).ok_or_else(|| self.shaped(body, format!("needs a port `{name}`")))
    }

    fn mismatch(&self, port: &str, detail: String) -> ControlError {
        ControlError::TypeMismatch {
            node: self.node.to_string(),
            port: port.to_string(),
            detail,
        }
    }

    fn agree(&self, port: &str, a: Option<&Port>, b: Option<&Port>) -> Result<(), ControlError> {
        match (a, b) {
            (Some(a), Some(b)) if same(&a.ty, &b.ty) => Ok(()),
            (Some(a), Some(b)) => Err(self.mismatch(
                port,
                format!(
                    "`then` has `{}` and `else` has `{}`",
                    display(&a.ty),
                    display(&b.ty)
                ),
            )),
            _ => Err(self.mismatch(port, "only one branch has this port".to_string())),
        }
    }

    fn branches(&self) -> Result<Ports, ControlError> {
        let (then, other) = (self.shape(THEN)?, self.shape(ELSE)?);
        for list in [
            (&then.inputs, &other.inputs),
            (&then.outputs, &other.outputs),
        ] {
            let names = list.0.iter().chain(list.1).map(|port| port.name.as_str());
            for name in names {
                self.agree(name, find(list.0, name), find(list.1, name))?;
            }
        }
        let mut inputs = vec![port(COND, bool_type())];
        inputs.extend(then.inputs);
        Ok(Ports {
            inputs,
            outputs: then.outputs,
        })
    }

    fn for_each(&self) -> Result<Ports, ControlError> {
        let shape = self.shape(BODY)?;
        let item = self.need(BODY, &shape.inputs, ITEM)?;
        let out = self.need(BODY, &shape.outputs, OUT)?;
        let list = |port: &Port| {
            list_of(&port.ty)
                .ok_or_else(|| self.mismatch(&port.name, "has no list type".to_string()))
        };
        Ok(Ports {
            inputs: vec![self::port(LIST, list(item)?)],
            outputs: vec![self::port(OUT, list(out)?)],
        })
    }

    fn until(&self) -> Result<Ports, ControlError> {
        let max = self.control.max;
        if !max.is_some_and(|max| (1..=MAX_LIMIT).contains(&max)) {
            return Err(ControlError::MaxInvalid {
                node: self.node.to_string(),
                value: max,
            });
        }
        let shape = self.shape(BODY)?;
        let input = self.need(BODY, &shape.inputs, STATE)?;
        let output = self.need(BODY, &shape.outputs, STATE)?;
        let done = self.need(BODY, &shape.outputs, DONE)?;
        if !same(&input.ty, &output.ty) {
            return Err(self.mismatch(STATE, "the body changes the state's type".to_string()));
        }
        if !same(&done.ty, &bool_type()) {
            return Err(self.mismatch(DONE, "must be a bool".to_string()));
        }
        Ok(Ports {
            inputs: vec![port(INIT, input.ty.clone())],
            outputs: vec![port(STATE, output.ty.clone())],
        })
    }
}

fn ports_of(check: &Check<'_>) -> Result<Ports, ControlError> {
    match check.control.op {
        ControlOp::If => check.branches(),
        ControlOp::ForEach => check.for_each(),
        ControlOp::Until => check.until(),
    }
}

/// Gives each control node its ports, which come from its bodies.
pub fn declare(work: &mut Work<'_>, registry: &Registry) {
    for index in 0..work.graph.nodes.len() {
        let node = &work.graph.nodes[index];
        if node.doc.kind != NodeKindDoc::Control {
            continue;
        }
        let id = node.doc.id.clone();
        let Some(control) = node.doc.control.clone() else {
            let error = ControlError::BodyMissing {
                node: id,
                body: BODY.to_string(),
            };
            work.errors.push(SolveError::Control(error));
            continue;
        };
        let check = Check {
            node: &id,
            control: &control,
            registry,
            config: work.config,
        };
        match ports_of(&check) {
            Ok(ports) => {
                let node = &mut work.graph.nodes[index];
                node.required = ports.inputs.iter().map(|port| port.name.clone()).collect();
                node.inputs = ports.inputs;
                node.outputs = ports.outputs;
            }
            Err(error) => work.errors.push(SolveError::Control(error)),
        }
    }
}
