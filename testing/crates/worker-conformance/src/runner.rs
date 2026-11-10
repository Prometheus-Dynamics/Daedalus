//! Drives a worker spec through the process backend: each fixture is compiled as a one-node
//! region, then run for both rounds, and its outputs compared with the expected bytes.

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, SlotState, Slots, StepCx, StepEnd};
use daedalus_backend::BodyIndex;
use daedalus_backend::{
    Backend, CompileCx, Compiled, NO_METADATA, NodeBody, PortRef, RateId, Region,
};
use daedalus_backend_process::{ProcessBackend, ProcessError, WorkerSpec};
use daedalus_registry::{OwnedBody, OwnedDecl, OwnedPort, Registry, TypeRef};
use daedalus_types::{BackendIndex, Field, NodeIndex, PortIndex, TypeExpr, TypeKey, Variant};
use daedalus_wire::WireType;

use crate::fixtures::{Expect, Fixture, declarations, fixtures};

/// A round that did not produce what it should.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Failure {
    pub fixture: &'static str,
    pub round: usize,
    pub reason: String,
}

fn leak<T>(value: T) -> &'static T {
    Box::leak(Box::new(value))
}

fn leak_str(text: &str) -> &'static str {
    Box::leak(text.to_owned().into_boxed_str())
}

/// The registry's type expression for a declared type.
pub fn type_expr(ty: &WireType) -> TypeExpr {
    match ty {
        WireType::Scalar(scalar) => TypeExpr::Scalar(*scalar),
        WireType::Opaque(key) => {
            TypeExpr::Opaque(TypeKey::shared(key).expect("a fixture key is valid"))
        }
        WireType::Optional(inner) => TypeExpr::Optional(leak(type_expr(inner))),
        WireType::List(inner) => TypeExpr::List(leak(type_expr(inner))),
        WireType::Map(key, value) => TypeExpr::Map(leak(type_expr(key)), leak(type_expr(value))),
        WireType::Tuple(items) => TypeExpr::Tuple(Box::leak(
            items
                .iter()
                .map(type_expr)
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )),
        WireType::Struct(fields) => TypeExpr::Struct(Box::leak(
            fields
                .iter()
                .map(|(name, ty)| Field {
                    name: leak_str(name),
                    ty: type_expr(ty),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )),
        WireType::Enum(variants) => TypeExpr::Enum(Box::leak(
            variants
                .iter()
                .map(|(name, payload)| Variant {
                    name: leak_str(name),
                    payload: payload.as_ref().map(type_expr),
                })
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        )),
    }
}

/// A registry holding the canonical nodes, as a compiled-in record would.
pub fn registry() -> Registry {
    let mut registry = Registry::new();
    for decl in declarations() {
        let port = |name: &str, ty: &WireType| OwnedPort {
            name: name.to_owned(),
            ty: TypeRef::Expr(type_expr(ty)),
            optional: false,
        };
        registry
            .register_owned(OwnedDecl {
                id: decl.id.clone(),
                source: "worker-conformance".into(),
                inputs: decl.inputs.iter().map(|p| port(&p.name, &p.ty)).collect(),
                outputs: decl.outputs.iter().map(|p| port(&p.name, &p.ty)).collect(),
                body: OwnedBody {
                    backend: "process".into(),
                    text: decl.id.clone(),
                },
            })
            .expect("the canonical declarations register");
    }
    registry
}

/// Runs every fixture through a worker started from `spec`, in two rounds each.
pub fn run_all(spec: WorkerSpec) -> Result<Vec<Failure>, ProcessError> {
    let registry = registry();
    let mut backend = ProcessBackend::new();
    backend.add_worker(spec, &registry)?;
    let mut failures = Vec::new();
    for fixture in fixtures() {
        for round in 0..2 {
            if let Err(reason) = run_round(&backend, &fixture, round) {
                failures.push(Failure {
                    fixture: fixture.name,
                    round,
                    reason,
                });
            }
        }
    }
    Ok(failures)
}

fn run_round(backend: &ProcessBackend, fixture: &Fixture, round: usize) -> Result<(), String> {
    let inputs: Vec<PortRef> = (0..fixture.inputs.len())
        .map(|port| PortRef {
            node: NodeIndex(0),
            port: PortIndex(port as u16),
        })
        .collect();
    let outputs: Vec<PortRef> = (0..fixture.outputs.len())
        .map(|port| PortRef {
            node: NodeIndex(0),
            port: PortIndex(port as u16),
        })
        .collect();
    let nodes = [NodeIndex(0)];
    let region = Region {
        backend: BackendIndex(0),
        nodes: &nodes,
        inputs: &inputs,
        outputs: &outputs,
        incoming: &[],
        rate: RateId(0),
    };
    let bodies = [NodeBody {
        node: NodeIndex(0),
        id: fixture.node,
        body: BodyIndex(0),
        inputs: fixture.inputs.len() as u16,
        outputs: fixture.outputs.len() as u16,
        merges: false,
        fire_all: false,
        label: None,
        metadata: &NO_METADATA,
        code: &(),
        text: fixture.node,
    }];
    let cx = CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &bodies,
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 64,
        merge_allowed: false,
        freeze_params: false,
    };
    let compiled = backend
        .compile(&region, &cx)
        .map_err(|refusal| format!("refused: {}", refusal.reason))?;
    let Compiled::Steps(mut units) = compiled else {
        return Err("the backend gave a form other than steps".into());
    };
    let mut step = units.pop().ok_or("no step")?.step;

    let round_data = &fixture.rounds[round];
    let mut bufs: Vec<Vec<u8>> = fixture
        .inputs
        .iter()
        .map(|port| {
            round_data
                .inputs
                .iter()
                .find(|(name, _)| *name == port.name)
                .map(|(_, bytes)| bytes.clone())
                .unwrap_or_default()
        })
        .collect();
    let mut cells: Vec<Slot> = Vec::new();
    for buf in bufs.iter_mut() {
        cells.push(Slot {
            state: SlotState::Full as u32,
            owner: 0,
            data: buf.as_mut_ptr(),
            len: buf.len(),
            cap: buf.len(),
            ..Slot::EMPTY
        });
    }
    cells.extend(std::iter::repeat_n(Slot::EMPTY, fixture.outputs.len()));
    let mut cx_step = StepCx::new();
    let end = run_once(&mut step, &mut Slots::of(&mut cells), &mut cx_step);
    let n_in = fixture.inputs.len();
    let result = match &round_data.expect {
        Expect::Outputs(expected) => {
            if !matches!(end, StepEnd::Done) {
                Err(format!(
                    "expected outputs, the step ended {:?}: {}",
                    end_name(&end),
                    cx_step.reason()
                ))
            } else {
                let mut result = Ok(());
                for (name, bytes) in expected {
                    let index = fixture.outputs.iter().position(|port| port.name == *name);
                    let actual = index.and_then(|index| cells[n_in + index].bytes());
                    if actual != Some(bytes.as_slice()) {
                        result = Err(format!("output {name}: expected {bytes:?}, got {actual:?}"));
                        break;
                    }
                }
                result
            }
        }
        Expect::Fault(code) => {
            if !matches!(end, StepEnd::Fault) {
                Err(format!(
                    "expected a fault, the step ended {:?}",
                    end_name(&end)
                ))
            } else if !cx_step.reason().contains(code) {
                Err(format!("expected a {code} fault, got {}", cx_step.reason()))
            } else {
                Ok(())
            }
        }
    };
    release(step);
    result
}

fn end_name(end: &StepEnd) -> &'static str {
    match end {
        StepEnd::Done => "done",
        StepEnd::Pending(_) => "pending",
        StepEnd::Skipped => "skipped",
        StepEnd::Fault => "fault",
    }
}
