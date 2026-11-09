//! Turning a region into a module. The output depends only on the graph.

use alloc::format;
use alloc::string::{String, ToString};
use alloc::vec;
use alloc::vec::Vec;
use core::fmt::Write as _;

use daedalus_backend::{
    Backend, CompileCx, Compiled, GeneratedCode, MemorySpace, NodeBody, NodeRef, Offer, PortRef,
    Refusal, Region, Transfer, UnitInfo,
};
use daedalus_graph::{EdgePolicyDoc, Lit};
use daedalus_types::{NodeIndex, PortIndex};

use super::graph_struct::graph_struct;
use crate::backend::{BACKEND_ID, CpuBackend, body_of};
use crate::blob::ScalarKind;
use crate::body::CpuBody;
use crate::queue_shape::queue_shape;
use crate::shape::{BodyShape, ParamShape, Role};
use daedalus_runner::Overflow;

pub(super) const CHIP: &str = "::daedalus_cpu::chip";

/// The CPU backend, generating a module for each region.
#[derive(Clone, Copy, Debug)]
pub struct SourceBackend {
    /// The struct's name, and the file's stem in lower case.
    pub name: &'static str,
    /// What to call the crate the nodes live in, usually `crate` when the module is included
    /// there. `None` keeps its own name.
    pub root: Option<&'static str>,
}

/// A queue between two ports, or between a port and the host.
pub(super) struct QueueEdge {
    pub ty: String,
    pub capacity: u32,
    pub overflow: Overflow,
    pub label: String,
}

/// Where an input reads from.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Feed {
    Queue(usize),
    Local(usize),
}

pub(super) struct HostPort {
    pub ident: String,
    pub name: String,
    pub ty: String,
    pub edges: Vec<usize>,
}

/// Everything the text is made from.
pub(super) struct Module {
    pub name: String,
    pub queues: Vec<QueueEdge>,
    pub states: Vec<(usize, String)>,
    pub ids: Vec<String>,
    pub inputs: Vec<HostPort>,
    pub outputs: Vec<HostPort>,
}

/// Rewrites every path in a type's name so it resolves from the generated module.
fn rooted(ty: &str, root: Option<&(String, String)>) -> String {
    let mut out = String::with_capacity(ty.len() + 8);
    let mut rest = ty;
    while let Some(start) = rest.find(|c: char| c.is_alphabetic() || c == '_') {
        out.push_str(&rest[..start]);
        rest = &rest[start..];
        let end = rest
            .find(|c: char| !(c.is_alphanumeric() || c == '_' || c == ':'))
            .unwrap_or(rest.len());
        let (path, after) = rest.split_at(end);
        match path.split_once("::") {
            Some((first, tail)) => match root {
                Some((from, to)) if first == from => {
                    out.push_str(to);
                    out.push_str("::");
                    out.push_str(tail);
                }
                _ => {
                    out.push_str("::");
                    out.push_str(path);
                }
            },
            None => out.push_str(path),
        }
        rest = after;
    }
    out.push_str(rest);
    out
}

fn sanitize(name: &str) -> String {
    let mut out: String = name
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '_' })
        .collect();
    if out.chars().next().is_none_or(|c| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

fn overflow_text(overflow: Overflow) -> &'static str {
    match overflow {
        Overflow::DropOldest => "DropOldest",
        Overflow::DropNewest => "DropNewest",
        Overflow::Error => "Error",
    }
}

fn literal(kind: ScalarKind, value: &Lit) -> Option<String> {
    let number = |text: String| Some(format!("{text}_{}", kind.key()));
    match (kind, value) {
        (ScalarKind::Bool, Lit::Bool(flag)) => Some(flag.to_string()),
        (ScalarKind::F32, Lit::Float(v)) if v.is_finite() => number(format!("{:?}", *v as f32)),
        (ScalarKind::F64, Lit::Float(v)) if v.is_finite() => number(format!("{v:?}")),
        (ScalarKind::F32, Lit::Int(v)) => number(format!("{:?}", *v as f32)),
        (ScalarKind::F64, Lit::Int(v)) => number(format!("{:?}", *v as f64)),
        (ScalarKind::Bool | ScalarKind::F32 | ScalarKind::F64, _) => None,
        (_, Lit::Int(v)) => {
            let width = u32::from(kind.size()) * 8;
            let signed = matches!(
                kind,
                ScalarKind::I8 | ScalarKind::I16 | ScalarKind::I32 | ScalarKind::I64
            );
            let v = i128::from(*v);
            let (low, high) = if signed {
                (-(1_i128 << (width - 1)), (1_i128 << (width - 1)) - 1)
            } else {
                (0, (1_i128 << width) - 1)
            };
            (low..=high)
                .contains(&v)
                .then(|| format!("{v}_{}", kind.key()))
        }
        _ => None,
    }
}

fn shape_of(body: &NodeBody<'_>) -> Result<BodyShape, Refusal> {
    let shape = body
        .code
        .downcast_ref::<CpuBody>()
        .map(|cpu| cpu.shape)
        .ok_or_else(|| Refusal::new(vec![body.node], "this body isn't a CPU function"))?;
    let port = |p: &ParamShape| has_port(p.role) || p.role == Role::FanIn;
    let held = shape
        .params
        .iter()
        .filter(|p| port(p) && !p.copy)
        .map(|p| p.ty);
    let sent = shape.outputs.iter().filter(|o| !o.copy).map(|o| o.ty);
    if let Some(ty) = held.chain(sent).next() {
        let why = format!(
            "generated code keeps values in fixed queues, and `{}` isn't Copy",
            ty()
        );
        return Err(Refusal::new(vec![body.node], &why));
    }
    Ok(shape)
}

/// A port's value type as it appears in the module.
fn input_ty(shape: &BodyShape, port: u16) -> Option<&'static str> {
    let fixed = shape
        .params
        .iter()
        .find(|p| has_port(p.role) && p.at == port);
    let found = fixed.or_else(|| shape.params.iter().find(|p| p.role == Role::FanIn))?;
    Some((found.ty)())
}

fn has_port(role: Role) -> bool {
    matches!(
        role,
        Role::Read | Role::Take | Role::Modify | Role::OptRead | Role::OptTake | Role::OptModify
    )
}

fn fixed_count(shape: &BodyShape) -> u16 {
    shape.params.iter().filter(|p| has_port(p.role)).count() as u16
}

struct Wires {
    feeds: Vec<(PortRef, Feed)>,
    drains: Vec<(PortRef, Feed)>,
    /// (output, queue) for the outputs that leave to the host.
    queues: Vec<QueueEdge>,
    locals: Vec<String>,
}

impl Wires {
    fn queue(&mut self, ty: String, policy: &EdgePolicyDoc, fifo: u32, label: String) -> Feed {
        let (capacity, overflow) = queue_shape(policy, fifo);
        self.queues.push(QueueEdge {
            ty,
            capacity,
            overflow,
            label,
        });
        Feed::Queue(self.queues.len() - 1)
    }
}

fn rooted_output(
    shape: &BodyShape,
    port: PortRef,
    root: Option<&(String, String)>,
) -> Result<String, Refusal> {
    let output = shape
        .outputs
        .get(usize::from(port.port.0))
        .ok_or_else(|| Refusal::new(vec![port.node], "no such output"))?;
    Ok(rooted((output.ty)(), root))
}

fn rooted_input(
    shape: &BodyShape,
    port: PortRef,
    root: Option<&(String, String)>,
) -> Result<String, Refusal> {
    let ty = input_ty(shape, port.port.0)
        .ok_or_else(|| Refusal::new(vec![port.node], "no such input"))?;
    Ok(rooted(ty, root))
}

fn collect_links(region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Vec<PortRef>, Refusal> {
    if !cx.merge_allowed {
        return Ok(Vec::new());
    }
    Ok(crate::merge::groups(region, cx)?
        .into_iter()
        .flat_map(|(_, links)| links)
        .collect())
}

fn shape_for(cx: &CompileCx<'_>, node: NodeIndex) -> Result<BodyShape, Refusal> {
    shape_of(body_of(node, cx)?)
}

fn ident_checked(
    seen: &mut Vec<(String, String)>,
    name: &str,
    node: NodeIndex,
) -> Result<String, Refusal> {
    let ident = sanitize(name);
    if let Some((_, other)) = seen
        .iter()
        .find(|(known, other)| *known == ident && other != name)
    {
        let reason = format!("host ports `{name}` and `{other}` both become `{ident}`");
        return Err(Refusal::new(vec![node], &reason));
    }
    seen.push((ident.clone(), name.to_string()));
    Ok(ident)
}

fn wires(
    region: &Region<'_>,
    cx: &CompileCx<'_>,
    root: Option<&(String, String)>,
    module: &mut (Vec<HostPort>, Vec<HostPort>),
) -> Result<Wires, Refusal> {
    let links = collect_links(region, cx)?;
    let mut wires = Wires {
        feeds: Vec::new(),
        drains: Vec::new(),
        queues: Vec::new(),
        locals: Vec::new(),
    };
    for edge in cx.edges {
        let ty = rooted_output(&shape_for(cx, edge.from.node)?, edge.from, root)?;
        let feed = if links.contains(&edge.from) {
            wires.locals.push(ty);
            Feed::Local(wires.locals.len() - 1)
        } else {
            let label = format!(
                "{}.{} -> {}.{}",
                body_of(edge.from.node, cx)?.id,
                edge.from.port.0,
                body_of(edge.to.node, cx)?.id,
                edge.to.port.0
            );
            wires.queue(ty, edge.policy, cx.fifo_capacity, label)
        };
        wires.feeds.push((edge.to, feed));
        wires.drains.push((edge.from, feed));
    }
    let mut seen = Vec::new();
    for binding in cx.host_inputs {
        let ty = rooted_input(&shape_for(cx, binding.port.node)?, binding.port, root)?;
        let label = format!(
            "host.{} -> {}",
            binding.name,
            body_of(binding.port.node, cx)?.id
        );
        let Feed::Queue(edge) = wires.queue(ty.clone(), binding.policy, cx.fifo_capacity, label)
        else {
            continue;
        };
        wires.feeds.push((binding.port, Feed::Queue(edge)));
        match module.0.iter_mut().find(|port| port.name == binding.name) {
            Some(port) => port.edges.push(edge),
            None => {
                let ident = ident_checked(&mut seen, binding.name, binding.port.node)?;
                module.0.push(HostPort {
                    ident,
                    name: binding.name.to_string(),
                    ty,
                    edges: vec![edge],
                });
            }
        }
    }
    let mut seen = Vec::new();
    for binding in cx.host_outputs {
        let ty = rooted_output(&shape_for(cx, binding.port.node)?, binding.port, root)?;
        let label = format!(
            "{} -> host.{}",
            body_of(binding.port.node, cx)?.id,
            binding.name
        );
        let Feed::Queue(edge) = wires.queue(ty.clone(), binding.policy, cx.fifo_capacity, label)
        else {
            continue;
        };
        wires.drains.push((binding.port, Feed::Queue(edge)));
        let ident = ident_checked(&mut seen, binding.name, binding.port.node)?;
        module.1.push(HostPort {
            ident,
            name: binding.name.to_string(),
            ty,
            edges: vec![edge],
        });
    }
    for port in region.inputs {
        if !wires.feeds.iter().any(|(fed, _)| fed == port) {
            let why =
                "generated code holds a whole graph, and this input comes from another backend";
            return Err(Refusal::new(vec![port.node], why));
        }
    }
    for port in region.outputs {
        if !wires.drains.iter().any(|(fed, _)| fed == port) {
            let why = "generated code holds a whole graph, and this output goes to another backend";
            return Err(Refusal::new(vec![port.node], why));
        }
    }
    Ok(wires)
}

/// `path` with its crate replaced, so it resolves from the module.
fn call_path(path: &str, root: Option<&(String, String)>) -> String {
    match (path.split_once("::"), root) {
        (Some((first, tail)), Some((from, to))) if first == from => format!("{to}::{tail}"),
        _ => format!("::{path}"),
    }
}

struct NodeEmit<'a> {
    step: usize,
    node: NodeIndex,
    all: bool,
    shape: BodyShape,
    inputs: u16,
    wires: &'a Wires,
    cx: &'a CompileCx<'a>,
    root: Option<&'a (String, String)>,
}

impl NodeEmit<'_> {
    fn feed(&self, port: u16) -> Option<Feed> {
        let at = PortRef {
            node: self.node,
            port: PortIndex(port),
        };
        self.wires
            .feeds
            .iter()
            .find(|(fed, _)| *fed == at)
            .map(|(_, feed)| *feed)
    }

    fn take(&self, out: &mut String, name: &str, feed: Feed) {
        let _ = match feed {
            Feed::Queue(queue) => {
                let _ = writeln!(out, "            let {name} = self.e{queue}.pop();");
                if self.all {
                    Ok(())
                } else {
                    writeln!(out, "            self.e{queue}.clear();")
                }
            }
            Feed::Local(local) => writeln!(out, "            let {name} = l{local}.take();"),
        };
    }

    fn constant(&self, param: &ParamShape) -> Result<Option<String>, Refusal> {
        let Some(one) = self
            .cx
            .consts
            .iter()
            .find(|one| one.node == self.node && one.port == param.port)
        else {
            return Ok(None);
        };
        let refuse = |why: String| Refusal::new(vec![self.node], &why);
        let kind = ScalarKind::from_key((param.ty)())
            .ok_or_else(|| refuse(format!("input `{}`: only scalar constants", param.port)))?;
        literal(kind, one.value)
            .map(Some)
            .ok_or_else(|| refuse(format!("input `{}`: the constant doesn't fit", param.port)))
    }

    fn body(&self, out: &mut String) -> Result<(), Refusal> {
        let required = |role: Role| matches!(role, Role::Read | Role::Take | Role::Modify);
        let port_at = |param: &ParamShape| self.feed(param.at);
        let label = self.step;
        if self.all {
            let waits: Vec<String> = self
                .shape
                .params
                .iter()
                .filter(|p| required(p.role))
                .filter_map(|p| match port_at(p) {
                    Some(Feed::Queue(queue)) => Some(format!("self.e{queue}.is_empty()")),
                    _ => None,
                })
                .collect();
            if !waits.is_empty() {
                let _ = writeln!(
                    out,
                    "            if {} {{\n                break 'n{label};\n            }}",
                    waits.join(" || ")
                );
            }
        }
        let mut args = Vec::new();
        let mut checks = Vec::new();
        for (index, param) in self.shape.params.iter().enumerate() {
            let name = format!("a{index}");
            match param.role {
                Role::State => args.push(format!(
                    "self.s{}.get_or_insert_with(Default::default)",
                    self.step
                )),
                Role::Context => args.push("&mut cx".to_string()),
                Role::ConfigRef | Role::ConfigVal => {
                    return Err(Refusal::new(
                        vec![self.node],
                        "generated code can't pass a config",
                    ));
                }
                Role::FanIn => self.fan_in(out, &name, &mut args),
                role => self.plain_input(out, param, &name, role, &mut args, &mut checks)?,
            }
        }
        for check in checks {
            let _ = writeln!(out, "{check}");
        }
        let path = call_path(self.shape.path, self.root);
        let _ = writeln!(out, "            let r = {path}({});", args.join(", "));
        if self.shape.fallible {
            let _ = writeln!(
                out,
                "            let r = match r {{\n                Ok(value) => value,\n                Err(error) => return Err({CHIP}::node_error({}, &error)),\n            }};",
                self.step
            );
        }
        self.outputs(out);
        Ok(())
    }

    fn fan_in(&self, out: &mut String, name: &str, args: &mut Vec<String>) {
        let fixed = fixed_count(&self.shape);
        let mut values = Vec::new();
        for port in fixed..self.inputs {
            let value = format!("{name}_{port}");
            if let Some(feed) = self.feed(port) {
                self.take(out, &value, feed);
                values.push(value);
            } else {
                values.push("None".to_string());
            }
        }
        let _ = writeln!(out, "            let {name} = [{}];", values.join(", "));
        args.push(format!("::daedalus_cpu::FanIn::values(&{name})"));
    }

    fn plain_input(
        &self,
        out: &mut String,
        param: &ParamShape,
        name: &str,
        role: Role,
        args: &mut Vec<String>,
        checks: &mut Vec<String>,
    ) -> Result<(), Refusal> {
        let label = self.step;
        let optional = matches!(role, Role::OptRead | Role::OptTake | Role::OptModify);
        let constant = self.constant(param)?;
        match (self.feed(param.at), constant) {
            (Some(feed), _) => {
                self.take(out, name, feed);
                if !optional {
                    checks.push(format!(
                        "            let Some({name}) = {name} else {{\n                break 'n{label};\n            }};"
                    ));
                }
            }
            (None, Some(value)) if optional => {
                let _ = writeln!(out, "            let {name} = Some({value});");
            }
            (None, Some(value)) => {
                let _ = writeln!(out, "            let {name} = {value};");
            }
            (None, None) if optional => {
                let _ = writeln!(out, "            let {name} = None;");
            }
            (None, None) => {
                let why = format!("input `{}` has no source", param.port);
                return Err(Refusal::new(vec![self.node], &why));
            }
        }
        args.push(match role {
            Role::Read => format!("&{name}"),
            Role::Modify => format!("&mut {{ {name} }}"),
            Role::OptRead => format!("{name}.as_ref()"),
            Role::OptModify => format!("{name}.as_mut()"),
            _ => name.to_string(),
        });
        Ok(())
    }

    fn outputs(&self, out: &mut String) {
        let count = self.shape.outputs.len();
        match count {
            0 => {}
            1 => out.push_str("            let o0 = r;\n"),
            _ => {
                let names: Vec<String> = (0..count).map(|o| format!("o{o}")).collect();
                let _ = writeln!(out, "            let ({}) = r;", names.join(", "));
            }
        }
        for (index, output) in self.shape.outputs.iter().enumerate() {
            let at = PortRef {
                node: self.node,
                port: PortIndex(index as u16),
            };
            let drains: Vec<Feed> = self
                .wires
                .drains
                .iter()
                .filter(|(from, _)| *from == at)
                .map(|(_, feed)| *feed)
                .collect();
            if drains.is_empty() {
                let _ = writeln!(out, "            let _ = o{index};");
                continue;
            }
            let indent = if output.optional {
                "                "
            } else {
                "            "
            };
            if output.optional {
                let _ = writeln!(out, "            if let Some(o{index}) = o{index} {{");
            }
            for drain in drains {
                let _ = match drain {
                    Feed::Local(local) => writeln!(out, "{indent}l{local} = Some(o{index});"),
                    Feed::Queue(queue) => {
                        let edge = &self.wires.queues[queue];
                        writeln!(
                            out,
                            "{indent}{CHIP}::push(&mut self.e{queue}, o{index}, {CHIP}::Overflow::{}, {queue})?;",
                            overflow_text(edge.overflow)
                        )
                    }
                };
            }
            if output.optional {
                out.push_str("            }\n");
            }
        }
    }
}

fn tick_text(
    region: &Region<'_>,
    cx: &CompileCx<'_>,
    wires: &Wires,
    root: Option<&(String, String)>,
    fires_all: &[bool],
) -> Result<String, Refusal> {
    let mut out = String::new();
    let uses_cx = region.nodes.iter().try_fold(false, |any, node| {
        let shape = shape_for(cx, *node)?;
        Ok::<_, Refusal>(any || shape.params.iter().any(|p| p.role == Role::Context))
    })?;
    let _ = writeln!(
        out,
        "    /// Runs every node once, in order, at time `now_ns`.\n    pub fn tick_at(&mut self, now_ns: u64) -> Result<(), {CHIP}::ChipError> {{"
    );
    if uses_cx {
        let _ = writeln!(out, "        let mut cx = {CHIP}::context(now_ns);");
    } else {
        out.push_str("        let _ = now_ns;\n");
    }
    for (local, ty) in wires.locals.iter().enumerate() {
        let _ = writeln!(out, "        let mut l{local}: Option<{ty}> = None;");
    }
    for (step, &node) in region.nodes.iter().enumerate() {
        let body = body_of(node, cx)?;
        let emit = NodeEmit {
            step,
            node,
            all: fires_all[step],
            shape: shape_of(body)?,
            inputs: body.inputs,
            wires,
            cx,
            root,
        };
        let _ = writeln!(out, "        'n{step}: {{");
        emit.body(&mut out)?;
        out.push_str("        }\n");
    }
    out.push_str("        Ok(())\n    }\n");
    Ok(out)
}

fn module_root(shape: &BodyShape, root: Option<&str>) -> Option<(String, String)> {
    let (first, _) = shape.path.split_once("::")?;
    Some((first.to_string(), root?.to_string()))
}

impl SourceBackend {
    fn generate(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<String, Refusal> {
        let first = region
            .nodes
            .first()
            .ok_or_else(|| Refusal::new(Vec::new(), "an empty region"))?;
        let root = module_root(&shape_for(cx, *first)?, self.root);
        let mut hosts = (Vec::new(), Vec::new());
        let wires = wires(region, cx, root.as_ref(), &mut hosts)?;
        let mut states = Vec::new();
        let mut fires_all = Vec::new();
        let mut ids = Vec::new();
        for (step, &node) in region.nodes.iter().enumerate() {
            let body = body_of(node, cx)?;
            let shape = shape_of(body)?;
            if let Some(state) = shape.params.iter().find(|p| p.role == Role::State) {
                states.push((step, rooted((state.ty)(), root.as_ref())));
            }
            fires_all.push(body.fire_all);
            ids.push(body.id.to_string());
        }
        let tick = tick_text(region, cx, &wires, root.as_ref(), &fires_all)?;
        let module = Module {
            name: self.name.to_string(),
            queues: wires.queues,
            states,
            ids,
            inputs: hosts.0,
            outputs: hosts.1,
        };
        Ok(graph_struct(&module, &tick))
    }
}

impl Backend for SourceBackend {
    fn id(&self) -> &str {
        BACKEND_ID
    }

    fn memory(&self) -> Vec<MemorySpace> {
        CpuBackend.memory()
    }

    fn transfers(&self) -> Vec<Transfer> {
        Vec::new()
    }

    fn can_take(&self, offer: &Offer<'_>) -> Vec<NodeRef> {
        CpuBackend.can_take(offer)
    }

    fn compile(&self, region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Compiled, Refusal> {
        let text = self.generate(region, cx)?;
        Ok(Compiled::Source {
            code: GeneratedCode {
                file: format!("{}.rs", self.name.to_lowercase()),
                text,
            },
            units: vec![UnitInfo {
                covers: region.nodes.to_vec(),
                reads: region.inputs.to_vec(),
                writes: region.outputs.to_vec(),
                absorbed: Vec::new(),
                may_pend: false,
            }],
        })
    }
}
