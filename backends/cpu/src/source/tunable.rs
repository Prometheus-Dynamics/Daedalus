//! The parts of a module that exist only when the graph marks parameters.

use alloc::format;
use alloc::string::String;
use alloc::vec::Vec;
use core::fmt::Write as _;

use daedalus_backend::{CompileCx, Refusal, Region};
use daedalus_types::NodeIndex;

use super::emit::{CHIP, has_port, literal, sanitize, shape_of};
use crate::backend::body_of;
use crate::chip::{ParamSpec, Scalar};
use crate::param_mark::{checked_spec, marks, param_name, scalar_of};
use crate::scalar_kind::ScalarKind;

pub(super) struct Param {
    pub node: NodeIndex,
    pub port: String,
    pub name: String,
    pub ident: String,
    pub kind: ScalarKind,
    /// The Rust literal of its starting value.
    pub value: String,
    pub spec: String,
}

fn scalar_text(value: Scalar, kind: ScalarKind) -> String {
    let text = match value {
        Scalar::Bool(v) => format!("{v}"),
        Scalar::F32(v) if !v.is_finite() => float_text("f32", v.is_sign_negative(), v.is_nan()),
        Scalar::F64(v) if !v.is_finite() => float_text("f64", v.is_sign_negative(), v.is_nan()),
        Scalar::F32(v) => format!("{v:?}_f32"),
        Scalar::F64(v) => format!("{v:?}_f64"),
        Scalar::I8(v) => format!("{v}_i8"),
        Scalar::I16(v) => format!("{v}_i16"),
        Scalar::I32(v) => format!("{v}_i32"),
        Scalar::I64(v) => format!("{v}_i64"),
        Scalar::U8(v) => format!("{v}_u8"),
        Scalar::U16(v) => format!("{v}_u16"),
        Scalar::U32(v) => format!("{v}_u32"),
        Scalar::U64(v) => format!("{v}_u64"),
    };
    format!("{CHIP}::Scalar::{kind:?}({text})")
}

fn float_text(ty: &str, negative: bool, nan: bool) -> String {
    match (nan, negative) {
        (true, _) => format!("{ty}::NAN"),
        (_, true) => format!("{ty}::NEG_INFINITY"),
        _ => format!("{ty}::INFINITY"),
    }
}

fn spec_text(spec: &ParamSpec, full: bool) -> String {
    let kind = spec.kind;
    if full {
        return format!("{CHIP}::ParamSpec::full({CHIP}::ScalarKind::{kind:?})");
    }
    format!(
        "{CHIP}::ParamSpec {{ kind: {CHIP}::ScalarKind::{kind:?}, min: {}, max: {} }}",
        scalar_text(spec.min, kind),
        scalar_text(spec.max, kind)
    )
}

/// The marked parameters, by run order and then input order.
pub(super) fn collect(region: &Region<'_>, cx: &CompileCx<'_>) -> Result<Vec<Param>, Refusal> {
    let mut params: Vec<Param> = Vec::new();
    for &node in region.nodes {
        let body = body_of(node, cx)?;
        let marked = marks(body, cx)?;
        if marked.is_empty() {
            continue;
        }
        let shape = shape_of(body)?;
        let refuse = |port: &str, why: &str| {
            Refusal::new(alloc::vec![node], &format!("input `{port}`: {why}"))
        };
        for mark in &marked {
            if !shape
                .params
                .iter()
                .any(|p| has_port(p.role) && p.port == mark.port)
            {
                return Err(refuse(
                    mark.port,
                    "marked as a parameter, but there's no such input",
                ));
            }
        }
        for param in shape.params.iter().filter(|p| has_port(p.role)) {
            let Some(mark) = marked.iter().find(|mark| mark.port == param.port) else {
                continue;
            };
            let Some(constant) = cx
                .consts
                .iter()
                .find(|one| one.node == node && one.port == param.port)
            else {
                return Err(refuse(mark.port, "a parameter needs a constant"));
            };
            let kind = ScalarKind::from_key((param.ty)())
                .ok_or_else(|| refuse(mark.port, "a parameter must be a scalar"))?;
            let spec = checked_spec(body, mark, kind, constant.value)?;
            let value = literal(kind, constant.value)
                .filter(|_| scalar_of(kind, constant.value).is_some())
                .ok_or_else(|| refuse(mark.port, "the constant doesn't fit"))?;
            let name = param_name(body, mark.port);
            let ident = sanitize(&name);
            if let Some(other) = params.iter().find(|known| known.ident == ident) {
                let why = format!(
                    "parameters `{name}` and `{}` both become `{ident}`",
                    other.name
                );
                return Err(Refusal::new(alloc::vec![node], &why));
            }
            params.push(Param {
                node,
                port: mark.port.into(),
                name,
                ident,
                kind,
                value,
                spec: spec_text(&spec, mark.range.is_none()),
            });
        }
    }
    Ok(params)
}

/// The tables after `EDGES`.
pub(super) fn tables(params: &[Param]) -> String {
    if params.is_empty() {
        return String::new();
    }
    let names: Vec<String> = params.iter().map(|p| format!("{:?}", p.name)).collect();
    let specs: Vec<String> = params.iter().map(|p| p.spec.clone()).collect();
    format!(
        "/// Parameters by id.\npub const PARAM_NAMES: [&str; {n}] = [{}];\n/// Each parameter's type and range.\npub const PARAMS: [{CHIP}::ParamSpec; {n}] = [\n    {},\n];\n\n",
        names.join(", "),
        specs.join(",\n    "),
        n = params.len(),
    )
}

/// A setter per parameter, inside the `impl` of the struct.
pub(super) fn setters(params: &[Param]) -> String {
    let mut out = String::new();
    for (id, param) in params.iter().enumerate() {
        let _ = writeln!(
            out,
            "\n    /// Sets `{}` (id {id}), from the next tick on.\n    pub fn set_{}(&mut self, value: {}) -> Result<(), {CHIP}::ParamError> {{\n        {CHIP}::Tunable::set_param(self, {id}, value.into())\n    }}",
            param.name,
            param.ident,
            param.kind.key(),
        );
    }
    out
}

/// `impl Tunable`, after the struct's `impl`.
pub(super) fn tunable_impl(name: &str, params: &[Param]) -> String {
    if params.is_empty() {
        return String::new();
    }
    let mut set = String::new();
    let mut get = String::new();
    for id in 0..params.len() {
        let _ = writeln!(
            set,
            "            {id} => self.p{id} = {CHIP}::checked(&PARAMS[{id}], value)?,"
        );
        let _ = writeln!(get, "            {id} => Some(self.p{id}.into()),");
    }
    format!(
        "\nimpl {CHIP}::Tunable for {name} {{\n    fn set_param(&mut self, id: u16, value: {CHIP}::Scalar) -> Result<(), {CHIP}::ParamError> {{\n        match id {{\n{set}            _ => return Err({CHIP}::ParamError::Id),\n        }}\n        Ok(())\n    }}\n\n    fn param(&self, id: u16) -> Option<{CHIP}::Scalar> {{\n        match id {{\n{get}            _ => None,\n        }}\n    }}\n}}\n"
    )
}
