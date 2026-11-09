//! Which constants a graph marks as changeable.

use alloc::format;
use alloc::string::String;
use alloc::vec;
use alloc::vec::Vec;

use daedalus_backend::{CompileCx, NodeBody, Refusal};
use daedalus_graph::Lit;

use crate::chip::{ParamSpec, Scalar};
use crate::scalar_kind::ScalarKind;

/// Node metadata: a list of input names, or a struct from input name to `[min, max]` or unit.
pub(crate) const PARAMS_KEY: &str = "daedalus.chip.params";

pub(crate) struct Mark<'a> {
    pub port: &'a str,
    pub range: Option<(&'a Lit, &'a Lit)>,
}

/// The marks on `body`. None when the plan freezes its parameters.
pub(crate) fn marks<'a>(body: &NodeBody<'a>, cx: &CompileCx<'_>) -> Result<Vec<Mark<'a>>, Refusal> {
    let Some(value) = body.metadata.get(PARAMS_KEY).filter(|_| !cx.freeze_params) else {
        return Ok(Vec::new());
    };
    let refuse = || {
        let why = format!("`{PARAMS_KEY}` must list inputs, or map each input to [min, max]");
        Refusal::new(vec![body.node], &why)
    };
    match value {
        Lit::List(names) => names
            .iter()
            .map(|name| match name {
                Lit::Str(port) => Ok(Mark { port, range: None }),
                _ => Err(refuse()),
            })
            .collect(),
        Lit::Struct(fields) => fields
            .iter()
            .map(|field| {
                let range = match &field.value {
                    Lit::Unit => None,
                    Lit::List(pair) => match pair.as_slice() {
                        [min, max] => Some((min, max)),
                        _ => return Err(refuse()),
                    },
                    _ => return Err(refuse()),
                };
                Ok(Mark {
                    port: &field.name,
                    range,
                })
            })
            .collect(),
        _ => Err(refuse()),
    }
}

/// What `port` is called outside the module.
pub(crate) fn param_name(body: &NodeBody<'_>, port: &str) -> String {
    format!("{}.{port}", body.label.unwrap_or(body.id))
}

pub(crate) fn scalar_of(kind: ScalarKind, value: &Lit) -> Option<Scalar> {
    match value {
        Lit::Bool(flag) => Scalar::Bool(*flag).coerce(kind),
        Lit::Int(number) => Scalar::I64(*number).coerce(kind),
        Lit::Float(number) => Scalar::F64(*number).coerce(kind),
        _ => None,
    }
}

/// The range of `mark` on a parameter whose constant is `value`, checking the constant is in it.
pub(crate) fn checked_spec(
    body: &NodeBody<'_>,
    mark: &Mark<'_>,
    kind: ScalarKind,
    value: &Lit,
) -> Result<ParamSpec, Refusal> {
    let refuse =
        |why: String| Refusal::new(vec![body.node], &format!("input `{}`: {why}", mark.port));
    let bound = |lit: &Lit| {
        scalar_of(kind, lit)
            .ok_or_else(|| refuse(format!("the range doesn't fit `{}`", kind.key())))
    };
    let spec = match mark.range {
        Some((min, max)) => ParamSpec {
            kind,
            min: bound(min)?,
            max: bound(max)?,
        },
        None => ParamSpec::full(kind),
    };
    let scalar =
        scalar_of(kind, value).ok_or_else(|| refuse(String::from("the constant doesn't fit")))?;
    spec.check(scalar)
        .map_err(|_| refuse(String::from("the constant is outside its range")))?;
    Ok(spec)
}
