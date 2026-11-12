//! Reads a per-element body: one WGSL function, `apply`, of the element it is given.
//!
//! An element body can be merged with its neighbours into one shader, because it can't see any
//! element but its own: it has no bindings, no workgroup memory, and no barriers or atomics.
//! Anything else is a whole shader, and says so here, when the node is compiled.

use naga::{Block, Statement, TypeInner};

/// One parameter of `apply` after the element.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Side {
    pub(crate) name: String,
    pub(crate) ty: String,
    pub(crate) stride: u32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Element {
    /// Every top-level name, to be prefixed when bodies are put together.
    pub(crate) names: Vec<String>,
    pub(crate) input: String,
    pub(crate) output: String,
    pub(crate) input_stride: u32,
    pub(crate) output_stride: u32,
    pub(crate) config: Option<(String, u32)>,
    pub(crate) sides: Vec<Side>,
    pub(crate) params: Vec<String>,
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a of the source with a closing separator, as `daedalus_wgpu::merge::source_hash` does.
pub(crate) fn source_hash(source: &str) -> u64 {
    let mut hash = FNV_OFFSET;
    for &byte in source.as_bytes() {
        hash = (hash ^ u64::from(byte)).wrapping_mul(FNV_PRIME);
    }
    (hash ^ 0xff).wrapping_mul(FNV_PRIME)
}

/// The WGSL name of a type an element can be: a number, a vector of them, or a struct.
fn type_name(module: &naga::Module, ty: naga::Handle<naga::Type>) -> Result<String, String> {
    let scalar = |scalar: &naga::Scalar| match (scalar.kind, scalar.width) {
        (naga::ScalarKind::Float, 4) => Ok("f32"),
        (naga::ScalarKind::Uint, 4) => Ok("u32"),
        (naga::ScalarKind::Sint, 4) => Ok("i32"),
        _ => Err("an element is made of f32, u32 or i32".to_string()),
    };
    let declared = &module.types[ty];
    match &declared.inner {
        TypeInner::Scalar(of) => Ok(scalar(of)?.to_string()),
        TypeInner::Vector { size, scalar: of } => {
            Ok(format!("vec{}<{}>", *size as u8, scalar(of)?))
        }
        TypeInner::Struct { .. } => declared
            .name
            .clone()
            .ok_or_else(|| "a struct with no name".to_string()),
        _ => Err("an element is a number, a vector of numbers, or a struct of them".to_string()),
    }
}

/// The first statement in `block` that only a whole shader may have.
fn forbidden(block: &Block) -> Option<&'static str> {
    block.iter().find_map(|statement| match statement {
        Statement::Atomic { .. } | Statement::ImageAtomic { .. } => Some("an atomic operation"),
        Statement::ControlBarrier(_) | Statement::MemoryBarrier(_) => Some("a barrier"),
        Statement::WorkGroupUniformLoad { .. } => Some("workgroup memory"),
        Statement::ImageStore { .. } => Some("a texture write"),
        Statement::Block(inner) => forbidden(inner),
        Statement::If { accept, reject, .. } => forbidden(accept).or_else(|| forbidden(reject)),
        Statement::Switch { cases, .. } => cases.iter().find_map(|case| forbidden(&case.body)),
        Statement::Loop {
            body, continuing, ..
        } => forbidden(body).or_else(|| forbidden(continuing)),
        _ => None,
    })
}

/// Reads `source` as an element body.
pub(crate) fn read(source: &str) -> Result<Element, String> {
    let module = naga::front::wgsl::parse_str(source).map_err(|error| {
        let text = error.emit_to_string(source);
        text.trim_end().trim_start_matches("error: ").to_string()
    })?;
    if let Some(point) = module.entry_points.first() {
        return Err(format!(
            "`{}` is an entry point: an element body is functions only, and `apply` is called for each element",
            point.name
        ));
    }
    if let Some((_, global)) = module.global_variables.iter().next() {
        return Err(format!(
            "`{}` is a global: an element body sees only the element `apply` is given, so it declares no bindings and no memory",
            global.name.as_deref().unwrap_or("?")
        ));
    }
    if module.overrides.iter().next().is_some() {
        return Err("an element body has no overrides".into());
    }
    let apply = module
        .functions
        .iter()
        .map(|(_, function)| function)
        .find(|function| function.name.as_deref() == Some("apply"))
        .ok_or("an element body has a function named `apply`")?;
    for (_, function) in module.functions.iter() {
        if let Some(what) = forbidden(&function.body) {
            let name = function.name.as_deref().unwrap_or("?");
            return Err(format!("`{name}` uses {what}, which an element body can't"));
        }
    }

    let mut layouter = naga::proc::Layouter::default();
    layouter
        .update(module.to_ctx())
        .map_err(|error| format!("the body's types can't be laid out: {error}"))?;
    let stride = |ty| layouter[ty].to_stride();

    let mut arguments = apply.arguments.iter();
    let element = arguments
        .next()
        .ok_or("`apply` takes the element as its first parameter")?;
    let result = apply
        .result
        .as_ref()
        .ok_or("`apply` returns the element it makes")?;
    let name_of = |argument: &naga::FunctionArgument| argument.name.clone().unwrap_or_default();
    let mut params = vec![name_of(element)];
    let mut config = None;
    let mut sides = Vec::new();
    for argument in arguments {
        let name = name_of(argument);
        let ty = type_name(&module, argument.ty).map_err(|why| format!("`{name}`: {why}"))?;
        if name == "cfg" {
            if !matches!(module.types[argument.ty].inner, TypeInner::Struct { .. }) {
                return Err("`cfg` is a struct".into());
            }
            config = Some((ty, layouter[argument.ty].size));
        } else {
            sides.push(Side {
                name: name.clone(),
                ty,
                stride: stride(argument.ty),
            });
        }
        params.push(name);
    }

    // Every top-level name gets a prefix when bodies are put together. A member, parameter or
    // local of the same name wouldn't, so the two may not share a name.
    let mut names: Vec<String> = Vec::new();
    let mut inner: Vec<String> = Vec::new();
    for (_, function) in module.functions.iter() {
        names.extend(function.name.clone());
        inner.extend(
            function
                .arguments
                .iter()
                .filter_map(|argument| argument.name.clone()),
        );
        inner.extend(
            function
                .local_variables
                .iter()
                .filter_map(|(_, local)| local.name.clone()),
        );
        inner.extend(function.named_expressions.values().cloned());
    }
    for (_, ty) in module.types.iter() {
        if let TypeInner::Struct { members, .. } = &ty.inner {
            names.extend(ty.name.clone());
            inner.extend(members.iter().filter_map(|member| member.name.clone()));
        }
    }
    names.extend(
        module
            .constants
            .iter()
            .filter_map(|(_, constant)| constant.name.clone()),
    );
    names.sort();
    names.dedup();
    if let Some(clash) = names.iter().find(|name| inner.contains(name)) {
        return Err(format!(
            "`{clash}` is both a top-level name and a member, parameter or local: rename one, so the body can be put beside others"
        ));
    }

    Ok(Element {
        names,
        input: type_name(&module, element.ty).map_err(|why| format!("the element: {why}"))?,
        output: type_name(&module, result.ty)
            .map_err(|why| format!("what `apply` returns: {why}"))?,
        input_stride: stride(element.ty),
        output_stride: stride(result.ty),
        config,
        sides,
        params,
    })
}

#[cfg(test)]
mod tests {
    use super::{Side, read, source_hash};

    #[test]
    fn a_element_body_is_read_from_its_function() {
        let element = read(
            "struct Config { gain: f32, bias: f32 }
             const HALF: f32 = 0.5;
             fn scale(x: vec4<f32>, by: f32) -> vec4<f32> { return x * by; }
             fn apply(c: vec4<f32>, mask: f32, cfg: Config) -> vec4<f32> {
                 return scale(c, cfg.gain * HALF) * mask + vec4<f32>(cfg.bias);
             }",
        )
        .expect("an element body");
        assert_eq!(element.names, ["Config", "HALF", "apply", "scale"]);
        assert_eq!(
            (element.input.as_str(), element.output.as_str()),
            ("vec4<f32>", "vec4<f32>")
        );
        assert_eq!((element.input_stride, element.output_stride), (16, 16));
        assert_eq!(element.config, Some(("Config".to_string(), 8)));
        let mask = Side {
            name: "mask".into(),
            ty: "f32".into(),
            stride: 4,
        };
        assert_eq!(element.sides, [mask]);
        assert_eq!(element.params, ["c", "mask", "cfg"]);
        // vec3 pads to 16 in an array.
        let padded = read("fn apply(c: vec3<f32>) -> u32 { return u32(c.x); }").expect("a body");
        assert_eq!((padded.input_stride, padded.output_stride), (16, 4));
    }

    #[test]
    fn what_can_see_past_its_own_element_is_not_a_element_body() {
        let refused = |source: &str, text: &str| {
            let error = read(source).expect_err("not an element body");
            assert!(error.contains(text), "{error}");
        };
        refused(
            "@group(0) @binding(0) var<storage, read> src: array<u32>;
             fn apply(c: u32) -> u32 { return src[c + 1u]; }",
            "`src` is a global",
        );
        refused(
            "var<private> seen: u32; fn apply(c: u32) -> u32 { seen = c; return seen; }",
            "`seen` is a global",
        );
        refused(
            "fn apply(c: u32) -> u32 { return c; } @compute @workgroup_size(1) fn main() {}",
            "`main` is an entry point",
        );
        refused(
            "fn other(c: u32) -> u32 { return c; }",
            "a function named `apply`",
        );
        refused(
            "fn apply() -> u32 { return 1u; }",
            "the element as its first parameter",
        );
        refused("fn apply(c: u32) { }", "returns the element");
        refused(
            "fn apply(c: u32, cfg: u32) -> u32 { return c + cfg; }",
            "`cfg` is a struct",
        );
        refused(
            "fn apply(c: mat2x2<f32>) -> f32 { return c[0][0]; }",
            "the element: an element is",
        );
        refused(
            "struct Config { apply: f32 } fn apply(c: f32, cfg: Config) -> f32 { return c; }",
            "`apply` is both a top-level name and a member",
        );
        refused("fn apply(c: u32) -> u32 { return d; }", "d");
    }

    #[test]
    fn the_source_hash_is_fnv_with_a_closing_separator() {
        // The same bytes daedalus_wgpu::merge::source_hash gives, which a test there pins.
        assert_eq!(source_hash(""), 0xaf64_724c_8602_eb6e);
        assert_ne!(source_hash("a"), source_hash("b"));
    }
}
