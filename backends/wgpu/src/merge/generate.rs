//! Writes the WGSL of a merged chain.

use std::fmt::Write as _;

use super::{Form, PixelSpec, Slot, layout};
use crate::WgpuBackend;
use crate::fragment::{FragmentError, FragmentPass};
use crate::shader::BindingKind;
use crate::texture::Format;

/// Invocations along x in one workgroup of a merged compute pass.
pub(crate) const WORKGROUP: u32 = 64;

pub(crate) fn kind_tag(kind: &BindingKind) -> [u8; 9] {
    let (tag, a, b) = match *kind {
        BindingKind::Storage { read_only, stride } => (1u8, u32::from(read_only), stride),
        BindingKind::Uniform { size } => (2, size, 0),
        _ => (0, 0, 0),
    };
    let mut out = [tag; 9];
    out[1..5].copy_from_slice(&a.to_le_bytes());
    out[5..9].copy_from_slice(&b.to_le_bytes());
    out
}

fn is_ident(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

/// `text` with every whole identifier in `names` given `prefix`. A name after a `.` is a
/// member, not a top-level name, and is left alone.
pub(crate) fn prefixed(text: &str, names: &[&str], prefix: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = String::with_capacity(text.len() + names.len() * prefix.len());
    let mut at = 0;
    while at < bytes.len() {
        let starts = is_ident(bytes[at]) && !bytes[at].is_ascii_digit();
        let inside = at > 0 && is_ident(bytes[at - 1]);
        if !starts || inside {
            // Not the start of an identifier: copy the character through, whole.
            let end = (at + 1..=bytes.len())
                .find(|&end| text.is_char_boundary(end))
                .unwrap_or(bytes.len());
            out.push_str(&text[at..end]);
            at = end;
            continue;
        }
        let end = (at..bytes.len())
            .find(|&end| !is_ident(bytes[end]))
            .unwrap_or(bytes.len());
        let word = &text[at..end];
        let member = text[..at].trim_end().ends_with('.');
        if !member && names.contains(&word) {
            out.push_str(prefix);
        }
        out.push_str(word);
        at = end;
    }
    out
}

fn prefix_of(at: usize) -> String {
    format!("n{at}_")
}

/// Each body with its names prefixed, one after the other.
fn bodies(chain: &[&PixelSpec]) -> String {
    let mut out = String::new();
    for (at, spec) in chain.iter().enumerate() {
        let _ = writeln!(out, "// {}", spec.name);
        out.push_str(&prefixed(spec.source, spec.names, &prefix_of(at)));
        out.push('\n');
    }
    out
}

/// The calls, in chain order: `let c1 = n0_apply(c0, ..);` and so on. `side` and `config` say
/// how a node's other arguments are read.
fn calls(
    chain: &[&PixelSpec],
    side: impl Fn(usize, usize) -> String,
    config: impl Fn(usize) -> String,
) -> String {
    let mut out = String::new();
    for (at, spec) in chain.iter().enumerate() {
        let mut args = vec![format!("c{at}")];
        let mut next_side = 0;
        for param in spec.params.iter().skip(1) {
            if *param == "cfg" && spec.config.is_some() {
                args.push(config(at));
            } else {
                args.push(side(at, next_side));
                next_side += 1;
            }
        }
        let _ = writeln!(
            out,
            "    let c{} = {}apply({});",
            at + 1,
            prefix_of(at),
            args.join(", ")
        );
    }
    out
}

/// The compute form: one pass over storage buffers, one invocation per pixel.
pub(crate) fn compute(chain: &[&PixelSpec]) -> String {
    let mut out = bodies(chain);
    let ty = |at: usize, text: &str| prefixed(text, chain[at].names, &prefix_of(at));
    for (binding, (slot, kind, name)) in layout(chain).iter().enumerate() {
        let declared = match (slot, kind) {
            (Slot::Input, _) => format!(
                "var<storage, read> {name}: array<{}>",
                ty(0, chain[0].input)
            ),
            (Slot::Output, _) => {
                let last = chain.len() - 1;
                format!(
                    "var<storage, read_write> {name}: array<{}>",
                    ty(last, chain[last].output)
                )
            }
            (Slot::Config(at), _) => {
                let config = chain[*at].config.map_or("", |(config, _)| config);
                format!("var<uniform> {name}: {}", ty(*at, config))
            }
            (Slot::Side(at, side), _) => {
                format!(
                    "var<storage, read> {name}: array<{}>",
                    ty(*at, chain[*at].sides[*side].ty)
                )
            }
        };
        let _ = writeln!(out, "@group(0) @binding({binding}) {declared};");
    }
    let _ = writeln!(
        out,
        "@compute @workgroup_size({WORKGROUP})\nfn main(@builtin(global_invocation_id) id: vec3<u32>) {{\n    let i = id.x;\n    if i >= arrayLength(&dst) {{ return; }}\n    let c0 = src[i];"
    );
    out.push_str(&calls(
        chain,
        |at, side| format!("side_{at}_{side}[i]"),
        |at| format!("cfg_{at}"),
    ));
    let _ = writeln!(out, "    dst[i] = c{};\n}}", chain.len());
    out
}

/// The texel type a format is loaded and written as, or `None` for one a fragment can't write.
fn texel(format: Format) -> &'static str {
    match format {
        Format::R8 | Format::Rg8 | Format::Rgba8 | Format::Bgra8 | Format::R32F | Format::Rg32F => {
            "f32"
        }
        Format::R8Uint | Format::Rgba16Uint | Format::Rg32Uint => "u32",
    }
}

/// The fragment form: one render pass over textures. Bindings are the input texture, then each
/// node's side textures in chain order, then each node's config as a storage buffer in chain
/// order, which is the order [`FragmentPass::bind`] takes them in.
pub(crate) fn fragment(chain: &[&PixelSpec], scalar: &str) -> String {
    let mut out = bodies(chain);
    let mut binding = 0;
    let _ = writeln!(
        out,
        "@group(0) @binding({binding}) var src: texture_2d<{scalar}>;"
    );
    for (at, spec) in chain.iter().enumerate() {
        for side in 0..spec.sides.len() {
            binding += 1;
            let _ = writeln!(
                out,
                "@group(0) @binding({binding}) var side_{at}_{side}: texture_2d<{scalar}>;"
            );
        }
    }
    for (at, spec) in chain.iter().enumerate() {
        if let Some((config, _)) = spec.config {
            binding += 1;
            let config = prefixed(config, spec.names, &prefix_of(at));
            let _ = writeln!(
                out,
                "@group(0) @binding({binding}) var<storage, read> cfg_{at}: {config};"
            );
        }
    }
    let _ = writeln!(
        out,
        "@fragment fn fs(@builtin(position) pos: vec4<f32>) -> @location(0) vec4<{scalar}> {{\n    let at = vec2<i32>(pos.xy);\n    let c0 = textureLoad(src, at, 0);"
    );
    out.push_str(&calls(
        chain,
        |at, side| format!("textureLoad(side_{at}_{side}, at, 0)"),
        |at| format!("cfg_{at}"),
    ));
    let _ = writeln!(out, "    return c{};\n}}", chain.len());
    out
}

/// A chain as one fragment pass that writes a target of `format`, for tile-based devices where a
/// render pass beats compute. Every node takes and returns a `vec4` of the format's scalar. Bind
/// the input texture, then every side texture in chain order, and then every config in chain
/// order as a storage buffer.
pub fn fragment_pass(
    backend: &WgpuBackend,
    chain: &[&PixelSpec],
    format: Format,
) -> Result<FragmentPass, FragmentError> {
    let context = backend.context().map_err(FragmentError::Open)?;
    fragment_pass_on(context, chain, format)
}

/// [`fragment_pass`] on `context`'s device.
pub(crate) fn fragment_pass_on(
    context: std::sync::Arc<crate::context::Context>,
    chain: &[&PixelSpec],
    format: Format,
) -> Result<FragmentPass, FragmentError> {
    let scalar = texel(format);
    let wanted = format!("vec4<{scalar}>");
    let texels = |ty: &str| ty.replace(' ', "") == wanted;
    let fits = chain.iter().all(|spec| {
        texels(spec.input) && texels(spec.output) && spec.sides.iter().all(|s| texels(s.ty))
    });
    if chain.is_empty() || !fits {
        return Err(FragmentError::Invalid(format!(
            "a chain drawn to {format:?} is one or more bodies that each take and return {wanted}"
        )));
    }
    FragmentPass::new_on(context, &fragment(chain, scalar), &[format])
}

/// Whether two WGSL type names are the same type, as far as a chain is concerned.
pub(crate) fn same_type(a: &str, b: &str) -> bool {
    a.split_whitespace().eq(b.split_whitespace())
}

/// Which form `Form` names, for the cache key's benefit.
pub(crate) const fn form_name(form: Form) -> &'static str {
    match form {
        Form::Compute => "compute",
        Form::Fragment => "fragment",
    }
}

#[cfg(test)]
mod tests {
    use super::prefixed;

    #[test]
    fn only_whole_top_level_names_are_prefixed() {
        let source = "struct Config { gain: f32 }\nfn helper(x: f32) -> f32 { return x; }\nfn apply(c: f32, cfg: Config) -> f32 { return helper(c) * cfg.gain + helper2(c).apply; }";
        let got = prefixed(source, &["Config", "helper", "apply"], "n3_");
        assert_eq!(
            got,
            "struct n3_Config { gain: f32 }\nfn n3_helper(x: f32) -> f32 { return x; }\nfn n3_apply(c: f32, cfg: n3_Config) -> f32 { return n3_helper(c) * cfg.gain + helper2(c).apply; }"
        );
        assert_eq!(prefixed("vec4<f32>", &["apply"], "n0_"), "vec4<f32>");
        assert_eq!(
            prefixed("é apply", &["apply"], "p_"),
            "é p_apply",
            "other text passes through"
        );
    }
}
