//! Per-pixel functions merged into one shader.
//!
//! A pixel body is one WGSL function, `apply`, of the pixel it is given: it can't see any other
//! pixel. So a chain of them, each fed only by the one before, is one loop over the pixels with
//! the functions called in turn, and the values between them never leave the shader. That is
//! what this module generates, in two forms: a compute pass over storage buffers, and a fragment
//! pass over textures for tile-based devices.

mod cache;
mod chain;
mod generate;

pub use generate::fragment_pass;

pub(crate) use cache::merged;
pub(crate) use chain::segments;
pub(crate) use generate::fragment_pass_on;

use crate::shader::{BindingKind, BindingSpec};

/// One more input of a pixel body, read at the same pixel from a buffer of its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PixelSide {
    /// The parameter's name, which is the port's.
    pub name: &'static str,
    /// Its WGSL type.
    pub ty: &'static str,
    /// Bytes of one element of an array of it.
    pub stride: u32,
}

/// A per-pixel function, as the authoring macro read it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct PixelSpec {
    pub name: &'static str,
    /// The WGSL as written: `fn apply(..) -> ..` and whatever it uses.
    pub source: &'static str,
    /// Every top-level name in `source`. They are prefixed when bodies are put together, so two
    /// bodies can both have a `Config` or a `helper`.
    pub names: &'static [&'static str],
    /// The WGSL type of the pixel it takes, and of the one it returns.
    pub input: &'static str,
    pub output: &'static str,
    /// Bytes of one element of an array of each.
    pub input_stride: u32,
    pub output_stride: u32,
    /// The type and size of its `cfg` parameter, when it has one.
    pub config: Option<(&'static str, u32)>,
    /// Its other parameters, in the order `apply` takes them.
    pub sides: &'static [PixelSide],
    /// The parameters of `apply` in order: the pixel, then sides and `cfg` as written.
    pub params: &'static [&'static str],
    /// For each of the node's input ports, in the order it declares them, the position in
    /// `params` of the parameter that port feeds.
    pub inputs: &'static [u32],
    /// FNV-1a of `source`, computed when the node was compiled.
    pub source_hash: u64,
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// FNV-1a over `bytes`, continuing from `hash`, with a separator after.
pub const fn fnv(mut hash: u64, bytes: &[u8]) -> u64 {
    let mut at = 0;
    while at < bytes.len() {
        hash = (hash ^ bytes[at] as u64).wrapping_mul(FNV_PRIME);
        at += 1;
    }
    (hash ^ 0xff).wrapping_mul(FNV_PRIME)
}

/// FNV-1a of a pixel body's source, as the macro computes it.
pub const fn source_hash(source: &str) -> u64 {
    fnv(FNV_OFFSET, source.as_bytes())
}

/// For each port, the position in `params` of the parameter with its name. The authoring macro
/// calls this in a const, so a port that names no parameter fails the build.
pub const fn pixel_ports<const N: usize>(
    ports: &[daedalus_registry::PortDecl],
    params: &[&str],
) -> [u32; N] {
    assert!(ports.len() == N, "one position per declared port");
    let mut out = [0; N];
    let mut port = 0;
    while port < N {
        let name = ports[port].name.as_bytes();
        let mut at = 0;
        let mut found = false;
        while at < params.len() && !found {
            let have = params[at].as_bytes();
            let mut same = have.len() == name.len();
            let mut byte = 0;
            while same && byte < name.len() {
                same = have[byte] == name[byte];
                byte += 1;
            }
            if same {
                out[port] = at as u32;
                found = true;
            }
            at += 1;
        }
        assert!(found, "a declared port names no parameter of `apply`");
        port += 1;
    }
    out
}

/// A chain of pixel bodies as one compute shader on `backend`'s device, for a chain run by
/// hand: in a graph, a region merges the chains it finds. It binds the input buffer, the output
/// buffer, then each body's config and side inputs in chain order.
pub fn chain_shader(
    backend: &crate::WgpuBackend,
    chain: &[&PixelSpec],
) -> Result<crate::shader::Shader, crate::shader::ShaderError> {
    use crate::shader::ShaderError;
    if chain.is_empty() {
        return Err(ShaderError::Build("a chain of no bodies".into()));
    }
    for pair in chain.windows(2) {
        if !generate::same_type(pair[0].output, pair[1].input) {
            return Err(ShaderError::Build(format!(
                "`{}` returns `{}`, and `{}` takes `{}`",
                pair[0].name, pair[0].output, pair[1].name, pair[1].input
            )));
        }
    }
    crate::shader::Shader::load(backend, merged(chain))
}

/// The two forms a chain is generated in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Form {
    /// One compute pass over storage buffers.
    Compute,
    /// One render pass over textures.
    Fragment,
}

/// What a merged chain binds, in binding order, and which node each binding belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Slot {
    /// The first node's pixel input.
    Input,
    /// The last node's output.
    Output,
    /// The `cfg` of the node at this position in the chain.
    Config(usize),
    /// The side input of this index of the node at this position.
    Side(usize, usize),
}

/// The bindings of a chain in its compute form: the input, the output, then each node's config
/// and sides in chain order.
pub(crate) fn layout(chain: &[&PixelSpec]) -> Vec<(Slot, BindingKind, String)> {
    let stride = |spec: Option<&&PixelSpec>, of: fn(&PixelSpec) -> u32| spec.map_or(0, |s| of(s));
    let mut out = vec![
        (
            Slot::Input,
            BindingKind::Storage {
                read_only: true,
                stride: stride(chain.first(), |spec| spec.input_stride),
            },
            "src".to_string(),
        ),
        (
            Slot::Output,
            BindingKind::Storage {
                read_only: false,
                stride: stride(chain.last(), |spec| spec.output_stride),
            },
            "dst".to_string(),
        ),
    ];
    for (at, spec) in chain.iter().enumerate() {
        if let Some((_, size)) = spec.config {
            out.push((
                Slot::Config(at),
                BindingKind::Uniform { size },
                format!("cfg_{at}"),
            ));
        }
        for (side, declared) in spec.sides.iter().enumerate() {
            let kind = BindingKind::Storage {
                read_only: true,
                stride: declared.stride,
            };
            out.push((Slot::Side(at, side), kind, format!("side_{at}_{side}")));
        }
    }
    out
}

/// The stable key of a chain in a form: FNV-1a over each node's name and source hash in order,
/// the binding table, the workgroup size and the form. It depends on content only, so it means
/// the same in every process, and the same chain compiled twice is one pipeline.
pub fn chain_key(chain: &[&PixelSpec], form: Form) -> u64 {
    let mut hash = FNV_OFFSET;
    for spec in chain {
        hash = fnv(hash, spec.name.as_bytes());
        hash = fnv(hash, &spec.source_hash.to_le_bytes());
    }
    for (at, (_, kind, name)) in layout(chain).iter().enumerate() {
        let spec = BindingSpec {
            index: at as u32,
            name: "",
            kind: *kind,
        };
        hash = fnv(hash, &spec.index.to_le_bytes());
        hash = fnv(hash, name.as_bytes());
        hash = fnv(hash, &generate::kind_tag(kind));
    }
    hash = fnv(hash, &generate::WORKGROUP.to_le_bytes());
    fnv(hash, &[form as u8])
}
