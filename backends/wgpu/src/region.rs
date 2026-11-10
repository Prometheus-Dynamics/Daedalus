//! A region of shader nodes as one step: every node's pass in one encoder, in the region's
//! order, and one submit.
//!
//! Values between the region's nodes stay on the device, in buffers the region owns. Nothing is
//! read back between them and no transfer is planned. The step returns its submission as a
//! fence, so the runner wakes the region's consumers when the device has finished.

mod submit;

use std::sync::Arc;

use daedalus_backend::{CompileCx, NodeBody, PortRef, Refusal, Region, StepUnit, UnitInfo};
use daedalus_graph::Lit;
use daedalus_types::{NodeIndex, PortIndex};

use crate::WgpuBackend;
use crate::buffer::Storage;
use crate::shader::{BindingKind, Shader, ShaderError, ShaderSpec};

pub(crate) use submit::RegionStep;

/// Where one binding of one node gets its resource.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Source {
    /// A storage buffer or texture in the step's input cell of this index.
    Cell(usize),
    /// The bytes of a uniform, in the step's input cell of this index.
    Bytes(usize),
    /// A uniform no port feeds: all zeroes.
    Zeroes,
    /// A buffer the region owns, by index.
    Buffer(usize),
    /// The shader's own sampler.
    Sampler,
}

/// How large a buffer the region owns is, decided each run from what the node was given.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Size {
    /// This many bytes, from the node's metadata.
    Bytes(u64),
    /// As many elements of `stride` bytes as the buffer at this binding position of the same
    /// node has elements of `of` bytes.
    Like {
        position: usize,
        of: u32,
        stride: u32,
    },
}

/// A buffer the region makes. One that leaves the region is handed to the runner each run, so
/// the region keeps a few and reuses one nobody else still holds.
pub(crate) struct Owned {
    pub(crate) size: Size,
    /// The step's output cell this buffer is published in, if it leaves the region.
    pub(crate) publish: Option<usize>,
    pub(crate) kept: Vec<Arc<Storage>>,
    /// The one this run uses, as an index into `kept`.
    pub(crate) current: usize,
}

pub(crate) struct Node {
    pub(crate) index: NodeIndex,
    pub(crate) shader: Shader,
    /// One per binding, in the binding table's order.
    pub(crate) sources: Vec<Source>,
    /// The buffers this node writes, which are sized before it runs: (binding position, buffer).
    pub(crate) makes: Vec<(usize, usize)>,
}

fn refuse(node: NodeIndex, reason: impl AsRef<str>) -> Refusal {
    Refusal::new(vec![node], reason.as_ref())
}

fn body_of<'a>(node: NodeIndex, cx: &'a CompileCx<'a>) -> Result<&'a NodeBody<'a>, Refusal> {
    cx.bodies
        .iter()
        .find(|body| body.node == node)
        .ok_or_else(|| refuse(node, "the region names a node with no body"))
}

/// The size rule for the buffer at binding `position` of a node.
fn size_of(
    node: NodeIndex,
    body: &NodeBody<'_>,
    spec: &ShaderSpec,
    position: usize,
) -> Result<Size, Refusal> {
    let binding = &spec.bindings[position];
    let BindingKind::Storage { stride, .. } = binding.kind else {
        return Err(refuse(
            node,
            format!(
                "`{}` is written by the shader and isn't a storage buffer: only buffers can be a shader node's output so far",
                binding.name
            ),
        ));
    };
    // The graph can say outright.
    if let Some(Lit::Int(bytes)) = body.metadata.get(&format!("bytes.{}", binding.name)) {
        return match u64::try_from(*bytes) {
            Ok(bytes) if bytes > 0 && bytes.is_multiple_of(4) => Ok(Size::Bytes(bytes)),
            _ => Err(refuse(
                node,
                format!(
                    "`bytes.{}` has to be a positive multiple of 4",
                    binding.name
                ),
            )),
        };
    }
    // Otherwise it has as many elements as the first buffer the shader reads.
    let like = spec
        .bindings
        .iter()
        .enumerate()
        .find_map(|(at, other)| match other.kind {
            BindingKind::Storage {
                read_only: true,
                stride: of,
            } if of > 0 => Some((at, of)),
            _ => None,
        });
    match like {
        Some((position, of)) if stride > 0 => Ok(Size::Like {
            position,
            of,
            stride,
        }),
        _ => Err(refuse(
            node,
            format!(
                "nothing says how large `{0}` is: the shader reads no array to match, so the node needs `bytes.{0}` in its metadata",
                binding.name
            ),
        )),
    }
}

/// Compiles `region` into one step that covers all of its nodes.
pub(crate) fn compile(
    backend: &WgpuBackend,
    region: &Region<'_>,
    cx: &CompileCx<'_>,
) -> Result<StepUnit, Refusal> {
    let context = backend
        .context()
        .map_err(|error| Refusal::whole_region(&format!("no device: {error}")))?;
    let mut owned: Vec<Owned> = Vec::new();
    // The buffer behind an output port of a node in this region.
    let mut made: Vec<(PortRef, usize)> = Vec::new();
    let mut nodes = Vec::with_capacity(region.nodes.len());

    for &index in region.nodes {
        let body = body_of(index, cx)?;
        let Some(spec) = body.code.downcast_ref::<ShaderSpec>() else {
            return Err(refuse(index, "this body isn't a shader"));
        };
        let ports_match = spec.inputs.len() == usize::from(body.inputs)
            && spec.outputs.len() == usize::from(body.outputs);
        if !ports_match {
            return Err(refuse(index, "the shader's ports aren't the node's ports"));
        }
        let shader = Shader::load(backend, spec).map_err(|error| match error {
            ShaderError::Limits(text) | ShaderError::Build(text) => refuse(index, text),
            other => refuse(index, other.to_string()),
        })?;

        let mut sources: Vec<Option<Source>> = vec![None; spec.bindings.len()];
        for (port, &position) in spec.inputs.iter().enumerate() {
            let here = PortRef {
                node: index,
                port: PortIndex(port as u16),
            };
            let uniform = matches!(
                spec.bindings[position as usize].kind,
                BindingKind::Uniform { .. }
            );
            let source = if let Some(cell) = region.inputs.iter().position(|input| *input == here) {
                if uniform {
                    Source::Bytes(cell)
                } else {
                    Source::Cell(cell)
                }
            } else if let Some(edge) = cx.edges.iter().find(|edge| edge.to == here) {
                let buffer = made.iter().find(|(port, _)| *port == edge.from);
                let Some(&(_, buffer)) = buffer else {
                    return Err(refuse(
                        index,
                        "an input comes from a node later in the region",
                    ));
                };
                Source::Buffer(buffer)
            } else {
                let name = spec.bindings[position as usize].name;
                return Err(refuse(index, format!("nothing is connected to `{name}`")));
            };
            sources[position as usize] = Some(source);
        }
        let mut makes = Vec::new();
        for (port, &position) in spec.outputs.iter().enumerate() {
            let here = PortRef {
                node: index,
                port: PortIndex(port as u16),
            };
            let position = position as usize;
            owned.push(Owned {
                size: size_of(index, body, spec, position)?,
                publish: region.outputs.iter().position(|output| *output == here),
                kept: Vec::new(),
                current: 0,
            });
            let buffer = owned.len() - 1;
            made.push((here, buffer));
            makes.push((position, buffer));
            sources[position] = Some(Source::Buffer(buffer));
        }
        // What no port feeds: a uniform is zeroes, a sampler is the shader's own, and anything
        // else has nowhere to come from.
        let mut resolved = Vec::with_capacity(sources.len());
        for (source, binding) in sources.into_iter().zip(spec.bindings) {
            resolved.push(match (source, &binding.kind) {
                (Some(source), _) => source,
                (None, BindingKind::Uniform { .. }) => Source::Zeroes,
                (None, BindingKind::Sampler { .. }) => Source::Sampler,
                (None, _) => {
                    let text = format!("no port of the node feeds the binding `{}`", binding.name);
                    return Err(refuse(index, text));
                }
            });
        }
        nodes.push(Node {
            index,
            shader,
            sources: resolved,
            makes,
        });
    }

    let step = RegionStep::new(context, nodes, owned, region.inputs.len());
    Ok(StepUnit {
        step: daedalus_abi::Step::boxed(step),
        info: UnitInfo {
            covers: region.nodes.to_vec(),
            reads: region.inputs.to_vec(),
            writes: region.outputs.to_vec(),
            absorbed: Vec::new(),
            may_pend: true,
        },
    })
}
