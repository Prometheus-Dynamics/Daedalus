//! A region of shader nodes as one step: every node's pass in one encoder, in the region's
//! order, and one submit.
//!
//! Values between the region's nodes stay on the device, in buffers the region owns. Nothing is
//! read back between them and no transfer is planned. The step returns its submission as a
//! fence, so the runner wakes the region's consumers when the device has finished.

#[cfg(target_os = "linux")]
mod chained;
mod submit;

use std::sync::Arc;

use daedalus_backend::{CompileCx, NodeBody, PortRef, Refusal, Region, StepUnit, UnitInfo};
use daedalus_graph::Lit;
use daedalus_types::{NodeIndex, PortIndex};

use crate::WgpuBackend;
use crate::buffer::Storage;
use crate::record::RecordBody;
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
    /// What the node's recording body says, for its output port of this index.
    Recorded { output: usize },
}

/// Where a buffer the region makes lives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Place {
    /// Device memory: where a value read by other shaders is.
    Device,
    /// Memory the host has mapped.
    Mapped,
    /// A dma-buf from the system heap, which the host reads through its cache.
    Heap,
}

/// Whether `here` leaves the region, as which output, and where the planner wants it written.
fn published(backend: &WgpuBackend, region: &Region<'_>, here: PortRef) -> (Option<usize>, Place) {
    use crate::memory::{SPACE_DMA_HEAP, SPACE_HOST_MAPPED};
    let at = region.outputs.iter().position(|output| *output == here);
    let device = backend.device_name();
    let is = |space: &str, id| crate::handoff::on_device(device, id) == space;
    let place = match at.and_then(|at| region.placed.get(at)) {
        Some(space) if is(space, SPACE_DMA_HEAP) => Place::Heap,
        Some(space) if is(space, SPACE_HOST_MAPPED) => Place::Mapped,
        _ => Place::Device,
    };
    (at, place)
}

/// A buffer the region makes. One that leaves the region is handed to the runner each run, so
/// the region keeps a few and reuses one nobody else still holds.
pub(crate) struct Owned {
    pub(crate) size: Size,
    /// Which memory the buffer is made in.
    pub(crate) place: Place,
    /// The step's output cell this buffer is published in, if it leaves the region.
    pub(crate) publish: Option<usize>,
    pub(crate) kept: Vec<Arc<Storage>>,
    /// The one this run uses, as an index into `kept`.
    pub(crate) current: usize,
    /// Set for a run in which the value is a texture and not a buffer: a chain of element bodies
    /// that was given a texture writes one.
    pub(crate) texture: Option<Arc<crate::texture::Texture>>,
}

/// A merged chain of element bodies, kept so it can run as a fragment pass when its input turns
/// out to be a texture. Whether a value is a buffer or a texture is only known when it arrives.
pub(crate) struct ElementChain {
    pub(crate) specs: Vec<crate::merge::ElementSpec>,
    /// Every side input in chain order, and every config with its size, as a fragment pass
    /// binds them.
    pub(crate) sides: Vec<Source>,
    pub(crate) configs: Vec<(Source, u32)>,
    /// The region buffer the chain's output is.
    pub(crate) output: usize,
    /// The pass, for the format it was last built for.
    pub(crate) pass: Option<(crate::texture::Format, crate::fragment::FragmentPass)>,
    /// Textures the chain has written, reused once nobody else holds them.
    pub(crate) targets: Vec<Arc<crate::texture::Texture>>,
    /// A small buffer for each config, which a fragment pass reads as storage.
    pub(crate) config_buffers: Vec<Storage>,
}

/// How a node does its work.
pub(crate) enum Work {
    /// One compute pass of a shader.
    Shader(Shader),
    /// The author's own recording.
    Record(RecordBody),
}

pub(crate) struct Node {
    pub(crate) index: NodeIndex,
    pub(crate) work: Work,
    /// For a shader, one per binding in the binding table's order. For a recording body, one
    /// per input port.
    pub(crate) sources: Vec<Source>,
    /// The buffers this node writes, which are sized before it runs: (position, buffer). The
    /// position is a binding's for a shader and an output port's for a recording body.
    pub(crate) makes: Vec<(usize, usize)>,
    /// Set for a merged chain of element bodies.
    pub(crate) element: Option<ElementChain>,
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

/// Where the value on an input port comes from: a cell of the step when the port is on the
/// region's boundary, and a buffer the region owns when a node before it in the region made it.
fn source_for(
    here: PortRef,
    uniform: bool,
    region: &Region<'_>,
    cx: &CompileCx<'_>,
    made: &[(PortRef, usize)],
) -> Result<Option<Source>, Refusal> {
    if let Some(cell) = region.inputs.iter().position(|input| *input == here) {
        return Ok(Some(if uniform {
            Source::Bytes(cell)
        } else {
            Source::Cell(cell)
        }));
    }
    let Some(edge) = cx.edges.iter().find(|edge| edge.to == here) else {
        return Ok(None);
    };
    match made.iter().find(|(port, _)| *port == edge.from) {
        Some(&(_, buffer)) => Ok(Some(Source::Buffer(buffer))),
        None => Err(refuse(
            here.node,
            "an input comes from a node later in the region",
        )),
    }
}

/// One node for a whole chain of element bodies: a single shader, generated from their functions,
/// that reads the first node's input and writes the last node's output. Nothing is written for
/// the values in between.
fn merged_node(
    backend: &WgpuBackend,
    chain: &[NodeIndex],
    region: &Region<'_>,
    cx: &CompileCx<'_>,
    context: &Arc<crate::context::Context>,
    owned: &mut Vec<Owned>,
    made: &mut Vec<(PortRef, usize)>,
) -> Result<Node, Refusal> {
    use crate::merge::{ElementSpec, Slot};
    let (head, tail) = (chain[0], chain[chain.len() - 1]);
    let mut specs: Vec<&ElementSpec> = Vec::with_capacity(chain.len());
    for &node in chain {
        let body = body_of(node, cx)?;
        let Some(spec) = body.code.downcast_ref::<ElementSpec>() else {
            return Err(refuse(
                node,
                "a chain holds a node that isn't an element body",
            ));
        };
        if spec.inputs.len() != usize::from(body.inputs) || body.outputs != 1 {
            return Err(refuse(
                node,
                "an element body has one output, and a port for each parameter",
            ));
        }
        specs.push(spec);
    }
    let spec = crate::merge::merged(&specs);
    let shader = Shader::load_on(Arc::clone(context), spec)
        .map_err(|error| refuse(tail, format!("the merged shader was refused: {error}")))?;
    // The port of the node at `at` that feeds its parameter `name`.
    let port_of = |at: usize, name: &str| {
        let param = specs[at].params.iter().position(|param| *param == name)?;
        let port = specs[at]
            .inputs
            .iter()
            .position(|&have| have as usize == param)?;
        Some(PortRef {
            node: chain[at],
            port: PortIndex(port as u16),
        })
    };
    let mut sources = Vec::new();
    let mut makes = Vec::new();
    let (mut sides, mut configs, mut output) = (Vec::new(), Vec::new(), 0);
    for (position, (slot, kind, _)) in crate::merge::layout(&specs).into_iter().enumerate() {
        let named = |source: Option<Source>, what: &str| {
            source.ok_or_else(|| refuse(tail, format!("nothing is connected to {what}")))
        };
        let source = match slot {
            Slot::Input => {
                let element = port_of(0, specs[0].params[0]);
                let source = match element {
                    Some(port) => source_for(port, false, region, cx, made)?,
                    None => None,
                };
                named(source, "the chain's input")?
            }
            Slot::Output => {
                let here = PortRef {
                    node: tail,
                    port: PortIndex(0),
                };
                let (publish, place) = published(backend, region, here);
                owned.push(Owned {
                    size: Size::Like {
                        position: 0,
                        of: specs[0].input_stride.max(1),
                        stride: specs[specs.len() - 1].output_stride.max(1),
                    },
                    publish,
                    place,
                    kept: Vec::new(),
                    current: 0,
                    texture: None,
                });
                made.push((here, owned.len() - 1));
                makes.push((position, owned.len() - 1));
                Source::Buffer(owned.len() - 1)
            }
            // A config no port feeds is zeroes.
            Slot::Config(at) => match port_of(at, "cfg") {
                Some(port) => source_for(port, true, region, cx, made)?.unwrap_or(Source::Zeroes),
                None => Source::Zeroes,
            },
            Slot::Side(at, side) => {
                let name = specs[at].sides[side].name;
                let source = match port_of(at, name) {
                    Some(port) => source_for(port, false, region, cx, made)?,
                    None => None,
                };
                named(source, &format!("`{name}` of `{}`", specs[at].name))?
            }
        };
        match (slot, kind) {
            (Slot::Output, _) => output = owned.len() - 1,
            (Slot::Config(_), BindingKind::Uniform { size }) => configs.push((source, size)),
            (Slot::Side(..), _) => sides.push(source),
            _ => {}
        }
        sources.push(source);
    }
    let _ = head;
    Ok(Node {
        index: tail,
        work: Work::Shader(shader),
        sources,
        makes,
        element: Some(ElementChain {
            specs: specs.iter().map(|spec| **spec).collect(),
            sides,
            configs,
            output,
            pass: None,
            targets: Vec::new(),
            config_buffers: Vec::new(),
        }),
    })
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

    // Element bodies run as merged chains, each where its last node stands.
    let chains = crate::merge::segments(region, cx, &context.record.compute)
        .map_err(|(node, reason)| refuse(node, reason))?;

    for &index in region.nodes {
        let body = body_of(index, cx)?;
        if let Some(chain) = chains.iter().find(|chain| chain.contains(&index)) {
            if chain.last() == Some(&index) {
                let node =
                    merged_node(backend, chain, region, cx, &context, &mut owned, &mut made)?;
                nodes.push(node);
            }
            continue;
        }
        if let Some(record) = body.code.downcast_ref::<RecordBody>() {
            // Its ports are its inputs and outputs by index: there is no binding table.
            let mut sources = Vec::with_capacity(usize::from(body.inputs));
            for port in 0..body.inputs {
                let here = PortRef {
                    node: index,
                    port: PortIndex(port),
                };
                sources.push(
                    if let Some(cell) = region.inputs.iter().position(|input| *input == here) {
                        Source::Cell(cell)
                    } else if let Some(edge) = cx.edges.iter().find(|edge| edge.to == here) {
                        match made.iter().find(|(port, _)| *port == edge.from) {
                            Some(&(_, buffer)) => Source::Buffer(buffer),
                            None => {
                                let text = "an input comes from a node later in the region";
                                return Err(refuse(index, text));
                            }
                        }
                    } else {
                        return Err(refuse(
                            index,
                            format!("nothing is connected to input {port}"),
                        ));
                    },
                );
            }
            let mut makes = Vec::with_capacity(usize::from(body.outputs));
            for port in 0..body.outputs {
                let here = PortRef {
                    node: index,
                    port: PortIndex(port),
                };
                let (publish, place) = published(backend, region, here);
                owned.push(Owned {
                    size: Size::Recorded {
                        output: usize::from(port),
                    },
                    publish,
                    place,
                    kept: Vec::new(),
                    current: 0,
                    texture: None,
                });
                made.push((here, owned.len() - 1));
                makes.push((usize::from(port), owned.len() - 1));
            }
            if sources.len() > crate::shader::pipeline::MAX_BINDINGS {
                return Err(refuse(index, "a recording body has at most 16 inputs"));
            }
            nodes.push(Node {
                index,
                work: Work::Record(*record),
                sources,
                makes,
                element: None,
            });
            continue;
        }
        let Some(spec) = body.code.downcast_ref::<ShaderSpec>() else {
            return Err(refuse(index, "this body isn't a shader or a recording"));
        };
        let ports_match = spec.inputs.len() == usize::from(body.inputs)
            && spec.outputs.len() == usize::from(body.outputs);
        if !ports_match {
            return Err(refuse(index, "the shader's ports aren't the node's ports"));
        }
        let shader = Shader::load_on(Arc::clone(&context), spec).map_err(|error| match error {
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
            let (publish, place) = published(backend, region, here);
            owned.push(Owned {
                size: size_of(index, body, spec, position)?,
                publish,
                place,
                kept: Vec::new(),
                current: 0,
                texture: None,
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
            work: Work::Shader(shader),
            sources: resolved,
            makes,
            element: None,
        });
    }

    let step = RegionStep::new(
        Arc::clone(backend.link()),
        context,
        nodes,
        owned,
        region.inputs.len(),
    );
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
