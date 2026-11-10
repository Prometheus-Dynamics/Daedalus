//! Which of a region's nodes form chains, and where a chain has to be cut.

use daedalus_backend::{CompileCx, PortRef, Region};
use daedalus_types::{NodeIndex, PortIndex};

use super::{PixelSpec, generate::same_type};
use crate::caps::ComputeCaps;

/// The most uniform buffers one stage binds on any WebGPU device.
const MAX_UNIFORMS: u32 = 12;

fn pixel<'a>(node: NodeIndex, cx: &'a CompileCx<'a>) -> Option<&'a PixelSpec> {
    let body = cx.bodies.iter().find(|body| body.node == node)?;
    body.code.downcast_ref::<PixelSpec>()
}

/// The node input port a pixel body's pixel arrives on.
fn pixel_port(node: NodeIndex, spec: &PixelSpec) -> Option<PortRef> {
    let port = spec.inputs.iter().position(|&param| param == 0)?;
    Some(PortRef {
        node,
        port: PortIndex(port as u16),
    })
}

/// The node that continues a chain after `tail`, if exactly one thing reads `tail`'s output and
/// it is the pixel of a pixel body of the same type.
fn next_of<'a>(
    tail: NodeIndex,
    spec: &PixelSpec,
    region: &Region<'_>,
    cx: &'a CompileCx<'a>,
) -> Option<(NodeIndex, &'a PixelSpec)> {
    let output = PortRef {
        node: tail,
        port: PortIndex(0),
    };
    // A second consumer, inside the region or outside it, needs the value in a buffer.
    if region.outputs.contains(&output) {
        return None;
    }
    let mut readers = cx.edges.iter().filter(|edge| edge.from == output);
    let (only, None) = (readers.next()?, readers.next()) else {
        return None;
    };
    let next = pixel(only.to.node, cx)?;
    let joins =
        pixel_port(only.to.node, next) == Some(only.to) && same_type(spec.output, next.input);
    joins.then_some((only.to.node, next))
}

/// How many storage and uniform buffers a chain binds.
fn counts(chain: &[&PixelSpec]) -> (u32, u32) {
    let sides: usize = chain.iter().map(|spec| spec.sides.len()).sum();
    let configs = chain.iter().filter(|spec| spec.config.is_some()).count();
    (2 + sides as u32, configs as u32)
}

/// Whether a chain fits one shader, on this device and on any device.
fn fits(chain: &[&PixelSpec], caps: &ComputeCaps) -> bool {
    let (storage, uniforms) = counts(chain);
    let floor = ComputeCaps::WEBGPU_DEFAULTS.max_storage_buffers_per_stage;
    storage <= floor.min(caps.max_storage_buffers_per_stage) && uniforms <= MAX_UNIFORMS
}

/// The region's pixel bodies as segments: runs of nodes that become one shader each. A chain
/// follows single-consumer links for as long as they hold, and is cut into the longest pieces
/// that fit a shader's binding limits. A node in no segment isn't a pixel body.
///
/// Each segment is the nodes in chain order. It runs where its last node stands in the region:
/// everything any of its nodes reads is made before that.
pub(crate) fn segments(
    region: &Region<'_>,
    cx: &CompileCx<'_>,
    caps: &ComputeCaps,
) -> Result<Vec<Vec<NodeIndex>>, (NodeIndex, String)> {
    let mut taken: Vec<NodeIndex> = Vec::new();
    let mut out = Vec::new();
    for &head in region.nodes {
        if taken.contains(&head) {
            continue;
        }
        let Some(first) = pixel(head, cx) else {
            continue;
        };
        let mut nodes = vec![head];
        let mut specs = vec![first];
        while let Some((next, spec)) =
            next_of(nodes[nodes.len() - 1], specs[specs.len() - 1], region, cx)
        {
            if taken.contains(&next) || nodes.contains(&next) {
                break;
            }
            nodes.push(next);
            specs.push(spec);
        }
        taken.extend(&nodes);
        // Cut into the longest pieces that fit.
        let mut start = 0;
        while start < nodes.len() {
            let mut end = start + 1;
            if !fits(&specs[start..end], caps) {
                let (storage, _) = counts(&specs[start..end]);
                let text = format!(
                    "`{}` alone binds {storage} storage buffers, more than one shader can",
                    specs[start].name
                );
                return Err((nodes[start], text));
            }
            while end < nodes.len() && fits(&specs[start..=end], caps) {
                end += 1;
            }
            out.push(nodes[start..end].to_vec());
            start = end;
        }
    }
    Ok(out)
}
