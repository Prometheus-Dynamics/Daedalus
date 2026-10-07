//! Copy/residency classification of edge adapter paths and the text form of plan explanations.

use core::fmt;

use daedalus_transport::{AdaptKind, FRAME_INTERFACE_KEY, Residency};

use super::{
    RuntimeEdgeExplanation, RuntimeEdgeHandoff, RuntimeEdgeTransport, RuntimePlanExplanation,
};

impl RuntimeEdgeTransport {
    /// Whether a step of the adapter path may copy payload data ([`AdaptKind::copies_data`]).
    /// Steps without a recorded kind count as copying.
    pub fn copies_data(&self) -> bool {
        (self.adapter_path.is_empty() && !self.adapter_steps.is_empty())
            || self.adapter_path.iter().any(|step| step.kind.copies_data())
    }

    /// Whether the adapter path moves the payload between residencies (CPU, GPU, external): a
    /// device-transfer step, a step producing another residency than the step before it, or a
    /// last residency other than the target port's.
    pub fn crosses_residency(&self) -> bool {
        let mut current = None;
        for step in &self.adapter_path {
            if step.kind.is_device_transfer() {
                return true;
            }
            if let Some(residency) = step.residency {
                if current.is_some_and(|current| current != residency) {
                    return true;
                }
                current = Some(residency);
            }
        }
        matches!((current, self.target_residency), (Some(last), Some(target)) if last != target)
    }

    /// Device uploads and downloads one run of the adapter path performs.
    pub fn device_transfers(&self) -> (u64, u64) {
        self.adapter_path
            .iter()
            .fold((0, 0), |(up, down), step| match step.kind {
                AdaptKind::DeviceUpload => (up + 1, down),
                AdaptKind::DeviceDownload => (up, down + 1),
                AdaptKind::DeviceTransfer
                    if matches!(step.residency, Some(Residency::Gpu | Residency::CpuAndGpu)) =>
                {
                    (up + 1, down)
                }
                AdaptKind::DeviceTransfer => (up, down + 1),
                _ => (up, down),
            })
    }

    /// Whether the edge carries a frame-like payload: the `daedalus:frame` interface, or a type
    /// key along the path naming a frame or an image.
    pub fn carries_frame(&self) -> bool {
        let frame_like = |key: &str| {
            key == FRAME_INTERFACE_KEY || {
                let key = key.to_ascii_lowercase();
                key.contains("frame") || key.contains("image")
            }
        };
        self.source_transport
            .iter()
            .chain(&self.target_transport)
            .chain(&self.transport_target)
            .chain(
                self.adapter_path
                    .iter()
                    .flat_map(|step| [&step.from, &step.to]),
            )
            .any(|key| frame_like(key.as_str()))
    }
}

impl fmt::Display for RuntimeEdgeHandoff {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            RuntimeEdgeHandoff::Queue => "queue",
            RuntimeEdgeHandoff::DirectSlot => "direct_slot",
        })
    }
}

impl fmt::Display for RuntimeEdgeExplanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "edge {}: {}.{} -> {}.{} [{}]",
            self.index,
            self.from_node_id,
            self.from_port,
            self.to_node_id,
            self.to_port,
            self.handoff
        )?;
        let path = self
            .transport
            .as_ref()
            .map(|transport| transport.adapter_path.as_slice())
            .unwrap_or_default();
        if !path.is_empty() {
            f.write_str(" adapters=")?;
            for (idx, step) in path.iter().enumerate() {
                let sep = if idx == 0 { "" } else { " -> " };
                write!(f, "{sep}{}:{}", step.adapter, step.kind)?;
            }
        } else if !self.adapter_steps.is_empty() {
            write!(f, " adapters={:?}", self.adapter_steps)?;
        }
        if self.copies_frame {
            f.write_str(" copies_frame")?;
        }
        if self.crosses_residency {
            f.write_str(" crosses_residency")?;
        }
        match &self.fusion_block {
            None if self.fused => f.write_str(" fused"),
            Some(block) => write!(f, " unfused: {block}"),
            None => Ok(()),
        }
    }
}

impl RuntimePlanExplanation {
    fn write_flagged(
        &self,
        f: &mut fmt::Formatter<'_>,
        name: &str,
        edges: &[usize],
    ) -> fmt::Result {
        write!(f, "{name}: ")?;
        if edges.is_empty() {
            return f.write_str("none");
        }
        for (idx, edge_idx) in edges.iter().enumerate() {
            let sep = if idx == 0 { "" } else { ", " };
            match self.edges.iter().find(|edge| edge.index == *edge_idx) {
                Some(edge) => write!(
                    f,
                    "{sep}edge {edge_idx} ({}.{} -> {}.{})",
                    edge.from_node_id, edge.from_port, edge.to_node_id, edge.to_port
                )?,
                None => write!(f, "{sep}edge {edge_idx}")?,
            }
        }
        Ok(())
    }
}

/// One line per node and edge, then the copying and residency-crossing edges.
impl fmt::Display for RuntimePlanExplanation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            f,
            "runtime plan: {} nodes, {} edges, backpressure={:?}",
            self.nodes.len(),
            self.edges.len(),
            self.backpressure
        )?;
        for node in &self.nodes {
            write!(f, "  node {}: {} ({:?})", node.index, node.id, node.compute)?;
            if let Some(label) = node.label.as_deref().filter(|label| *label != node.id) {
                write!(f, " label={label}")?;
            }
            writeln!(f)?;
        }
        for edge in &self.edges {
            writeln!(f, "  {edge}")?;
        }
        self.write_flagged(f, "copies_frame", &self.copying_edges)?;
        writeln!(f)?;
        self.write_flagged(f, "crosses_residency", &self.crossing_edges)?;
        f.write_str("\nfused_units: ")?;
        if self.fused_units.is_empty() {
            return f.write_str("none");
        }
        for (idx, unit) in self.fused_units.iter().enumerate() {
            let sep = if idx == 0 { "" } else { "; " };
            write!(
                f,
                "{sep}[{}] edges {:?}",
                unit.node_ids.join(" -> "),
                unit.edges
            )?;
        }
        Ok(())
    }
}
