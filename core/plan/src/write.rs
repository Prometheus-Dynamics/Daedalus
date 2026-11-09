//! Writing a Blob.

#![cfg(feature = "alloc")]

use alloc::vec::Vec;

use daedalus_types::fnv1a64;

use crate::format::{HEADER_LEN, MAGIC, PlanError, SECTION_COUNT, StepRecord, VERSION};

/// The same records as a Blob, owned.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PlanData {
    pub steps: Vec<StepRecord>,
    pub nodes: Vec<u32>,
    pub slots: Vec<u32>,
    pub spaces: Vec<u32>,
    pub types: Vec<u32>,
    pub bodies: Vec<u32>,
    /// Words a form keeps for itself.
    pub extra: Vec<u32>,
}

fn too_big(section: &'static str) -> PlanError {
    PlanError::Bounds {
        section,
        at: u32::MAX,
    }
}

fn count(items: usize, section: &'static str) -> Result<u32, PlanError> {
    u32::try_from(items).map_err(|_| too_big(section))
}

fn push_step(out: &mut Vec<u8>, step: &StepRecord) {
    out.extend_from_slice(&[step.kind as u8, 0, 0, 0]);
    for field in [
        step.body,
        step.covers.start,
        step.covers.len,
        step.reads.start,
        step.reads.len,
        step.writes.start,
        step.writes.len,
    ] {
        out.extend_from_slice(&field.to_le_bytes());
    }
}

/// Appends the Blob for `plan` to `out`.
pub fn encode(plan: &PlanData, out: &mut Vec<u8>) -> Result<(), PlanError> {
    let mut body = Vec::new();
    let mut table = [(0u32, 0u32); SECTION_COUNT];
    let offset =
        |body: &Vec<u8>, name| u32::try_from(HEADER_LEN + body.len()).map_err(|_| too_big(name));
    table[0] = (offset(&body, "steps")?, count(plan.steps.len(), "steps")?);
    plan.steps
        .iter()
        .for_each(|step| push_step(&mut body, step));
    let words: [(&[u32], &'static str); 6] = [
        (&plan.nodes, "nodes"),
        (&plan.slots, "slots"),
        (&plan.spaces, "spaces"),
        (&plan.types, "types"),
        (&plan.bodies, "bodies"),
        (&plan.extra, "extra"),
    ];
    for (index, (section, name)) in words.into_iter().enumerate() {
        table[index + 1] = (offset(&body, name)?, count(section.len(), name)?);
        section
            .iter()
            .for_each(|word| body.extend_from_slice(&word.to_le_bytes()));
    }
    out.reserve(HEADER_LEN + body.len());
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&[0; 6]);
    out.extend_from_slice(&fnv1a64(&body).to_le_bytes());
    for (at, items) in table {
        out.extend_from_slice(&at.to_le_bytes());
        out.extend_from_slice(&items.to_le_bytes());
    }
    out.extend_from_slice(&body);
    Ok(())
}
