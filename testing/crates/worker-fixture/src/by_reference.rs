//! Values by reference, for a host that agreed them.
//!
//! An input that came as a place is read where it is: in the host's lent buffer, or in a buffer
//! sent by descriptor, mapped read-only. `fixture.raw_io` reverses its bytes straight into the
//! value buffer the run's note named, so neither side copies the value. Any other node is given
//! its inputs' bytes, and a large `bytes` output it returns is written into the value buffer.
//!
//! `fixture.dmabuf_probe` is served only with `DAEDALUS_FIXTURE_PROBE` set: it takes a `t:shot`,
//! a buffer by descriptor, and reports what the worker saw of it. So are `fixture.padded_echo`,
//! `fixture.floats_echo` and `fixture.floats_count`.

use std::collections::BTreeMap;

use daedalus_process_shm::{Brackets, Lent, View, Writes};
use daedalus_wire::codec::Writer;
use daedalus_wire::shared::{At, Place};
use daedalus_wire::{
    BoundaryValue, Declaration, Dir, FaultCode, PortSpec, Schema, WireType, WireValue,
    decode_placed, encode_bare,
};

use crate::nodes;

/// The probe's input type: a buffer by descriptor.
pub const SHOT: &str = "t:shot";

/// The probe's declarations: `fixture.dmabuf_probe`, and `fixture.dmabuf_fail`, which reads its
/// shot like the probe and then fails, so a test can see the read's brackets close on a fault too.
/// And `fixture.padded_echo`, which gives back a struct of a `u8`, a `u32` and a `u16`: seven
/// bytes on the wire, which a Rust struct of the same fields lays out with padding. And
/// `fixture.floats_echo`, which gives back a list of `f32`, and `fixture.floats_count`, which
/// gives back how many there are.
pub fn probes() -> [Declaration; 5] {
    let mut fail = probe();
    fail.id = "fixture.dmabuf_fail".into();
    let mut count = floats();
    count.id = "fixture.floats_count".into();
    count.outputs = vec![PortSpec::new(
        "count",
        WireType::Scalar(daedalus_types::Scalar::U32),
    )];
    [probe(), fail, padded(), floats(), count]
}

fn floats() -> Declaration {
    let list = WireType::List(Box::new(WireType::Scalar(daedalus_types::Scalar::F32)));
    Declaration {
        id: "fixture.floats_echo".into(),
        inputs: vec![PortSpec::new("in", list.clone())],
        outputs: vec![PortSpec::new("out", list)],
        config: Vec::new(),
        state: None,
        capabilities: Vec::new(),
    }
}

/// The struct `fixture.padded_echo` takes and gives back.
pub fn padded_type() -> WireType {
    use daedalus_types::Scalar;
    WireType::Struct(vec![
        ("a".into(), WireType::Scalar(Scalar::U8)),
        ("b".into(), WireType::Scalar(Scalar::U32)),
        ("c".into(), WireType::Scalar(Scalar::U16)),
    ])
}

fn padded() -> Declaration {
    Declaration {
        id: "fixture.padded_echo".into(),
        inputs: vec![PortSpec::new("in", padded_type())],
        outputs: vec![PortSpec::new("out", padded_type())],
        config: Vec::new(),
        state: None,
        capabilities: Vec::new(),
    }
}

fn probe() -> Declaration {
    Declaration {
        id: "fixture.dmabuf_probe".into(),
        inputs: vec![PortSpec::new("shot", WireType::Opaque(SHOT.into()))],
        outputs: vec![PortSpec::new(
            "report",
            WireType::Scalar(daedalus_types::Scalar::String),
        )],
        config: Vec::new(),
        state: None,
        capabilities: Vec::new(),
    }
}

/// Runs a run's nodes with its inputs where they are.
pub fn execute(
    lent: &Lent,
    schema: &Schema,
    node: &str,
    inputs: &[BoundaryValue],
    state: &mut BTreeMap<String, WireValue>,
) -> Result<Vec<BoundaryValue>, (FaultCode, String)> {
    lent.view(|view| {
        let slots: Vec<u32> = inputs
            .iter()
            .filter_map(|entry| match &entry.value {
                WireValue::Placed(Place {
                    at: At::Fd(slot), ..
                }) => Some(*slot),
                _ => None,
            })
            .collect();
        // Every read of a buffer by descriptor is inside the kernel's brackets.
        for slot in &slots {
            view.begin_read(*slot)
                .map_err(|reason| (FaultCode::Internal, reason))?;
        }
        let result = run_in(view, schema, node, inputs, state);
        for slot in &slots {
            view.end_read(*slot);
        }
        result
    })
}

fn run_in(
    view: &mut View<'_>,
    schema: &Schema,
    node: &str,
    inputs: &[BoundaryValue],
    state: &mut BTreeMap<String, WireValue>,
) -> Result<Vec<BoundaryValue>, (FaultCode, String)> {
    let threshold = view.threshold();
    let brackets = view.brackets();
    let (reads, mut writes) = view.split();
    let bytes_of = |place: &Place| {
        reads
            .bytes(place)
            .ok_or_else(|| (FaultCode::BadInput, format!("{place:?} is not lent memory")))
    };
    let found = |port: &str| {
        inputs
            .iter()
            .find(|entry| entry.node == node && entry.port == port)
            .map(|entry| &entry.value)
    };
    match node {
        "fixture.raw_io" => {
            if let Some(WireValue::Placed(place)) = found("payload") {
                let from = bytes_of(place)?;
                return Ok(vec![output(node, "out", reversed(from, &mut writes))]);
            }
        }
        "fixture.dmabuf_probe" => {
            let report = match found("shot") {
                Some(WireValue::Placed(place)) => report(bytes_of(place)?, true, brackets),
                Some(WireValue::Opaque(bytes)) => report(bytes, false, brackets),
                _ => return Err((FaultCode::BadInput, "no shot".into())),
            };
            return Ok(vec![output(node, "report", WireValue::String(report))]);
        }
        "fixture.dmabuf_fail" => {
            if let Some(WireValue::Placed(place)) = found("shot") {
                let bytes = bytes_of(place)?;
                let sum = bytes
                    .iter()
                    .fold(0u64, |sum, b| sum.wrapping_add(u64::from(*b)));
                return Err((FaultCode::Internal, format!("failed after reading {sum}")));
            }
            return Err((FaultCode::BadInput, "no shot by reference".into()));
        }
        _ => {}
    }
    // Any other node is given its inputs' values, read from where they are.
    let mut owned = Vec::with_capacity(inputs.len());
    for entry in inputs {
        let value = match &entry.value {
            WireValue::Placed(place) => {
                let ty = schema
                    .lookup(&entry.node, Dir::Input, &entry.port)
                    .map_err(|error| (FaultCode::BadInput, error.to_string()))?;
                decode_placed(ty, bytes_of(place)?, schema)
                    .map_err(|error| (FaultCode::BadInput, error.to_string()))?
            }
            other => other.clone(),
        };
        owned.push(BoundaryValue {
            node: entry.node.clone(),
            port: entry.port.clone(),
            value,
        });
    }
    let mut values = nodes::execute(node, &owned, state)?;
    // A large output of a type with a form is written into the value buffer as the host's cell
    // will hold it, with an array's items aligned.
    let mut bare = Writer::new();
    for value in &mut values {
        let Ok(ty) = schema.lookup(&value.node, Dir::Output, &value.port) else {
            continue;
        };
        if !schema.has_form(ty) {
            continue;
        }
        bare.buf.clear();
        if encode_bare(ty, &value.value, schema, &mut bare).is_err() || bare.buf.len() < threshold {
            continue;
        }
        let items_at = ty.layout().items_at().unwrap_or(0);
        if let Some((place, room)) = writes.room_for(bare.buf.len(), items_at) {
            room.copy_from_slice(&bare.buf);
            value.value = WireValue::Placed(place);
        }
    }
    Ok(values)
}

/// `from` reversed: into the value buffer when there is room, so the value is never copied
/// again, and into the packet otherwise.
fn reversed(from: &[u8], writes: &mut Writes<'_>) -> WireValue {
    match writes.room(from.len()) {
        Some((place, room)) => {
            for (to, byte) in room.iter_mut().zip(from.iter().rev()) {
                *to = *byte;
            }
            WireValue::Placed(place)
        }
        None => WireValue::Bytes(from.iter().rev().copied().collect()),
    }
}

/// What the probe saw: the bytes' sum and length, how the worker holds them (`r--s` is a shared
/// read-only mapping, `copy` bytes that came in the packet), and what it did to its buffers by
/// descriptor so far.
fn report(bytes: &[u8], placed: bool, brackets: Brackets) -> String {
    let sum = bytes
        .iter()
        .fold(0u64, |sum, byte| sum.wrapping_add(u64::from(*byte)));
    let held = if placed {
        mapping_of(bytes.as_ptr() as usize).unwrap_or_else(|| "unmapped".into())
    } else {
        "copy".into()
    };
    format!(
        "sum={sum} len={} held={held} attached={} begun={} ended={} unbracketed={}",
        bytes.len(),
        brackets.attached,
        brackets.begun,
        brackets.ended,
        brackets.unbracketed
    )
}

/// The permissions of the mapping that holds `address`, from `/proc/self/maps`.
fn mapping_of(address: usize) -> Option<String> {
    let maps = std::fs::read_to_string("/proc/self/maps").ok()?;
    maps.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        let (start, end) = fields.next()?.split_once('-')?;
        let start = usize::from_str_radix(start, 16).ok()?;
        let end = usize::from_str_radix(end, 16).ok()?;
        (start..end)
            .contains(&address)
            .then(|| fields.next().unwrap_or("").to_owned())
    })
}

fn output(node: &str, port: &str, value: WireValue) -> BoundaryValue {
    BoundaryValue {
        node: node.to_owned(),
        port: port.to_owned(),
        value,
    }
}
