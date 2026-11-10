//! The bodies of the canonical fixtures, as the worker runs them.

use std::collections::BTreeMap;

use daedalus_wire::{BoundaryValue, FaultCode, WireValue};

/// Finds an input by node and port name.
fn input<'a>(inputs: &'a [BoundaryValue], node: &str, port: &str) -> Option<&'a WireValue> {
    inputs
        .iter()
        .find(|entry| entry.node == node && entry.port == port)
        .map(|entry| &entry.value)
}

fn output(node: &str, port: &str, value: WireValue) -> BoundaryValue {
    BoundaryValue {
        node: node.to_owned(),
        port: port.to_owned(),
        value,
    }
}

fn missing(node: &str, port: &str) -> (FaultCode, String) {
    (FaultCode::BadInput, format!("{node} has no input {port}"))
}

/// Runs one node. `state` holds each stateful node's state between runs.
pub fn execute(
    node: &str,
    inputs: &[BoundaryValue],
    state: &mut BTreeMap<String, WireValue>,
) -> Result<Vec<BoundaryValue>, (FaultCode, String)> {
    let get = |port: &str| {
        input(inputs, node, port)
            .cloned()
            .ok_or_else(|| missing(node, port))
    };
    let identity =
        |port_in: &str, port_out: &str| -> Result<Vec<BoundaryValue>, (FaultCode, String)> {
            Ok(vec![output(node, port_out, get(port_in)?)])
        };
    match node {
        "fixture.scalar_add" => match (get("a")?, get("b")?) {
            (WireValue::I64(a), WireValue::I64(b)) => {
                Ok(vec![output(node, "out", WireValue::I64(a.wrapping_add(b)))])
            }
            _ => Err((FaultCode::BadInput, "scalar_add wants two i64".into())),
        },
        "fixture.bytes_echo"
        | "fixture.struct_record"
        | "fixture.enum_choice"
        | "fixture.optional_value"
        | "fixture.tuple_pair"
        | "fixture.map_pairs"
        | "fixture.custom_type_key" => identity("in", "out"),
        "fixture.list_reverse" => match get("in")? {
            WireValue::List(mut items) => {
                items.reverse();
                Ok(vec![output(node, "out", WireValue::List(items))])
            }
            _ => Err((FaultCode::BadInput, "list_reverse wants a list".into())),
        },
        "fixture.multi_output" => match get("x")? {
            WireValue::U32(x) => Ok(vec![
                output(node, "a", WireValue::U32(x)),
                output(node, "b", WireValue::U32(x.wrapping_mul(2))),
            ]),
            _ => Err((FaultCode::BadInput, "multi_output wants a u32".into())),
        },
        "fixture.raw_io" => match get("payload")? {
            WireValue::Bytes(mut data) => {
                data.reverse();
                Ok(vec![output(node, "out", WireValue::Bytes(data))])
            }
            _ => Err((FaultCode::BadInput, "raw_io wants bytes".into())),
        },
        "fixture.stateful_counter" => match get("delta")? {
            WireValue::I64(delta) => {
                let current = match state.get(node) {
                    Some(WireValue::I64(value)) => *value,
                    _ => 0,
                };
                let total = current.wrapping_add(delta);
                state.insert(node.to_owned(), WireValue::I64(total));
                Ok(vec![output(node, "total", WireValue::I64(total))])
            }
            _ => Err((FaultCode::BadInput, "stateful_counter wants an i64".into())),
        },
        "fixture.failure_diagnostic" => {
            let _ = get("value")?;
            Err((
                FaultCode::NodeError,
                "diagnostic: the value is rejected".into(),
            ))
        }
        other => Err((FaultCode::UnknownNode, format!("no node {other}"))),
    }
}
