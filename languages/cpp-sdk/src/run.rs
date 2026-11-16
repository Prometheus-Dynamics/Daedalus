//! A run message: its inputs are decoded, its nodes are called in order, and its outputs are sent.
//! A failure ends the run with its fault, then outputs with the fault status, so the host's wait for
//! the run's outputs always ends.

use std::ptr;

use daedalus_wire::codec::Reader;
use daedalus_wire::{
    BoundaryValue, FaultCode, Outputs, RunStatus, Schema, WireError, WireValue, WorkerMsg,
    decode_value,
};

use crate::arena::RunArena;
use crate::convert::{from_wire, to_wire};
use crate::ctx::{Ctx, DdCtx};
use crate::ffi::{DdValue, MAX_NODES, PORTS};
use crate::send::{Packets, encode, send_fault};
use crate::serve::{Slot, Worker};

/// One input a run carries, bound to its node and port by index.
struct Bound {
    node: usize,
    port: usize,
    value: WireValue,
}

/// Answers one run message. `message` is the packet after its kind byte.
pub fn handle_run(worker: &mut Worker, packets: &mut Packets, message: &[u8]) {
    let mut input = Reader::new(message);
    let Ok((run, node_count)) = header(&mut input) else {
        send_fault(
            packets,
            0,
            None,
            FaultCode::MalformedPacket,
            "the run message does not decode",
        );
        return;
    };
    if node_count > MAX_NODES {
        fail_run(
            packets,
            run,
            None,
            FaultCode::Unsupported,
            "a run has more nodes than the SDK serves at once",
        );
        return;
    }
    let mut picked: Vec<Option<usize>> = Vec::with_capacity(node_count);
    let mut unknown = false;
    for _ in 0..node_count {
        let Ok(id) = input.bytes() else {
            fail_run(
                packets,
                run,
                None,
                FaultCode::MalformedPacket,
                "a node id does not decode",
            );
            return;
        };
        let index = worker.find_node(&id);
        if index.is_none() {
            send_fault(packets, run, None, FaultCode::UnknownNode, "no such node");
            unknown = true;
        }
        picked.push(index);
    }
    let Ok(input_count) = input.count() else {
        fail_run(
            packets,
            run,
            None,
            FaultCode::MalformedPacket,
            "the inputs do not decode",
        );
        return;
    };
    let mut bound = Vec::with_capacity(input_count);
    for _ in 0..input_count {
        let (Ok(node_name), Ok(port_name)) = (input.bytes(), input.bytes()) else {
            fail_run(
                packets,
                run,
                None,
                FaultCode::MalformedPacket,
                "an input does not decode",
            );
            return;
        };
        let Some((node, port)) = worker.find_port(&node_name, &port_name) else {
            fail_run(
                packets,
                run,
                None,
                FaultCode::BadInput,
                "an input names no declared port",
            );
            return;
        };
        let value = match decode_value(
            &worker.served[node].inputs[port].1,
            &mut input,
            &worker.schema,
        ) {
            Ok(value) => value,
            Err(_) => {
                let id = worker.served[node].id.clone();
                fail_run(
                    packets,
                    run,
                    Some(&id),
                    FaultCode::MalformedPacket,
                    "an input value does not decode",
                );
                return;
            }
        };
        bound.push(Bound { node, port, value });
    }
    if input.finish().is_err() {
        fail_run(
            packets,
            run,
            None,
            FaultCode::MalformedPacket,
            "bytes after the run message",
        );
        return;
    }
    if unknown {
        send_outputs(packets, worker, run, &[], &[], false);
        return;
    }

    let nodes: Vec<usize> = picked.into_iter().flatten().collect();
    let mut arena = RunArena::default();
    let mut results = Vec::with_capacity(nodes.len());
    let mut ok = true;
    for &index in &nodes {
        match run_node(worker, packets, &mut arena, run, &bound, index) {
            Some(values) => results.push(values),
            None => {
                ok = false;
                break;
            }
        }
    }
    send_outputs(packets, worker, run, &nodes, &results, ok);
}

/// The run's id and its node count.
fn header(input: &mut Reader<'_>) -> Result<(u64, usize), WireError> {
    let run = input.u64()?;
    // The region is the host's placement of the run; the worker does not use it.
    input.u32()?;
    Ok((run, input.count()?))
}

/// Calls one node: binds its inputs, runs it, and stores its state. Returns its outputs, or `None`
/// when the node failed, after its fault is sent.
fn run_node(
    worker: &mut Worker,
    packets: &mut Packets,
    arena: &mut RunArena,
    run: u64,
    bound: &[Bound],
    index: usize,
) -> Option<[DdValue; PORTS]> {
    let node = &worker.served[index];
    let Some(run_fn) = node.run else {
        send_fault(
            packets,
            run,
            Some(&node.id),
            FaultCode::Internal,
            "the node has no function",
        );
        return None;
    };
    let mut inputs = [DdValue::ZERO; PORTS];
    for port in 0..node.inputs.len().min(PORTS) {
        let Some(found) = bound
            .iter()
            .rev()
            .find(|b| b.node == index && b.port == port)
        else {
            send_fault(
                packets,
                run,
                Some(&node.id),
                FaultCode::BadInput,
                "a declared input has no value",
            );
            return None;
        };
        inputs[port] = from_wire(found.value.clone(), arena);
    }
    if node.outputs.len() > PORTS {
        send_fault(
            packets,
            run,
            Some(&node.id),
            FaultCode::Unsupported,
            "a node has more than eight outputs",
        );
        return None;
    }
    let state = match (&node.state, &worker.slots[index]) {
        (Some(_), Some(Slot::Value(value))) => {
            let value = from_wire(value.clone(), arena);
            arena.keep_values(vec![value])
        }
        _ => ptr::null_mut(),
    };

    let mut cx = Ctx {
        arena: ptr::from_mut(arena),
        packets: ptr::from_mut(packets),
        run,
        fault: None,
        state,
        state_store: DdValue::ZERO,
        state_set: false,
    };
    let mut results = [DdValue::ZERO; PORTS];
    let cx_ptr = ptr::from_mut(&mut cx).cast::<DdCtx>();
    // SAFETY: `run_fn` is the C function the node table names, which takes a context, the inputs and
    // the outputs. The inputs and outputs have `PORTS` entries, and `cx_ptr` is the live context.
    let code = unsafe { run_fn(cx_ptr, inputs.as_ptr(), results.as_mut_ptr()) };
    if code != 0 {
        let (fault_code, reason) = cx
            .fault
            .take()
            .unwrap_or((FaultCode::NodeError, "the node failed".to_owned()));
        send_fault(packets, run, Some(&node.id), fault_code, &reason);
        return None;
    }
    if cx.state_set
        && let Some(ty) = node.state.as_ref()
    {
        // SAFETY: the node set its state as a value of its state type, and the arena still holds
        // the values the state points to.
        let slot = match unsafe { to_wire(ty, &cx.state_store, 0) } {
            Ok(value) => Slot::Value(value),
            Err(_) => Slot::Invalid,
        };
        worker.slots[index] = Some(slot);
    }
    Some(results)
}

/// Sends a run's outputs: its status, then each output value in node order. When the run failed,
/// or an output does not encode, the outputs are sent with the fault status and none in them.
fn send_outputs(
    packets: &mut Packets,
    worker: &Worker,
    run: u64,
    nodes: &[usize],
    results: &[[DdValue; PORTS]],
    ok: bool,
) {
    let full = ok
        .then(|| collect(worker, nodes, results))
        .flatten()
        .and_then(|outputs| {
            let msg = WorkerMsg::Outputs(Outputs {
                run,
                status: RunStatus::Ok,
                outputs,
            });
            encode(&msg, &worker.schema).ok()
        });
    let body = match full {
        Some(body) => body,
        None => fault_outputs(run),
    };
    let _ = packets.write(&body);
}

/// The outputs of the run's nodes, each value read as its declared type. `None` when one does not.
fn collect(
    worker: &Worker,
    nodes: &[usize],
    results: &[[DdValue; PORTS]],
) -> Option<Vec<BoundaryValue>> {
    let mut outputs = Vec::new();
    for (&index, values) in nodes.iter().zip(results) {
        let node = &worker.served[index];
        for (port, (name, ty)) in node.outputs.iter().enumerate() {
            // SAFETY: the node wrote this output as its declared type, and the run's arena still holds
            // the values the output points to.
            let value = unsafe { to_wire(ty, &values[port], 0) }.ok()?;
            outputs.push(BoundaryValue {
                node: node.id.clone(),
                port: name.clone(),
                value,
            });
        }
    }
    Some(outputs)
}

/// The body of a run's outputs with the fault status and no values.
fn fault_outputs(run: u64) -> Vec<u8> {
    let msg = WorkerMsg::Outputs(Outputs {
        run,
        status: RunStatus::Fault,
        outputs: Vec::new(),
    });
    encode(&msg, &Schema::new()).unwrap_or_default()
}

/// Ends a run that cannot continue: its fault, then outputs with the fault status.
fn fail_run(packets: &mut Packets, run: u64, node: Option<&str>, code: FaultCode, reason: &str) {
    send_fault(packets, run, node, code, reason);
    let _ = packets.write(&fault_outputs(run));
}
