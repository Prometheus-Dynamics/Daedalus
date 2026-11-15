//! Where placing a value starts to pay for a worker that hasn't agreed any type by reference: a
//! sweep of payload sizes, each handed to `fixture.raw_io` three ways.
//!
//! - `by_reference`: the worker agreed values by reference for bytes alone (`BY_REFERENCE`
//!   without `ANY_TYPE`). The payload goes as a place and the worker reverses it straight into its
//!   value buffer.
//! - `packet`: the worker agreed shared memory for whole packets alone. The run's packet, payload
//!   and all, is written into the shared buffer instead of the socket.
//! - `socket`: the worker agreed nothing, and the packet goes on the socket.
//!
//! The node's work, reversing the bytes, is the same each way. For `by_reference` and `packet`,
//! the threshold is the smallest size from which the way is no slower than `socket` at every
//! larger size: the default for a worker of that kind is set from it. Each row also gives every
//! way's cost per MiB.

use std::path::Path;

use daedalus_abi::testing::release;
use daedalus_backend_process::{ProcessBackend, WorkerSpec};
use daedalus_worker_conformance::runner::registry;

use crate::minor_faults;
use crate::step::one_node;
use crate::sweep::{SIZES, threshold, time};

/// A worker-fixture at `program` that asks for what `env` says, placing values from
/// `shared_from` bytes.
fn backend(program: &Path, env: &str, shared_from: usize) -> ProcessBackend {
    let mut backend = ProcessBackend::new();
    backend
        .add_worker(
            WorkerSpec::new(program)
                .env(env, "1")
                .shared_from(shared_from),
            &registry(),
        )
        .expect("the fixture registers its nodes");
    backend
}

fn per_mib(ns: f64, bytes: usize) -> f64 {
    ns * f64::from(1u32 << 20) / bytes as f64
}

/// Runs the sweep with the worker-fixture at `program`, `runs` timed runs per size and way.
pub fn sweep(program: &Path, runs: usize) {
    let by_reference = backend(program, "DAEDALUS_FIXTURE_BYTES_ONLY", 0);
    let packet = backend(program, "DAEDALUS_FIXTURE_NO_REFERENCE", 0);
    let socket = backend(program, "DAEDALUS_FIXTURE_PLAIN", usize::MAX);
    let mut steps = [
        one_node(&by_reference, "fixture.raw_io", 1, 1),
        one_node(&packet, "fixture.raw_io", 1, 1),
        one_node(&socket, "fixture.raw_io", 1, 1),
    ];
    let mut rows = Vec::with_capacity(SIZES.len());
    for size in SIZES {
        let mut payload: Vec<u8> = (0..size).map(|at| (at * 31 + 7) as u8).collect();
        let mut times = [(0.0, 0.0); 3];
        let mut faults = [0.0; 3];
        for ((step, time_of), faults) in steps.iter_mut().zip(&mut times).zip(&mut faults) {
            let before = minor_faults();
            *time_of = time(step, &mut payload, runs);
            // Over the warm-up and the timed runs.
            *faults = (minor_faults() - before) as f64 * 1000.0 / (runs + runs / 10 + 4) as f64;
        }
        let [
            (reference_ns, reference_p99),
            (packet_ns, packet_p99),
            (socket_ns, socket_p99),
        ] = times;
        rows.push((size, reference_ns, packet_ns, socket_ns));
        println!(
            "bytes bytes {size} by_reference_ns {reference_ns:.0} by_reference_p99_ns {reference_p99:.0} packet_ns {packet_ns:.0} packet_p99_ns {packet_p99:.0} socket_ns {socket_ns:.0} socket_p99_ns {socket_p99:.0} by_reference_ns_per_mib {:.0} packet_ns_per_mib {:.0} socket_ns_per_mib {:.0} by_reference_faults_per_1000 {:.0} packet_faults_per_1000 {:.0} socket_faults_per_1000 {:.0}",
            per_mib(reference_ns, size),
            per_mib(packet_ns, size),
            per_mib(socket_ns, size),
            faults[0],
            faults[1],
            faults[2],
        );
    }
    let reference = threshold(&rows, |row| row.1);
    let whole = threshold(&rows, |row| row.2);
    println!("threshold bytes by_reference_bytes {reference} packet_bytes {whole}");
    let telemetry = by_reference.telemetry();
    println!(
        "agreed by_reference {} packet {} socket {}",
        telemetry.agreed,
        packet.telemetry().grants,
        socket.telemetry().grants
    );
    for step in steps {
        release(step);
    }
}
