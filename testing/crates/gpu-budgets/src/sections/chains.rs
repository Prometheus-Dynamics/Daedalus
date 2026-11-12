//! A chain of four element bodies as one merged shader against the same four as four separate
//! shaders, one after the other with the values carried between them in buffers.

use daedalus_gpu_budgets::chain;
use daedalus_wgpu::merge::chain_shader;
use daedalus_wgpu::shader::{Bound, Run};
use daedalus_wgpu::{Storage, WgpuBackend};

use super::{RUNS, WARM, median, ms, word_size};

/// Values, one word each, as the merge budgets run them.
const VALUES: u64 = 1_000_000;

pub fn run(backend: &WgpuBackend) {
    let bodies = chain(2);
    let words: Vec<u8> = (0..VALUES as u32)
        .flat_map(|v| (v as u16).to_le_bytes().into_iter().chain([0, 0]))
        .collect();
    let size = word_size(4 * VALUES);
    let first = Storage::new(backend, size).expect("a buffer");
    first.write(0, &words).expect("in range");
    let stages: Vec<Storage> = (0..4)
        .map(|_| Storage::new(backend, size).expect("a buffer"))
        .collect();

    let merged = chain_shader(backend, &bodies).expect("merges");
    let one = median(WARM, RUNS, || {
        let Run::Started(ticket) = merged
            .run(&[Bound::Storage(&first), Bound::Storage(&stages[0])])
            .expect("runs")
        else {
            panic!("nothing else is in flight");
        };
        ticket.wait().expect("finishes");
    });

    let separate: Vec<_> = bodies
        .iter()
        .map(|body| chain_shader(backend, &[*body]).expect("a body"))
        .collect();
    // Each separate node is waited for before the next starts: a shader answers `Busy` while
    // another run of it or of a neighbour is in flight, so the nodes run one after the other.
    let apart = median(WARM, RUNS, || {
        let mut from = &first;
        for (i, shader) in separate.iter().enumerate() {
            let to = &stages[i];
            let run = shader
                .run(&[Bound::Storage(from), Bound::Storage(to)])
                .expect("runs");
            let Run::Started(ticket) = run else {
                panic!("nothing else is in flight");
            };
            ticket.wait().expect("finishes");
            from = to;
        }
    });
    println!(
        "four element bodies, {VALUES} values: one merged shader {:.3} ms; four separate shaders {:.3} ms",
        ms(one),
        ms(apart)
    );
}
