//! Each kernel op at 4 MiB of input, on the device and on the CPU kernel of the same name, and
//! the merged chain of two ops against the two ops one after the other.

use std::hint::black_box;
use std::time::Duration;

use daedalus_kernels_sys as cpu;
use daedalus_wgpu::kernels::{F32_TO_UNORM8, HALF_TO_F32, Kernel, Kernels};
use daedalus_wgpu::merge::chain_shader;
use daedalus_wgpu::shader::{Bound, Run};
use daedalus_wgpu::{Storage, WgpuBackend};

use super::{MIB, RUNS, WARM, median, median_of, ms, pattern, word_size};

const INPUT: u64 = 4 * MIB;
/// Values in the chain of two ops, as in the merge budgets.
const CHAIN_VALUES: u64 = 1_000_000;

/// The CPU kernel of the same name, into buffers that are already allocated. Its output is
/// written to `out` and never read, so the call is what is timed.
fn cpu_op(kernel: Kernel, input: &[u8], floats: &[f32], text: &[u8]) -> Box<dyn FnMut()> {
    let n = input.len();
    let input = input.to_vec();
    let floats = floats.to_vec();
    let text = text.to_vec();
    match kernel {
        Kernel::F32FromLe => {
            let mut out = vec![0f32; n / 4];
            Box::new(move || cpu::f32_from_le(black_box(&input), &mut out))
        }
        Kernel::F16ToF32 => {
            let mut out = vec![0f32; n / 2];
            Box::new(move || cpu::f16_to_f32(black_box(&input), &mut out))
        }
        Kernel::F32ToUnorm8 => {
            let mut out = vec![0u8; floats.len()];
            Box::new(move || cpu::f32_to_unorm8(black_box(&floats), &mut out))
        }
        Kernel::Copy => {
            let mut out = vec![0u8; n];
            Box::new(move || cpu::copy_bytes(black_box(&input), &mut out))
        }
        Kernel::HexLower => {
            let mut out = vec![0u8; 2 * n];
            Box::new(move || cpu::hex_lower(black_box(&input), &mut out))
        }
        Kernel::Base64Encode => {
            let mut out = vec![0u8; cpu::base64_encoded_len(n)];
            Box::new(move || {
                black_box(cpu::base64_encode_into(black_box(&input), &mut out));
            })
        }
        Kernel::Base64Decode => {
            let mut out = vec![0u8; n / 4 * 3];
            Box::new(move || {
                let _ = black_box(cpu::base64_decode_into(black_box(&text), &mut out));
            })
        }
        Kernel::Crc32Chunks => Box::new(move || {
            // One CRC per 4 KiB chunk, as the device writes them.
            let mut acc = 0u32;
            for chunk in black_box(&input).chunks(4096) {
                acc ^= cpu::crc32(chunk);
            }
            black_box(acc);
        }),
        _ => unreachable!("every op is listed in Kernel::ALL"),
    }
}

/// Prints each op: the device's time from submit to done, the device's own timestamp time when
/// it has one, the CPU kernel's time, and which side is faster.
pub fn run(backend: &WgpuBackend) {
    let kernels = Kernels::new(backend).expect("a device");
    println!(
        "input {} bytes per op, median of {RUNS} after {WARM} warm-up runs",
        INPUT
    );
    for kernel in Kernel::ALL {
        // Base64 encode takes whole groups of 48 bytes on its vector shape: its count is the largest
        // multiple of 48 under 4 MiB, 16 bytes less than the others.
        let n = match kernel {
            Kernel::Base64Encode => (INPUT / 48) * 48,
            _ => INPUT / kernel.input_len(1),
        };
        let input = if kernel == Kernel::Base64Decode {
            // A base64 text of 4 MiB: 3 MiB of bytes, encoded.
            cpu::base64_encode(&pattern(3 * MIB as usize, 9)).into_bytes()
        } else {
            pattern(kernel.input_len(n) as usize, 7)
        };
        let floats = cpu::reference::f32_from_le(&pattern(INPUT as usize, 3));
        let text = input.clone();
        let cpu_time = {
            let mut body = cpu_op(kernel, &input, &floats, &text);
            median(WARM, RUNS, &mut *body)
        };
        let n = if kernel == Kernel::Base64Decode {
            input.len() as u64
        } else {
            n
        };
        let src = Storage::new(backend, word_size(input.len() as u64)).expect("a buffer");
        src.write(0, &input).expect("in range");
        let dst = Storage::new(backend, word_size(kernel.output_len(n))).expect("a buffer");
        let mut device = Vec::with_capacity(RUNS);
        let mut timed = Vec::with_capacity(RUNS);
        let one = || -> (Duration, Option<Duration>) {
            let start = std::time::Instant::now();
            let (wall, timer) = if kernel == Kernel::Base64Decode {
                let decoding = kernels.base64_decode(&src, &dst, n).expect("runs");
                decoding.wait().expect("finishes");
                (start.elapsed(), None)
            } else {
                let ticket = kernels.run(kernel, &src, &dst, n).expect("runs");
                ticket.wait().expect("finishes");
                let wall = start.elapsed();
                (wall, ticket.gpu_time().map(|(time, _)| time))
            };
            (wall, timer)
        };
        for _ in 0..WARM {
            one();
        }
        for _ in 0..RUNS {
            let (wall, timer) = one();
            timed.push(wall);
            if let Some(timer) = timer {
                device.push(timer);
            }
        }
        let gpu = median_of(&mut timed);
        let device_line = if device.is_empty() {
            "device timestamps: none".to_string()
        } else {
            format!("device timestamps {:.3} ms", ms(median_of(&mut device)))
        };
        let winner = if gpu < cpu_time { "device" } else { "cpu" };
        println!(
            "{kernel:?}: device {:.3} ms (submit to done), {device_line}; cpu {:.3} ms; faster: {winner}",
            ms(gpu),
            ms(cpu_time)
        );
    }
    // A copy of 4 MiB and four bytes: the vector copy over the whole vectors, then a word copy of
    // the tail. An odd length costs what an even one does.
    {
        let n = INPUT + 4;
        let input = pattern(n as usize, 7);
        let src = Storage::new(backend, word_size(n)).expect("a buffer");
        src.write(0, &input).expect("in range");
        let dst = Storage::new(backend, word_size(n)).expect("a buffer");
        let copied = median(WARM, RUNS, || {
            kernels
                .run(Kernel::Copy, &src, &dst, n)
                .expect("runs")
                .wait()
                .expect("finishes");
        });
        println!(
            "Copy, 4 MiB + 4 bytes (vector prefix, word tail): submit to done {:.3} ms",
            ms(copied)
        );
    }
    // Diagnostic: a decode of one invocation, to see the host path's share of the 4 MiB decode.
    {
        let text = cpu::base64_encode(&pattern(48, 9)).into_bytes();
        let src = Storage::new(backend, word_size(text.len() as u64)).expect("a buffer");
        src.write(0, &text).expect("in range");
        let dst = Storage::new(backend, 64).expect("a buffer");
        let small = median(WARM, RUNS, || {
            kernels
                .base64_decode(&src, &dst, 64)
                .expect("runs")
                .wait()
                .expect("finishes");
        });
        println!(
            "Base64Decode one invocation (64 symbols): {:.4} ms",
            ms(small)
        );
    }
    chain(backend, &kernels);
}

/// The chain of half to f32 to unorm8 over 1 M values, as one merged shader, against the two
/// ops run one after the other.
fn chain(backend: &WgpuBackend, kernels: &Kernels) {
    let halves = pattern(2 * CHAIN_VALUES as usize, 11);
    // The merged chain reads one word a value, as its tests do.
    let words: Vec<u8> = halves
        .as_chunks::<2>()
        .0
        .iter()
        .flat_map(|half| u32::from(u16::from_le_bytes(*half)).to_le_bytes())
        .collect();
    let src = Storage::new(backend, word_size(words.len() as u64)).expect("a buffer");
    src.write(0, &words).expect("in range");
    let dst = Storage::new(backend, word_size(4 * CHAIN_VALUES)).expect("a buffer");
    let shader = chain_shader(backend, &[&HALF_TO_F32, &F32_TO_UNORM8]).expect("merges");
    let merged = median(WARM, RUNS, || {
        let Run::Started(ticket) = shader
            .run(&[Bound::Storage(&src), Bound::Storage(&dst)])
            .expect("runs")
        else {
            panic!("nothing else is in flight");
        };
        ticket.wait().expect("finishes");
    });
    // The two ops, each on its own packed layout: halves to f32, then f32 to unorm8.
    let mid = Storage::new(backend, word_size(4 * CHAIN_VALUES)).expect("a buffer");
    let out = Storage::new(backend, word_size(CHAIN_VALUES)).expect("a buffer");
    let packed = Storage::new(backend, word_size(2 * CHAIN_VALUES)).expect("a buffer");
    packed.write(0, &halves).expect("in range");
    let apart = median(WARM, RUNS, || {
        kernels
            .run(Kernel::F16ToF32, &packed, &mid, CHAIN_VALUES)
            .expect("runs");
        kernels
            .run(Kernel::F32ToUnorm8, &mid, &out, CHAIN_VALUES)
            .expect("runs")
            .wait()
            .expect("finishes");
    });
    println!(
        "chain HALF_TO_F32 then F32_TO_UNORM8, {CHAIN_VALUES} values: merged shader {:.3} ms, two ops one after the other {:.3} ms",
        ms(merged),
        ms(apart)
    );
}
