//! The kernel ops as the lane sees them: each is one pass, they batch into one submit, and they
//! give the CPU reference's bytes. The exhaustive comparison is in the backend's own tests.

use daedalus_kernels_sys::reference as r;
use daedalus_wgpu::Storage;
use daedalus_wgpu::kernels::{Kernel, Kernels};
use daedalus_wgpu_conformance::{measure, private};

fn bytes(len: usize, seed: u32) -> Vec<u8> {
    let mut x = seed.wrapping_mul(2_654_435_761) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 17;
            x ^= x << 5;
            x as u8
        })
        .collect()
}

fn on_device(backend: &daedalus_wgpu::WgpuBackend, data: &[u8]) -> Storage {
    let mut padded = data.to_vec();
    padded.resize(data.len().next_multiple_of(4).max(4), 0);
    let storage = Storage::new(backend, padded.len() as u64).expect("a buffer");
    storage.write(0, &padded).expect("in range");
    storage
}

fn read(storage: &Storage, len: usize) -> Vec<u8> {
    let mut out = vec![0; storage.size() as usize];
    storage.read_into(0, &mut out).expect("a readback");
    out.truncate(len);
    out
}

/// The reference's bytes for `kernel` over `input`.
fn reference(kernel: Kernel, input: &[u8]) -> Vec<u8> {
    let floats = |values: Vec<f32>| values.iter().flat_map(|v| v.to_le_bytes()).collect();
    match kernel {
        Kernel::F32FromLe => floats(r::f32_from_le(input)),
        Kernel::F16ToF32 => floats(r::f16_to_f32(input)),
        Kernel::F32ToUnorm8 => r::f32_to_unorm8(&r::f32_from_le(input)),
        Kernel::Copy => input.to_vec(),
        Kernel::HexLower => r::hex_lower(input),
        Kernel::Base64Encode => r::base64_encode(input),
        Kernel::Crc32Chunks => input
            .chunks(4096)
            .flat_map(|chunk| r::crc32(chunk).to_le_bytes())
            .collect(),
        _ => unreachable!("run through its own call"),
    }
}

const OPS: [(Kernel, u64); 7] = [
    (Kernel::F32FromLe, 3000),
    (Kernel::F16ToF32, 3000),
    (Kernel::F32ToUnorm8, 3001),
    (Kernel::Copy, 9001),
    (Kernel::HexLower, 9001),
    (Kernel::Base64Encode, 9001),
    (Kernel::Crc32Chunks, 9001),
];

#[test]
fn each_op_is_one_pass_and_seven_batch_into_one_submit() {
    let Some(backend) = private() else { return };
    let kernels = Kernels::new(&backend).expect("a device");
    let work: Vec<(Kernel, u64, Vec<u8>, Storage, Storage)> = OPS
        .iter()
        .enumerate()
        .map(|(at, &(kernel, n))| {
            let input = bytes(kernel.input_len(n) as usize, at as u32 + 1);
            let src = on_device(&backend, &input);
            let size = kernel.output_len(n).next_multiple_of(4);
            let dst = Storage::new(&backend, size).expect("a buffer");
            (kernel, n, input, src, dst)
        })
        .collect();
    // Warm: the shaders are built on first use.
    for (kernel, n, _, src, dst) in &work {
        kernels
            .run(*kernel, src, dst, *n)
            .expect("runs")
            .wait()
            .expect("finishes");
    }
    let (ticket, counted) = measure(&backend, || {
        let mut encoder = kernels.encoder();
        for (kernel, n, _, src, dst) in &work {
            kernels
                .encode(&mut encoder, *kernel, src, dst, *n)
                .expect("fits");
        }
        kernels.submit(encoder)
    });
    ticket.wait().expect("finishes");
    assert_eq!(
        (counted.passes, counted.submits),
        (7, 1),
        "a pass an op, one submit"
    );
    assert_eq!(counted.pipelines, 0, "nothing is built by a warm run");
    for (kernel, n, input, _, dst) in &work {
        let want = reference(*kernel, input);
        assert_eq!(want.len() as u64, kernel.output_len(*n));
        assert_eq!(read(dst, want.len()), want, "{kernel:?}");
    }
}

#[test]
fn decode_and_checksum_report_what_the_reference_does() {
    let Some(backend) = private() else { return };
    let kernels = Kernels::new(&backend).expect("a device");
    let input = bytes(5000, 9);
    let encoded = r::base64_encode(&input);
    let src = on_device(&backend, &encoded);
    let dst = Storage::new(&backend, 5004).expect("a buffer");
    let len = kernels
        .base64_decode(&src, &dst, encoded.len() as u64)
        .expect("runs")
        .wait()
        .expect("valid");
    assert_eq!(len, 5000);
    assert_eq!(read(&dst, 5000), input);

    let src = on_device(&backend, &input);
    let crc = kernels
        .crc32(&src, 5000)
        .expect("runs")
        .wait()
        .expect("finishes");
    assert_eq!(crc, r::crc32(&input));
}
