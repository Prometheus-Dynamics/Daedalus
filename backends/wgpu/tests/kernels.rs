//! The kernel ops on the device against the CPU references, bit for bit.

use daedalus_kernels_sys::reference as r;
use daedalus_wgpu::kernels::{F32_TO_UNORM8, HALF_TO_F32, Kernel, KernelError, Kernels};
use daedalus_wgpu::merge::chain_shader;
use daedalus_wgpu::shader::{Bound, Run};
use daedalus_wgpu::{Options, Storage, WgpuBackend};

/// A device of this test's own, so its pass counters are its alone.
fn private() -> Option<WgpuBackend> {
    let shared = WgpuBackend::open(Options::default());
    let record = match shared.start() {
        Ok(record) => record,
        Err(error) => {
            assert!(
                std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
                "{error}"
            );
            return None;
        }
    };
    Some(WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    }))
}

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

/// `data` on the device, padded to a word.
fn on_device(backend: &WgpuBackend, data: &[u8]) -> Storage {
    let mut padded = data.to_vec();
    padded.resize(data.len().next_multiple_of(4).max(4), 0);
    let storage = Storage::new(backend, padded.len() as u64).expect("a buffer");
    storage.write(0, &padded).expect("in range");
    storage
}

/// A buffer of at least `len` bytes, filled with ones so a byte the op should zero shows.
fn output(backend: &WgpuBackend, len: u64) -> Storage {
    let size = len.next_multiple_of(4).max(4);
    let storage = Storage::new(backend, size).expect("a buffer");
    storage
        .write(0, &vec![0xff; size as usize])
        .expect("in range");
    storage
}

fn read(storage: &Storage, len: usize) -> Vec<u8> {
    let mut out = vec![0; storage.size() as usize];
    storage.read_into(0, &mut out).expect("a readback");
    out.truncate(len);
    out
}

/// Runs `kernel` over `n` elements of `input` and gives its output bytes.
fn run(backend: &WgpuBackend, kernels: &Kernels, kernel: Kernel, input: &[u8], n: u64) -> Vec<u8> {
    assert_eq!(input.len() as u64, kernel.input_len(n));
    let src = on_device(backend, input);
    let dst = output(backend, kernel.output_len(n));
    kernels
        .run(kernel, &src, &dst, n)
        .expect("runs")
        .wait()
        .expect("finishes");
    read(&dst, kernel.output_len(n) as usize)
}

fn f32_bytes(values: &[f32]) -> Vec<u8> {
    values
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect()
}

/// Floats that matter: the ends, the halves a rounding rule differs on, and what isn't a number.
fn floats(count: usize) -> Vec<f32> {
    let special = [
        0.0,
        -0.0,
        1.0,
        -1.0,
        0.5,
        0.5 / 255.0,
        1.5 / 255.0,
        2.5 / 255.0,
        254.5 / 255.0,
        0.999_999_94,
        1.000_000_1,
        2.0,
        f32::MIN_POSITIVE,
        1e-45,
        f32::INFINITY,
        f32::NEG_INFINITY,
        f32::NAN,
        -f32::NAN,
        f32::from_bits(0x7f80_0001),
    ];
    let random = bytes(count * 4, 7);
    special
        .into_iter()
        .chain(
            random
                .as_chunks::<4>()
                .0
                .iter()
                .map(|word| f32::from_bits(u32::from_le_bytes(*word))),
        )
        .chain((0..256).map(|at| at as f32 / 255.0))
        .take(count)
        .collect()
}

/// Every binary16 value there is.
fn halves() -> Vec<u8> {
    (0..=u16::MAX).flat_map(u16::to_le_bytes).collect()
}

#[test]
fn float_kernels_match_the_references() {
    let Some(backend) = private() else { return };
    let kernels = Kernels::new(&backend).expect("a device");

    for n in [1usize, 3, 4, 5, 8, 16, 24, 1023, 1024, 4096, 4099] {
        let input = bytes(n * 4, n as u32);
        let got = run(&backend, &kernels, Kernel::F32FromLe, &input, n as u64);
        assert_eq!(got, f32_bytes(&r::f32_from_le(&input)), "f32_from_le {n}");
    }

    let all = halves();
    let got = run(&backend, &kernels, Kernel::F16ToF32, &all, 65536);
    assert_eq!(got, f32_bytes(&r::f16_to_f32(&all)), "every half");
    let got = run(&backend, &kernels, Kernel::F16ToF32, &all[..2 * 7], 7);
    assert_eq!(
        got,
        f32_bytes(&r::f16_to_f32(&all[..2 * 7])),
        "an odd count"
    );

    for n in [1usize, 2, 3, 4, 16, 19, 32, 277, 4096, 4097] {
        let values = floats(n);
        let got = run(
            &backend,
            &kernels,
            Kernel::F32ToUnorm8,
            &f32_bytes(&values),
            n as u64,
        );
        assert_eq!(got, r::f32_to_unorm8(&values), "f32_to_unorm8 {n}");
    }
}

#[test]
fn copy_and_codec_kernels_match_the_references() {
    let Some(backend) = private() else { return };
    let kernels = Kernels::new(&backend).expect("a device");
    for n in [
        1usize,
        2,
        3,
        4,
        5,
        6,
        7,
        15,
        16,
        17,
        31,
        32,
        48,
        63,
        64,
        65,
        4095,
        4096,
        12_289,
        // A tail of one to fifteen bytes after the whole vectors, and whole words within it.
        20,
        36,
        4100,
        (1 << 20) + 7,
        1 << 20,
    ] {
        let input = bytes(n, n as u32 + 11);
        let n64 = n as u64;
        assert_eq!(
            run(&backend, &kernels, Kernel::Copy, &input, n64),
            input,
            "copy {n}"
        );
        assert_eq!(
            run(&backend, &kernels, Kernel::HexLower, &input, n64),
            r::hex_lower(&input),
            "hex {n}"
        );
        let encoded = run(&backend, &kernels, Kernel::Base64Encode, &input, n64);
        assert_eq!(encoded, r::base64_encode(&input), "base64 encode {n}");

        // And back: the decode of what was encoded, with its length.
        let src = on_device(&backend, &encoded);
        let dst = output(
            &backend,
            Kernel::Base64Decode.output_len(encoded.len() as u64),
        );
        let len = kernels
            .base64_decode(&src, &dst, encoded.len() as u64)
            .expect("runs")
            .wait()
            .expect("valid");
        assert_eq!(len, n64);
        assert_eq!(read(&dst, n), input, "base64 decode {n}");
        assert_eq!(r::base64_decode(&encoded).as_deref(), Some(&input[..]));
    }

    // The bytes of the last word past the output are zeroed, not left.
    let src = on_device(&backend, &[1, 2, 3, 4, 5]);
    let dst = output(&backend, 8);
    kernels
        .run(Kernel::Copy, &src, &dst, 5)
        .unwrap()
        .wait()
        .unwrap();
    assert_eq!(read(&dst, 8), [1, 2, 3, 4, 5, 0, 0, 0]);
}

/// Base64 input, and its bytes or the offset it is invalid at.
type Case = (&'static [u8], Result<&'static [u8], u64>);

#[test]
fn gpu_base64_decode_reports_length_and_errors() {
    let Some(backend) = private() else { return };
    let kernels = Kernels::new(&backend).expect("a device");
    let cases: [Case; 8] = [
        (b"TWFu", Ok(b"Man")),
        (b"TWE=", Ok(b"Ma")),
        (b"TQ==", Ok(b"M")),
        (b"TWFuTWE=", Ok(b"ManMa")),
        // Bits in the last symbol that no byte takes.
        (b"TWF=", Err(2)),
        (b"T*Fu", Err(1)),
        // Padding that isn't at the end is no symbol.
        (b"TW=u", Err(2)),
        (b"TWFuT*==", Err(5)),
    ];
    for (input, want) in cases {
        let src = on_device(&backend, input);
        let dst = output(&backend, 8);
        let got = kernels
            .base64_decode(&src, &dst, input.len() as u64)
            .expect("runs")
            .wait();
        match want {
            Ok(bytes) => {
                assert_eq!(got, Ok(bytes.len() as u64), "{input:?}");
                assert_eq!(read(&dst, bytes.len()), bytes);
            }
            Err(at) => assert_eq!(got, Err(KernelError::InvalidSymbol { at }), "{input:?}"),
        }
        // The CPU decoder agrees on which are valid, and where they aren't.
        let mut host = [0; 8];
        match (
            daedalus_kernels_sys::base64_decode_into(input, &mut host),
            want,
        ) {
            (Ok(len), Ok(bytes)) => assert_eq!(&host[..len], bytes),
            (Err(error), Err(at)) => {
                use daedalus_kernels_sys::Base64Error::{InvalidByte, InvalidLastSymbol};
                let (InvalidByte { offset, .. } | InvalidLastSymbol { offset, .. }) = error else {
                    panic!("{error}");
                };
                assert_eq!(offset as u64, at, "{input:?}");
            }
            (host, _) => panic!("{input:?}: the host says {host:?}"),
        }
    }

    // Nothing to decode is nothing, without a submit, and a ragged length is refused.
    let src = on_device(&backend, b"TWFu");
    let dst = output(&backend, 4);
    assert_eq!(kernels.base64_decode(&src, &dst, 0).unwrap().wait(), Ok(0));
    assert!(matches!(
        kernels.base64_decode(&src, &dst, 3),
        Err(KernelError::Size(_))
    ));
    // Through `encode` it would have nowhere to report.
    let mut encoder = kernels.encoder();
    assert!(
        kernels
            .encode(&mut encoder, Kernel::Base64Decode, &src, &dst, 4)
            .is_err()
    );
}

#[test]
fn copy_engine_copies_gpu_resident_bytes() {
    let Some(backend) = private() else { return };
    let kernels = Kernels::new(&backend).expect("a device");
    let input = bytes(4096, 3);
    let src = on_device(&backend, &input);
    let dst = output(&backend, 4096);
    let before = backend.shader_stats().expect("open").passes;
    let mut encoder = kernels.encoder();
    kernels
        .copy_with_engine(&mut encoder, &src, &dst, 4096)
        .expect("fits");
    kernels.submit(encoder).wait().expect("finishes");
    assert_eq!(read(&dst, 4096), input);
    assert_eq!(
        backend.shader_stats().expect("open").passes,
        before,
        "no shader ran"
    );

    let mut encoder = kernels.encoder();
    for (n, why) in [
        (4098, "not a multiple of 4"),
        (8192, "longer than the buffers"),
    ] {
        let refused = kernels.copy_with_engine(&mut encoder, &src, &dst, n);
        assert!(matches!(refused, Err(KernelError::Size(_))), "{why}");
    }
    // An op is refused the same way when a buffer is too small for it.
    let small = output(&backend, 4);
    let refused = kernels.encode(&mut encoder, Kernel::HexLower, &src, &small, 4096);
    assert!(matches!(refused, Err(KernelError::Size(_))));
}

#[test]
fn gpu_crc32_matches_the_reference() {
    let Some(backend) = private() else { return };
    let kernels = Kernels::new(&backend).expect("a device");
    assert_eq!(
        r::crc32(b"123456789"),
        0xcbf4_3926,
        "the reference is CRC-32"
    );
    for n in [1usize, 9, 4095, 4096, 4097, 8192, 70_001] {
        let input = match n {
            9 => b"123456789".to_vec(),
            _ => bytes(n, n as u32),
        };
        let src = on_device(&backend, &input);
        let crc = kernels
            .crc32(&src, n as u64)
            .expect("runs")
            .wait()
            .expect("finishes");
        assert_eq!(crc, r::crc32(&input), "crc32 of {n}");
    }
    let src = on_device(&backend, &[0; 4]);
    assert_eq!(kernels.crc32(&src, 0).unwrap().wait(), Ok(0));
}

#[test]
fn two_ops_in_one_encoder_each_see_their_own_counts() {
    let Some(backend) = private() else { return };
    let kernels = Kernels::new(&backend).expect("a device");
    let (a, b) = (bytes(40, 1), bytes(7, 2));
    let (src_a, src_b) = (on_device(&backend, &a), on_device(&backend, &b));
    let (dst_a, dst_b) = (output(&backend, 80), output(&backend, 14));
    let before = backend.shader_stats().expect("open");
    let mut encoder = kernels.encoder();
    kernels
        .encode(&mut encoder, Kernel::HexLower, &src_a, &dst_a, 40)
        .unwrap();
    kernels
        .encode(&mut encoder, Kernel::HexLower, &src_b, &dst_b, 7)
        .unwrap();
    kernels.submit(encoder).wait().expect("finishes");
    let after = backend.shader_stats().expect("open");
    // Recorded one at a time into a caller's encoder, each op is a pass of its own.
    assert_eq!(
        (
            after.dispatches - before.dispatches,
            after.passes - before.passes,
            after.submits - before.submits
        ),
        (2, 2, 1)
    );
    // A batch shares one pass, and each op still sees its own counts.
    let before = backend.shader_stats().expect("open");
    let mut batch = kernels.batch();
    batch.add(Kernel::HexLower, &src_a, &dst_a, 40).unwrap();
    batch.add(Kernel::HexLower, &src_b, &dst_b, 7).unwrap();
    batch.submit().wait().expect("finishes");
    let after = backend.shader_stats().expect("open");
    assert_eq!(
        (
            after.dispatches - before.dispatches,
            after.passes - before.passes,
            after.submits - before.submits
        ),
        (2, 1, 1)
    );
    assert_eq!(read(&dst_a, 80), r::hex_lower(&a));
    assert_eq!(read(&dst_b, 14), r::hex_lower(&b));
}

#[test]
fn a_chain_of_two_per_pixel_kernels_is_one_shader() {
    let Some(backend) = private() else { return };
    // Halves, one to a word, to f32 and on to unorm8: two bodies, one pass.
    let all = halves();
    let words: Vec<u8> = all
        .as_chunks::<2>()
        .0
        .iter()
        .flat_map(|half| u32::from(u16::from_le_bytes(*half)).to_le_bytes())
        .collect();
    let src = on_device(&backend, &words);
    let dst = output(&backend, words.len() as u64);
    let before = backend.shader_stats().expect("open");
    let shader = chain_shader(&backend, &[&HALF_TO_F32, &F32_TO_UNORM8]).expect("merges");
    let run = shader
        .run(&[Bound::Storage(&src), Bound::Storage(&dst)])
        .expect("runs");
    let Run::Started(ticket) = run else {
        panic!("nothing else is in flight");
    };
    ticket.wait().expect("finishes");
    let after = backend.shader_stats().expect("open");
    assert_eq!(
        after.pipelines - before.pipelines,
        1,
        "one shader for the two"
    );
    assert_eq!(after.passes - before.passes, 1);

    let want = r::f32_to_unorm8(&r::f16_to_f32(&all));
    let got: Vec<u8> = read(&dst, words.len())
        .as_chunks::<4>()
        .0
        .iter()
        .map(|word| {
            assert_eq!(word[1..], [0, 0, 0]);
            word[0]
        })
        .collect();
    assert_eq!(
        got, want,
        "what the two CPU kernels give, one after the other"
    );

    // Each alone is its op: the same function, so the same bits.
    let alone = chain_shader(&backend, &[&HALF_TO_F32]).expect("a chain of one");
    let Run::Started(ticket) = alone
        .run(&[Bound::Storage(&src), Bound::Storage(&dst)])
        .expect("runs")
    else {
        panic!("nothing else is in flight");
    };
    ticket.wait().expect("finishes");
    assert_eq!(read(&dst, words.len()), f32_bytes(&r::f16_to_f32(&all)));
}

/// The vector base64 shapes, at lengths that fill whole invocations, against the CPU: valid text
/// in both directions, padded last groups, and invalid symbols and stray bits at group and chunk
/// boundaries, where the first invalid position is reported as the CPU reports it.
#[test]
fn base64_vector_shapes_match_the_cpu_at_group_and_chunk_boundaries() {
    use daedalus_kernels_sys::Base64Error::{InvalidByte, InvalidLastSymbol};
    let Some(backend) = private() else { return };
    let kernels = Kernels::new(&backend).expect("a device");
    for symbols in [64usize, 128, 4096, 65_536] {
        let data = bytes(symbols / 4 * 3, symbols as u32);
        let text = r::base64_encode(&data);
        let (src, dst) = (
            on_device(&backend, &text),
            output(&backend, data.len() as u64),
        );
        let got = kernels
            .base64_decode(&src, &dst, text.len() as u64)
            .expect("runs")
            .wait();
        assert_eq!(got, Ok(data.len() as u64), "decode {symbols}");
        assert_eq!(read(&dst, data.len()), data, "decode bytes {symbols}");

        let src = on_device(&backend, &data);
        let dst = output(&backend, text.len() as u64);
        kernels
            .run(Kernel::Base64Encode, &src, &dst, data.len() as u64)
            .expect("runs")
            .wait()
            .expect("finishes");
        assert_eq!(read(&dst, text.len()), text, "encode {symbols}");

        // Padded last groups: one '=' and two.
        for pad_bytes in [data.len() - 1, data.len() - 2] {
            let short = &data[..pad_bytes];
            let padded = r::base64_encode(short);
            assert_eq!(padded.len(), symbols);
            let src = on_device(&backend, &padded);
            let dst = output(&backend, short.len() as u64);
            let got = kernels
                .base64_decode(&src, &dst, padded.len() as u64)
                .expect("runs")
                .wait();
            assert_eq!(got, Ok(short.len() as u64), "pad {pad_bytes}");
            assert_eq!(read(&dst, short.len()), short, "pad bytes {pad_bytes}");
        }

        // Invalid symbols, and a last group with bits no byte takes, at the boundaries: the device
        // reports the first invalid position, and the CPU reports the same offset.
        let mut cases: Vec<Vec<u8>> = Vec::new();
        for at in [0usize, 3, 4, 63, 64, 65, 127, 128, symbols - 2] {
            if at < symbols {
                let mut bad = text.clone();
                bad[at] = b'*';
                if at + 10 < symbols {
                    bad[at + 10] = b'!';
                }
                cases.push(bad);
            }
        }
        let mut stray = text.clone();
        let tail = symbols - 4;
        stray[tail..].copy_from_slice(b"TWF=");
        cases.push(stray);
        for input in cases {
            let src = on_device(&backend, &input);
            let dst = output(&backend, (symbols / 4 * 3) as u64);
            let got = kernels
                .base64_decode(&src, &dst, input.len() as u64)
                .expect("runs")
                .wait();
            let mut host = vec![0u8; symbols];
            let want = match daedalus_kernels_sys::base64_decode_into(&input, &mut host) {
                Ok(_) => panic!("the CPU accepted a bad input of {symbols}"),
                Err(InvalidByte { offset, .. } | InvalidLastSymbol { offset, .. }) => offset,
                Err(error) => panic!("{error}"),
            };
            assert_eq!(
                got,
                Err(KernelError::InvalidSymbol { at: want as u64 }),
                "{symbols} symbols"
            );
        }
    }
}
