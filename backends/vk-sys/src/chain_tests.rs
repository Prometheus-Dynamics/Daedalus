//! The compute chain, against a real device: the machine's best adapter, and its software one
//! when it has one.

use std::sync::Arc;
use std::time::Duration;

use crate::chain::{Arg, Chain};
use crate::compute::{Binding, ComputePipeline};
use crate::device::test_device;
use crate::memory::{self, Placement};
use crate::{Error, SubmitLock};

const WORDS: u32 = 256;
const BYTES: u64 = WORDS as u64 * 4;

/// out[i] = in[i] * cfg.scale + cfg.add
const SCALE: &str = "
struct Words { words: array<u32> }
struct Config { scale: u32, add: u32 }
@group(0) @binding(0) var<storage, read> src: Words;
@group(0) @binding(1) var<storage, read_write> dst: Words;
@group(0) @binding(2) var<uniform> cfg: Config;
@compute @workgroup_size(64)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
    if id.x < arrayLength(&dst.words) {
        dst.words[id.x] = src.words[id.x] * cfg.scale + cfg.add;
    }
}
";

const SAMPLED: &str = "
@group(0) @binding(0) var image: texture_2d<f32>;
@group(0) @binding(1) var<storage, read_write> dst: array<u32>;
@compute @workgroup_size(1)
fn main() { dst[0] = textureDimensions(image).x; }
";

fn devices() -> Vec<(wgpu::Device, wgpu::Queue)> {
    let found: Vec<_> = [false, true].into_iter().filter_map(test_device).collect();
    assert!(
        !found.is_empty() || std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
        "this lane needs an adapter"
    );
    found
}

fn words(of: impl Fn(u32) -> u32) -> Vec<u8> {
    (0..WORDS).flat_map(|at| of(at).to_le_bytes()).collect()
}

fn config(scale: u32, add: u32) -> [u8; 8] {
    let mut bytes = [0; 8];
    bytes[..4].copy_from_slice(&scale.to_le_bytes());
    bytes[4..].copy_from_slice(&add.to_le_bytes());
    bytes
}

fn mapped(device: &wgpu::Device, bytes: u64) -> (wgpu::Buffer, memory::HostMapping) {
    let (buffer, map) =
        memory::storage_buffer(device, bytes, Placement::HostMapped, None).expect("a buffer");
    (buffer, map.expect("a mapping"))
}

fn read(map: &memory::HostMapping) -> Vec<u8> {
    let mut out = vec![0; BYTES as usize];
    map.read(0, &mut out).expect("a read");
    out
}

#[test]
fn a_pipeline_reads_its_bindings_from_the_shader() {
    for (device, _) in devices() {
        let pipeline = ComputePipeline::from_wgsl(&device, SCALE, "main").expect("a pipeline");
        let slots = pipeline.slots();
        let kinds: Vec<_> = slots.iter().map(|slot| (slot.number, slot.kind)).collect();
        assert_eq!(
            kinds,
            [
                (0, Binding::Storage),
                (1, Binding::Storage),
                (2, Binding::Uniform)
            ]
        );
        // An array of no fixed length counts as one element.
        assert_eq!(slots[0].min_size, 4);
        assert_eq!(slots[2].min_size, 8);
        // A texture isn't a buffer, an entry point has to be there, and so does the shader.
        for (source, entry) in [(SAMPLED, "main"), (SCALE, "other"), ("not wgsl", "main")] {
            let refused = ComputePipeline::from_wgsl(&device, source, entry).err();
            assert_eq!(refused, Some(Error::Unsupported));
        }
    }
}

#[test]
fn stages_run_in_order_and_each_reads_what_the_one_before_wrote() {
    for (device, queue) in devices() {
        let lock = SubmitLock::default();
        let pipeline =
            Arc::new(ComputePipeline::from_wgsl(&device, SCALE, "main").expect("a pipeline"));
        let pipelines = [Arc::clone(&pipeline), Arc::clone(&pipeline), pipeline];
        let mut chain = Chain::new(&device, &queue, &lock, &pipelines, 2).expect("a chain");
        let (first, first_map) = mapped(&device, BYTES);
        let (middle, _) = mapped(&device, BYTES);
        let (after, _) = mapped(&device, BYTES);
        let (last, last_map) = mapped(&device, BYTES);
        // The third stage's uniform comes from a buffer on the device.
        let (on_device, on_device_map) = mapped(&device, 8);
        on_device_map.write(0, &config(1, 7)).expect("a write");
        let storage = |buffer| Arg::Storage {
            buffer,
            size: BYTES,
        };
        for round in 0..4u32 {
            first_map
                .write(0, &words(|at| at + round))
                .expect("a write");
            chain.begin();
            let counts = [WORDS.div_ceil(64), 1, 1];
            let (two, three) = (config(2, 1), config(3, round));
            chain
                .dispatch(
                    0,
                    &[storage(&first), storage(&middle), Arg::Bytes(&two)],
                    counts,
                )
                .expect("the first stage");
            chain
                .dispatch(
                    1,
                    &[storage(&middle), storage(&after), Arg::Bytes(&three)],
                    counts,
                )
                .expect("the second stage");
            chain
                .dispatch(
                    2,
                    &[storage(&after), storage(&last), Arg::Copy(&on_device)],
                    counts,
                )
                .expect("the third stage");
            chain.submit().expect("a submit");
            assert!(chain.wait(Duration::from_secs(30)));
            assert!(chain.is_idle());
            let want = words(|at| ((at + round) * 2 + 1) * 3 + round + 7);
            assert_eq!(read(&last_map), want, "round {round}");
        }
    }
}

#[test]
fn what_a_stage_is_given_is_checked_before_anything_is_recorded() {
    for (device, queue) in devices() {
        let lock = SubmitLock::default();
        let pipeline =
            Arc::new(ComputePipeline::from_wgsl(&device, SCALE, "main").expect("a pipeline"));
        let pipelines = [pipeline];
        let mut chain = Chain::new(&device, &queue, &lock, &pipelines, 2).expect("a chain");
        let (src, _) = mapped(&device, BYTES);
        let (dst, dst_map) = mapped(&device, BYTES);
        let cfg = config(1, 0);
        let whole = |buffer| Arg::Storage {
            buffer,
            size: BYTES,
        };
        let good = [whole(&src), whole(&dst), Arg::Bytes(&cfg)];
        // Not begun.
        assert_eq!(chain.dispatch(0, &good, [1, 1, 1]), Err(Error::Range));
        assert_eq!(chain.submit(), Err(Error::Range));
        chain.begin();
        let plain = device.create_buffer(&wgpu::BufferDescriptor {
            label: None,
            size: BYTES,
            usage: wgpu::BufferUsages::COPY_DST,
            mapped_at_creation: false,
        });
        let too_long = Arg::Storage {
            buffer: &src,
            size: BYTES + 4,
        };
        let empty = Arg::Storage {
            buffer: &src,
            size: 0,
        };
        let bad: [(&[Arg<'_>], [u32; 3]); 7] = [
            // Too few, the wrong kinds, more bytes than the buffer has, none, a buffer with
            // no storage usage, a short uniform, a copy from a buffer with no copy usage.
            (&good[..2], [1, 1, 1]),
            (&[good[2], good[1], good[0]], [1, 1, 1]),
            (&[too_long, good[1], good[2]], [1, 1, 1]),
            (&[empty, good[1], good[2]], [1, 1, 1]),
            (&[whole(&plain), good[1], good[2]], [1, 1, 1]),
            (&[good[0], good[1], Arg::Bytes(&cfg[..4])], [1, 1, 1]),
            (&[good[0], good[1], Arg::Copy(&plain)], [1, 1, 1]),
        ];
        for (at, (args, counts)) in bad.into_iter().enumerate() {
            assert_eq!(chain.dispatch(0, args, counts), Err(Error::Range), "{at}");
        }
        // More workgroups than the device dispatches, where it has a limit.
        let max = pipelines[0].max_counts()[0];
        if let Some(over) = max.checked_add(1) {
            assert_eq!(chain.dispatch(0, &good, [over, 1, 1]), Err(Error::Range));
        }
        assert_eq!(chain.dispatch(1, &good, [1, 1, 1]), Err(Error::Range));
        // The stage still runs, once.
        chain.dispatch(0, &good, [4, 1, 1]).expect("a dispatch");
        assert_eq!(chain.dispatch(0, &good, [4, 1, 1]), Err(Error::Range));
        chain.submit().expect("a submit");
        assert!(chain.wait(Duration::from_secs(30)));
        assert_eq!(read(&dst_map).len(), BYTES as usize);
    }
}

#[test]
fn a_run_allocates_nothing_once_its_buffers_are_known() {
    use daedalus_abi::counting::counted;
    for (device, queue) in devices() {
        let lock = SubmitLock::default();
        let pipeline =
            Arc::new(ComputePipeline::from_wgsl(&device, SCALE, "main").expect("a pipeline"));
        let pipelines = [Arc::clone(&pipeline), pipeline];
        let mut chain = Chain::new(&device, &queue, &lock, &pipelines, 4).expect("a chain");
        let (src, _) = mapped(&device, BYTES);
        let (dst, _) = mapped(&device, BYTES);
        // A stage that sees a different buffer each run finds each one again.
        let middles: Vec<_> = (0..3).map(|_| mapped(&device, BYTES).0).collect();
        let (on_device, _) = mapped(&device, 8);
        let cfg = config(2, 1);
        let mut round = 0;
        let mut frame = || {
            let middle = &middles[round % middles.len()];
            round += 1;
            let storage = |buffer| Arg::Storage {
                buffer,
                size: BYTES,
            };
            chain.begin();
            chain
                .dispatch(
                    0,
                    &[storage(&src), storage(middle), Arg::Bytes(&cfg)],
                    [4, 1, 1],
                )
                .expect("the first stage");
            chain
                .dispatch(
                    1,
                    &[storage(middle), storage(&dst), Arg::Copy(&on_device)],
                    [4, 1, 1],
                )
                .expect("the second stage");
            chain.submit().expect("a submit");
            assert!(chain.wait(Duration::from_secs(30)));
            assert!(chain.is_idle());
        };
        for _ in 0..8 {
            frame();
        }
        // The driver may allocate on its own account now and then, so take the cheapest of
        // several: what this crate adds is in every one of them.
        let cheapest = (0..16).map(|_| counted(&mut frame).1).min().unwrap_or(0);
        assert_eq!(cheapest, 0, "begin, two dispatches, submit and wait");
    }
}
