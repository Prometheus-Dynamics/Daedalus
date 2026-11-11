//! Two devices in one process: each a backend of its own to the planner, with its own memory,
//! and the only way between them the one through the host.

mod common;

use std::sync::Arc;

use daedalus_abi::testing::{release, run_once};
use daedalus_abi::{Slot, Slots, Step, StepCx, StepEnd};
use daedalus_backend::{Backend, CompileCx, SpaceKind, TransferCost};
use daedalus_planner::regions::RegionError;
use daedalus_planner::stitch::{Item, StitchError, Stitched};
use daedalus_planner_tests::{Mem, decl, edge, node, stitched, u8_ty};
use daedalus_registry::Registry;
use daedalus_runner::{drop_owned, put_shared, put_typed, take_typed};
use daedalus_types::{SpaceIndex, TypeIndex};
use daedalus_wgpu::devices::{DeviceError, MAX_NAMED};
use daedalus_wgpu::handoff::crosses_devices;
use daedalus_wgpu::shader::{BindingKind, BindingSpec, Bound, Dispatch, Shader, ShaderSpec};
use daedalus_wgpu::{Options, Storage, WgpuBackend};

const TY: TypeIndex = TypeIndex(0);
const SPACE: SpaceIndex = SpaceIndex(0);

fn cx() -> CompileCx<'static> {
    CompileCx {
        types: &[],
        consts: &[],
        spaces: &[],
        bodies: &[],
        edges: &[],
        host_inputs: &[],
        host_outputs: &[],
        fifo_capacity: 1,
        merge_allowed: true,
        freeze_params: false,
        sharing: &[],
    }
}

fn step(backend: &WgpuBackend, id: &str) -> Step {
    let transfers = backend.transfers();
    let transfer = transfers.iter().find(|t| t.id == id).expect("offered");
    (transfer.make)(&cx()).expect("a step")
}

fn through(step: &mut Step, input: Slot) -> (StepEnd, Slot, String) {
    let mut cells = [input, Slot::EMPTY];
    let mut cx = StepCx::new();
    let end = run_once(step, &mut Slots::of(&mut cells), &mut cx);
    drop_owned(cells[0]);
    (end, cells[1], cx.reason().to_string())
}

/// The table of named devices is the process's, and holds three: one test at a time.
static NAMES: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn names() -> std::sync::MutexGuard<'static, ()> {
    NAMES
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// What the planner is told about `backend`: its id, its memory and its moves, exactly. The
/// stand-in takes any node, where the real backend takes only shaders.
fn told(backend: &WgpuBackend) -> Mem {
    let keep = |text: &str| -> &'static str { text.to_string().leak() };
    Mem {
        id: keep(backend.id()),
        spaces: backend
            .memory()
            .iter()
            .map(|space| (keep(&space.id), space.kind, space.readable))
            .collect(),
        moves: backend
            .transfers()
            .iter()
            .map(|t| (t.id, keep(&t.from), keep(&t.to), t.cost))
            .collect(),
        ..Mem::default()
    }
}

fn moves(stitched: &Stitched) -> Vec<&str> {
    stitched
        .layout
        .iter()
        .filter_map(|item| match item {
            Item::Move { transfer, .. } => Some(stitched.transfers[*transfer as usize].id.as_str()),
            _ => None,
        })
        .collect()
}

#[test]
fn a_named_device_is_another_backend_with_its_own_memory() {
    let _one = names();
    let main = WgpuBackend::open(Options::default());
    let other = WgpuBackend::open_named("b", Options::default()).expect("a free name");
    assert_eq!((main.id(), other.id()), ("wgpu", "wgpu.b"));
    assert_eq!((main.device_name(), other.device_name()), (None, Some("b")));

    let ids = |backend: &WgpuBackend| -> Vec<String> {
        backend
            .memory()
            .iter()
            .map(|space| space.id.to_string())
            .collect()
    };
    assert_eq!(
        ids(&main),
        [
            "wgpu.buffer",
            "wgpu.mapped",
            "wgpu.cached",
            "wgpu.dmaheap",
            "wgpu.texture",
            "cpu.host"
        ]
    );
    assert_eq!(
        ids(&other),
        [
            "wgpu.b.buffer",
            "wgpu.b.mapped",
            "wgpu.b.cached",
            "wgpu.b.dmaheap",
            "wgpu.b.texture",
            "cpu.host"
        ]
    );

    // Nothing either lists goes from one device to the other, and no id is used twice.
    let table: Vec<_> = main
        .transfers()
        .into_iter()
        .chain(other.transfers())
        .collect();
    for transfer in &table {
        assert!(
            !crosses_devices(&transfer.from, &transfer.to),
            "{}",
            transfer.id
        );
    }
    let mut seen: Vec<&str> = table.iter().map(|transfer| transfer.id).collect();
    seen.sort_unstable();
    seen.dedup();
    assert_eq!(seen.len(), table.len());
    assert!(seen.contains(&"wgpu.b.upload") && seen.contains(&"wgpu.upload"));

    // A name is one device's while it lives, and anyone's after.
    assert_eq!(
        WgpuBackend::open_named("b", Options::default()).err(),
        Some(DeviceError::Taken("b".into()))
    );
    for bad in ["", "B", "a.b", "buffer"] {
        assert_eq!(
            WgpuBackend::open_named(bad, Options::default()).err(),
            Some(DeviceError::Name(bad.into()))
        );
    }
    let rest: Vec<WgpuBackend> = (1..MAX_NAMED)
        .map(|at| WgpuBackend::open_named(&format!("fill{at}"), Options::default()).unwrap())
        .collect();
    assert_eq!(
        WgpuBackend::open_named("one-more", Options::default()).err(),
        Some(DeviceError::Full)
    );
    drop(rest);
    drop(other);
    assert!(WgpuBackend::open_named("b", Options::default()).is_ok());
}

#[test]
fn a_value_crosses_devices_only_by_the_host_and_fails_to_plan_where_it_cant() {
    let _one = names();
    let main = WgpuBackend::open(Options::default());
    let other = WgpuBackend::open_named("plan", Options::default()).expect("a free name");
    let (main, other) = (told(&main), told(&other));
    let mut registry = Registry::new();
    for (id, backend) in [("t.a", "wgpu"), ("t.b", "wgpu.plan"), ("t.cam", "cam")] {
        decl(id)
            .optional("x", u8_ty())
            .output("out", u8_ty())
            .body(backend)
            .add(&mut registry);
    }
    let host = Mem {
        id: "cpu",
        spaces: vec![("cpu.host", SpaceKind::Host, true)],
        ..Mem::default()
    };
    // A producer whose images import as textures on the default device, and go nowhere else.
    let producer = Mem {
        id: "cam",
        spaces: vec![("cam.frames", SpaceKind::Shared, true)],
        moves: vec![(
            "cam.import",
            "cam.frames",
            "wgpu.texture",
            TransferCost::Import,
        )],
        ..Mem::default()
    };
    let backends: [&dyn Backend; 4] = [&main, &other, &producer, &host];
    let line = |from: &str, to: &str| (vec![node(from), node(to)], vec![edge(0, "out", 1, "x")]);

    // From one device to the other: read back, handed to the host, uploaded. Every leg is in
    // the plan, and none is one hop.
    let (nodes, edges) = line("t.a", "t.b");
    let (_, plan) = stitched(&registry, nodes, edges, &backends).expect("plans");
    assert_eq!(
        moves(&plan),
        ["wgpu.readback", "wgpu.map", "wgpu.plan.upload"]
    );
    let (nodes, edges) = line("t.b", "t.a");
    let (_, plan) = stitched(&registry, nodes, edges, &backends).expect("plans");
    assert_eq!(
        moves(&plan),
        ["wgpu.plan.readback", "wgpu.plan.map", "wgpu.upload"]
    );

    // An image from a producer reaches the default device, where its import is listed.
    let (nodes, edges) = line("t.cam", "t.a");
    let (_, plan) = stitched(&registry, nodes, edges, &backends).expect("plans");
    assert_eq!(moves(&plan), ["cam.import"]);
    // Nothing takes it to the other device, or from a texture to anywhere: planning fails and
    // names the port and both spaces.
    let (nodes, edges) = line("t.cam", "t.b");
    let error = stitched(&registry, nodes, edges, &backends)
        .map(|_| ())
        .unwrap_err();
    assert_eq!(
        error,
        RegionError::Stitch(StitchError::NoTransfer {
            port: "t.b.x".into(),
            from_space: "cam.frames".into(),
            to_space: "wgpu.plan.buffer".into(),
        })
    );
}

const DOUBLE: &str = "
    @group(0) @binding(0) var<storage, read> src: array<u32>;
    @group(0) @binding(1) var<storage, read_write> dst: array<u32>;
    @compute @workgroup_size(64)
    fn main(@builtin(global_invocation_id) id: vec3<u32>) {
        if id.x < arrayLength(&dst) { dst[id.x] = src[id.x] * 2u; }
    }
";

const fn storage(index: u32, name: &'static str, read_only: bool) -> BindingSpec {
    BindingSpec {
        index,
        name,
        kind: BindingKind::Storage {
            read_only,
            stride: 4,
        },
    }
}

const DOUBLE_SPEC: ShaderSpec = ShaderSpec {
    name: "test.double",
    entry: "main",
    source: DOUBLE,
    workgroup: [64, 1, 1],
    bindings: &[storage(0, "src", true), storage(1, "dst", false)],
    dispatch: Dispatch::PerElement {
        binding: 1,
        stride: 4,
    },
    inputs: &[],
    outputs: &[],
};

/// Two backends with a device each: the best adapter and the next, or the best twice where the
/// machine has one.
fn two() -> Option<(WgpuBackend, WgpuBackend)> {
    let record = common::adapter()?;
    let first = WgpuBackend::open_named(
        "first",
        Options {
            index: Some(0),
            ..Options::default()
        },
    )
    .expect("a free name");
    let second = WgpuBackend::open_named(
        "second",
        Options {
            index: Some(1),
            ..Options::default()
        },
    )
    .expect("a free name");
    first.start().expect("the best adapter opens");
    if second.start().is_err() {
        // One adapter: a second device on it is still another device.
        drop(second);
        let again = WgpuBackend::open_named(
            "second",
            Options {
                adapter: Some(record.adapter.name),
                ..Options::default()
            },
        )
        .expect("the name is free again");
        again.start().expect("opens");
        return Some((first, again));
    }
    Some((first, second))
}

#[test]
fn bytes_cross_from_one_device_to_the_other_through_the_host() {
    let _one = names();
    let Some((first, second)) = two() else { return };
    let (a, b) = (
        first.device_key().expect("open"),
        second.device_key().expect("open"),
    );
    assert_ne!(a, b, "two devices");

    let words: Vec<u32> = (0..1000).map(|at| at * 3 + 1).collect();
    let bytes: Vec<u8> = words.iter().flat_map(|word| word.to_le_bytes()).collect();
    let mut cell = Slot::EMPTY;
    put_shared(&mut cell, TY, SPACE, Arc::from(bytes.as_slice()));
    let mut upload = step(&first, "wgpu.first.upload");
    let (end, mut out, why) = through(&mut upload, cell);
    assert!(matches!(end, StepEnd::Done), "{why}");
    let on_first = take_typed::<Storage>(&mut out).expect("a buffer");
    assert_eq!(on_first.device_key(), a, "an upload goes to its own device");

    // A shader on the other device won't take it as it is.
    let shader = Shader::load(&second, &DOUBLE_SPEC).expect("loads");
    let target = Storage::new(&second, 4000).expect("a buffer");
    let refused = shader.run(&[Bound::Storage(&on_first), Bound::Storage(&target)]);
    assert!(refused.is_err_and(|error| error.to_string().contains("another device")));

    // The planned way: read back on the first, handed to the host, uploaded on the second.
    let mut readback = step(&first, "wgpu.first.readback");
    let mut map = step(&first, "wgpu.first.map");
    let mut across = step(&second, "wgpu.second.upload");
    let mut cell = Slot::EMPTY;
    put_typed(&mut cell, TY, SPACE, Arc::clone(&on_first));
    let (end, mut out, why) = through(&mut readback, cell);
    assert!(matches!(end, StepEnd::Pending(_)), "{why}");
    let readable = take_typed::<Storage>(&mut out).expect("a buffer");
    // The runner holds the next step until the copy is done. Here a device read does.
    let mut seen = vec![0; 4000];
    on_first.read_into(0, &mut seen).expect("a readback");
    let mut cell = Slot::EMPTY;
    put_typed(&mut cell, TY, SPACE, readable);
    let (end, out, why) = through(&mut map, cell);
    assert!(matches!(end, StepEnd::Done), "{why}");
    let before = second.transfer_stats().expect("open");
    let (end, mut out, why) = through(&mut across, out);
    assert!(matches!(end, StepEnd::Done), "{why}");
    let on_second = take_typed::<Storage>(&mut out).expect("a buffer");
    assert_eq!(on_second.device_key(), b, "copied, not passed along");
    let after = second.transfer_stats().expect("open");
    assert_eq!(
        after.uploads - before.uploads,
        1,
        "counted as the upload it is"
    );
    assert_eq!(after.upload_bytes - before.upload_bytes, 4000);

    // And there it runs.
    let run = shader
        .run(&[Bound::Storage(&on_second), Bound::Storage(&target)])
        .expect("runs");
    let daedalus_wgpu::shader::Run::Started(ticket) = run else {
        panic!("nothing else is in flight");
    };
    ticket.wait().expect("finishes");
    let mut got = vec![0; 4000];
    target.read_into(0, &mut got).expect("a readback");
    let doubled: Vec<u8> = words
        .iter()
        .flat_map(|word| (word * 2).to_le_bytes())
        .collect();
    assert_eq!(got, doubled);

    // A buffer already on the device passes through its upload untouched.
    let mut cell = Slot::EMPTY;
    put_typed(&mut cell, TY, SPACE, Arc::clone(&on_second));
    let (_, mut out, _) = through(&mut across, cell);
    let same = take_typed::<Storage>(&mut out).expect("a buffer");
    assert!(Arc::ptr_eq(&same, &on_second));

    for step in [upload, readback, map, across] {
        release(step);
    }
}

#[test]
fn each_device_has_its_own_caches_and_a_new_device_a_new_key() {
    let _one = names();
    let Some((first, second)) = two() else { return };
    let before = second.shader_stats().expect("open").pipelines;
    let on_first = Shader::load(&first, &DOUBLE_SPEC).expect("loads");
    assert_eq!(first.shader_stats().expect("open").pipelines, 1);
    assert_eq!(
        second.shader_stats().expect("open").pipelines,
        before,
        "built on one device, not on the other"
    );
    let on_second = Shader::load(&second, &DOUBLE_SPEC).expect("loads");
    assert_eq!(second.shader_stats().expect("open").pipelines, before + 1);

    // Letting one device go leaves the other's cache as it was.
    let key = first.device_key().expect("open");
    drop(on_first);
    first.stop();
    assert_eq!(first.device_key(), None);
    assert_eq!(second.shader_stats().expect("open").pipelines, before + 1);
    let again = Shader::load(&second, &DOUBLE_SPEC).expect("found");
    assert_eq!(second.shader_stats().expect("open").pipelines, before + 1);

    // The device opened in its place is a new one, with nothing cached.
    first.start().expect("opens again");
    let fresh = first.device_key().expect("open");
    assert_ne!(fresh, key, "a fresh key");
    assert_ne!(Some(fresh), second.device_key());
    assert_eq!(first.shader_stats().expect("open").pipelines, 0);
    drop((on_second, again));
}

#[test]
fn repeated_wgpu_backend_creation_registers_fresh_device_cache_keys() {
    let Some(record) = common::adapter() else {
        return;
    };
    let mut keys = Vec::new();
    for _ in 0..4 {
        let backend = WgpuBackend::open(Options {
            adapter: Some(record.adapter.name.clone()),
            ..Options::default()
        });
        backend.start().expect("opens");
        let key = backend.device_key().expect("open");
        assert!(!keys.contains(&key), "a key is never used again");
        keys.push(key);
        // A buffer says which device it is on.
        assert_eq!(
            Storage::new(&backend, 16).expect("a buffer").device_key(),
            key
        );
    }
}
