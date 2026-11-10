//! Shader nodes as an author writes them: the record the macro makes, and the shader running on
//! a real device against the same arithmetic on the CPU.

use std::any::Any;

use daedalus_macros::node;
use daedalus_registry::{Registry, TypeRef};
use daedalus_types::Typed;
use daedalus_wgpu::shader::{BindingKind, Bound, Dispatch, Run, Shader, ShaderError, ShaderSpec};
use daedalus_wgpu::{Options, Storage, WgpuBackend};

#[node(id = "t.copy", inputs(src), outputs(dst), backend = daedalus_wgpu_author(src: u32, dst: u32))]
const COPY: &str = "
    @group(0) @binding(0) var<storage, read> src: array<u32>;
    @group(0) @binding(1) var<storage, read_write> dst: array<u32>;
    @compute @workgroup_size(64)
    fn main(@builtin(global_invocation_id) id: vec3<u32>) {
        if id.x < arrayLength(&dst) { dst[id.x] = src[id.x]; }
    }
";

#[node(id = "t.scale", inputs(src), outputs(dst), backend = daedalus_wgpu_author(src: f32, dst: f32))]
const SCALE: &str = include_str!("scale.wgsl");

/// The same text as `COPY`, in another place.
#[node(id = "t.copy_again", inputs(src), outputs(dst), backend = daedalus_wgpu_author(src: u32, dst: u32))]
const COPY_AGAIN: &str = "
    @group(0) @binding(0) var<storage, read> src: array<u32>;
    @group(0) @binding(1) var<storage, read_write> dst: array<u32>;
    @compute @workgroup_size(64)
    fn main(@builtin(global_invocation_id) id: vec3<u32>) {
        if id.x < arrayLength(&dst) { dst[id.x] = src[id.x]; }
    }
";

#[node(id = "t.fill", outputs(dst), backend = daedalus_wgpu_author(dst: u32, count = [2], entry = "fill"))]
const FILL: &str = "
    @group(0) @binding(0) var<storage, read_write> dst: array<u32>;
    @compute @workgroup_size(4) fn other() {}
    @compute @workgroup_size(4)
    fn fill(@builtin(global_invocation_id) id: vec3<u32>) { dst[id.x] = id.x + 10u; }
";

fn spec(code: &'static (dyn Any + Send + Sync)) -> &'static ShaderSpec {
    (code as &dyn Any).downcast_ref().expect("a shader spec")
}

fn backend() -> Option<WgpuBackend> {
    let backend = WgpuBackend::open(Options::default());
    match backend.start() {
        Ok(_) => Some(backend),
        Err(error) => {
            assert!(
                std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
                "{error}"
            );
            None
        }
    }
}

/// Runs the shader and waits for it, waiting out a busy device first.
fn go(shader: &Shader, bound: &[Bound<'_>]) {
    loop {
        match shader.run(bound).expect("runs") {
            Run::Started(ticket) => return ticket.wait().expect("finishes"),
            Run::Busy(oldest) => oldest.wait().expect("finishes"),
        }
    }
}

fn words(bytes: &[u8]) -> Vec<u32> {
    bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|word| u32::from_le_bytes(*word))
        .collect()
}

#[test]
fn the_record_holds_what_the_shader_declares() {
    let body = &SCALE_NODE.body;
    assert_eq!(body.backend, "wgpu");
    assert_eq!(body.text, SCALE);
    assert_eq!(
        body.needs,
        [
            "workgroup=64x1x1",
            "workgroup_storage=0",
            "storage_buffers=2",
            "storage_textures=0"
        ]
    );
    assert_eq!(
        body.ports,
        [
            ("src", TypeRef::Key(f32::KEY)),
            ("dst", TypeRef::Key(f32::KEY))
        ]
    );
    let spec = spec(body.code);
    assert_eq!(
        (spec.name, spec.entry, spec.workgroup),
        ("SCALE", "main", [64, 1, 1])
    );
    let kinds: Vec<(u32, &str, BindingKind)> = spec
        .bindings
        .iter()
        .map(|b| (b.index, b.name, b.kind))
        .collect();
    assert_eq!(
        kinds,
        [
            (
                0,
                "src",
                BindingKind::Storage {
                    read_only: true,
                    stride: 4
                }
            ),
            (
                1,
                "dst",
                BindingKind::Storage {
                    read_only: false,
                    stride: 4
                }
            ),
            (2, "config", BindingKind::Uniform { size: 8 }),
        ]
    );
    assert_eq!(
        spec.dispatch,
        Dispatch::PerElement {
            binding: 1,
            stride: 4
        }
    );
    assert_eq!(
        self::spec(FILL_NODE.body.code).dispatch,
        Dispatch::Counts([2, 1, 1])
    );
    assert_eq!(self::spec(FILL_NODE.body.code).entry, "fill");

    // The registry takes the port types from the body.
    let mut registry = Registry::new();
    registry.register(&SCALE_NODE).expect("registers");
    let entry = registry.entry("t.scale").expect("an entry");
    assert_eq!(
        entry.port_type(&entry.decl.inputs[0]),
        TypeRef::Key(f32::KEY)
    );
}

#[test]
fn the_pipeline_key_is_pinned() {
    // FNV-1a over name, entry, source and bindings. A change here breaks every pipeline cache
    // on disk, so it has to be meant.
    assert_eq!(spec(FILL_NODE.body.code).key(), 0xa598_fdc0_dbcc_32e5);
    assert_ne!(
        spec(COPY_NODE.body.code).key(),
        spec(COPY_AGAIN_NODE.body.code).key(),
        "the name counts"
    );
}

#[test]
fn a_copy_shader_matches_the_cpu() {
    let Some(backend) = backend() else { return };
    let input: Vec<u32> = (0..1000u32)
        .map(|at| at.wrapping_mul(2_654_435_761))
        .collect();
    let bytes: Vec<u8> = input.iter().flat_map(|word| word.to_le_bytes()).collect();
    let src = Storage::new(&backend, 4000).expect("a buffer");
    let dst = Storage::new(&backend, 4000).expect("a buffer");
    src.write(0, &bytes).expect("in range");
    let shader = Shader::load(&backend, spec(COPY_NODE.body.code)).expect("loads");
    go(&shader, &[Bound::Storage(&src), Bound::Storage(&dst)]);
    let mut seen = vec![0; 4000];
    dst.read_into(0, &mut seen).expect("a readback");
    assert_eq!(words(&seen), input);

    // With a uniform, against the same arithmetic here.
    let scale = Shader::load(&backend, spec(SCALE_NODE.body.code)).expect("loads");
    let floats: Vec<f32> = (0..1000).map(|at| at as f32 * 0.25).collect();
    let float_bytes: Vec<u8> = floats
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect();
    src.write(0, &float_bytes).expect("in range");
    let config: Vec<u8> = [3.0f32, -1.5]
        .iter()
        .flat_map(|value| value.to_le_bytes())
        .collect();
    let bound = [
        Bound::Storage(&src),
        Bound::Storage(&dst),
        Bound::Uniform(&config),
    ];
    go(&scale, &bound);
    dst.read_into(0, &mut seen).expect("a readback");
    let got: Vec<f32> = seen
        .as_chunks::<4>()
        .0
        .iter()
        .map(|v| f32::from_le_bytes(*v))
        .collect();
    let want: Vec<f32> = floats.iter().map(|value| value * 3.0 - 1.5).collect();
    assert_eq!(got, want);

    // Explicit counts and a named entry point: two workgroups of four.
    let fill = Shader::load(&backend, spec(FILL_NODE.body.code)).expect("loads");
    let small = Storage::new(&backend, 32).expect("a buffer");
    go(&fill, &[Bound::Storage(&small)]);
    let mut eight = [0; 32];
    small.read_into(0, &mut eight).expect("a readback");
    assert_eq!(words(&eight), [10, 11, 12, 13, 14, 15, 16, 17]);

    // What doesn't match the binding table is refused before anything is submitted.
    let wrong = shader.run(&[Bound::Storage(&src)]);
    assert!(
        matches!(wrong.map(drop), Err(ShaderError::Binding(text)) if text.contains("2 bindings"))
    );
    let wrong = scale.run(&[
        Bound::Storage(&src),
        Bound::Storage(&dst),
        Bound::Uniform(&[0; 4]),
    ]);
    assert!(
        matches!(wrong.map(drop), Err(ShaderError::Binding(text)) if text.contains("`config`"))
    );
    let wrong = shader.run(&[Bound::Sampler, Bound::Storage(&dst)]);
    assert!(matches!(wrong.map(drop), Err(ShaderError::Binding(text)) if text.contains("`src`")));
}

#[test]
fn pipelines_and_bind_groups_are_kept_by_content() {
    let Some(shared) = backend() else { return };
    let record = shared.start().expect("open");
    // A device of this test's own, so the counters are its alone.
    let backend = WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    });
    static FIRST: ShaderSpec = ShaderSpec { ..*COPY_SPEC };
    static SECOND: ShaderSpec = ShaderSpec { ..*COPY_SPEC };
    const COPY_SPEC: &ShaderSpec = &ShaderSpec {
        name: "copy",
        entry: "main",
        source: COPY,
        workgroup: [64, 1, 1],
        bindings: &[
            daedalus_wgpu::shader::BindingSpec {
                index: 0,
                name: "src",
                kind: BindingKind::Storage {
                    read_only: true,
                    stride: 4,
                },
            },
            daedalus_wgpu::shader::BindingSpec {
                index: 1,
                name: "dst",
                kind: BindingKind::Storage {
                    read_only: false,
                    stride: 4,
                },
            },
        ],
        dispatch: Dispatch::PerElement {
            binding: 1,
            stride: 4,
        },
    };
    assert!(!std::ptr::eq(&FIRST, &SECOND));
    let first = Shader::load(&backend, &FIRST).expect("loads");
    let second = Shader::load(&backend, &SECOND).expect("loads");
    assert_eq!(
        backend.shader_stats().expect("open").pipelines,
        1,
        "one pipeline for one content"
    );

    let (a, b) = (
        Storage::new(&backend, 256).unwrap(),
        Storage::new(&backend, 256).unwrap(),
    );
    for _ in 0..5 {
        go(&first, &[Bound::Storage(&a), Bound::Storage(&b)]);
        go(&second, &[Bound::Storage(&a), Bound::Storage(&b)]);
    }
    assert_eq!(
        backend.shader_stats().expect("open").bind_groups,
        1,
        "the same buffers, one bind group"
    );
    go(&first, &[Bound::Storage(&b), Bound::Storage(&a)]);
    assert_eq!(backend.shader_stats().expect("open").bind_groups, 2);

    // A buffer on another device is refused.
    let foreign = Storage::new(&shared, 256).unwrap();
    let wrong = first.run(&[Bound::Storage(&foreign), Bound::Storage(&b)]);
    assert!(
        matches!(wrong.map(drop), Err(ShaderError::Binding(text)) if text.contains("another device"))
    );
}
