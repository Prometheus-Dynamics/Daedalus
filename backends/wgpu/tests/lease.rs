//! The host's lease on the GPU is asked for before the device is given work and given back when
//! the work is done, on a real device.

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus_wgpu::shader::{BindingKind, BindingSpec, Bound, Dispatch, Run, Shader, ShaderSpec};
use daedalus_wgpu::{GpuLease, Options, Storage, WgpuBackend};

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
    name: "test.lease.double",
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

/// Counts its calls, and checks they come in pairs: never a second `acquire` before a `release`.
#[derive(Default)]
struct Counted {
    acquired: AtomicUsize,
    released: AtomicUsize,
}

impl Counted {
    fn calls(&self) -> (usize, usize) {
        (
            self.acquired.load(Ordering::Acquire),
            self.released.load(Ordering::Acquire),
        )
    }
}

impl GpuLease for Counted {
    fn acquire(&self) {
        let acquired = self.acquired.fetch_add(1, Ordering::AcqRel) + 1;
        let released = self.released.load(Ordering::Acquire);
        assert_eq!(acquired, released + 1, "acquired while already held");
    }
    fn release(&self) {
        let released = self.released.fetch_add(1, Ordering::AcqRel) + 1;
        assert_eq!(
            released,
            self.acquired.load(Ordering::Acquire),
            "released while not held"
        );
    }
}

/// A backend with a device of its own, so the lease sees this test's work alone.
fn private() -> Option<WgpuBackend> {
    let record = common::adapter()?;
    Some(WgpuBackend::open(Options {
        adapter: Some(record.adapter.name),
        ..Options::default()
    }))
}

/// Doubles `words` on the device and reads them back.
fn double(backend: &WgpuBackend, shader: &Shader, words: &[u32]) -> Vec<u32> {
    let bytes: Vec<u8> = words.iter().flat_map(|word| word.to_le_bytes()).collect();
    let src = Storage::new(backend, bytes.len() as u64).expect("a buffer");
    src.write(0, &bytes).expect("in range");
    let dst = Storage::new(backend, bytes.len() as u64).expect("a buffer");
    let run = shader
        .run(&[Bound::Storage(&src), Bound::Storage(&dst)])
        .expect("runs");
    let Run::Started(ticket) = run else {
        panic!("nothing else is in flight");
    };
    ticket.wait().expect("finishes");
    let mut got = vec![0; bytes.len()];
    dst.read_into(0, &mut got).expect("a readback");
    got.as_chunks::<4>()
        .0
        .iter()
        .map(|word| u32::from_le_bytes(*word))
        .collect()
}

#[test]
fn the_lease_is_held_while_the_device_has_work_and_given_back_after() {
    let Some(backend) = private() else { return };
    let words: Vec<u32> = (0..1000).collect();
    let doubled: Vec<u32> = words.iter().map(|word| word * 2).collect();

    // Set before the device opens: it applies to the device once it does.
    let lease = Arc::new(Counted::default());
    backend.set_lease(Some(lease.clone()));
    assert_eq!(
        lease.calls(),
        (0, 0),
        "setting a lease opens nothing and asks nothing"
    );
    let shader = Shader::load(&backend, &DOUBLE_SPEC).expect("loads");

    for round in 1..=3 {
        let before = lease.calls();
        assert_eq!(double(&backend, &shader, &words), doubled, "round {round}");
        let (acquired, released) = lease.calls();
        assert!(
            acquired > before.0,
            "round {round}: the work asked for the lease"
        );
        assert_eq!(
            acquired, released,
            "round {round}: given back once the work was done"
        );
    }

    // Taken away: the same work asks nothing.
    backend.set_lease(None);
    let before = lease.calls();
    assert_eq!(double(&backend, &shader, &words), doubled);
    assert_eq!(lease.calls(), before, "no lease, no calls");

    // Set again on the open device: it is asked from the next work on.
    backend.set_lease(Some(lease.clone()));
    assert_eq!(double(&backend, &shader, &words), doubled);
    let (acquired, released) = lease.calls();
    assert!(acquired > before.0);
    assert_eq!(acquired, released);
}
