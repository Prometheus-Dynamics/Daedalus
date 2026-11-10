//! Planning opens no device: `can_take` answers from what is already known.

use daedalus_backend::{Backend, BodyIndex, NodeRef, Offer, OfferNode};
use daedalus_types::NodeIndex;
use daedalus_wgpu::{
    AdapterInfo, Api, CapsRecord, ComputeCaps, ComputeFeatures, DeviceKind, DeviceProfile, Options,
    WgpuBackend, devices_created,
};

fn node(index: u32, needs: &'static [&'static str]) -> OfferNode<'static> {
    OfferNode {
        node: NodeIndex(index),
        body: BodyIndex(0),
        inputs: &[],
        outputs: &[],
        needs,
    }
}

fn taken(backend: &WgpuBackend, nodes: &[OfferNode<'_>]) -> Vec<u32> {
    backend
        .can_take(&Offer { nodes })
        .into_iter()
        .map(|NodeRef { node, .. }| node.0)
        .collect()
}

#[test]
fn planning_creates_no_device() {
    let nodes = [
        node(0, &[]),
        node(1, &["workgroup=8x8x1", "storage_buffers=4"]),
        node(2, &["workgroup=32x32"]),
        node(3, &["f16"]),
        node(4, &["something this backend has never heard of"]),
    ];
    let backend = WgpuBackend::open(Options::default());
    assert_eq!(backend.version(), 0);
    // With nothing known, the WebGPU default limits decide.
    assert_eq!(taken(&backend, &nodes), [0, 1]);
    assert_eq!(
        backend.memory().first().map(|space| &*space.id),
        Some("wgpu.buffer")
    );
    assert!(backend.transfers().is_empty());

    // With a record from an earlier run, that decides.
    let adapter = AdapterInfo {
        name: "A GPU".into(),
        vendor: 0x10DE,
        device: 1,
        api: Api::Vulkan,
        kind: DeviceKind::Discrete,
    };
    let known = CapsRecord {
        profile: DeviceProfile::classify(&adapter),
        adapter,
        compute: ComputeCaps {
            max_invocations_per_workgroup: 1024,
            features: ComputeFeatures {
                shader_f16: true,
                ..ComputeFeatures::NONE
            },
            ..ComputeCaps::WEBGPU_DEFAULTS
        },
    };
    let informed = WgpuBackend::open(Options {
        known: Some(known.clone()),
        ..Options::default()
    });
    assert_eq!(informed.caps(), Some(known));
    assert_ne!(informed.version(), 0);
    assert_eq!(taken(&informed, &nodes), [0, 1, 2, 3]);

    assert_eq!(devices_created(), 0, "planning opened a device");
}
