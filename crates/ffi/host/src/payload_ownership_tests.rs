use std::sync::Arc;

use daedalus_ffi_core::{WirePayloadHandle, WireValue};
use daedalus_ffi_cpp::{
    CppPayloadResolveError, resolve_cpp_payload_handle, resolve_cpp_payload_handle_mut,
};
use daedalus_ffi_java::{JavaPayloadTransport, JavaPayloadView, resolve_java_payload_handle};
use daedalus_ffi_node::{NodePayloadTransport, NodePayloadView, resolve_node_payload_handle};
use daedalus_ffi_python::{
    PythonPayloadTransport, PythonPayloadView, resolve_python_payload_handle,
};
use daedalus_transport::{AccessMode, Payload};

use crate::{FfiHostTelemetry, PayloadLeaseScope, RunnerPool, RunnerPoolError};

#[derive(Clone, Copy)]
enum OwnershipMode {
    ZeroCopyView,
    SharedReference,
    CowMaterialize,
    MutableBorrow,
    OwnedMove,
}

impl OwnershipMode {
    fn access(self) -> AccessMode {
        match self {
            Self::ZeroCopyView => AccessMode::View,
            Self::SharedReference => AccessMode::Read,
            Self::CowMaterialize | Self::MutableBorrow => AccessMode::Modify,
            Self::OwnedMove => AccessMode::Move,
        }
    }

    fn lease_id(self) -> &'static str {
        match self {
            Self::ZeroCopyView => "zero-copy",
            Self::SharedReference => "shared-ref",
            Self::CowMaterialize => "cow",
            Self::MutableBorrow => "mutable",
            Self::OwnedMove => "owned",
        }
    }
}

#[test]
fn payload_ownership_modes_resolve_across_all_language_surfaces() {
    let telemetry = FfiHostTelemetry::new();
    let pool = RunnerPool::new().with_ffi_telemetry(telemetry.clone());
    let mut handles = Vec::new();
    for mode in [
        OwnershipMode::ZeroCopyView,
        OwnershipMode::SharedReference,
        OwnershipMode::CowMaterialize,
        OwnershipMode::MutableBorrow,
        OwnershipMode::OwnedMove,
    ] {
        let payload = Payload::bytes_with_type_key("bytes", Arc::<[u8]>::from(vec![1_u8, 2, 3, 4]));
        let wire = pool
            .lease_payload(
                mode.lease_id(),
                payload,
                mode.access(),
                PayloadLeaseScope::Invoke,
            )
            .expect("lease payload");
        let WireValue::Handle(mut handle) = wire else {
            panic!("expected handle");
        };
        add_cross_process_backing_metadata(&mut handle, mode);
        handles.push((mode, handle));
    }

    for (mode, handle) in &handles {
        assert_eq!(
            pool.resolve_payload_ref(handle)
                .expect("resolve")
                .bytes_estimate(),
            Some(4)
        );
        assert_python_resolves(*mode, handle);
        assert_node_resolves(*mode, handle);
        assert_java_resolves(*mode, handle);
        assert_cpp_resolves(*mode, handle, &pool);
    }

    assert_eq!(pool.telemetry().active_payload_leases, 5);
    let report = telemetry.snapshot();
    assert_eq!(report.payloads.handles_created, 5);
    assert_eq!(report.payloads.handles_resolved, 9);
    assert_eq!(report.payloads.zero_copy_hits, 2);
    assert_eq!(report.payloads.shared_reference_hits, 2);
    assert_eq!(report.payloads.cow_materializations, 2);
    assert_eq!(report.payloads.mutable_in_place_hits, 1);
    assert_eq!(report.payloads.owned_moves, 2);
    assert_eq!(pool.release_invoke_payload_refs().expect("release"), 5);
    assert_eq!(pool.telemetry().active_payload_leases, 0);
    for (_, handle) in handles {
        assert!(matches!(
            pool.resolve_payload_ref(&handle),
            Err(RunnerPoolError::MissingPayloadLease(id)) if id == handle.id
        ));
    }
}

fn add_cross_process_backing_metadata(handle: &mut WirePayloadHandle, mode: OwnershipMode) {
    match mode {
        OwnershipMode::ZeroCopyView | OwnershipMode::SharedReference | OwnershipMode::OwnedMove => {
            handle
                .metadata
                .insert("bytes_estimate".into(), serde_json::json!(4));
        }
        OwnershipMode::CowMaterialize | OwnershipMode::MutableBorrow => {
            if matches!(mode, OwnershipMode::CowMaterialize) {
                handle
                    .metadata
                    .insert("ownership_mode".into(), serde_json::json!("cow"));
            }
            handle.metadata.insert(
                "mmap_path".into(),
                serde_json::json!(format!("/tmp/daedalus-{}", handle.id)),
            );
            handle
                .metadata
                .insert("mmap_len".into(), serde_json::json!(4));
            handle.metadata.insert(
                "shared_memory_name".into(),
                serde_json::json!(format!("daedalus-{}", handle.id)),
            );
            handle
                .metadata
                .insert("shared_memory_len".into(), serde_json::json!(4));
        }
    }
}

fn assert_python_resolves(mode: OwnershipMode, handle: &WirePayloadHandle) {
    let resolved =
        resolve_python_payload_handle(handle, &PythonPayloadTransport::memoryview_and_mmap())
            .expect("python resolver");
    match mode {
        OwnershipMode::CowMaterialize | OwnershipMode::MutableBorrow => {
            assert!(matches!(
                resolved.view,
                PythonPayloadView::Mmap { len: 4, .. }
            ));
        }
        _ => assert_eq!(
            resolved.view,
            PythonPayloadView::MemoryView { bytes_estimate: 4 }
        ),
    }
    assert_eq!(resolved.access, handle.access.to_string());
}

fn assert_node_resolves(mode: OwnershipMode, handle: &WirePayloadHandle) {
    let resolved =
        resolve_node_payload_handle(handle, &NodePayloadTransport::buffer_and_shared_memory())
            .expect("node resolver");
    match mode {
        OwnershipMode::CowMaterialize | OwnershipMode::MutableBorrow => {
            assert!(matches!(
                resolved.view,
                NodePayloadView::SharedMemory { len: 4, .. }
            ));
        }
        _ => assert_eq!(resolved.view, NodePayloadView::Buffer { bytes_estimate: 4 }),
    }
    assert_eq!(resolved.access, handle.access.to_string());
}

fn assert_java_resolves(mode: OwnershipMode, handle: &WirePayloadHandle) {
    let resolved =
        resolve_java_payload_handle(handle, &JavaPayloadTransport::direct_byte_buffer_and_mmap())
            .expect("java resolver");
    match mode {
        OwnershipMode::CowMaterialize | OwnershipMode::MutableBorrow => {
            assert!(matches!(
                resolved.view,
                JavaPayloadView::Mmap { len: 4, .. }
            ));
        }
        _ => assert_eq!(
            resolved.view,
            JavaPayloadView::DirectByteBuffer { bytes_estimate: 4 }
        ),
    }
    assert_eq!(resolved.access, handle.access.to_string());
}

fn assert_cpp_resolves(mode: OwnershipMode, handle: &WirePayloadHandle, pool: &RunnerPool) {
    match mode {
        OwnershipMode::MutableBorrow => {
            let mut bytes = vec![1_u8, 2, 3, 4];
            let view = resolve_cpp_payload_handle_mut(handle, &mut bytes).expect("cpp mut view");
            assert_eq!(view.len, 4);
            assert!(view.mut_ptr.is_some());
        }
        OwnershipMode::CowMaterialize => {
            let payload = pool.resolve_payload_ref(handle).expect("payload");
            assert!(matches!(
                resolve_cpp_payload_handle(handle, &payload, AccessMode::Modify),
                Err(CppPayloadResolveError::MutableRequiresUniqueStorage(_))
            ));
        }
        OwnershipMode::OwnedMove => {
            let payload = pool.resolve_payload_ref(handle).expect("payload");
            assert!(matches!(
                resolve_cpp_payload_handle(handle, &payload, AccessMode::Move),
                Err(CppPayloadResolveError::MutableRequiresUniqueStorage(_))
            ));
        }
        OwnershipMode::ZeroCopyView | OwnershipMode::SharedReference => {
            let payload = pool.resolve_payload_ref(handle).expect("payload");
            let view =
                resolve_cpp_payload_handle(handle, &payload, mode.access()).expect("cpp view");
            assert_eq!(view.len, 4);
            assert!(view.mut_ptr.is_none());
        }
    }
}
