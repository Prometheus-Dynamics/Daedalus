//! Generic payload transport primitives for Daedalus.
//!
//! This crate is intentionally low-level and dependency-light. It defines the transport
//! vocabulary shared by registry, planner, runtime, GPU, FFI, and plugin layers without depending
//! on those layers.

mod adapter;
mod boundary_contract;
mod boundary_storage;
mod device;
mod ids;
mod kinds;
mod payload;
mod payload_lifecycle;
mod stream_policy;

pub use adapter::{
    AdaptCost, AdaptRequest, AdapterTable, CopyCost, FanoutAction, FanoutConsumer, FanoutPlan,
    TransportAdapter, TransportError, TransportOp, plan_fanout,
};
pub use boundary_contract::{
    BoundaryCapabilities, BoundaryContractError, BoundaryTypeContract, boundary_contract_for_type,
    register_boundary_contract,
};
pub use boundary_storage::{BoundaryStorage, BoundaryTakeError, BoundaryVTable};
pub use device::{Cpu, Device, DeviceClass, DeviceTransfer, Gpu, TransferFrom, TransferTo};
pub use ids::{AdapterId, Layout, LayoutHash, SourceId, TypeKey};
pub use kinds::{AccessMode, AdaptKind, AdapterKind, Residency};
pub use payload::{
    BoundaryPayloadError, OpaquePayloadHandle, Payload, PayloadStorage, ResidencyCacheKey,
};
pub use payload_lifecycle::{
    BranchKind, BranchPayload, CorrelationId, PayloadLifecycleStage, PayloadLineage,
    PayloadRelease, PayloadReleaseQueue, ReleaseContext, ReleaseMode,
};
pub use stream_policy::{
    CoalesceStrategy, DropReason, FeedOutcome, FreshnessPolicy, OverflowPolicy,
    PolicyValidationError, PressurePolicy, validate_stream_policy,
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;
    use std::sync::Arc;

    #[derive(Debug, PartialEq, Eq)]
    struct Frame {
        bytes: Vec<u8>,
    }

    #[test]
    fn type_key_round_trips_as_string() {
        let key = TypeKey::opaque("image:dynamic");
        assert_eq!(key.as_str(), "image:dynamic");
        assert_eq!(key.to_string(), "image:dynamic");
    }

    #[test]
    fn payload_extracts_typed_ref_and_arc() {
        let payload = Payload::owned(
            "demo:frame",
            Frame {
                bytes: vec![1, 2, 3],
            },
        );

        assert_eq!(payload.type_key().as_str(), "demo:frame");
        assert_eq!(payload.get_ref::<Frame>().unwrap().bytes, vec![1, 2, 3]);
        assert_eq!(payload.get_arc::<Frame>().unwrap().bytes, vec![1, 2, 3]);
        assert!(payload.get_ref::<String>().is_none());
    }

    #[test]
    fn payload_arc_sharing_is_zero_copy() {
        let frame = Arc::new(Frame {
            bytes: vec![9, 8, 7],
        });
        let payload = Payload::shared("demo:frame", frame.clone());
        let extracted = payload.get_arc::<Frame>().unwrap();

        assert!(Arc::ptr_eq(&frame, &extracted));
    }

    #[test]
    fn payload_tracks_cached_cpu_and_gpu_residents() {
        #[derive(Debug, PartialEq, Eq)]
        struct GpuFrame {
            bytes: Vec<u8>,
        }

        let cpu = Payload::owned(
            "demo:frame",
            Frame {
                bytes: vec![1, 2, 3],
            },
        );
        let gpu = Payload::shared_with(
            "demo:frame@gpu",
            Arc::new(GpuFrame {
                bytes: vec![1, 2, 3],
            }),
            Residency::Gpu,
            None,
            Some(3),
        )
        .with_cached_resident(cpu.clone());

        assert_eq!(gpu.residency(), Residency::Gpu);
        assert_eq!(gpu.residency_cache_len(), 1);
        assert!(gpu.has_resident(cpu.type_key(), Residency::Cpu, None));
        assert_eq!(
            gpu.resident_ref::<Frame>(cpu.type_key(), Residency::Cpu, None)
                .unwrap()
                .bytes,
            vec![1, 2, 3]
        );
        assert_eq!(
            gpu.resident_ref::<GpuFrame>(gpu.type_key(), Residency::Gpu, None)
                .unwrap()
                .bytes,
            vec![1, 2, 3]
        );
    }

    #[test]
    fn payload_mutation_requires_unique_storage_and_value() {
        let mut payload = Payload::owned("demo:frame", Frame { bytes: vec![1] });

        payload.get_mut::<Frame>().unwrap().bytes.push(2);
        assert_eq!(payload.get_ref::<Frame>().unwrap().bytes, vec![1, 2]);
        assert!(payload.is_typed_unique::<Frame>());

        let mut cloned = payload.clone();
        assert!(payload.get_mut::<Frame>().is_none());
        assert!(cloned.get_mut::<Frame>().is_none());
        drop(payload);

        cloned.get_mut::<Frame>().unwrap().bytes.push(3);
        assert_eq!(cloned.get_ref::<Frame>().unwrap().bytes, vec![1, 2, 3]);
    }

    #[test]
    fn preserve_all_requires_bounded_or_buffer_all_pressure() {
        assert!(
            validate_stream_policy(&PressurePolicy::default(), &FreshnessPolicy::PreserveAll)
                .is_ok()
        );
        assert!(
            validate_stream_policy(&PressurePolicy::BufferAll, &FreshnessPolicy::PreserveAll)
                .is_ok()
        );
        assert_eq!(
            validate_stream_policy(&PressurePolicy::DropNewest, &FreshnessPolicy::PreserveAll),
            Err(PolicyValidationError::UnboundedPreserveAll)
        );
    }

    #[test]
    fn payload_drop_does_not_invoke_release_queue_hooks() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct Hook(Arc<AtomicUsize>);

        impl PayloadRelease for Hook {
            fn release_mode(&self) -> ReleaseMode {
                ReleaseMode::DeferredToRuntime
            }

            fn release(self: Box<Self>, _ctx: ReleaseContext) {
                self.0.fetch_add(1, Ordering::SeqCst);
            }
        }

        let calls = Arc::new(AtomicUsize::new(0));
        let queue = PayloadReleaseQueue::default();
        queue.push(Box::new(Hook(calls.clone())));
        let payload = Payload::owned("demo:frame", Frame { bytes: vec![1] });
        drop(payload);

        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            queue.drain(ReleaseContext {
                correlation_id: 1,
                type_key: TypeKey::new("demo:frame"),
            }),
            1
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn fanout_planner_branches_mutation_when_shared() {
        let plan = plan_fanout(&[
            FanoutConsumer {
                access: AccessMode::Read,
                exclusive: false,
            },
            FanoutConsumer {
                access: AccessMode::Modify,
                exclusive: false,
            },
        ]);

        assert_eq!(
            plan.actions,
            vec![FanoutAction::Share, FanoutAction::CowBranch]
        );
    }

    #[test]
    fn opaque_payload_handle_round_trips_unique_payload() {
        let handle =
            OpaquePayloadHandle::new(Payload::owned("demo:frame", Frame { bytes: vec![4] }));
        let payload = handle.into_payload().ok().unwrap();
        assert_eq!(payload.get_ref::<Frame>().unwrap().bytes, vec![4]);
    }

    #[test]
    fn payload_mutation_rejects_shared_producer_value() {
        let frame = Arc::new(Frame { bytes: vec![4] });
        let mut payload = Payload::shared("demo:frame", frame.clone());

        assert!(payload.is_storage_unique());
        assert_eq!(payload.typed_strong_count::<Frame>(), Some(2));
        assert!(!payload.is_typed_unique::<Frame>());
        assert!(payload.get_mut::<Frame>().is_none());
        drop(frame);

        payload.get_mut::<Frame>().unwrap().bytes.push(5);
        assert_eq!(payload.get_ref::<Frame>().unwrap().bytes, vec![4, 5]);
    }

    #[test]
    fn bytes_payload_reports_size_and_shares_buffer() {
        let bytes: Arc<[u8]> = Arc::from(&b"hello"[..]);
        let payload = Payload::bytes(bytes.clone());

        assert_eq!(payload.type_key().as_str(), "bytes");
        assert_eq!(payload.bytes_estimate(), Some(5));
        assert_eq!(payload.get_bytes().unwrap().as_ref(), b"hello");
        assert!(Arc::ptr_eq(&bytes, &payload.get_bytes().unwrap()));
    }

    #[test]
    fn adapter_cost_orders_zero_copy_before_materialize_and_transfer() {
        assert!(AdaptCost::identity() < AdaptCost::view());
        assert!(AdaptCost::view() < AdaptCost::materialize());
        assert!(AdaptCost::materialize() < AdaptCost::device_transfer());
    }

    #[test]
    fn access_mode_satisfaction_is_conservative() {
        assert!(AccessMode::Read.satisfies(AccessMode::Read));
        assert!(AccessMode::Modify.satisfies(AccessMode::Move));
        assert!(!AccessMode::Read.satisfies(AccessMode::Modify));
        assert!(!AccessMode::View.satisfies(AccessMode::Move));
    }

    #[test]
    fn enum_names_round_trip_through_shared_string_conversions() {
        for (mode, name) in [
            (AccessMode::Read, "read"),
            (AccessMode::Move, "move"),
            (AccessMode::Modify, "modify"),
            (AccessMode::View, "view"),
        ] {
            assert_eq!(mode.as_str(), name);
            assert_eq!(mode.to_string(), name);
            assert_eq!(AccessMode::from_str(name), Ok(mode));
        }

        for (residency, name) in [
            (Residency::Cpu, "cpu"),
            (Residency::Gpu, "gpu"),
            (Residency::CpuAndGpu, "cpu_and_gpu"),
            (Residency::External, "external"),
        ] {
            assert_eq!(residency.as_str(), name);
            assert_eq!(residency.to_string(), name);
            assert_eq!(Residency::from_str(name), Ok(residency));
        }

        for (kind, name) in [
            (AdaptKind::Identity, "identity"),
            (AdaptKind::Reinterpret, "reinterpret"),
            (AdaptKind::View, "view"),
            (AdaptKind::SharedView, "shared_view"),
            (AdaptKind::Cow, "cow"),
            (AdaptKind::CowView, "cow_view"),
            (AdaptKind::MetadataOnly, "metadata_only"),
            (AdaptKind::Branch, "branch"),
            (AdaptKind::MutateInPlace, "mutate_in_place"),
            (AdaptKind::Materialize, "materialize"),
            (AdaptKind::DeviceTransfer, "device_transfer"),
            (AdaptKind::DeviceUpload, "device_upload"),
            (AdaptKind::DeviceDownload, "device_download"),
            (AdaptKind::Serialize, "serialize"),
            (AdaptKind::Deserialize, "deserialize"),
            (AdaptKind::Custom, "custom"),
        ] {
            assert_eq!(kind.as_str(), name);
            assert_eq!(kind.to_string(), name);
            assert_eq!(AdaptKind::from_str(name), Ok(kind));
        }
    }

    #[test]
    fn adapter_table_runs_registered_function_by_id() {
        let mut table = AdapterTable::new();
        table
            .register_fn("demo.identity", |payload, request| {
                if payload.type_key() != &request.target {
                    return Err(TransportError::TypeMismatch {
                        expected: request.target.clone(),
                        found: payload.type_key().clone(),
                    });
                }
                Ok(payload)
            })
            .unwrap();

        let payload = Payload::owned("demo:frame", Frame { bytes: vec![1] });
        let request = AdaptRequest::new("demo:frame");
        let out = table
            .adapt(&AdapterId::new("demo.identity"), payload, &request)
            .unwrap();

        assert_eq!(out.get_ref::<Frame>().unwrap().bytes, vec![1]);
    }

    #[test]
    fn adapter_table_rejects_duplicate_and_missing_ids() {
        let mut table = AdapterTable::new();
        table
            .register_fn("demo.identity", |payload, _request| Ok(payload))
            .unwrap();

        let duplicate = table
            .register_fn("demo.identity", |payload, _request| Ok(payload))
            .unwrap_err();
        assert_eq!(
            duplicate,
            TransportError::DuplicateAdapter {
                adapter: AdapterId::new("demo.identity")
            }
        );

        let missing = table
            .adapt(
                &AdapterId::new("demo.missing"),
                Payload::owned("demo:frame", Frame { bytes: vec![] }),
                &AdaptRequest::new("demo:frame"),
            )
            .unwrap_err();
        assert_eq!(
            missing,
            TransportError::MissingAdapter {
                adapter: AdapterId::new("demo.missing")
            }
        );
    }
}
