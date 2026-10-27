//! A stable-path plugin built against `daedalus:frame` v2 (Daedalus 3.0.0) in a v3 host: it is
//! installed, fed v3 frames lent under the v2 identity, and the frames it hands back (passed on
//! or made by its v2 build) are read through v3.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};

use super::*;
use crate::runtime::executor::{CorrelatedPayload, NodeHandler};
use crate::runtime::io::NodeIo;
use crate::runtime::{ExecutionContext, RuntimeNode, StateStore};
use crate::transport::{
    FRAME_INTERFACE_KEY, FRAME_INTERFACE_V2, ForeignHandle, ForeignOwner, FrameColorimetry,
    FrameInterfaceV2, FramePlane, FrameResidency, FrameSource, FrameVTableV2, Payload,
    ProvideForeign, Residency, fourcc,
};

/// A gray frame whose drops are counted.
struct Gray {
    pixels: Vec<u8>,
    drops: Arc<AtomicUsize>,
}

impl Drop for Gray {
    fn drop(&mut self) {
        self.drops.fetch_add(1, SeqCst);
    }
}

impl FrameSource for Gray {
    fn width(&self) -> u32 {
        self.pixels.len() as u32
    }
    fn height(&self) -> u32 {
        1
    }
    fn format(&self) -> u32 {
        fourcc(b"R8  ")
    }
    fn residency(&self) -> FrameResidency {
        FrameResidency::Cpu
    }
    fn plane_count(&self) -> u32 {
        1
    }
    fn plane(&self, index: u32) -> Option<FramePlane> {
        (index == 0).then(|| FramePlane::cpu(&self.pixels, self.pixels.len() as u64))
    }
    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        (index == 0).then_some(&self.pixels[..])
    }
    fn colorimetry(&self) -> FrameColorimetry {
        FrameColorimetry::SRGB
    }
}

/// What a v2 build's provider table looks like: v2's accessors only.
fn v2_table() -> &'static FrameVTableV2 {
    let v3 = <Gray as ProvideForeign<FrameInterface>>::vtable();
    Box::leak(Box::new(FrameVTableV2 {
        width: v3.width,
        height: v3.height,
        format: v3.format,
        format_kind: v3.format_kind,
        modifier: v3.modifier,
        timestamp_ns: v3.timestamp_ns,
        sequence: v3.sequence,
        residency: v3.residency,
        plane_count: v3.plane_count,
        plane_fd: v3.plane_fd,
        plane_offset: v3.plane_offset,
        plane_stride: v3.plane_stride,
        plane_len: v3.plane_len,
        plane_mapping: v3.plane_mapping,
        plane_data: v3.plane_data,
        plane_end_cpu_access: v3.plane_end_cpu_access,
    }))
}

static MADE_DROPS: AtomicUsize = AtomicUsize::new(0);

/// The plugin's `relay_frame` node, written as a v2 build would run it: it reads its input
/// through v2 (failing on anything else), passes it on, and makes a v2 frame of its own.
pub(super) fn register(ctx: &mut PluginInstallContext<'_>) -> PluginResult<()> {
    let frame = || TypeExpr::opaque(FRAME_INTERFACE_KEY);
    ctx.register_node_decl(
        NodeDecl::new("relay_frame")
            .input(PortDecl::new("frame", FRAME_INTERFACE_KEY).schema(frame()))
            .output(PortDecl::new("width", "u32").schema(TypeExpr::Scalar(ValueType::U32)))
            .output(PortDecl::new("same", FRAME_INTERFACE_KEY).schema(frame()))
            .output(PortDecl::new("made", FRAME_INTERFACE_KEY).schema(frame())),
    )?;
    ctx.handlers.on("relay_frame", |_node, _ctx, io| {
        let handle = io
            .get_payload("frame")
            .and_then(Payload::foreign_handle)
            .cloned()
            .ok_or_else(|| NodeError::InvalidInput("no frame".into()))?;
        let view = handle
            .view::<FrameInterfaceV2>()
            .map_err(|err| NodeError::InvalidInput(err.to_string()))?;
        // Safety: a checked v2 table with its data pointer.
        let width = unsafe { (view.vtable().width)(view.data()) };
        io.push_as(Some("width"), TypeKey::new("u32"), width);
        io.push_payload(
            "same",
            Payload::foreign(FRAME_INTERFACE_KEY, handle, Residency::Cpu),
        );
        let made = Arc::new(Gray {
            pixels: vec![7; 5],
            drops: Arc::new(AtomicUsize::new(0)),
        });
        let data = Arc::as_ptr(&made).cast();
        // Safety: the table's functions take a `Gray`, kept alive by the owner reference.
        let made = unsafe {
            ForeignHandle::from_raw_parts::<FrameInterfaceV2>(
                data,
                v2_table(),
                ForeignOwner::from_arc(Arc::new(CountDrop(made))),
            )
        };
        io.push_payload(
            "made",
            Payload::foreign_local(FRAME_INTERFACE_KEY, made, Residency::Cpu),
        );
        Ok(())
    });
    Ok(())
}

/// Keeps a `Gray` alive and counts the made frame's release in [`MADE_DROPS`].
struct CountDrop(#[allow(dead_code)] Arc<Gray>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        MADE_DROPS.fetch_add(1, SeqCst);
    }
}

/// A plugin built against `daedalus:frame` v2.
unsafe extern "C" fn v2_foreign_interfaces(table: *mut ForeignInterfaceTable, _: StrSink) -> bool {
    let entries = vec![FRAME_INTERFACE_V2].leak();
    // Safety: the host passes a writable table.
    unsafe {
        *table = ForeignInterfaceTable {
            entries: entries.as_ptr(),
            len: entries.len(),
        }
    };
    true
}

#[test]
fn a_frame_v2_plugin_runs_through_the_stable_path_of_a_v3_host() {
    let library = load_with(|descriptor| {
        old_toolchain(descriptor);
        descriptor.foreign_interfaces = v2_foreign_interfaces;
    })
    .unwrap();
    assert_eq!(library.foreign_interfaces(), [FRAME_INTERFACE_V2]);
    let mut registry = PluginRegistry::new();
    registry
        .register_foreign_interface::<FrameInterface>()
        .unwrap();
    assert_eq!(
        library.install_into(&mut registry).unwrap(),
        InstallPath::Stable
    );
    // The host keeps using v3.
    let key = TypeKey::new(FRAME_INTERFACE_KEY);
    assert_eq!(registry.foreign_interfaces()[&key], *FrameInterface::info());

    let drops = Arc::new(AtomicUsize::new(0));
    let frame = Arc::new(Gray {
        pixels: vec![1, 2, 3],
        drops: drops.clone(),
    });
    let data = Arc::as_ptr(&frame).cast::<std::ffi::c_void>();
    let input = Payload::foreign_local(
        FRAME_INTERFACE_KEY,
        ForeignHandle::from_arc::<_, FrameInterface>(frame),
        Residency::Cpu,
    );
    let mut io = NodeIo::from_inputs([("frame".into(), CorrelatedPayload::from_edge(input))]);
    let ctx = ExecutionContext::detached(StateStore::default(), "relay-1".into());
    registry
        .handlers
        .run(&RuntimeNode::new("relay_frame"), &ctx, &mut io)
        .unwrap();
    let mut outputs = io.take_outputs();
    let mut take = |port: &str| {
        let index = outputs.iter().position(|(name, _)| name.as_str() == port);
        outputs.swap_remove(index.expect(port)).1.inner
    };
    assert_eq!(take("width").get_ref::<u32>(), Some(&3), "read through v2");
    {
        // The host's own frame comes back as itself: v3 identity, same data, no adapter.
        let payload = take("same");
        let same = payload.foreign_handle().unwrap();
        assert!(std::ptr::eq(same.interface(), FrameInterface::info()));
        assert_eq!(same.data(), data);
        let view = same.view::<FrameInterface>().unwrap();
        assert_eq!(
            view.colorimetry(),
            FrameColorimetry::SRGB,
            "v3 accessors work"
        );
    }
    {
        // The v2 build's own frame is read through v3, its additions unknown.
        let payload = take("made");
        let made = payload
            .foreign_handle()
            .unwrap()
            .view::<FrameInterface>()
            .unwrap();
        assert_eq!(&*made.plane_bytes(0).unwrap(), &[7; 5]);
        assert_eq!(made.colorimetry(), FrameColorimetry::UNSPECIFIED);
    }
    drop((outputs, ctx));
    assert_eq!(drops.load(SeqCst), 1, "the host frame released once");
    assert_eq!(
        MADE_DROPS.load(SeqCst),
        1,
        "the plugin's frame released once"
    );
}

#[test]
fn frame_v2_plugins_are_refused_on_the_rust_abi_path() {
    let library =
        load_with(|descriptor| descriptor.foreign_interfaces = v2_foreign_interfaces).unwrap();
    let mut registry = PluginRegistry::new();
    registry
        .register_foreign_interface::<FrameInterface>()
        .unwrap();
    let err = library
        .install_into_as(&mut registry, InstallPath::RustAbi)
        .unwrap_err();
    assert!(
        matches!(&err, PluginLibraryError::ForeignInterfaceMismatch { mismatches, .. }
            if mismatches[0].plugin == FRAME_INTERFACE_V2),
        "{err:?}"
    );
}
