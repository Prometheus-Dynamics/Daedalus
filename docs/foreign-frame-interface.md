# `daedalus:frame` v1

A C-safe accessor interface for image frames, for library authors (camera stacks such as
styx-core, image libraries such as Eidos, decoders) who want their frame type consumed by any
Daedalus node, including nodes in plugins built separately from the library. It is defined in
`daedalus-transport` (`daedalus::transport::{FrameInterface, FrameVTable, FrameSource,
FrameView}`), has no dependencies, and is a public contract: anything that changes the vtable
is a new version.

The general mechanism (foreign interfaces, handles, providers) is described in
[Foreign Interfaces](node-authoring.md#foreign-interfaces).

## Identity

| | |
| --- | --- |
| key | `daedalus:frame` |
| version | `1` |
| layout hash | `foreign_layout_hash` of the `FrameVTable` declaration below (field names and types, whitespace ignored) plus its size and alignment |

A consumer accepts a frame only when all three match its own copy of the interface. Plugins
report the interfaces they use in their descriptor, and `PluginLibrary::install_into` refuses a
plugin whose copy differs from the host's (`PluginLibraryError::ForeignInterfaceMismatch`).

## Vtable

`#[repr(C)]`; every function takes the handle's data pointer first and must not unwind.

| field | signature | meaning |
| --- | --- | --- |
| `width` | `fn(data) -> u32` | width in pixels |
| `height` | `fn(data) -> u32` | height in pixels |
| `format` | `fn(data) -> u32` | DRM fourcc (`fourcc(b"NV12")`, ...) |
| `modifier` | `fn(data) -> u64` | DRM format modifier (`DRM_FORMAT_MOD_LINEAR` = 0, `DRM_FORMAT_MOD_INVALID` when unknown) |
| `timestamp_ns` | `fn(data) -> u64` | capture time in ns (owner's clock, typically monotonic), 0 if unknown |
| `sequence` | `fn(data) -> u64` | frame sequence number, 0 if unknown |
| `residency` | `fn(data) -> u32` | 0 = CPU (all planes mapped), 1 = external (e.g. dmabuf; planes may be mapped), 2 = GPU; unknown values read as external |
| `plane_count` | `fn(data) -> u32` | number of planes |
| `plane_data` | `fn(data, index: u32) -> *const u8` | mapped bytes of the plane, null when not CPU-mapped |
| `plane_len` | `fn(data, index: u32) -> usize` | plane size in bytes (also when not mapped) |
| `plane_stride` | `fn(data, index: u32) -> u32` | bytes per row |
| `plane_offset` | `fn(data, index: u32) -> u32` | offset of the plane in its dmabuf |
| `plane_fd` | `fn(data, index: u32) -> i32` | dmabuf fd of the plane, -1 if none; borrowed (duplicate it to keep it) |

(`fn` stands for `unsafe extern "C" fn`, `data` for `*const c_void`.) Per-plane functions are
only called with `index < plane_count`.

There is no `retain`/`release` in the vtable: every foreign handle carries an ownership pair
(`ForeignOwner { ptr, retain, release }`) built by Daedalus from the owner's `Arc`, so reference
counting is the same for every interface and always runs the owner's code.

## Guarantees the owner gives

- Every value is immutable while shared: plane pointers, lengths and the other fields stay
  valid and unchanged until the last handle is released.
- Accessors are cheap, thread-safe (`Send + Sync`) and never panic (a panic aborts, since it
  would unwind out of an `extern "C"` function).

## Implementing it

Implement `FrameSource` for the frame type in the library's optional `daedalus` feature; it
provides the interface (`ProvideForeign<FrameInterface>`) with generated thunks, no `unsafe`:

```rust
use std::sync::Arc;
use daedalus::transport::{fourcc, FramePlane, FrameResidency, FrameSource};

pub struct FrameLease {
    inner: Arc<Buffer>, // the mapped or dmabuf-backed buffer, released when the lease drops
    meta: FrameMeta,
}

#[cfg(feature = "daedalus")]
impl FrameSource for FrameLease {
    fn width(&self) -> u32 { self.meta.width }
    fn height(&self) -> u32 { self.meta.height }
    fn format(&self) -> u32 { fourcc(b"NV12") }
    fn modifier(&self) -> u64 { self.meta.modifier }
    fn timestamp_ns(&self) -> u64 { self.meta.timestamp_ns }
    fn sequence(&self) -> u64 { self.meta.sequence }
    fn residency(&self) -> FrameResidency {
        if self.inner.is_dmabuf() { FrameResidency::External } else { FrameResidency::Cpu }
    }
    fn plane_count(&self) -> u32 { self.meta.planes.len() as u32 }
    fn plane(&self, index: u32) -> Option<FramePlane<'_>> {
        let plane = self.meta.planes.get(index as usize)?;
        Some(FramePlane {
            data: self.inner.mapped_plane(index), // Option<&[u8]>, None when not mapped
            len: plane.len,
            stride: plane.stride,
            offset: plane.offset,
            dmabuf_fd: self.inner.dmabuf_fd(), // Option<i32>
        })
    }
}
```

Register the provider once, next to the type's other Daedalus registration:

```rust
#[daedalus::plugin(
    id = "styx",
    types(crate::FrameLease),
    foreign_providers(crate::FrameLease => daedalus::transport::FrameInterface),
)]
pub struct StyxPlugin;
// or, in an install hook:
// registry.register_foreign_provider::<FrameLease, FrameInterface>()?;
```

The host keeps wrapping frames as before
(`Payload::shared_with("styx:framelease", Arc::new(lease), Residency::External, ..)`). A node
anywhere takes `frame: FrameView<'_>`; the planner inserts the provider's `View` adapter, which
retypes the payload as `daedalus:frame` with the provider attached, and the node reads the
lease itself through the vtable: no pixel copy, no allocation and no reference count change per
frame or per consumer. Payloads crossing into a stable-ABI plugin are wrapped in a handle to the
same `Arc` (one reference count increment).

## Reading it

```rust
#[node(id = "luma_mean", inputs("frame"), outputs("mean"))]
fn luma_mean(frame: FrameView<'_>) -> Result<f64, NodeError> {
    let luma = frame.plane(0).and_then(|plane| plane.data)
        .ok_or(NodeError::InvalidInput("frame is not CPU-mapped".into()))?;
    Ok(luma.iter().map(|&p| f64::from(p)).sum::<f64>() / luma.len() as f64)
}
```

`FrameView` offers `width`, `height`, `format`, `modifier`, `timestamp_ns`, `sequence`,
`residency`, `plane_count`, `plane(i)` and `planes()`; plane slices borrow the view. For GPU
import, pass `dmabuf_fd`, `offset`, `stride`, `format` and `modifier` to
`GpuContextHandle::import_dmabuf` (`gpu-dmabuf` feature).

## Out of scope in v1

- Producing frames through the interface (a node returning a `FrameView`): nodes that create
  frames return their own type, which separately built consumers then read through the
  interface again.
- Mutation, plane writes, fences, color space and crop metadata. A later version can add them
  as `daedalus:frame` v2. A registry holds one version per key
  (`PluginError::ForeignInterfaceConflict` otherwise), so providers and consumers move to a new
  version together.
