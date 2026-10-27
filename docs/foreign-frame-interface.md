# `daedalus:frame` v3

A C-safe accessor interface for image frames, for library authors (a camera stack, an image
library, a decoder) who want their frame type consumed by any
Daedalus node, including nodes in plugins built separately from the library. It is defined in
`daedalus-transport` (`daedalus::transport::{FrameInterface, FrameVTable, FrameSource,
FrameView, SharedFrame}`), has no dependencies, and is a public contract: anything that changes
the vtable is a new version.

The general mechanism (foreign interfaces, handles, providers) is described in
[Foreign Interfaces](node-authoring.md#foreign-interfaces).

v2 separates **metadata** from **CPU access**: reading a frame's geometry, format and plane
layout (fd, offset, stride, length) never maps or syncs its memory, so a consumer that only
forwards fds (a GPU dma-buf importer) costs the provider no `mmap` and no cache maintenance.
CPU bytes are mapped only when a consumer asks for them, inside a begin/end access bracket.
v3 adds [colorimetry](#colorimetry) and keyed [metadata](#metadata) after v2's accessors, and
nodes can own frames ([`SharedFrame`](#owning-a-frame-sharedframe)): keep them, return them,
make them.

## Identity

| | |
| --- | --- |
| key | `daedalus:frame` |
| version | `3` |
| layout hash | `foreign_layout_hash` of the `FrameVTable` declaration below (field names and types, whitespace ignored) plus its size and alignment |

A consumer accepts a frame only when all three match its own copy of the interface. Plugins
report the interfaces they use in their descriptor, and `PluginLibrary::install_into` refuses a
plugin whose copy differs from the host's (`PluginLibraryError::ForeignInterfaceMismatch`); a
registry holds one version per key (`PluginError::ForeignInterfaceConflict`), with one
exception: a stable-ABI plugin built against v2 runs in a v3 host (see
[v2 plugins in a v3 host](#v2-plugins-in-a-v3-host)). v1 never mixes with v2 or v3: a v1 plugin
is refused at install, a v1 handle fails `view::<FrameInterface>()`. `FRAME_INTERFACE_V1` and
`FRAME_INTERFACE_V2` are the older identities.

## Vtable

`#[repr(C)]`; every function takes the handle's data pointer first and must not unwind.
Per-plane functions are only called with `index < plane_count`.

| field | signature | meaning |
| --- | --- | --- |
| `width` | `fn(data) -> u32` | width in pixels (coded size for compressed frames) |
| `height` | `fn(data) -> u32` | height in pixels |
| `format` | `fn(data) -> u32` | fourcc; see [Formats](#formats) |
| `format_kind` | `fn(data) -> u32` | `FrameFormatKind`: 0 unknown, 1 pixel, 2 Bayer/raw, 3 compressed; unknown values read as 0 |
| `modifier` | `fn(data) -> u64` | DRM format modifier (`DRM_FORMAT_MOD_LINEAR` = 0, `DRM_FORMAT_MOD_INVALID` when none) |
| `timestamp_ns` | `fn(data) -> u64` | capture time in ns (owner's clock, typically monotonic), 0 if unknown |
| `sequence` | `fn(data) -> u64` | frame sequence number, 0 if unknown |
| `residency` | `fn(data) -> u32` | 0 = CPU (`plane_data` succeeds for every plane), 1 = external (e.g. dma-buf), 2 = GPU; unknown values read as external |
| `plane_count` | `fn(data) -> u32` | number of planes |
| `plane_fd` | `fn(data, index: u32) -> i32` | dma-buf fd of the plane, -1 if none; borrowed (duplicate it to keep it) |
| `plane_offset` | `fn(data, index: u32) -> u64` | offset of the plane in its dma-buf (or buffer) |
| `plane_stride` | `fn(data, index: u32) -> u64` | bytes per row (0 for compressed payloads) |
| `plane_len` | `fn(data, index: u32) -> u64` | plane size in bytes |
| `plane_mapping` | `fn(data, index: u32) -> u32` | `PlaneMapping`: 0 cached, 1 uncached, 2 write-combined, 3 unmapped (`plane_data` returns null); unknown values read as uncached |
| `plane_data` | `fn(data, index: u32, len: *mut u64) -> *const u8` | begin CPU access: map (lazily), sync for CPU reads, return the bytes and store their length in `*len`; null when the CPU cannot read the plane |
| `plane_end_cpu_access` | `fn(data, index: u32)` | end the access a non-null `plane_data` began |
| `colorimetry` | `fn(data) -> FrameColorimetry` | v3: primaries, transfer, matrix (H.273 code points) and range, 4 bytes by value; see [Colorimetry](#colorimetry) |
| `metadata_u64` | `fn(data, key: u32, value: *mut u64) -> bool` | v3: numeric metadata under `key` into `*value`; false (and `*value` untouched) when the frame has none |
| `metadata_bytes` | `fn(data, key: u32, len: *mut u64) -> *const u8` | v3: byte metadata under `key`, `*len` set to its length; null when none. Valid and unchanged while the frame lives |

(`fn` stands for `unsafe extern "C" fn`, `data` for `*const c_void`.) Every field but
`plane_data` is metadata: it must not map, fault in or sync plane memory. The first sixteen
fields are v2's table, unchanged and in the same order; v3's three follow them, so a v3 table is
a valid v2 table (`FrameVTableV2`, checked at compile time).

There is no `retain`/`release` in the vtable: every foreign handle carries an ownership pair
(`ForeignOwner { ptr, retain, release }`) built by Daedalus from the owner's `Arc`, so reference
counting is the same for every interface and always runs the owner's code.

### CPU access and dma-buf sync

- Every non-null `plane_data(i)` is followed by exactly one `plane_end_cpu_access(i)`; a null
  result has no end call. `FrameView::plane_bytes` enforces this with a guard (`PlaneBytes`)
  whose drop ends the access, so a node cannot keep the bytes past the end.
- Accesses overlap: several consumers (a fan-out, parallel nodes) read the same frame at once.
  The provider counts open accesses per plane (or per buffer) and syncs on the transitions:
  `DMA_BUF_IOCTL_SYNC(SYNC_START | SYNC_READ)` when the first access begins, `SYNC_END |
  SYNC_READ` when the last ends. Syncing is the provider's job; consumers never issue the ioctl.
- The mapping is created on first use and kept until the frame drops (re-mapping per access
  would cost an `mmap`/`munmap` pair per frame). The bytes stay valid and unchanged until the
  matching end call.
- Read `plane_mapping` before reading bytes: uncached and write-combined memory is very slow to
  read repeatedly. Copy it out once, or import the fd on the GPU instead.

## Colorimetry

`FrameColorimetry { primaries, transfer, matrix, range }`: the first three are ITU-T H.273
("CICP") code points, as used by video codecs, ffmpeg and Vulkan; `range` is `ColorRange` (0
unknown, 1 limited, 2 full). Unknown codes pass through: a consumer that does not know one
treats it as unspecified. Presets cover what cameras report (libcamera's `ColorSpace` names):

| preset | primaries | transfer | matrix | range |
| --- | --- | --- | --- | --- |
| `UNSPECIFIED` (default, and every v2 frame) | 2 | 2 | 2 | unknown |
| `SRGB` (RGB data) | 1 (BT.709) | 13 (sRGB) | 0 (identity) | full |
| `SYCC` (JPEG, most USB/ISP YUV) | 1 | 13 | 6 (BT.601) | full |
| `SMPTE170M` | 6 | 1 | 6 | limited |
| `BT709` | 1 | 1 | 1 | limited |
| `BT2020` | 9 | 1 | 9 | limited |

GPU imports (`ExternalFrameDescriptor::from_frame_view`) carry it on the descriptor; YCbCr to
RGB conversion and range expansion stay in the shader, which picks its matrix from it.

## Metadata

One key space of `u32` keys (`frame_meta`), each numeric (`metadata_u64`) or bytes
(`metadata_bytes`). A provider answers the keys it knows and nothing else, so new keys never
change the vtable or the version:

| key | value |
| --- | --- |
| `EXPOSURE_NS` (1) | numeric: exposure time in ns |
| `ANALOGUE_GAIN` (2) | numeric: `f64::to_bits` of the gain (1.0 = none) |
| `DIGITAL_GAIN` (3) | numeric: `f64` bits |
| `COLOR_TEMPERATURE_K` (4) | numeric: kelvin |
| `FRAME_DURATION_NS` (5) | numeric: ns from this frame's start to the next's |
| `SENSOR_ID` (64) | bytes: UTF-8 sensor or camera id |
| `VENDOR_BASE` (`0x8000_0000`) and up | provider-private; consumers check the frame's source first |

`FrameView` reads them with `metadata_u64(key)` / `metadata_bytes(key)` and the shorthands
`exposure_ns()`, `analogue_gain()` and `sensor_id()`. New keys are assigned in
`daedalus-transport` (below `VENDOR_BASE`), as a minor change.

## Formats

`format_kind` says what `format` encodes. Providers should always set it: v1 overloaded
format 0 for "not a pixel format", and Bayer codes plus vendor modifiers are ambiguous without
it.

| kind | `format` | `modifier` |
| --- | --- | --- |
| `Pixel` | DRM fourcc (`NV12`, `XR24`, `R8  `, ...) | DRM modifier |
| `Bayer` | Bayer/raw DRM code as libcamera defines them (`RGGB`, `RG10`, `RG12`, `R10 `, ...) | `DRM_FORMAT_MOD_LINEAR` (unpacked), `MIPI_FORMAT_MOD_CSI2_PACKED`, or a vendor compression modifier |
| `Compressed` | V4L2 fourcc of the bitstream (`MJPG`, `H264`, `HEVC`) | `DRM_FORMAT_MOD_INVALID` |
| `Unknown` | anything; consumers guess (as with v1) | |

A compressed frame has one plane: `len` is the payload size, `stride` is 0.

**Modifier vendor collision.** libcamera defines `DRM_FORMAT_MOD_VENDOR_MIPI = 0x0b` and
`MIPI_FORMAT_MOD_CSI2_PACKED = fourcc_mod_code(MIPI, 1)` for CSI-2 packed Bayer
(`0x0b00000000000001`), but upstream `drm_fourcc.h` assigns vendor `0x0b` to MediaTek
(`DRM_FORMAT_MOD_VENDOR_MTK`). A modifier with vendor byte `0x0b` therefore means:

- CSI-2 packing when `format_kind == Bayer` (`FrameView::is_csi2_packed()` checks exactly this);
- a MediaTek layout when `format_kind == Pixel`;
- with `Unknown`, decide from `format`: a Bayer/raw code (as listed by libcamera's
  `formats.yaml`) means CSI-2 packing, else MediaTek. Provider hints (the owner's type key, see
  the example below) can settle the rest.

Consumers that do not understand a frame's kind or modifier refuse it rather than guessing a
layout; `ExternalFrameDescriptor::from_frame_view` refuses `Bayer` and `Compressed` frames.

## Guarantees the owner gives

- Metadata is immutable while shared: plane fds, offsets, strides, lengths and the other fields
  stay valid and unchanged until the last handle is released.
- Metadata accessors are cheap, thread-safe (`Send + Sync`), never map or sync memory, and
  never panic (a panic aborts, since it would unwind out of an `extern "C"` function).
- `plane_data` / `plane_end_cpu_access` follow [CPU access](#cpu-access-and-dma-buf-sync).

## Implementing it

Implement `FrameSource` for the frame type in the library's optional `daedalus` feature; it
provides the interface (`ProvideForeign<FrameInterface>`) with generated thunks, no `unsafe`.
`plane` returns metadata (`FramePlane { dmabuf_fd, offset, stride, len, mapping }`),
`plane_data` begins a CPU access and `end_cpu_access` ends it (both default to "no CPU
access"). A dma-buf frame that maps lazily:

```rust
use std::sync::{Arc, Mutex, OnceLock};
use daedalus::transport::{
    fourcc, FrameFormatKind, FramePlane, FrameResidency, FrameSource, PlaneMapping,
};

/// A captured buffer: one dma-buf, shared by the planes at different offsets.
pub struct DmabufFrame {
    buffer: Arc<Dmabuf>,          // fd + size, returned to the camera when the last Arc drops
    meta: FrameMeta,              // width, height, planes (offset, stride, len), sequence, ...
    map: OnceLock<Option<Mapping>>, // created on the first CPU access, unmapped on drop
    readers: Mutex<u32>,          // open CPU accesses
}

#[cfg(feature = "daedalus")]
impl FrameSource for DmabufFrame {
    fn width(&self) -> u32 { self.meta.width }
    fn height(&self) -> u32 { self.meta.height }
    fn format(&self) -> u32 { fourcc(b"NV12") }
    fn format_kind(&self) -> FrameFormatKind { FrameFormatKind::Pixel }
    fn timestamp_ns(&self) -> u64 { self.meta.timestamp_ns }
    fn sequence(&self) -> u64 { self.meta.sequence }
    fn residency(&self) -> FrameResidency { FrameResidency::External }
    fn plane_count(&self) -> u32 { self.meta.planes.len() as u32 }

    // Metadata only: no mmap, no sync.
    fn plane(&self, index: u32) -> Option<FramePlane> {
        let plane = self.meta.planes.get(index as usize)?;
        Some(FramePlane::dmabuf(self.buffer.raw_fd(), plane.offset, plane.stride, plane.len)
            .with_mapping(PlaneMapping::Cached)) // what the heap's mmap gives (Uncached for some)
    }

    fn plane_data(&self, index: u32) -> Option<&[u8]> {
        let plane = self.meta.planes.get(index as usize)?;
        let map = self.map.get_or_init(|| Mapping::new(&self.buffer).ok()).as_ref()?;
        let bytes = map.bytes().get(plane.offset as usize..)?.get(..plane.len as usize)?;
        let mut readers = self.readers.lock().unwrap_or_else(|e| e.into_inner());
        if *readers == 0 {
            self.buffer.sync(DMA_BUF_SYNC_START | DMA_BUF_SYNC_READ);
        }
        *readers += 1;
        Some(bytes)
    }

    fn end_cpu_access(&self, _index: u32) {
        let mut readers = self.readers.lock().unwrap_or_else(|e| e.into_inner());
        *readers -= 1;
        if *readers == 0 {
            self.buffer.sync(DMA_BUF_SYNC_END | DMA_BUF_SYNC_READ);
        }
    }
}
```

Frames in plain host memory return
`FramePlane::cpu(&bytes, stride)` from `plane` and `Some(&bytes)` from `plane_data`, and keep
the default no-op `end_cpu_access`. `daedalus-frame-bench`'s `SyntheticFrame` is a runnable
example over memfd and dma-heap buffers, with map and access counters.

Register the provider once, next to the type's other Daedalus registration. This is the one
camera-stack example in this document (Styx); the same shape applies to any frame type:

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

In that example the host keeps wrapping frames as before
(`Payload::shared_with("styx:framelease", Arc::new(lease), Residency::External, ..)`). A node
anywhere takes `frame: FrameView<'_>`; the planner inserts the provider's `View` adapter, which
retypes the payload as `daedalus:frame` with the provider attached, and the node reads the
lease itself through the vtable: no pixel copy, no allocation and no reference count change per
frame or per consumer. Payloads crossing into a stable-ABI plugin are wrapped in a handle to the
same `Arc` (one reference count increment) that carries the vtable.

### Adopting v3 downstream

Styx implements v3 for `FrameLease` (Styx a8ebe51, `styx_core::daedalus::colorimetry`, described
in Styx's `docs/daedalus.md`):

- **Colorimetry.** Styx's `MediaFormat::color` has no range, so the range follows the source:
  YUV and luma labelled `Srgb` are `SYCC` (BT.601, full range: the PiSP and the software ISP
  write JFIF YCbCr, and libcamera maps only its full-range spaces to `Srgb`); `Bt709` and
  `Bt2020` are limited range; RGB is the identity matrix, full range; MJPG/JPEG is `SYCC`;
  Bayer, H.264/H.265 and `Unknown` are `UNSPECIFIED`. Caveat: `uvcvideo` labels raw YUYV
  `V4L2_COLORSPACE_SRGB`, which V4L2 defines as limited range; Styx treats it as full range,
  as its own decoders do, which is not verified per camera.
- **Metadata.** `metadata_u64` gives `EXPOSURE_NS`, `ANALOGUE_GAIN`, `DIGITAL_GAIN` and
  `FRAME_DURATION_NS` for native and libcamera frames, and `COLOR_TEMPERATURE_K` for native
  frames; V4L2/UVC frames carry none. There is no `SENSOR_ID`: a lease does not know its
  camera's id.

Nothing else changes for Styx: the provider registration, `frame_payload` and the host feeding
stay as they are, and every `FrameView` / `SharedFrame` consumer, separately built or not, sees
the new fields. A HeliOS node that keeps or forwards frames and should also run from a
separately built plugin takes `SharedFrame` instead of `FrameLease` (`frame_passthrough(frame:
SharedFrame) -> Result<SharedFrame, _>`); the host still takes `FrameLease` outputs back with
`payload.get_ref::<FrameLease>()` when the frame it gets is its own (`SharedFrame::from_payload`
otherwise).

Eidos's detectors read frames as `&FrameLease`, a Rust type, so they install through the Rust
ABI only. To run them from a stable-ABI build, an Eidos node takes `FrameView<'_>` (or
`SharedFrame` to keep it) and builds its row view from the plane (`plane_bytes(0)` for the
bytes, `plane(0).stride`, `width()`, `height()`, `format()`); that needs a public borrowed
constructor in `eidos-frame` (its row views are crate-private over `FrameLease` today), plus a
fallback for what the interface does not carry (companion pyramids).

## Reading it

```rust
#[node(id = "luma_mean", inputs("frame"), outputs("mean"))]
fn luma_mean(frame: FrameView<'_>) -> Result<f64, NodeError> {
    let luma = frame.plane_bytes(0) // maps and syncs; the access ends when `luma` drops
        .ok_or(NodeError::InvalidInput("frame is not CPU-readable".into()))?;
    Ok(luma.iter().map(|&p| f64::from(p)).sum::<f64>() / luma.len() as f64)
}
```

`FrameView` offers `width`, `height`, `format`, `format_kind`, `modifier`, `is_csi2_packed`,
`timestamp_ns`, `sequence`, `residency`, `plane_count`, metadata-only `plane(i)` / `planes()`,
the mapping `plane_bytes(i)` / `cpu_planes()`, and (v3) `colorimetry`, `metadata_u64`,
`metadata_bytes`, `exposure_ns`, `analogue_gain` and `sensor_id`. For GPU import,
`ExternalFrameDescriptor::from_frame_view(&frame)` (`daedalus-gpu`, Linux) builds the dma-buf
descriptor from the metadata alone (duplicated fds, `u64` offsets and strides, fourcc,
modifier, colorimetry); attach the frame payload as its keepalive and pass it to
`GpuContextHandle::import_dmabuf` (`gpu-dmabuf` feature), or start from a `SharedFrame` with
`ExternalFrameDescriptor::from_shared_frame`, which attaches the keepalive itself.

## Owning a frame: `SharedFrame`

`FrameView<'_>` borrows the input for one call. `SharedFrame` owns a reference instead: a
`ForeignHandle` checked against v3 once, cloned by retaining through the owner's code (for a
host frame, its `Arc`) and released on drop. It is a `#[node]` input and output on the Rust-ABI
and stable plugin paths alike, typed `Opaque("daedalus:frame")` like `FrameView`:

```rust
use std::sync::Arc;
use daedalus::transport::{FrameSource, SharedFrame};

#[derive(Default)]
struct Previous(Option<SharedFrame>);

/// Keeps the frame past the call and returns the one before it: nothing is copied.
#[node(id = "delay", inputs("frame"), outputs("frame"), state(Previous))]
fn delay(frame: SharedFrame, state: &mut Previous) -> Result<Option<SharedFrame>, NodeError> {
    Ok(state.0.replace(frame))
}

/// Makes a frame (any `FrameSource` of the node's own).
#[node(id = "invert", inputs("frame"), outputs("frame"))]
fn invert(frame: SharedFrame) -> Result<SharedFrame, NodeError> {
    let view = frame.as_view(); // a FrameView: everything above works on it
    let pixels = view.plane_bytes(0).ok_or(NodeError::InvalidInput("not CPU-readable".into()))?;
    Ok(SharedFrame::new(Arc::new(MyGray::inverted(&view, &pixels))))
}
```

Rules:

- **Exactly once.** The owner value drops once, when the last `SharedFrame`, payload or handle
  referring to it drops, on whichever side of a plugin boundary that is. A plugin dropping a
  frame during the call, keeping it in node state, returning it, or panicking while it holds
  it (the stable path unwinds inside the plugin) all release it exactly once; a plugin reload
  releases what the old build's node states held. The facade's `dylib_frames` test checks each
  case with a drop counter, statically and through the stable path.
- **Immutable.** A shared frame's planes are never written. A node that changes pixels makes a
  new frame; one that changes metadata wraps the input: a `FrameSource` holding the
  `SharedFrame` that forwards to it (`SharedFrame` implements `FrameSource`) and overrides
  what it changes, so the pixels stay zero copy.
- **fds are borrowed.** `plane(i).dmabuf_fd` is the owner's descriptor, valid while any
  reference lives; consumers never close it and `dup` it to keep it longer
  (`ExternalFrameDescriptor::from_frame_view` does; `from_shared_frame` also keeps the frame
  as the import's keepalive, so the producer cannot reuse the buffer while the GPU reads it).
- **Plugin-made frames pin their plugin.** Every accessor and the release of a frame a plugin
  made run that plugin's code, so a host that receives one through the stable path keeps the
  plugin's library loaded for good (as for any handle the plugin created).

Within one build a `SharedFrame` output costs one allocation (the payload storage). Crossing the
stable boundary adds a few small allocations per frame on each side and no pixel work: about
0.6 µs over a statically installed node for a 1080p NV12 frame in and back out, the same as a
`SharedBytes` buffer (see `docs/dynamic-plugins.md`).

## v2 plugins in a v3 host

A stable-ABI plugin built against v2 (Daedalus 3.0.0) keeps working. Its foreign interface table
lists v2, which the host accepts on the stable path only, registering v3 itself. Each frame
input is lent to it under the v2 identity (`frame_handle_to_v2`: the same handle with the
v2 interface info, since a v3 table is a v2 table; one retain and one small allocation), and
the frames it returns are read through v3 (`frame_handle_from_v2`): a frame the host lent comes
back as itself, a frame the v2 build made is wrapped in an adapter that reports colorimetry and
metadata as unknown. Rust-ABI plugins share the host's Daedalus build, so they are always v3.

## Migrating from v2

Nothing is required: v3 only adds. `FrameSource` gains `colorimetry`, `metadata_u64` and
`metadata_bytes` with defaults (unspecified, none), so v2 providers compile unchanged and
report nothing new. Providers that know their color space or capture metadata implement them.
Rebuilt plugins and hosts speak v3; v2 stable-ABI plugins keep running as described above.

## Migrating from v1

v1 and v2 do not interoperate: providers, hosts and plugins move together (rebuild plugins).

| v1 | v2 |
| --- | --- |
| `FrameSource::plane(i) -> Option<FramePlane<'_>>` with `data` | `plane(i) -> Option<FramePlane>` (metadata) + `plane_data(i) -> Option<&[u8]>` + `end_cpu_access(i)` |
| `FramePlane { data, len: usize, stride: u32, offset: u32, dmabuf_fd }` | `FramePlane { dmabuf_fd, offset: u64, stride: u64, len: u64, mapping }` |
| `FramePlane::mapped(bytes, stride)` | `FramePlane::cpu(bytes, stride)` (and return `bytes` from `plane_data`) |
| `FramePlane::dmabuf(fd, offset, stride, len).with_data(bytes)` | `FramePlane::dmabuf(fd, offset, stride, len).with_mapping(..)`; bytes from `plane_data` |
| `format() == 0` for "no pixel format" | `format_kind()` (`Pixel`, `Bayer`, `Compressed`, `Unknown`) |
| `view.plane(i)?.data` | `view.plane_bytes(i)` (a guard; deref to `&[u8]`) |
| `view.planes()` (fetched bytes too) | `view.planes()` (metadata only) / `view.cpu_planes()` |

For the Styx provider above (v1 to v2): move the `plane_at(index).data()` lookup from `plane` into
`plane_data` (bracketing dma-buf backings with the begin/end sync of
[CPU access](#cpu-access-and-dma-buf-sync) instead of syncing when the lease is mapped), report
the CPU mapping kind (uncached mappings as `PlaneMapping::Uncached`, unreadable planes as
`Unmapped`), drop the `u32` saturation of offsets and strides, and return `Compressed` for
packets, `Bayer` for raw formats (with the CSI-2 packed modifier where it applies) and `Pixel`
otherwise.

## Out of scope in v3

- Mutation and plane writes: frames are immutable; nodes make new ones (`SharedFrame::new`).
- Fences: a producer still writing hands the frame over only once done (GPU imports take a
  `sync_file` through `ExternalFrameDescriptor::with_acquire_fence` instead).
- Crop rectangles and companion frames (pyramids, second ISP outputs): provider-private
  metadata keys until a later version names them.
