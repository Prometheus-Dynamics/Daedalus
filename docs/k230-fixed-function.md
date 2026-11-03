# K230D fixed-function units: design note

What Linux on the Kendryte K230D can reach for the fixed-function image ops Daedalus provides on
the Rockchip RGA (`daedalus-rga`, `daedalus.image.*`), and how a binding would look. There is no code
yet and no board run; everything below comes from reading the K230 Linux SDK and its kernel.

**Sources:** the K230 Linux SDK v0.6.1 (`k230_linux_sdk`, `buildroot-overlay/package/`) and the
kernel it builds (ruyisdk `linux-xuantie-kernel` at `ae4a7e65`, as Gaia's K230D Zero image builds
it). The SDK is Linux only: there is no RT-Smart or MPP code in it, so units owned by the big
core's media stack are out of view.

## Units Linux can reach

| Unit | Kernel interface | Userspace | Built for the K230D Zero |
|---|---|---|---|
| VGLite 2.5D GPU (VeriSilicon GCNanoUltraV, `verisilicon,gc8000ul`, `gpu@90800000`) | `/dev/vg_lite`, a vendor char device with one ioctl (`VG_LITE_IOCTL`: allocate, map, submit, wait, cache) | `libvg_lite.so` and `vg_lite.h`, shipped as source (MIT / GPL) | kernel `CONFIG_GPU_VGLITE=y`, device-tree node enabled; Gaia's image does not build the library (`BR2_PACKAGE_VG_LITE` unset) |
| AI2D (`k230-ai2d`, `ai2d@80400c00`) | `/dev/k230-ai2d`: dmabuf import and a completion interrupt only; no job ioctl, no mmap, no uAPI header | nncase's `ai2d_builder`, a prebuilt binary runtime | kernel `CONFIG_K230_AI2D_DRIVER=y`, node enabled; no nncase in Gaia's image |
| Display planes (DRM `canaan`) | KMS planes: 90 and 270 degree rotation, X and Y reflection, 1/8 to 8x scaling | DRM | scanout only, not a memory-to-memory engine |

The "Image 2D" engine in Kendryte's material (crop, flip, rotation, OSD) has no Linux driver in
this SDK: only a GSDMA clock gate exists. If it exists, it belongs to the RT-Smart side.

## VGLite: what it does for the `daedalus.image.*` ops

The library's feature flags (`VGLite/vg_lite_options.h`) decide what the hardware takes. They
narrow the header's format list a lot.

| Op | VGLite | Notes |
|---|---|---|
| `resize_bilinear` | `vg_lite_blit` with a scale matrix, `VG_LITE_FILTER_BI_LINEAR` | phase and rounding unknown |
| `resize_area` | no averaging filter (point, linear, bilinear only) | a 2x bilinear blit at the half-pixel phase averages 2x2 exactly in theory; to measure |
| `crop` | `vg_lite_blit_rect` | |
| `flip` | a blit with a negative scale (mirror matrix) | `gcFEATURE_VG_MIRROR` is 0, so no mirror shortcut |
| `rotate` | a blit with `vg_lite_rotate` | |
| `fill` | `vg_lite_clear` on a rectangle | |
| `blend` | SRC_OVER, DST_OVER, PREMULTIPLY_SRC_OVER and others | no plain SRC mode named; a blit with blending off is SRC |
| `palette` | `INDEX_8` source with a colour table | to confirm against the blit path |
| `color_key_mask` | none | `gcFEATURE_VG_COLOR_KEY` is 0 |
| `mask_rop` | none | no raster operations |
| line drawing | `vg_lite_draw` paths | the one fixed-function line route on the target SoCs (RGA2 and RGA3 have no line mode) |

**Formats:** RGBA / BGRA / ARGB / ABGR / XRGB 8888, 565, 4444, 5551, A8, L8 and the indexed
formats. Planar YUV input is off (`YUV_INPUT` 0), so NV12 is not a source; YUY2 is a source
only; 24-bit RGB is off. Luma pyramids would run on L8.

**Limits:** source strides align to 16 bytes for A8 / L8 (more for wider formats), and allocated
strides pad to 16 pixels. The GPU's DMA mask is 32-bit.

## The dmabuf catch

`vg_lite_map(.., VG_LITE_MAP_DMABUF, fd)` attaches a dmabuf, but the kernel's loop over the
scatter list keeps only the last entry's address (`vg_lite_hal.c:439`). So only physically
contiguous dmabufs (one entry) map correctly. Daedalus's system-heap buffers are not contiguous.
Zero-copy needs one of:

- buffers from the K230's contiguous allocator (`CONFIG_K230_MMZ`, `libmmz`) or a CMA heap, or
- a kernel fix that maps the whole scatter list (a vendor patch Daedalus does not own).

Otherwise frames go through a VGLite-allocated buffer, with a copy.

## How a binding would look

- **Crate:** `daedalus-vglite`, beside `daedalus-rga`, Linux only. It would load `libvg_lite.so`
  at run time as `Librga` does, so nothing links a vendor library. Calling the ioctl directly
  would mean reimplementing the library's command-buffer building, which is most of its source.
- **API:** the shapes `daedalus-rga` already has. `resize`, `crop`, `transform`, `fill` and
  `blend` take an image (format, size, stride) and a buffer. Strict refusals stay: a format or
  filter the unit lacks is `Unsupported`, never a substitute.
- **Capabilities:** the K230D entries in `daedalus_rga::caps` gain the VGLite route and its ops,
  each `Unmeasured`. The descriptor may move to its own crate once two vendors share it.
- **Matrix:** `fixed_function` rows with a `vglite` variant on the existing `daedalus.image.*`
  ops, with board tests like the RGA's.
- **AI2D:** not planned. It is reachable only through the nncase binary runtime and has no
  public register interface. Its resize and affine ops would come in through nncase if Eidos runs
  models there anyway.

## Open before code

1. A K230D board slot, and an image with `BR2_PACKAGE_VG_LITE` and `libmmz` built in (Gaia's
   K230D Zero defconfig leaves both out).
2. Whether a 2x bilinear blit at the half-pixel phase gives the rounded 2x2 mean (`resize_area`),
   and VGLite's rounding in general.
3. The contiguous-buffer route for zero copy (MMZ or a CMA heap), or a copy budget.
4. Whether the RT-Smart side owns an Image 2D engine that Linux could reach through IPC; this SDK
   cannot answer it.
