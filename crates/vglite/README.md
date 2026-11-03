# daedalus-vglite

The VeriSilicon VGLite 2.5D GPU (GCNanoUltraV) on the Kendryte K230 and K230D, through the
vendor's `libvg_lite.so`, loaded at run time; nothing links it. It is the fixed-function
`vglite` variant of the `daedalus.image.*` ops.

- **Resize:** `resize_bytes` / `resize_dmabuf` with a `Filter`. `Bilinear` scales either way
  through a blit with a scale matrix. VGLite samples at the half-pixel phase, as OpenCV does.
  VGLite has no averaging filter, so `Area` is offered only at 2x. A bilinear blit at half scale
  puts each output centre on the corner of four source pixels, which gives the 2x2 mean if the
  GPU's rounding agrees (a board measurement). Other area factors are `Unsupported`.
- **Crop, flip, rotate:** `crop_*` and `transform_*` (`Transform`: flips and 90, 180 and 270
  degree clockwise turns). Each is a translated, mirrored or rotated point-sampled blit. The
  K230's build has no mirror shortcut.
- **Fill, blend, palette:** `fill_*` (`vg_lite_clear`; masks as A8) and `blend_*` (`BlendMode`:
  SRC, SRC_OVER, DST_OVER). Blends take straight alpha only: the K230's build premultiplies
  every RGBA source itself, so a premultiplied source is `Unsupported`. `palette_*` expands an
  `Index8` source through a 256-entry colour table.
- **Lines:** `line_bytes` / `line_dmabuf` draw a one-pixel line, both ends included. Each is a
  1-wide stroke with square caps through the pixel centres, drawn by `vg_lite_draw` with no
  anti-aliasing. In theory that covers Bresenham's pixels on axes and diagonals; other slopes
  are a board measurement. This is the one fixed-function line route on the target SoCs: the
  RGA has no line mode.
- **Not on this GPU:** colour key and ROP (`vg_lite_options.h`), 24-bit RGB, planar YUV input.
- **Memory:** the kernel driver maps dmabufs only, and keeps only the last scatter-list entry,
  so only a physically contiguous dmabuf maps correctly. That property cannot be read from user
  space, so the caller states it with `Frame::backing`:
  - `Contiguous`: a CMA heap or MMZ dmabuf with rows aligned for VGLite (64 bytes for RGBA,
    16 for 8-bit). It is mapped in place.
  - Anything else: byte buffers, `Scattered` dmabufs and other strides are copied through a
    contiguous staging buffer that the library allocates. Every call returns the bytes it
    copied (`Transfer::staged_bytes`).

  `daedalus_rga::caps` lists the route (`Route::Vglite`, `Staging::UnlessContiguous`) with
  every op `Unmeasured`.
- **Status:** written, untested on board. The unit tests run the code against software
  stand-ins for the library. `tests/vglite.rs` runs on a K230 board through `scripts/board.sh
  k230d` and skips without the library. It needs an image with `libvg_lite.so`, and ideally
  `libmmz` or a CMA heap.

Linux only. The declarations follow `vg_lite.h` of the K230 Linux SDK v0.6.1 (VGLite API 3.0,
MIT). See `docs/k230-fixed-function.md` for the survey behind it.
