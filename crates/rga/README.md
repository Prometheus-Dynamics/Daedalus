# daedalus-rga

Rockchip's RGA 2D engine: scaling, cropping and pixel-format conversion between dmabufs with no
CPU copy. Byte buffers work too, through `MMAP` buffers. This is the binding Daedalus and Styx
share.

- **Backend:** the kernel's V4L2 mem2mem interface. On boards that is the mainline
  `rockchip-rga` driver (RGA2). It works with any mem2mem device, and the tests run it against
  the kernel's `vim2m` test driver and `/dev/dma_heap/system` dmabufs.
- **librga:** `Librga` reaches the engine through the vendor library instead, for RGA3 on vendor
  kernels, gray input and Y-only output.
- **Resize:** `Rga::resize` / `resize_bytes` / `resize_luma` and `Librga::resize_dmabuf` /
  `resize_bytes` take a `Filter`: `Area` (integer downscale, rounded block means, a trailing
  partial block cropped) or `Bilinear`. The mainline driver has no filter control (it averages
  when it downscales, interpolates when it upscales), so it refuses bilinear downscale with
  `Unsupported` rather than substitute; librga does both. Mainline has no gray format, so
  `resize_luma` goes through NV12 with neutral chroma (1.5 times the bytes).
- **Crop, flip, rotate:** `Rga::crop` / `crop_bytes` (the output queue's crop selection,
  unscaled; a full-frame rectangle is a copy) and `Librga::crop_*` and `transform_*`
  (`Transform`: horizontal and vertical flip, 90, 180 and 270 degree turns, clockwise assumed).
  An output that is not the rectangle's or the turn's size is refused. On mainline,
  `Rga::transform` / `transform_bytes` set the context's `V4L2_CID_HFLIP`, `VFLIP` or `ROTATE`
  control through Lemnos and refuse a value the driver does not keep.
- **Palette and colour key (librga only):** `Librga::palette_bytes` / `palette_dmabuf` (8-bit
  indices through a 256-entry RGBA table) and `color_key_bytes` / `color_key_dmabuf` /
  `color_key_mask` (the pixels whose R, G and B lie in a range drawn opaque onto a cleared RGBA
  frame, whose alpha is the mask). The mainline driver has neither. A librga without
  `impalette_t` or `imcolorkey_t` makes that call `Unsupported`.
- **Capabilities:** `caps::caps(soc)` lists each SoC's units (RGA2, RGA3, K230D Image 2D and
  VGLite), the route (mainline V4L2, librga, design only), size and scale limits, the luma cost,
  each filter's direction and exactness, and the other operations (palette, colour key). Every exactness is `Unmeasured` until a board run.
- **Status:** V4L2 path tested on `vim2m`; both paths written, untested on a Rockchip board.

Linux-only. The V4L2 session is Lemnos's (`lemnos-linux`, `v4l2::m2m::M2mSession`); librga is
loaded at run time.
