# daedalus-rga

Rockchip's RGA 2D engine: scaling, cropping and pixel-format conversion between dmabufs with no
CPU copy. Byte buffers work too, through `MMAP` buffers. This is the binding Daedalus and Styx
share.

- **Backend:** the kernel's V4L2 mem2mem interface. On boards that is the mainline
  `rockchip-rga` driver (RGA2). It works with any mem2mem device, and the tests run it against
  the kernel's `vim2m` test driver and `/dev/dma_heap/system` dmabufs.
- **Planned:** librga (RGA3 on vendor kernels), behind the same API.
- **Status:** written; RGA on a board is untested.

Depends only on `libc`, and is Linux-only.
