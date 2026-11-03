# daedalus-accelerator

The accelerator interface for NPUs and custom accelerators beside the GPU backend: load a
program, allocate, upload or import (dmabuf) buffers, submit with waits, and complete through a
fence that can be waited on, polled, or exported as a `sync_file`.

This crate is the interface only; no accelerator is implemented here. The design, and how it maps
onto the GPU path and onto RKNPU and the K230 KPU, is in
[`docs/accelerator-interface.md`](../../docs/accelerator-interface.md).
