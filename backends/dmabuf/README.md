# daedalus-dmabuf

A backend that runs no nodes. It supplies one memory space, `dmabuf`, and the two moves between it and the CPU. Linux only: on other systems the crate is empty.

- A `Block` is memory with an fd and a length. It knows nothing about width, format or planes: what the bytes mean, and their strides, are the business of whoever fills them.
- Blocks come from a dma-heap (`Source::Heap("system")`) or from a memfd (`Source::Memfd`), which needs no device.
- A `Pool` reuses blocks. It keeps up to 4 idle blocks of each size, and a block is idle again once its last holder has dropped it. Taking a block that was used before allocates nothing.
- The host reaches a block's bytes through a guard: `block.access(Access::Write)`. The guard's drop ends the access, which a dma-heap block needs so devices see the bytes.

| Transfer | From | To | Cost |
| --- | --- | --- | --- |
| `dmabuf.map` | `dmabuf` | `cpu.host` | import: the block itself, readable through its guard |
| `dmabuf.copy` | `cpu.host` | `dmabuf` | copy: the bytes go into a block from the step's pool |

Both moves are for any type. A block that arrives at `dmabuf.copy` already as a block passes through.
