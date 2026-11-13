# Values Daedalus did not make

Daedalus has no value types of its own beyond the scalars, and it never looks inside a value. A
value is a type, a memory space and an owner (`daedalus_value::Value`), and the core stores and
passes those three. So a value that lives somewhere else, in memory another library owns or in a
handle from an operating system, is passed like any other.

The Rust in this page is in `core/daedalus/tests/foreign_values.rs`, which compiles and runs it.
The docs test in `core/daedalus/tests/docs.rs` checks that each Rust block here appears there
verbatim, so this page cannot quote code that does not compile.

## A type of your own

A value's type is a `Typed` impl. `#[derive(Type)]` writes one for a struct by reading the `Typed`
impl of each field, so it only works when every field has one. `OwnedFd` has none, and the core
does not look inside a value anyway, so a struct that holds a file descriptor is written by hand.
Its expression is `TypeExpr::Opaque` under its key:

```rust
struct Buffer {
    fd: OwnedFd,
    width: u32,
    height: u32,
    stride: u32,
}

impl Typed for Buffer {
    const KEY: TypeKey = TypeKey::new("acme:buffer");
    const EXPR: TypeExpr = TypeExpr::Opaque(Self::KEY);
}
```

The imports are `std::os::fd::OwnedFd` and `daedalus::layers::types::{TypeExpr, TypeKey, Typed}`.
Types are compared by key, so a key is the name of the type across a graph.

## Holding it in a value

A value holds the struct in an owner. Inside one build that is `Owner::Typed`, an `Arc` of the Rust
value. Handing the value from node to node moves that `Arc` and does not copy the struct, and
dropping the last holder drops the struct, which closes the descriptor.

```rust
fn hold(buffer: Buffer) -> Value {
    Value {
        ty: TypeIndex(0),
        space: SpaceIndex(0),
        owner: Owner::Typed(Arc::new(buffer)),
    }
}
```

The imports are `daedalus::layers::types::{SpaceIndex, TypeIndex}`,
`daedalus::layers::value::{Owner, Value}` and `std::sync::Arc`. The type and space indices are the
planner's numbers for them. This example uses zero because it runs no plan.

## A value owned by code outside this build

A value that another library owns is held through a `ForeignHandle`: a pointer to the value, the
interface it is reached through, and its owner's retain and release functions, all `repr(C)`.
`Value::foreign(ty, space, handle)` holds one. Cloning the value shares the handle, and the last
holder to let go calls the release function, once.

Both sides name an interface by a key, a version and a layout hash. `foreign_layout_hash` hashes
the interface's signature text with whitespace removed, then its size and alignment. A consumer asks
for the interface it understands with `ForeignHandle::view::<I>()`. A handle that is not that
interface (by key, version or layout hash) is refused with an error that names both, before
anything is read through it.

Writing an interface and a handle takes `unsafe` (`ForeignInterface` and `ProvideForeign` are
unsafe traits, and the `foreign_interface!` and `provide_foreign!` macros expand to them). The
workspace forbids unsafe code in this crate's tests, so the handle is not built in
`foreign_values.rs`. The working example is
`core/value/tests/foreign.rs`, which builds one, checks a view, and checks that the release runs
once.

## Memory spaces

A node says where its output lives with `memory(port = "space")` on `#[node]`. The space names are
the ones a backend returns from its `memory` method (`core/backend/src/lib.rs`). The wgpu backend's
names are constants in `backends/wgpu/src/memory.rs`:

- `wgpu.buffer`: a buffer in device memory, where its outputs live.
- `wgpu.mapped`: a buffer the host can also read and write in place.
- `wgpu.cached`: host-mapped, in cached memory.
- `wgpu.dmaheap`: a buffer whose memory is a dma-buf from the system heap.
- `wgpu.texture`: a texture on the device, such as an imported image.

```text
#[node(id = "acme.read", outputs(image), memory(image = "wgpu.dmaheap"))]
```

When a producer and a consumer of the same type are in different memory spaces, the planner routes
the value along the transfers registered between them. When no path exists, planning fails with an
error that names the port and the two spaces (`core/planner/src/stitch/mod.rs`).

A transfer is written with `#[transfer]`, which takes `from` and `to` space names and a `cost` of
`handoff`, `import` or `copy`. The planner prefers the cheapest path. The `#[node]` and `#[transfer]`
attributes expand into their backend's authoring crate, which is `daedalus_cpu_author` unless
`backend =` names another. This crate does not depend on one, so those attributes are not compiled
here. `backends/cpu/tests/transfer.rs` is a transfer that is compiled and run.

Bytes in a dma-buf are imported by the wgpu backend, not by a `#[transfer]` you write. A type that
implements `daedalus_wgpu::DmaBufBacked` says which dma-buf holds its bytes and how many there are,
and `WgpuBackend::with_dmabuf::<T>()` lists the transfer that imports them into a device buffer at
cost `Import`, while the open device can import dma-bufs. The backend knows nothing else about the
value: no format, no layout.

`backends/dma-sys` is the binding to the dma-buf and dma-heap kernel interfaces (`DmaBuf`,
`DmaHeap`). It declares no memory space and runs no nodes.

## Across a plugin boundary

A plain-data type crosses a plugin boundary as a block, with the key in `#[plain(key = "...")]` and
a layout hash of its size, alignment and field offsets (`core/derive/src/plain.rs`). A receiver
refuses a block with another key or layout hash, and names both, before it reads the bytes
(`core/loader/src/pod.rs`). A value a plugin made is bytes its library owns, handed
over as `Owner::Handed`. The loader keeps that library open while a step built from it is alive
(`core/loader/src/generation.rs`). See [plugins.md](plugins.md).

## Across the service

`daedalusd` passes a value to a client only if its type has a wire codec. Every scalar has one. A
port of a type with none is refused with `unsupported`, both for a feed and for a take.
