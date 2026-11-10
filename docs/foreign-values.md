# Values Daedalus did not make

Daedalus has no value types of its own beyond the scalars, and it never looks inside a value. A
value is a type key, a memory space and an owner, and the core stores and passes those three.
So a value that lives somewhere else, in memory another library owns or in a handle from an
operating system, is passed like any other.

## A type of your own

```rust
#[derive(Type)]
#[type_key("acme:image")]
struct Image { fd: OwnedFd, width: u32, height: u32, stride: u32 }
```

The struct is the value. Nodes take it by reference, and it is behind one handle however large
what it refers to is. Dropping the last handle drops the struct, which is where its owner
releases what it holds.

## Memory spaces

A node says where its output lives:

```rust
#[node(id = "acme.read", outputs(image), memory(image = dmabuf))]
fn read(source: &mut Source) -> Image { .. }
```

When a producer and a consumer of the same type are in different memory spaces, the planner
puts a transfer between them, and fails planning, naming both ports and both spaces, when no
transfer is registered for that type between those spaces.

A transfer is one function:

```rust
#[transfer(from = "dmabuf.block", to = "wgpu.buffer", cost = import, backend = daedalus_wgpu_author)]
fn to_gpu(image: &Image, gpu: &GpuCx) -> Texture { gpu.import_external(&image.describe()) }
```

Its cost class is `handoff`, `import` or `copy`, and the planner takes the cheapest path.

A backend does not have to run nodes. `backends/dmabuf` is a memory backend: it declares a
memory space and a pool of blocks, each an fd and a length, and knows nothing of what is in them.

## A value owned by code outside this build

A value can be owned through a `ForeignHandle`: a pointer to the value, the interface it is
reached through, and its owner's retain and release functions, all `repr(C)`. `Value::foreign` holds one. Cloning the value shares the handle, and the last
holder to let go calls the release function, once.

Both sides name an interface by a key and a layout hash made from its `repr(C)` shape
(`foreign_layout_hash`). A consumer asks for the interface it understands, and a handle with
another hash is refused with both hashes named, before anything is read through it.

## Across a plugin boundary

A value a plugin made is bytes the plugin's library owns, behind a shared handle that keeps the
library open while the value lives. A plain-data type crosses as a block with its key and its
layout hash, which the reader checks before it reads. See [plugins.md](plugins.md).

## Across the service

`daedalusd` passes a value to a client only if its type has a wire codec. Every scalar has
one. A port of a type with none is refused with `unsupported`, both for a feed and for a take.
