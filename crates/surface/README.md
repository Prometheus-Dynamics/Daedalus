# daedalus-surface

The shared-library surface of Daedalus: a small C ABI (`libdaedalus_surface.so`) over the
in-process graph runtime, for shared mode (see `docs/shared-library.md`). Built on the `lemnos-abi`
primitives: `Status`, generation-counted `Handle`s, `BufferDesc`/`FenceDesc`, the `AbiVersion`
handshake and the panic guard.

- Header: `include/daedalus_surface.h` (hand-written; `tests/header.rs` keeps it in sync with the
  exports and the `lemnos-abi` layouts, and checks that it compiles as C11).
- Consumer: `daedalus-host` (`crates/host-wrapper`) is the safe Rust wrapper. C callers use the
  header directly.
- Built as `cdylib` and `rlib`. Its own code is portable Rust; Linux is the tested platform.

## Entry points

Version and features: `daedalus_surface_version`, `daedalus_surface_features`.

Host (a plugin registry and its engine): `daedalus_surface_host_create`,
`daedalus_surface_host_release` (`BUSY` while graphs from it are alive),
`daedalus_surface_host_install` (a library by path, through the stable handler path),
`daedalus_surface_host_schemas` (JSON).

Graphs: `daedalus_surface_graph_compile` (a `daedalus.graph` document, prepared),
`daedalus_surface_graph_release`, `daedalus_surface_graph_ports`,
`daedalus_surface_graph_explain`, `daedalus_surface_graph_telemetry` (JSON).

Per frame, one call each: `daedalus_surface_push_{i64,f64,bool,bytes}`,
`daedalus_surface_tick`, `daedalus_surface_take_{i64,f64,bool,bytes}`.

Frames (Linux; `NOT_SUPPORTED` elsewhere): `daedalus_surface_push_frame` pushes a dma-buf frame
(one `BufferDesc` descriptor per plane, with an acquire fence) into a `daedalus:frame` port,
`daedalus_surface_take_frame` takes one out as a lease (owned, duplicated descriptors), and
`daedalus_surface_frame_release` releases the lease. Descriptor ownership follows
`BufferDesc::OWNED`; see the header.

Every call returns a `daedalus_status`: 0 for success, a negative errno, or one of the
`DAEDALUS_*` codes. A handle that was released, or never issued, is `DAEDALUS_STALE_HANDLE` and
is never dereferenced.

## Building

    cargo build -p daedalus-surface --release

The `alloc-count` feature adds a counting global allocator and exports
`daedalus_surface_allocations`. The shared-mode test builds the surface with it; no consumer should
enable it.
