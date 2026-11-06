# Shared and static builds

Design note for Daedalus's part of the shared-library plan (user decision, 2026-11-03; the
core/surface/wrapper split is HeliOS's design). The first version is built: `daedalus-surface`
(`crates/surface`) and `daedalus-host` (`crates/host-wrapper`), on `lemnos-abi` at the workspace
revision. The status section below says what is built and what is not.

## The split

- **Core:** today's crates (`daedalus-runtime`, `-engine`, `-transport`, `-planner`, ...),
  generic and static, barely changed. Static mode links the core directly, as every consumer does
  today.
- **Surface:** a small versioned C ABI, `daedalus-surface` (`cdylib`), built once with one feature
  set. Opaque handles, status codes, fds and byte buffers only; no Rust type crosses it.
- **Wrapper:** `daedalus-host` (static `rlib`), linked into the caller in shared mode. It gives back
  typed ports, generics, lifetimes, typed errors and async, and calls the surface.

A process links either the core (static mode) or wrapper + surface (shared mode), never both;
Gaia's build checks that. The stable plugin path (`docs/dynamic-plugins.md`) is already the surface
for plugins, so a plugin built against the stable ABI runs under either mode unchanged.

## Granularity

Calls across the surface are per frame or per job, never per node, per pixel or in an inner loop.
Gaia's A/B on Raze #2 (`/mnt/ssd-samsung/gaia/shared-scratch/board/report2.txt`) measured why: a
Rust `dylib` facade with the runtime behind it put per-node calls across the boundary, and LTO
cannot inline across it. The mirror bench went from 167.5k to 201.0k instructions per tick (199.0k
with LTO), 49.6 µs to 62.5 µs. The full graph tick was unchanged at 1,040 µs, with about 2 MB more
memory and an image 2.13 MB smaller.

So the whole executor, `NodeIo`, payloads and the handlers stay on one side of the surface: inside
`daedalus-surface`, together with the nodes it runs. The caller crosses once per push, tick and take.

## What the surface carries

| Call | Per | Crosses as |
| --- | --- | --- |
| version handshake | load | `lemnos-abi` handshake: surface version, feature set, layout hashes |
| registry: install plugin libraries by path, list node schemas | setup | paths in, JSON schema out |
| compile a graph document, prepare | setup | graph JSON in, graph handle out |
| patch constants, set input/output policies | rare | JSON or scalars |
| push to a host port | frame | scalar, byte buffer, `SharedBytes` handle, frame descriptor (planes, dma-buf fds, fence) |
| tick, tick ready lanes | frame | graph handle; status code |
| take from a host port | frame | the same kinds as push, or a `Value` encoded once |
| telemetry snapshot, explain plan, inspect outputs | on demand | JSON, or recycled byte buffers |
| release a handle | any | generation-counted handle |

Frames cross as `daedalus:frame` handles or `lemnos-abi` buffer and fence descriptors, never by
copy. Handles are generation-counted (a stale handle is refused, not dereferenced), every entry
point runs under the `catch_unwind` guard, and buffers come from recycled slabs, so a warm frame
allocates nothing on either side.

## Where node code goes in shared mode

- **Node crates built in the same image build** (HeliOS's own nodes; Eidos when it reopens) link
  into the `daedalus-surface` `cdylib` itself. They are ordinary `#[node]` handlers with full Rust
  calls and LTO, and pay no stable-ABI cost. The surface build takes the list of in-image node
  crates as its input.
- **Separately built plugins** load through the stable ABI inside the surface. The bench in
  `benchmarks-log.md` ("Stable plugin path") is the cost they pay: about 2.5 µs per handle, and
  85–110 µs per frame if Eidos's 310 constants crossed on every call. So anything on a hot path
  should be in-image.
- Rust-ABI plugins and host-side Rust adapters are static mode only.

GPU access for stable-ABI plugins in shared mode is the stable plugin GPU vtable (see
`gpu-seam.md`); in-image nodes inside the surface keep `GpuContextHandle`.

## Status

Built (3.0.1-dev, Linux tested):

- **Version handshake.** `daedalus_surface_version` writes the surface's `AbiVersion` (major 1,
  minor 0, `lemnos-abi`'s `LAYOUT_HASH`). `daedalus_surface_features` writes the feature bits: an
  `AbiVersion` has no room for them, so they are a second call. The wrapper refuses a surface that
  does not accept its own version.
- **Host.** Create (an empty registry and a default engine), release (`BUSY` while graphs compiled
  from it are alive), install a plugin library by path through the stable handler path
  (`InstallPath::Stable`), and the node schemas as JSON.
- **Graphs.** Compile a `daedalus.graph` document (`GraphDocument::from_json`) and prepare it;
  release; the host's input and output ports as JSON; the plan explanation as JSON; the last tick's
  telemetry as JSON.
- **Per frame.** Push `i64`, `f64`, `bool` and bytes (`SharedBytes` ports); tick; take `i64`, `f64`,
  `bool` and bytes. Each is one call. A warm scalar frame allocates nothing in the surface. A bytes
  frame allocates what the engine's own bytes path does, the same as the static graph.
- **Errors and handles.** Every entry point runs under the `lemnos-abi` guard. Handles are
  generation-counted: a stale or null one is `STALE_HANDLE`. Messages (a bad document, a failing
  node, a plugin the stable ABI refuses) go to a caller sink. Buffers use the needed-size protocol;
  a take that does not fit keeps the bytes for the next take.
- **Test.** `crates/host-wrapper/tests/shared_vs_static.rs` runs one graph (the `stable_abi`
  plugin's `accumulate`, a conditional `every`, and `shared_sum` on bytes) over 300 frames, once
  statically and once through `daedalus-host` and the surface. The outputs are identical on every
  frame. The surface's allocations are counted by its `alloc-count` build.

Declared, built as stubs (`NOT_SUPPORTED`): `daedalus_surface_push_frame` and
`daedalus_surface_take_frame`. A frame crosses as a `lemnos-abi` `BufferDesc` with its
`FenceDesc`; wiring those to the `daedalus:frame` host ports is the next step for frames.

Not built yet: patching constants and setting input or output policies through the surface;
`inspect_outputs` across the surface; graph ports for `Vec<u8>` (the ABI moves `SharedBytes`
only; a `Vec<u8>` port is listed as `other`); the stable plugin ABI on `lemnos-abi` primitives
(order step 2 below). Node crates linked into the surface (in-image nodes) are not wired: the
surface loads separate plugins only.

Measured in a debug build (`cargo test -p daedalus-host --test shared_vs_static -- --nocapture`):
about 50 µs per scalar frame through the static graph and about 90 µs through the surface, with
the same outputs and the same allocations (none for scalars). The gap is not profiled yet; the
release numbers are the ones that count.

## Order

1. `lemnos-abi` lands (Lemnos). Done: pinned at `952e1c7`.
2. The stable plugin ABI moves onto its primitives (handles, status codes, guard, slabs), keeping
   `STABLE_ABI_VERSION` compatibility. Not started.
3. `daedalus-surface` and `daedalus-host`, with a test that runs the same graph statically and
   through wrapper + surface and compares outputs and allocations. Built (see Status); the per-frame
   cost on the CM5 is still to measure, and the frame entry points are still stubs.
4. Hand the result to Gaia for the link modes and the combined build.
