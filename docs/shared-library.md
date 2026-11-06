# Shared and static builds

Design note for Daedalus's part of the shared-library plan (user decision, 2026-11-03; the
core/surface/wrapper split is HeliOS's design). Nothing here is built yet: the surface waits on
Lemnos's `lemnos-abi` primitives.

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

## Order

1. `lemnos-abi` lands (Lemnos).
2. The stable plugin ABI moves onto its primitives (handles, status codes, guard, slabs), keeping
   `STABLE_ABI_VERSION` compatibility.
3. `daedalus-surface` and `daedalus-host`, with a test that runs the same graph statically and
   through wrapper + surface and compares outputs, allocations and per-frame cost on the CM5.
4. Hand the result to Gaia for the link modes and the combined build.
