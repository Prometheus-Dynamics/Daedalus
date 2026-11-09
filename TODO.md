# TODO (dev): work now happens as PRs into `next`

Work now happens as PRs into `next`, planned in `TODO.md` and `docs/design.md` on that branch. `dev` stays frozen as the reference. This file lists only what is still open on dev.

## Planned PRs (open here)

- **B8 Regions:** whole-subgraph crossing across a plugin's nodes. Only if the I2 numbers justify it.
- **C3 Host IO:** batch `push_value`, with entries resolved without a port key. A JSON helper for host values (unconfirmed).
- **E5 Shared submit:** host-side GPU ops (the GPU half of Leap item 7).
- **E9 dma-buf import:** the `Timeline` fence mode blocks the next submission on Mesa and trips `VUID-vkQueueSubmit-pWaitSemaphores-03238`. Keep `SyncFd` as the default until it is fixed upstream.
- **G3 Stable plugin path:** stable ABI 7 codecs for the catalog nodes, and plugin-owned values.
- **G10 C surface:** by-index frame calls, bytes frames at 0 allocations, `inspect_outputs` and `Vec<u8>` ports, constant patching, and the C vtable for plugins (Leap item 7). The `Payload::providing` size estimate goes to F2.
- **I2 Budget harness:** per-node cost on the stable path (about 3,100 instructions per node per tick) and `StablePod` at 0 warm allocations. Counter reads run on the Raze as a board run.

## Board runs (need hardware; not PRs)

- **Raze #1 (CM5):** re-measure the frame-path `frame_chain` (I3 board run).
- **Luckfox Pico Pro (RV1106, stands in for Flame):** armv7 CPU graph tests and kernel conformance (D1, D8). Decide the `crc32()` ARMv7 default from `crc32_throughput` (D8); slice-by-8 stays until then.
- **K230D:** CPU graph tests and kernel conformance at the board's VLEN (D1, D8).
- **RK3588S (Mali-G610, ROCK 5C):** `gpu_probe`, GPU kernels and the fragment passes (E1 to E12).
- **Cubie A7A (Allwinner A733, PowerVR):** confirm the warp size and the driver choice (E1).
- **Luckfox Aura (RV1126B):** CPU board run (D1).
- **RK3576, RK3566, RK3562 (Mali):** GPU reports, or a confirmed CPU fallback (E1).
- **Raspberry Pi Pico:** MCU firmware stack use and tick time, in each mode (D5, D7).
