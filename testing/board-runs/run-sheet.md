# Run sheet: the next CM5 batch (raze2, V3D)

One batched session on the CM5, for six figures: E13, E9, E14, E6, ticks (D2 and I2) and E11, in that
order. Nothing else is in this batch. C1 needs no board time (see C1.md, correction 2026-11-20). I3, the
ListView fan and the other GPU tests are out of it.

## Rules set before the run

Every pass rule below is fixed here before the session, and the judge (`scripts/board/judge.py figures`)
applies them as written. These three were chosen and approved beforehand, not fitted to the results:

- A share "at least 45 of 50" (E9's fence unsignalled at export, E14's producer still writing at the
  consumer's submit, and the planted early readers of E9 (b) and (c) and E14 seeing the sentinel): a
  write calibrated to be just long enough may finish before a few of them. Every other share is 50 of 50.
- E14's allocations per import: at most 50, the top of the reference's 44 to 50.
- E14's second device is opened by naming the adapter, so the board opens two devices on V3D.

## Stopping

- The run ends, and the coordinator is told, when: the preflight fails; E13's planted call goes
  unnamed (the GPU figures after it don't run); the board doesn't answer for 15 minutes
  (`BATCH_RETURN_MINUTES`); or the GPU is left wedged with the kernel alive.
- Nothing in this run reboots or power-cycles the board. Only the user does. A watchdog restart is the
  board's own; after one the GPU is checked again.

## How it runs

On the host, from a clone on NVMe:

1. `VVL_DIR=/mnt/nvme-wd/scratch/vgl/vvl/aarch64 scripts/board/batch.sh build`, with the layer
   recorded under "What the board needs". It builds the bundle under `$CARGO_TARGET_DIR/board/batch`: `daedalus-gpu-budgets`, `daedalus-budget-harness`, its `ticks`
   bench (`ticks-bench`), `bounded`, the `vk-sys` unit tests (`vk-sys.lib`), the wgpu `all` and
   `region` tests, the layer, `batch-on-board.sh` and `figures/*.sh`.
2. `scripts/board/batch.sh rehearse` on this machine first. It must pass.
3. Only after the board's keeper gives the word: `BOARD_SSH=<board> scripts/board/batch.sh go`. It
   copies the bundle to `/tmp/dd-batch`, runs `sh ./batch-on-board.sh` there detached under both GPU
   locks, brings `results/` back, and removes `/tmp/dd-batch`.
4. `scripts/board/judge.py figures $CARGO_TARGET_DIR/board/batch/results`. It applies the rules below
   and says PASS or FAIL with the reason.

On the board the script runs: preflight, E13, E9, E14, E6, ticks, E11. Ticks is the CPU's figure and
runs whatever E13 said; the GPU figures run only when E13 opened. Each figure is `sh figures/<f>.sh`.
Every step runs under `bin/bounded 900` (`BATCH_SECONDS`), with `DAEDALUS_GPU_REQUIRED=1` and
`DAEDALUS_GPU_ADAPTERS=hardware`. Each figure writes `results/<f>.txt` and `results/<f>-gate.txt`. The
run ends with a GPU check (`results/gpu-health.txt`) and `results/done`.

The scripts are busybox-ash scripts. There is no dash or busybox on the machine they were written on, so
each was checked with `sh -n` (bash) and `bash --posix -n` only.

## Preflight

- Command: the `preflight` function of `batch-on-board.sh`. Result: `results/preflight.txt`.
- It checks that every script parses (`sh -n`), every command the scripts name is on the board, and
  every program in `bin/` starts.
- Pass: the last line is `preflight ok`. Anything else ends the run there, with nothing measured.
- `results/probe.txt` records the board as found: image, kernel, user, `/dev/dma_heap` and `/dev/dri`
  permissions, the watchdog, the v3d module's parameters, and the Vulkan extensions below.

## E13: validation, first

- Commands (`figures/e13.sh`), with `VK_LAYER_PATH=./vvl`, `VK_INSTANCE_LAYERS=VK_LAYER_KHRONOS_validation`
  and synchronization validation on:
  - planted, first: `env DAEDALUS_PLANT_VALIDATION_ERROR=1 bin/vk-sys.lib planted --nocapture --test-threads=1`
  - then, only if the planted run named a rule:
    `bin/vk-sys.lib --nocapture --test-threads=1`;
    `bin/wgpu.all fragment:: --nocapture --test-threads=1`;
    `bin/wgpu.region --nocapture --test-threads=1`;
    `bin/daedalus-gpu-budgets raw-submit fragment-pass`.
- Expected: the planted run names `VUID-VkBufferCreateInfo-size-00912`. It has not run on the board
  before (E13, 2026-11-16: no test made the planted call). The clean runs print nothing: 16 of 16 `vk-sys`
  tests with no validation line on 2026-11-17 (E13), and a clean fragment run on 2026-11-17 (E7). On
  2026-11-16 the fragment run printed 14 `VUID-StandaloneSpirv-None-10684` errors (E7); on
  2026-11-17 the same run was clean.
- Pass: the planted run names a `VUID-` rule, and the four clean runs print no validation line (no
  `Validation Error`, `Warning`, `Performance Warning` or `Information`, no `VUID-`, `SYNC-HAZARD` or
  `UNASSIGNED-`; the layer's own "layer active" note does not count). Every step exits 0.
- Fails it: no `VUID-` line in `e13-planted.txt`; any validation line in a clean run; no layer in the
  bundle. If the planted call goes unnamed, E9, E14, E6 and E11 do not run, and each of their results
  says `not run: E13 did not open: ...`. Those count as failures too. Ticks, the CPU's figure, still
  runs.

## E9: the device's fence on an imported dma-buf, with planted early readers

Reworked 2026-11-20 for explicit sync. The 2026-11-20 run found that v3dv attaches nothing to an
imported dma-buf, so the backend now attaches each submit's fence itself (`docs/gpu-nodes.md`, "When the
device is done with a dma-buf"). This figure measures that, and proves it can tell a reader that waits
from one that doesn't.

- Command (`figures/e9.sh`): `bin/daedalus-gpu-budgets dma-fence`. The E9 lines start `dma-fence:`.
- What it does: a 1 MiB dma-heap buffer (with 64 KiB room past it) is imported into the device. The GPU
  writes it with a loop whose length is calibrated until three writes in a row are still running after a
  1 MiB read made right after the submit. Before each run the CPU writes a sentinel over the buffer,
  inside a write sync. 50 runs after 5 warm-ups for each of:
  - (a) the CPU reads inside the read sync, with no other wait, right after the submit;
  - (b) planted: a plain read of the mapping right after the submit, with no sync;
  - (c) planted: as (a), on a second device opened with attaching turned off
    (`Options::no_fence_attach`).
  Then the attach cost: the host's time per attach from `sync_stats()`, and the submit-to-done time of a
  one-round write with attaching on and off.
- Expected: a fence attached on every submit, no host wait. The fence exported at the submit is a
  sync_file and unsignalled in all 50. (a) sees the GPU's values in all 50. (b) and (c) see the sentinel
  in all 50, and in (c) the fence was signalled at export.
- Pass: attached plus host waits covers every submit, and a host wait names its reason; sync_file 50 of
  50; unsignalled at export at least 45 of 50; (a) 50 of 50; (b) at least 45 of 50; both (c) shares at
  least 45 of 50; an attach cost line; the step exits 0.
- Fails it: `dma-fence: skipped:` on the board; (a) seeing the sentinel even once; (b) or (c) under 45,
  which means the figure can't catch an early reader; a host wait with no reason; a crash.
- Reported, not judged: the attach cost, for the coordinator's ruling on attaching per submit or only
  when a value leaves the host's ordering.

## E14: steady imports and the device-side wait

- Commands (`figures/e14.sh`): `bin/daedalus-gpu-budgets dma-steady`, then
  `bin/daedalus-gpu-budgets dma-fence` again for its `dma-wait:` lines.
- Steady state: eight 1 MiB dma-heap buffers imported in rotation for 1000 imports. Cache hits are the
  rotations less the backend's import count (`TransferStats.imports`), which grows only on a real
  import. Each import now also makes the value's own buffer over the kept import, which carries the
  dma-buf and the value's owner (two allocations; the 2026-11-20 median was 1).
- Device-side wait: a second device on the same GPU imports the same dma-buf and copies it out, in a
  raw submit that waits on the fence exported at the first device's submit, which the backend attached.
  The host waits for nothing before that submit. Planted: the same copy with no wait, after the CPU
  wrote the sentinel, must see the sentinel.
- Expected: 8 imports, 992 hits, 0 copies. Allocations per import after the first eight: about 3. The
  copy done after the fence signals: near the reference's 0.2 ms wait (E14), plus a 1 MiB copy. The
  planted copy sees the sentinel in all 50.
- Pass: imports 8, hits 992, copies 0; the median allocations per import after the first eight at most
  50; the producer still writing at the consumer's submit in at least 45 of 50; the copy correct in 50
  of 50; the planted copy with no wait sees the sentinel in at least 45 of 50; both steps exit 0.
- Fails it: `dma-steady: skipped:` or `dma-wait: skipped:` on the board (for the wait, also a device
  that can't import a sync fd: `VK_KHR_external_semaphore_fd`); more imports than buffers; more than 50
  allocations; a copy that read stale bytes; a planted copy that saw the GPU's values.
- Dropped: the producer-frame tests (tiled, UIF or SAND, XRGB, NV12). Daedalus contains no camera, image
  or frame-layout code, by the user's ruling.

## E6: element chains, merged and unmerged

- Command (`figures/e6.sh`): `bin/daedalus-gpu-budgets elements`, three rounds. The `e6` lines give
  each form's median of 50 after 5, with the submits and passes of one run.
- Expected, from E6 2026-11-17 (medians of three rounds, 1280x800):
  - fragment merged: 4 nodes 1.339 ms, 8 nodes 1.345 ms;
  - compute merged: 7.304 and 7.392 ms;
  - compute unmerged (one region per node): 28.283 and 56.485 ms;
  - fragment unmerged (one pass per node): no earlier figure. About one fragment pass and one wait per
    node: the single tap pass was 0.836 ms (E7, 2026-11-16).
- Pass, for 4 and 8 nodes, in each of three rounds: the merged fragment pass is 1 submit and 1 pass;
  merged takes no more submits and passes than unmerged, in the fragment and the compute form; the
  median of the three rounds is no higher merged than unmerged, in both forms.
- Fails it: a merged fragment of more than one submit or pass; merged above unmerged; fewer than three
  rounds.
- Dropped: the box 3x3 comparison. The pixel vocabulary is out of Daedalus by the user's ruling.

## Ticks (D2 and I2): the CPU tick, before E11

The 2026-11-11 figures predate the landed work since, the zero-allocation socket path among it, so
D2 and I2 close on rows from this run.

- Commands (`figures/ticks.sh`), three rounds, 10,000,000 ticks each (`BATCH_TICKS`):
  `bin/daedalus-budget-harness chain <ticks> 16`, `chain <ticks> 16 unfused`, `chain <ticks> 1` and
  `idle <ticks> 16`. Each prints its wall time in nanoseconds a tick (`Instant` around the loop).
  Then `bin/ticks-bench --bench --noplot` filtered to `idle_tick_16`, `fused_chain_16_tick`,
  `unfused_chain_16_tick` and `detector_frame`, with `CRITERION_HOME` under the bundle.
- Expected, from 2026-11-11 at 6245c925: a tick of 1,115 ns merged, 1,960 ns unmerged, 395 ns for one
  stage and 35 ns idle (wall clock); criterion 34.9 ns, 1.112 µs, 1.921 µs and 1.225 µs.
- Pass, on the median of the three rounds and on criterion's medians:
  - per stage with the 1-stage tick subtracted ((16-stage − 1-stage) ÷ 15), merged is below unmerged;
  - criterion's fused tick is below its unfused tick;
  - neither a tick ÷ 16 is worse than the reference's 498 ns merged and 655 ns unmerged, in the wall
    runs and in criterion's;
  - `detector_frame` is at most the reference's 7 µs.
- Reported, not judged: a figure more than 1.25 times its 2026-11-11 value is listed, and D2 and I2 give
  the reason with it, as D2 asks of a number that gets worse.
- Fails it: fewer than three rounds; a criterion median missing; merged not below unmerged; a
  per-stage figure worse than the reference's; a detector frame above 7 µs.
- D2 and I2 close on this run's rows. The PMU counts from `counters` (2026-11-16 and 11-17) stand in
  for callgrind, which is not on the board; they are aarch64 counts and not compared with dev's x86
  callgrind 27,064. The RX 6800 XT row is dropped, not pending: by rule no gate or bench opens the
  desktop GPU.

## E11: device loss by the driver, last

- Command (`figures/e11.sh`), only with the watchdog armed:
  `env DAEDALUS_GRIND_ROUNDS=<n> DAEDALUS_GRIND_REPEAT=1 bin/daedalus-gpu-budgets grind`, for n =
  1,000,000, 10,000,000 and 100,000,000, then n = 4,000,000,000 up to six times, until the kernel log
  has three more `Resetting GPU` lines than at the start, or a wait fails, or the lost callback fires.
  Every run ends with `grind: summary: wait=... lost_callback=... known_run=... same_device=...
  reopen_s=...`.
- Expected, from E11 2026-11-16 and 2026-11-17: every wait finishes in about 0.52 s, inside the 5 s
  bound, and the kernel resets V3D about every 0.54 s. Whether the lost callback fires was never
  recorded. So (b) is the likely outcome.
- Pass, one of two:
  - (a) the lost callback fires, the backend reopens the device, and a known run on it is correct. The
    reopen time is recorded.
  - (b) after three or more kernel resets, no loss is reported, and the known run on the same device is
    correct in every grind run. E11 then closes as: V3D recovers a hung job in the kernel; the device
    survives, so driver-caused loss is not reachable by a hang on this hardware.
- Fails it: the callback fired and no reopened device ran the known run correctly; no loss and a known
  run wrong or on a changed device; fewer than three resets; a failed wait with no loss reported; E11
  not run on the board (no armed watchdog, or a kernel log it can't read).

## What the board needs

- Image: the batch runs on 2026.2.0-dev.993+e1097ec7 (kernel 7.2.9-v8-16k), the board's default slot,
  so its figures compare with those of 2026-11-10 to 2026-11-17. It does not run on a trial slot. For
  reference, the features it needs from any image:
- Image and kernel features: as on 993, or another
  with the same features: the v3d DRM driver, the dma-heap system heap, dma-buf sync_file export (Linux
  6.0 or later, `DMA_BUF_IOCTL_EXPORT_SYNC_FILE`), and a readable kernel log (`dmesg`) for E11.
- dma-heap access: `/dev/dma_heap/system` is `root:video`, mode 0660. The run's user is root or in
  `video`. The earlier batches ran as root.
- The v3dv Vulkan driver (Mesa's Broadcom ICD; V3D 7.1.10.2, Vulkan 1.3.354, driver 26.1.8 on
  2026-11-10), with `VK_EXT_external_memory_dma_buf` and `VK_KHR_external_fence_fd` (both recorded in
  E13, 2026-11-17), and `VK_KHR_external_semaphore_fd` for E14's wait (not recorded before; the probe
  lists it).
- For E11: the systemd runtime watchdog armed on the hardware watchdog (`RuntimeWatchdogUSec` not 0;
  it was 15 s on 2026-11-16), so a wedged kernel returns the board by itself.
- The aarch64 Khronos validation layer, as `VVL_DIR` on the host: a directory with
  `libVkLayer_khronos_validation.so` and `VkLayer_khronos_validation.json` (its `library_path` relative
  to the json). It is not in this repo, and the earlier batches did not record where theirs came from.
  This batch's layer, built 2026-11-20 and kept outside the repo:
  - Source: Khronos Vulkan-ValidationLayers, tag `v1.4.354`, commit
    `ccaf4c149d8730338012614a5be8080d0b92e95f`, with the dependencies its `scripts/known_good.json`
    pins (Vulkan-Headers and Vulkan-Utility-Libraries `v1.4.354`, SPIRV-Headers `1e770e7d`,
    SPIRV-Tools `902e360d`). The tag matches the header version the board's driver reports
    (Vulkan 1.3.354).
  - Build: cross-built on the host with `zig c++ -target aarch64-linux-gnu.2.36` (the bundle's glibc),
    Release, tests off, no window-system support (xcb, xlib and wayland off), linked with
    `-z nodelete`, stripped. The highest glibc symbol it needs is GLIBC_2.34. Without `-z nodelete`
    (the 2026-11-20 batch) a destructor of zig's static libc++ ran after the loader unloaded the
    layer, and every process that made a device exited 139 (E13, 2026-11-20); the same build for
    x86_64 shows it on lavapipe and passes with the flag. Its json's `library_path` is `./libVkLayer_khronos_validation.so`.
  - Path: `VVL_DIR=/mnt/nvme-wd/scratch/vgl/vvl/aarch64` (the build script is
    `/mnt/nvme-wd/scratch/vgl/vvl/tools/build.sh`).
  - sha256: `libVkLayer_khronos_validation.so`
    `4833dd1ba02bdff6489f692fe3079f37a0f0cec1ca0e5824b8273bd13a307a8c`;
    `VkLayer_khronos_validation.json`
    `0d09368222e25d301a501410bd49e865e93ab12c7f3f961859cd16fd10981e98`.
- The locks: `go` holds both `/run/lock/raze-gpu.lock` and `/run/lock/board-gpu.lock` for the whole run
  (`BOARD_LOCKS`, both by default).
- Other software on the board: only that it holds neither GPU lock during the run. The bundle depends
  on nothing installed on the board but the kernel, the Vulkan loader and driver, and busybox.
