# Board run: GPU results read in place (#198)

Status: run on CM5 2026-11-11.

## Check

`daedalus-gpu-budgets transfers`, release, on the CM5. For each size, the line `placed in dma-heap, after a device write` reports four figures, each the median of the section's runs after its warm-up:
- **in place:** the host taking a shader's output in place, as `MappedBytes`. This is the sync after the device's write, then the view.
- **in place and read once:** the same, plus one pass over every byte.
- **copied out and read once:** the old route, `read_into` a `Vec`, then the same pass.
- **shader write:** the copy shader's own write into device memory and into dma-heap memory, waited on.

The figures to compare against are on the line `step readback device to dma-heap ... (with the host read)`, the two-copy route this replaces, and the old branch's 3.92 ms for a 16 MiB read.

## Result

Run on 2026-11-11 on the CM5 (raze2): V3D 7.1 through the Broadcom ICD, aarch64, 4 cores. Tested tree: `pr/read-in-place` on `next` at a936d4d3, which is the code merged as #198 (7224453b). The binary was cross-built in release on NVMe and run as `flock /run/lock/raze-gpu.lock ./daedalus-gpu-budgets transfers`. The board directory was removed after.

Shader write into each memory, second run:

| Size | Device memory | dma-heap |
|---|---|---|
| 4 KiB | 0.084 ms | 0.085 ms |
| 256 KiB | 0.200 ms | 0.224 ms |
| 4 MiB | 2.191 ms | 2.182 ms |
| 16 MiB | 8.464 ms | 8.467 ms |

Getting the result to the host after the write. The first figure in each cell is the first run, the second is the second run.

| Size | In place | In place and read once | Copied out and read once | Old step: readback to dma-heap, with the host read |
|---|---|---|---|---|
| 4 KiB | 0.002 / 0.002 ms | 0.003 / 0.003 ms | 0.003 / 0.003 ms | 0.044 ms |
| 256 KiB | 0.009 / 0.008 ms | 0.036 / 0.046 ms | 0.045 / 0.043 ms | 0.203 ms |
| 4 MiB | 0.130 / 0.137 ms | 0.556 / 0.575 ms | 1.390 / 1.467 ms | 2.077 ms |
| 16 MiB | 0.608 / 0.624 ms | 2.123 / 2.176 ms | 5.190 / 5.615 ms | 7.960 ms |

The same first run measured the routes through host-mapped memory for 16 MiB: the step readback to mapped, with the host read, 15.993 ms; a direct read of host-mapped memory, 12.273 ms.

In short, a 16 MiB result reaches the host in about 0.6 ms in place, against 7.96 ms on the two-copy route and 3.92 ms on the old branch. The shader writes either memory at the same speed.
