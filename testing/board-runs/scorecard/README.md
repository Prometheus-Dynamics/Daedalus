# The scorecard: `next` against the Daedalus the dev image ships

The dev image pins Daedalus on the `dev` branch at `02e2b3b229e7f38316b40fe9830e53eae56dfa41`
(the image's own lock names it). A scorecard is the same test on the same board on the same
day for both, or it says that a row has no counterpart.

## Setting it up

Check the dev revision out beside this repository's directory, as `dd-dev`:

```
git worktree add --detach ../dd-dev 02e2b3b229e7f38316b40fe9830e53eae56dfa41
```

`scripts/board/run.sh cm5-measure` then builds `testing/board-runs/scorecard/dev-measure` against it and
puts it in the bundle, and the board run writes `dev.txt` beside its own figures. Without the
checkout the run measures `next` alone.

## What is like for like

`dev-measure` builds the same graphs as `cpu-timings` and prints the same words:

| Row | `next` | dev | Like for like |
|---|---|---|---|
| A host round trip: feed, tick, take | `host_round_trip` | `host_round_trip` | Yes. One node between the host's ports on both. |
| A tick of a sixteen-step chain, counters off | `probe_off` | `probe_off` (metrics off) | Yes. |
| The same with counters on | `probe_every_1` | `probe_every_1` (metrics at their default) | Yes, as each one's own counters. They don't count the same things. |
| Allocations in a thousand round trips | the gate's counting tests: none | `allocations` | Yes. |
| Time from `main` to the first step, and memory held | `startup` | `startup` | Yes. |
| The size of a host linked into an application | `examples/02_host` | `dev-measure` itself | Near enough: both are one small graph and its host, static. |
| The daemon: memory, start | `daemon.txt` | none | No. The dev image embeds Daedalus in its own engine and runs no daemon. |
| A worker's hand-over | `worker.txt` | none measured | No. The dev worker path is another protocol, and no equal test of it exists. |

On dev a value between two nodes is a payload with its own allocation, where `next` carries a
small value inside its cell. That is part of what the rows measure, not a difference in the
test.

## Results

A board run's figures go in this folder, with the image and the kernel named.

- [2026-11-14, CM5](2026-11-14-cm5.md): `next` is 3.8 to 5.3 times faster on every like-for-like
  row, allocates nothing in steady state where dev allocates on every round trip, and holds less
  memory.
