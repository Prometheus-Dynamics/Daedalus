# On small hardware

What Daedalus costs on a board with little flash and little memory: how large it is, what the
size is made of, what a running graph holds, and which knobs change each. Lean is the default,
not a mode: what costs nothing to remove is removed. What is left here is a real trade, with
its number.

Figures here are from the development machine's cross builds, for size, and from the source,
for memory. Times and resident memory on a board are in `testing/board-runs/`, and only a
board's are results.

## How large it is

Static binaries (musl), in bytes, at `next` with the workspace's release profile:

| Binary | aarch64 | armv7 | riscv64 |
|---|---|---|---|
| `daedalusd`, the daemon | 2 238 072 | 2 290 936 | 2 203 720 |
| `daedalus-ctl`, its command line | 801 352 | 822 664 | 811 624 |
| a host with one small graph (`examples/02_host`) | 1 449 376 | 1 482 912 | 1 440 816 |

The release profile builds one unit and optimises across crates (`lto = "fat"`,
`codegen-units = 1`). That is about a fifth smaller than the compiler's default (the daemon was
2 772 144 bytes on aarch64). It costs build time. Whether it changes a step's time is for the
board run to say.

What the daemon's code is made of, by crate (x86-64, the same proportions elsewhere):

| Part | Share of the code |
|---|---|
| The planner | 29 % |
| The standard library | 22 % |
| Graph documents and their JSON | 18 % |
| The host | 12 % |
| The daemon's config (TOML) | 5 % |
| The wire protocols, the loader, the registry, the runner | 2 % each or less |

The runner, which is what runs a graph, is under 2 % of it: 41 KiB.

## Knobs for size

| Knob | What it saves | What it costs |
|---|---|---|
| `opt-level = "s"` in the release profile | With the next, the daemon goes to 1 589 504 bytes on aarch64 (−29 % again). | Slower steps. Measure on the board before choosing it. |
| `panic = "abort"` | Part of the figure above. | A node that panics takes the process down, where it is otherwise a fault of that node and the graph runs on. Not for a host that runs nodes it didn't write. |
| Plan somewhere else | The planner and the graph documents are 47 % of the daemon's code. A board that only ever runs one graph can carry its plan as a blob made at build time (`backends/cpu/src/blob`), as a chip does, and link neither. | The graph can't be changed on the board. |
| No daemon | A host linked into the application has no protocol, no TOML and no listener: the small host above is 0.8 MB smaller than the daemon. | No remote control of the graph. |

## What a running graph holds

Everything a run needs is sized when its plan loads, and nothing is allocated while it runs.
In bytes, on a 64-bit board:

| For each | Bytes | Knob |
|---|---|---|
| Step: its record, health, fault, progress | 44 + 24 + 160 + 8 | none: a step has these |
| Step, with the probe on: its time and bytes | 144 | `LoadOptions::overhead_window`, 0 (off) unless set |
| Step that writes a fault's detail | up to 16 384, when it first writes one | `LoadOptions::fault_detail_bytes`, 0 keeps none |
| Edge: a queue | 64, and 48 for each value it can hold, plus 16 where the value sits in the cell | `LoadOptions::fifo_capacity`, 64 values unless the edge says |
| Run: the ring of failure reports | 200 each | `LoadOptions::fault_reports`, 32 |

So an edge with the default capacity is about 4 KiB, and a graph of 50 steps and 60 edges holds
about 260 KiB before its values. An edge that declares a capacity of one is 128 bytes instead of
4 KiB.

The daemon also holds, for the whole process: the last 1024 requests served (`TRACE_RING`), 64
events for each watch that isn't being read (`WATCH_QUEUE`), and one thread for each client
connection, of at most 64 (`ListenOptions`). A worker process costs the host 64 KiB of its last
stderr and two packet buffers, each the size of the largest value it has carried.

## What was removed

A pass over every public item for one that nothing uses found two accessors, and a pass over
every manifest found three dependencies nothing named. They are gone. The rewrite left no
retired type, feature or code path behind beyond those.

Two costs on the worker path were removed with them: a packet's buffer was zeroed in full before
every read, and a value too long for a cell was given new memory every time it came back from a
worker. A megabyte handed to a worker and back now allocates nothing on the host's thread, and
`a_steady_run_of_megabyte_values_allocates_nothing_on_the_host_thread` holds that.

## Parked

Ideas with no measurement behind them yet. None is being worked on.

- A larger pipe to a worker (`F_SETPIPE_SZ`), for values of a megabyte and more: fewer
  switches between the two processes, for up to a megabyte of kernel memory a pipe.
- Decoding a worker's long output straight into the memory it is handed on in, which would
  take one more copy out of a megabyte's way.
- Smaller stacks for the daemon's connection threads. It would lower address space, not
  resident memory.
- A host feature that leaves the planner out for a board that loads a plan made elsewhere.
