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
| `daedalusd`, the daemon, with the planner (the default) | 2 429 896 | not re-measured | not re-measured |
| `daedalus-ctl`, its command line | 801 352 | 822 664 | 811 624 |
| a host with one small graph (`examples/02_host`) | 1 449 376 | 1 482 912 | 1 440 816 |

The daemon, aarch64 musl release, `stat -c %s` for the file and `llvm-size` for `.text`, before
this change (`base`) and after it, with the planner and without it (`--no-default-features`, which
loads only made plans; see the knob below):

| Daemon, aarch64 musl release | Bytes | `.text` |
|---|---|---|
| Before (`base`, with the planner) | 2 247 864 | 2 202 816 |
| After, with the planner (the default) | 2 429 896 | 2 384 196 |
| After, without the planner (lean) | 1 318 720 | 1 285 964 |

With the planner, the daemon grows by 182 032 bytes over `base`. That growth is the feature, not copies: the made-plan format's encode,
decode, bind and recipe code is about 49 KB; the listener's `put_plan`, bound and planning parts are
about 30 KB; the planner's `made` and `facts` about 11 KB; the wire codec about 6 KB. The four
together are about 96 KB, and the rest of the growth is spread over the host and the planner's
changed code, not counted separately. Nothing is compiled twice. The planner-less daemon is 929 144 bytes
smaller than `base`, because it no longer carries the planner at all. The planner-less figure is
the one a board needs.

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
| No planner | A daemon or host built without the planner (`--no-default-features`, the host with `--features listen`) loads a made plan that `put_plan` stores or `plan_dir` names. It links no planner, and the size is in the table above. | The graph can't be changed on the board: a made plan is made on another machine with `make-plan`. |
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
stderr and two packet buffers, each the size of the largest value it has carried. On Linux it
also maps two shared buffers for that worker, 1 MiB each to start, which grow to the next power of
two of the largest packet it has carried in them. A worker that takes values by reference
gets four more of the same size for its output values, and the host keeps at most 16 of its dma-buf
descriptors open for it.

## What was removed

A pass over every public item for one that nothing uses found two accessors, and a pass over
every manifest found three dependencies nothing named. They are gone. The rewrite left no
retired type, feature or code path behind beyond those.

Two costs on the worker path were removed with them: a packet's buffer was zeroed in full before
every read, and a value too long for a cell was given new memory every time it came back from a
worker. A megabyte handed to a worker and back now allocates nothing on the host's thread, and
`a_steady_run_of_megabyte_values_allocates_nothing_on_the_host_thread` holds that.

## Integrity checks

A host that checks its plugins against a lock hashes each file when the file loads or a worker
starts. Nothing is cached, so an unchanged file is hashed again at each start. The cost has not
been measured on a board, and a cache would be one more thing to trust. Nothing hashes on a run's
tick.

## Decided

Ideas that were listed here with no measurement behind them, each now done or dropped:

- **Dropped:** a larger pipe to a worker (`F_SETPIPE_SZ`) for values of a megabyte and more. A
  value that large no longer goes through the pipe: it goes by reference, through shared memory,
  and a CPU node's output is written into the worker's lent memory
  ([out-view.md](../testing/board-runs/out-view.md)). What is left on the pipe is small values.
- **Dropped:** decoding a worker's long output straight into the memory it is handed on in. The
  same: a long output is handed on as the place it is at, and goes back into a worker with no copy.
- **Done:** smaller stacks for the daemon's connection threads. See Thread stacks, below.
- **Done:** a host and a daemon that leave the planner out, for a board that loads a plan made
  elsewhere. The binary sizes saved are in the table above.
- **Done:** a query for the offset alignment a device needs to import a dma-buf in place,
  `dmabuf_offset_alignment`, beside `dmabuf_status`. `dmabuf_offset_status` says, for one offset,
  when a value is copied for being misaligned, and names the offset and the alignment. See
  [gpu-nodes.md](gpu-nodes.md).

## Thread stacks

A thread Rust starts reserves 2 MiB of address space for its stack, and only the pages it touches
are resident. The threads that move bytes and call nothing deep now ask for less:

| Thread | Stack |
|---|---|
| a session's writer, and the acceptor | 64 KiB |
| a worker's stdout reader and its stderr drain | 64 KiB |
| a client's reader | 128 KiB |

Each passes every host, daemon, ctl, client and process test with a quarter of that, in a debug
build, which uses more stack than a release one. A session's own thread, the daemon's stdio
session and its plugin loader keep the default: a run's ticks run on the session that steps it,
so node code runs there, and a plugin's registration runs on the loader. The runner's lanes and a
stream's worker run node code too, and keep it as well.

Measured on the development machine (x86-64, 32 cores, release build) with `scripts/board/run.sh
local-measure`, which also records the lean build. The board's figures come from a board run, in
`testing/board-runs/`. The daemon is idle, then has a graph planned (or a made plan loaded, for the
lean build), then has run one step, then 200 round trips, and then 32 connections open:

| Daemon, x86-64 release | Full (with the planner) | Lean (made plans only) |
|---|---|---|
| Idle: address space, resident | 266 MiB, 3.7 MiB | 199 MiB, 3.7 MiB |
| Graph planned, or plan loaded: resident | 4.8 MiB | 3.7 MiB |
| After the first step: resident | 4.9 MiB | 3.8 MiB |
| After 200 round trips: resident (peak) | 5.0 MiB | 3.9 MiB |
| 32 connections: address space, resident | 4.13 GiB, 6.2 MiB | 4.13 GiB, 5.1 MiB |
| Threads, 0 and 32 connections | 2 and 66 | 2 and 66 |
| Time from start to the first answer | 29.7 ms | 21.4 ms |

The lean build's resident memory is the same at idle and about 1.1 MiB lower once a plan is loaded.
On a CM5 the full daemon peaks at 4.1 MiB resident and the lean daemon at 3.1 MiB
([lean-daemon.md](../testing/board-runs/lean-daemon.md)).
The address space at 32 connections is glibc's arenas, which `MALLOC_ARENA_MAX=2` (below) reduces.

Measured on the desktop (x86-64, 32 cores), release build, the daemon idle and then with 32
connections held open by `daedalus-ctl watch all`. `scripts/board/measure-on-board.sh` takes the
same figures on a board.

| | Idle: address space, resident | 32 connections: address space, resident |
|---|---|---|
| Before | 203 MiB, 3.3 MiB | 4,296 MiB, 4.6 MiB |
| Smaller stacks | 199 MiB, 3.1 MiB | 4,232 MiB, 4.4 MiB |
| Smaller stacks and `MALLOC_ARENA_MAX=2` | | 137 MiB, 4.3 MiB |

The stacks save 2 MiB of address space a connection and little resident memory, as expected. The
rest of the address space was glibc's: each thread that allocates gets an arena of its own, up to
eight a core, with 64 MiB reserved for each. The packaged service sets `MALLOC_ARENA_MAX=2`
(`service/daedalusd/packaging/daedalusd.service`), which matters on a board with little memory
and strict overcommit, or a 32-bit one. A run's steady state allocates nothing, so two arenas
aren't contended.
