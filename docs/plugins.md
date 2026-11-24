# Plugins

A plugin is a dynamic library that carries nodes, backends, or both. A host loads it at run
time through `daedalus-loader`. Nothing in the host is rebuilt.

## What a plugin exports

Two symbols. `daedalus_plugin_shapes` describes the layout the plugin was built against, and
`daedalus_plugin_describe` gives the host a table of node declarations and a table of backends.
The export macro writes both:

```rust
use daedalus_loader::export::{NodeDef, PluginDef, PortDef};

static NODES: [NodeDef; 1] = [NodeDef {
    id: "acme.pass",
    source: "acme-plugin",
    backend: "acme.cpu",
    text: "pass",
    inputs: &[PortDef { name: "in", key: "u32", optional: false }],
    outputs: &[PortDef { name: "out", key: "u32", optional: false }],
    run: pass,
}];

daedalus_loader::export_plugin!(PluginDef {
    name: "acme-plugin",
    backends: &["acme.cpu"],
    nodes: &NODES,
    made: &[],
    codecs: &[],
    produce: None,
});
```

`run` is a function of the step's cells. [`testing/fixtures/dylib`](../testing/fixtures/dylib)
is a complete plugin.

## A crate of `#[node]` functions as a plugin

A crate whose nodes are written with `#[node]` ships as a plugin without rewriting them. It is
built as a `cdylib`, depends on `daedalus-cpu-plugin`, and adds one line:

```rust
daedalus_cpu_plugin::export_nodes!("acme-nodes", NODES);
```

`NODES` is the array of node records the crate registers when it is linked into a host. The
plugin carries the CPU backend with it, as a backend of its own under the plugin's name, and
builds each node's step itself. Nothing about the compiler that built the host has to match:
the check at load is the one every plugin gets. `testing/crates/node-plugin` is such a crate,
and its tests load it beside the same nodes linked in.

What such a node gets, and what it does not yet:

- The same outputs as linked, its state between ticks, state kept across a graph reload, a
  fault with its reason and code, and a restart.
- A port on an edge that crosses the plugin's boundary is read and written in its type's
  crossing form: a number or other plain data of 16 bytes or less in place, bytes by handle,
  anything else as its wire form. So a type on such a port needs `#[derive(WireForm)]`, and a
  node with one of a type that has none is refused in words: ``output `kept` of np.keep is read
  by a node outside this plugin: the edge crosses the plugin's boundary, but Kept has no
  crossing form``.
- An edge between two nodes of the same plugin, whose value nothing outside the plugin reads,
  carries the Rust value as it is: both ends are the plugin's own code. The host hands the
  plugin these edges with the view it compiles. The value goes in a box of the plugin's own,
  handed over as a record the host counts and releases through the plugin, like any bytes a
  plugin hands over, so the library stays loaded while a cell holds one, and the box goes back
  to the output for its next value once released. A handle (`Arc<T>`) and a list an `OutView`
  wrote go in the box too, so the host never runs the plugin's code on a cell itself. A reader
  borrows the value where it is, and the last one moves it out. A reader takes a value only from a record
  its own build made and only of its own type; any other fails the step with words (`an input
  held a value of another form`), and is never read as its type. A plugin run in a worker
  process is handed no inner edges, and reads and writes every port in its crossing form.
- A port's type needs a key of its own (`#[type_key("...")]`). A type compared by what it is made
  of, a list or a tuple, has no name an entry can carry.
- A constant the graph gives an input reaches the node: the host hands the plugin each
  constant of the region's nodes with the view it compiles, in the form a stored plan keeps it,
  and the node's step decodes it as a linked step does. A config does not reach it yet: the
  entry does not carry a config's fields, so the planner refuses a config for such a node.
  A constant changed while the graph runs (`patch_const`) is not offered to the plugin yet, and
  is refused as one no step reads.
- A plugin that cannot build a node's step says why, and the words reach the host's refusal:
  `backend node-plugin refused: np.sum takes a fan-in, which a plugin's node cannot yet`.
- The entry carries a node's id and its ports' names, type keys and whether each may stay
  unwired. Labels, defaults, config fields and metrics are not declared, so a host lists no
  metrics for such a node and the node's reports go nowhere.
- No fan-in. No merging with other nodes, of the host or of the plugin: each node is a step.
  No blob or source form.
- Cost, in instructions a tick (`testing/crates/node-plugin/examples/measure.rs`): one node
  1,733 linked and 1,887 loaded. Two nodes with a small struct between them 2,700 linked and
  2,920 loaded, where the struct's wire form is written in its cell. Two nodes with a list in a
  struct that has no crossing form between them (`measure kept`) 2,930 linked and 3,260 loaded,
  where the struct goes in a box the plugin reuses. Two nodes with a megabyte of `Vec<u8>`
  between them 60,370 linked and 60,730 loaded. A megabyte made by a linked node and read by a
  plugin's node costs 176,600 (`measure handed`): it is written in its wire form and read back
  as one block.
- A value inside the plugin whose wire form fits in its cell (16 bytes or less) is written
  there, as at the boundary, since that is less work than a box.

## The check at load

A host update must not strand the plugins built before it. So the first thing a host does with
a plugin is read how the plugin lays out each shape that crosses the boundary, and check that
against its own, a shape at a time. There are no version numbers to compare.

A shape is one of two kinds (`core/abi/src/layout.rs` lists them):

- **Frozen**: passed by value, or in an array the other side walks with its own stride. `Slot`,
  `Slots`, `Step`, `Fence`, `Text`, `PortC`, `BoundaryPort`, `ConstC`, `UnitOut`, `BytesC`,
  `HandedRecord`, `DetailSink`, `Status`, `Handle`, the foreign-value handles, and the
  descriptors themselves. A frozen shape has to be the same on both sides.
- **Growable**: a single value passed by pointer, or a table read through the size its maker
  declares. `StepCx`, `PluginEntry`, `RegionView`, and the plugin's `NodeC`, `BackendTable` and
  `CodecC` tables. A growable shape gains fields only at its end, and a field added later
  means "absent" when it is zero.

A plugin fits when every frozen shape is the same and what it has of every growable shape is
exactly the start of what the host has: the same fields, by name, offset and size. That is
computed, not promised. A plugin built before a field was added is such a start, so it loads: it
never touches the field, the host reads its tables at the plugin's own stride, and what the
plugin doesn't have reads as absent. Adding a field to the end of a growable shape is therefore
not a breaking change, and nothing is renumbered when one is added.

What is refused, each with the shape named:

- A plugin built against a newer Daedalus than the host: `plugin.so: this plugin needs a newer
  host: StepCx has 11 fields here, 13 there`. It could reach for what the host doesn't have.
- A plugin for which a frozen shape, or the shared start of a growable one, is laid out
  differently: `this plugin was built for a different Daedalus: Slot is 48 bytes here, 56
  there`.
- A plugin from before plugins described their shapes, which has only the old entry symbol:
  it is told to be built again, once.

A test pins every frozen shape and the required start of every growable one
(`core/abi/tests/abi.rs`), so a change that would break plugins already built is a visible edit
to that table, with the shape's name on the line.

## What crosses the boundary

- Only `repr(C)` data. A plugin's strings are copied into host storage at load.
- A node's inputs and outputs are cells. Plain data of 16 bytes or less is in place, and anything
  larger is bytes behind a shared handle. A plugin never hands the host memory to free.
- A plugin's node can hand bytes to the host without a copy. See "Handing bytes over" below.
- A panic in a plugin's node does not cross. It is a fault of that step: the tick goes on and the
  host keeps running.

## Handing an input on

A node that passes a value it was given on as it is, a run of bytes for example, must not copy
the input cell into the output cell: both would then own the one reference, and it would be
released twice. It marks the output instead, with `daedalus_abi::forward(output, input)`: an empty
cell with `reserved` set to `FORWARD_PENDING` (4) and `len` the input's index. When the step
returns, the runner hands that input's value on to the output. It moves the reference when the
node is the input's only reader, and takes another one when the value is still another reader's,
which only the runner knows. Nothing is copied. A host that predates the mark sees an empty output.
The fixture's `fixture.pass_bytes` does this, and `core/loader/tests/forward.rs` checks both cases.

## Handing bytes over

A node hands bytes over by marking one of its output cells and describing the bytes in
`StepCx::handover`, which has one `BytesC` for each of the first two outputs:

- Set `handover[k]` to the bytes: `ptr` and `len` are valid, and stay unchanged, until `release` is
  called with `data`. `release` is the plugin's own function.
- Set output cell `k` to `state` full, `owner` to `OWNER_HANDED` (6), and `reserved` to
  `HANDOVER_PENDING` (1). The cell's `data` and `len` are the bytes, and `cap` is not read.

When the step returns, the loader turns the cell into a counted record that the host holds. A
record comes from a pool made when the backend loaded. When the pool has none to spare, the host
copies the bytes, calls the plugin's `release` straight away, and counts the copy. Either way the
host sees the bytes at the same address the plugin gave, and reads them without a copy.

**The release may be called from any thread.** The host calls it once, on whichever thread drops
the last count on the bytes: a graph's step, the host, or a backend's thread. A plugin's release
must therefore be safe to call from any thread, and must not depend on the step that made the
bytes still running.

The bytes keep the plugin's library open. A count on a record holds the plugin's generation, so a
graph reload or a plugin reload to a new generation leaves a held value readable, and the library
closes after the last count is gone.

A C or C++ plugin in the same process hands bytes over the same way, with the same `repr(C)`
layouts. A C or C++ worker in a separate process does not: its outputs are copied over the wire.

## Loading and loading again

```rust
let mut loader = Loader::new();
let plugin = loader.open(path)?;
plugin.join(&mut registry)?;        // its nodes join the registry
```

- A node with a known id must have the same declaration. A plugin that brings another body for a
  known node joins it.
- Loading a plugin's name again is a new generation. The old library stays open until the last
  step compiled from it is released, and is then closed.
- A load that fails is a typed error. `load_with_retry` tries again with a delay that doubles
  from 250 ms and never exceeds 8 s, and stops only when it is told to.

## In the daemon

`daedalusd` loads every library in its plugin directory at start, and `load_plugin` loads one
on request. A library that fails to load is tried again in the background for as long as its
file is there, and never stops the daemon. Every backend the daemon has comes from a plugin,
except the process backend of a daemon built with the `workers` feature, which runs the worker
programs its config names (see [ffi.md](ffi.md#workers-in-the-daemon)).
A reload of a run after its plugin was loaded again moves the run onto the new generation, and
its nodes start fresh.

## Isolating a plugin

A plugin's node runs in the host's process. To keep a crash out of the host, run the plugin in
a worker: `daedalus-plugin-worker <library>` serves the plugin's nodes over the worker protocol,
and the process backend supervises it like any other worker. See [ffi.md](ffi.md). With
`--lock FILE`, the worker checks the library against the lock first, as the daemon does. See
[Integrity and the lock](#integrity-and-the-lock).

## A backend in a plugin

A plugin's backend takes regions and returns one step a node. It produces steps only: the blob
and source forms need a backend that is linked in.

## Beside nodes linked into the host

A plugin's nodes and nodes linked into the host run in one graph. A host that only loads a file
needs nothing else: `PluginBackend` lists the two moves, and the planner places them.

- A plugin's values live in the space `plugin.host`. Every plugin on `PluginBackend` shares it,
  so a value passes from one plugin's node to another's with no move.
- A linked CPU node's values live in `cpu.host`, as Rust values in the host's memory. Such a
  value never reaches a plugin: there is no move between `cpu.host` and `plugin.host`.
- What crosses is a value in its crossing form, in the CPU backend's space `cpu.wire`. A linked
  node whose output a plugin reads writes that form, and a linked node that reads a plugin's
  output reads it. The moves `plugin.from_cpu_wire` and `plugin.to_cpu_wire` hand the cell on as
  it is, and the plan's explanation names them on the edge.
  - A number or other plain data of 16 bytes or less crosses in place.
  - Bytes cross by handle. A megabyte a plugin hands over is read by a linked node at the
    address the plugin has it at.
  - Anything else crosses as its type's wire form: the linked node writes it once.
- A plugin's own plain-data type reaches other nodes through its codec. The codec's four
  conversions join the registry, the planner puts one in where the types call for it, and the
  host runs it with the codec's own function over the value's bytes. A result of 16 bytes or
  less is written in place and nothing is allocated for it; a longer one is one allocation,
  held behind a count.
- A type with no crossing form is refused when the graph is planned, by the node, the port and
  the type: ``output `y` of t.seal is read outside this program (by a worker or a plugin), but
  Kept has no crossing form: add #[derive(WireForm)] to Kept``.

What does not work yet:

- A plugin whose backend the host wraps to give it memory of its own (a device) beside a plugin
  on `PluginBackend`: nothing moves a value between `plugin.host` and the wrapper's space, and
  the graph is refused, naming the edge and both spaces.

`testing/crates/fake-npu-plugin/tests/mixed.rs` runs each of these, with two plugins.

## Integrity and the lock

A host can approve the files it loads. The approval is a file named `daedalus.lock`. It lists the
SHA-256 digest of each approved file:

```text
# daedalus.lock 1
2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae  libfoo.so
9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08  /opt/daedalus/bin/worker
```

The header line is required. Each entry is 64 lowercase hex digits, two spaces, and a key. Entries
are sorted by key, and a key is listed once. The format is the one `sha256sum` writes, so
`sha256sum -c daedalus.lock` checks the files from the lock's directory.

The key is the file's name as the file is named, not resolved through symbolic links: `libfoo.so`
matches any path whose file name is `libfoo.so`. A key with a `/` is an absolute path and matches
that path exactly, so two different files with one name can be told apart. The digest decides what
is approved. The key only picks the digest to compare with. A lock made on a build machine is valid
under any install prefix.

The mode is the daemon's `integrity` key, and the worker's `--lock` and `--record` flags:

| Mode | What it does |
|---|---|
| `off` | Checks nothing. The default. |
| `locked` | Loads a file only if the lock approves its digest. The file is opened once, hashed through that open file, and loaded from it, so the bytes checked are the bytes loaded. |
| `record` | Loads files as `off` does, and writes each one that loads into the lock. A file whose entry is already in the lock under another digest is refused. Approve the change with `daedalus-lock update`. |

`locked` and `record` need Linux, where `/proc/self/fd` exists. Elsewhere a host refuses them at
start, with the reason. `off` works everywhere.

The daemon's `daedalus-lock` tool records, checks and updates the lock. `record PATH...` adds files,
`check` compares them with the lock (or every entry, with no path), and `update` approves the
current file for an entry and shows the old and new digest. Its exit codes are 0 for ok, 1 for a
file that does not match or has no entry, 2 for a usage error, and 3 for a lock or file that cannot
be read.

A library that is refused is not loaded, and the loaded ones keep running. The daemon logs the
refusal, with both digests. A `LoadPlugin` request for a refused library returns the same message
on the wire. The daemon tries a refused library again, with a delay that doubles from 250 ms and is
never more than 8 s, until the lock approves the file or the file is removed. The retry never stops
on its own, and the daemon never turns it off. Removing the file is how an operator stops it.

A worker's program is checked before each start, including each restart. A program named by a bare
name is found on `PATH` first, and the file found is the one checked. A program that starts with
`#!` is refused under `locked`, because the kernel would run its interpreter, not the file. Name the
interpreter as the program, and add the script with `also_check`. A refused worker stays down: a run
faults with the reason, and later runs are skipped with it, until a restart passes the check. Each
restart checks the files again, and the retries never stop on their own.

A check at a restart runs on a thread of its own. Nothing hashes a file on a run's tick.

After a library loads, the host checks that the mapped file is the hashed one. It compares the inode of the mapping with the inode of the hashed descriptor, not the device number: on a filesystem with subvolumes, such as btrfs, the mapping and the descriptor report different devices for one file.

The threat model, as the design states it:

1. Locked mode stops a file that was swapped by rename, corrupted, stale, or never approved from
   being loaded or executed, including a path a wire client names in `LoadPlugin`.
2. It closes the check-then-open race, but not an in-place write to the same inode between hash and
   map by someone who can write the file itself (sealing a memfd copy would; it is not proposed).
3. Anyone who can write the lockfile, the daemon config, or the host binary defeats it, and there
   are no signatures.
4. It says nothing about code that is in the lock: a malicious or buggy approved plugin runs with
   the host's full rights, and nothing is checked after load.
5. It covers only the named file: not shared-library dependencies, not `ld.so`, and for interpreted
   workers only the interpreter (scripts under `also_check` are checked before start, not bound to
   what the interpreter reads).

A daemon that starts with a lock, or a lock directory, that its group or others can write says so
in its log. Keep the lock owned by the account that runs the daemon, and writable by no one else.
