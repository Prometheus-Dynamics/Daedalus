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
    codecs: &[],
    produce: None,
});
```

`run` is a function of the step's cells. [`testing/fixtures/dylib`](../testing/fixtures/dylib)
is a complete plugin.

## The check at load

A host update must not strand the plugins built before it. So the first thing a host does with
a plugin is read how the plugin lays out each shape that crosses the boundary, and check that
against its own, a shape at a time. There are no version numbers to compare.

A shape is one of two kinds (`core/abi/src/layout.rs` lists them):

- **Frozen**: passed by value, or in an array the other side walks with its own stride. `Slot`,
  `Slots`, `Step`, `Fence`, `Text`, `PortC`, `BoundaryPort`, `UnitOut`, `BytesC`,
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
file is there, and never stops the daemon. Every backend the daemon has comes from a plugin.
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
