# Plugins

A plugin is a dynamic library that carries nodes, backends, or both. A host loads it at run
time through `daedalus-loader`. Nothing in the host is rebuilt.

## What a plugin exports

One symbol, `daedalus_plugin_entry`. It gives the host a table of node declarations and a table
of backends. The export macro writes it:

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
});
```

`run` is a function of the step's cells. [`testing/fixtures/dylib`](../testing/fixtures/dylib)
is a complete plugin.

## The check at load

The first thing a host and a plugin compare is one number: the layout hash of the step
interface. A plugin built against another layout is refused before the host reads anything else
from it, and the error names both hashes. There are no version symbols and no negotiation.

## What crosses the boundary

- Only `repr(C)` data. A plugin's strings are copied into host storage at load.
- A node's inputs and outputs are cells. Plain data of 16 bytes or less is in place, and anything
  larger is bytes behind a shared handle. A plugin never hands the host memory to free.
- A panic in a plugin's node does not cross. It is a fault of that step: the tick goes on and the
  host keeps running.

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
and the process backend supervises it like any other worker. See [ffi.md](ffi.md).

## A backend in a plugin

A plugin's backend takes regions and returns one step a node. It produces steps only: the blob
and source forms need a backend that is linked in.
