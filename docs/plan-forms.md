# The forms of a plan

A solved graph can be delivered three ways. The graph, the nodes and the planner are the same
for all three: `Config::form` picks one.

| Form | What it is | Used for |
|---|---|---|
| `Form::Steps` | Function pointers and their data, in this process | Running now, under a host |
| `Form::Blob` | The stored encoding of the plan | Loading a graph later without planning it again, and on a chip |
| `Form::Source` | Generated Rust | A graph sealed into a build: chips, and the fastest path anywhere |

## Steps

The default. `LoadedHost::load` takes a plan in this form and runs it. See [host.md](host.md).

## Blob

```rust
let config = Config { form: Form::Blob, ..Config::default() };
let planned = plan(&graph, &registry, &[&CpuBackend], &config)?;
planned.write("graph.plan")?;      // or planned.blob()? for the bytes
```

A blob holds the step list, the slots and the wiring, and names each node body by its id. The
small runner in `daedalus_cpu::blob` reads it against a library of node bodies, checks it, and
runs it with no planner, no registry and no heap. The check is done once, when the blob is
loaded.

## Source

```rust
// build.rs
let config = Config { form: Form::Source, ..Config::default() };
plan(&graph, &registry, &[&CpuBackend], &config)?.write("graph.rs")?;
```

The generated file is a struct with `push_<port>` for each host input, `pop_<port>` for each host output, and `tick` for that one graph, calling the
node functions directly. It needs no allocator. [mcu.md](mcu.md) shows it on a chip.

## Which backends give which form

A backend says which forms it can produce, and a plan in a form one of its backends cannot give
fails with the backend's name. The CPU backend gives all three. The GPU backend, the process
backend and a plugin's backend give steps only.

## Explaining and caching

- `daedalus_planner::explain::explain_plan(&planned)` says why each node went where it did:
  forced, the only candidate, or chosen for fewer crossings.
- `Config { lints: true, .. }` fills `planned.warnings`.
- `PlanCache` keeps plans by a key made of the graph, the settings, and each backend's id and
  version. A plan is only reused under the same key.
