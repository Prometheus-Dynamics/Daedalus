# Daedalus on a chip

A graph can run on a microcontroller with no heap. You plan it on your computer, and the plan is written out as a Rust module that calls your node functions directly. The firmware compiles that module and has no planner in it.

## Workflow

1. Write nodes with `#[node]` in a `#![no_std]` crate. Depend on `daedalus-cpu-author` with `default-features = false`.
2. Describe the graph in a `graph.json`, with a `host_bridge` node for the values your firmware pushes and pops.
3. On your computer, plan it with `Form::Source` and `SourceBackend`, and check the output in. [`examples/chip_blink`](../examples/chip_blink) does this in `nodes/examples/generate.rs`, and `tests/generated.rs` fails if the file goes stale.
4. In the firmware, `include!` the module and use its `Graph`:

```rust
static mut GRAPH: Graph = Graph::new();   // inside #[entry]
graph.push_sample(raw)?;
graph.tick_at(now)?;
let led = graph.pop_led();
```

Each host input is a `push_<port>`, each host output a `pop_<port>`. `tick_at` runs every node once in order. `tick()` is the same with the time at zero, and `tick_with(&clock)` reads a `chip::Clock`.

`Graph::new()` is a `const fn`, so the whole graph, its queues and its node state, can sit in a `static` with no setup.

## What runs when

Every edge gets one fixed-size queue, or a local variable where two nodes always run one after the other.

| Edge policy | Queue |
| --- | --- |
| FIFO | `Config.fifo_capacity` slots (default 4), and a full queue is an error |
| `latest_only` | 1 slot, newest wins |
| `bounded` | its capacity, with its overflow rule |

A node waits for its required inputs. If one is missing, it doesn't run this tick. A node that fails, or a queue with the error policy that fills up, stops the tick and returns a `ChipError`. `NODE_IDS` and `EDGES` turn the numbers in it into names.

## Changing constants while running

Mark constants in the node's metadata, under `daedalus.chip.params`: either a list of input names, or a struct from input name to `[min, max]`, or to unit for the type's full range. Each marked input must be a scalar constant inside its range.

The generated module then has one field per parameter, a `set_<name>` for each, `PARAM_NAMES`, `PARAMS` and `impl Tunable`. A parameter is named `<label>.<input>`, or `<node id>.<input>` if the node has no label. Ids follow run order, then input order. A new value is used from the next tick. The firmware needs the `chip-params` feature of `daedalus-cpu`.

A blob graph can be changed the same way: `Interpreter` implements `Tunable`.

A host sends a 3 to 14 byte message. `daedalus_cpu::param_host::encode_update` writes it from the module's `PARAM_NAMES` and `PARAMS`, with the checks the chip will apply, and the chip calls `apply_update`. `threshold.on = 2.5` as id 1 is `01 09 00 00 20 40`.

Setting `Config.freeze_params` turns every marker back into a plain constant, and gives the same module as a graph with no markers. Without the `f64` feature, no `f64` arithmetic is linked: an `f64` value sent to an `f32` parameter still converts, in integer arithmetic.

## One set of nodes for a host and a chip

A `#[node]` needs nothing to run on both. `examples/chip_shared` declares its nodes once, and the same records go into a host's registry, the generated module and a chip's stored plans. Only the `host` feature adds the facade, for tests, so a chip build doesn't pull it.

## Limitations

- Port values must be `Copy`. Planning refuses anything else and names the type.
- Node state must be `Default`. It's created the first time the node runs.
- A module holds one connected graph. Planning refuses a graph with separate parts.
- Two host ports whose names become the same Rust identifier are refused by name.

## On a board

`examples/chip_board` runs the blink graph on a Raspberry Pi Pico in all three modes, reading ADC0 and driving the LED, and reports tick times and stack depth over defmt-RTT. `tests/host.rs` runs the same loop with a mock board. It hasn't run on a real board yet: `testing/board-runs/D12.md` has the procedure.

## Measurements

Every figure here is from `scripts/ci.d/chip.sh`, on the commit and toolchain in
[`testing/board-runs/F1.md`](../testing/board-runs/F1.md). Nothing here was measured on a board.

Five-node blink graph, profile `chip`:

| Target | Flash | RAM |
| --- | --- | --- |
| `thumbv7em-none-eabihf` | 2128 B | 112 B |
| `thumbv6m-none-eabi` | 2672 B | 112 B |

Budget: 12 KiB flash, 584 B RAM. The figures are pinned exactly, so a change that moves them has to say so in the same commit.

The blink graph in its three modes, in bytes:

| Mode (binary) | `thumbv7em-none-eabihf` flash | `thumbv6m-none-eabi` flash | RAM | Budget |
| --- | --- | --- | --- | --- |
| Compiled (`daedalus-chip-blink`) | 2128 | 2672 | 112 | 12 KiB, 584 B, pinned exactly |
| Tunable, 4 parameters (`-tunable`) | 4368 | 5592 | 128 | 6 KiB, 512 B |
| Stored plans, a 7-node library and two plans (`-loaded`) | 16064 | 17612 | 576 | 17.5 KiB, 584 B |

The runner's RAM is zeroed memory, so it takes no flash.

The Pico firmware, with all three modes, the HAL and defmt, is 19220 B flash and 1928 B RAM. Budget: 20 KiB, 2 KiB.
