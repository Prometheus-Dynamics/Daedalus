# Microcontrollers

A graph runs on a microcontroller with no heap and no planner on the chip. The chip is a machine
with a small CPU backend and the same `#[node]`: there is no separate macro and no separate node
type.

[chips.md](chips.md) is the full guide. In short:

1. Write the nodes with `#[node]` in a `#![no_std]` crate.
2. Describe the graph in JSON, with a host bridge for what the firmware pushes and pops.
3. On your computer, plan it and write it out, then build the firmware with what was written.

There are three ways to put a graph on a chip:

| Mode | The chip holds | Changing the graph |
|---|---|---|
| Compiled | Generated Rust (`Form::Source`) that calls the node functions directly | Rebuild the firmware |
| Tunable | The same, with marked constants that can be set while it runs | Send a parameter update |
| Loaded | A library of node bodies and a stored plan (`Form::Blob`) read by a small runner | Send another plan |

The generated module has a `Graph` with a `push_<port>` for each host input, a `pop_<port>` for
each host output, and `tick`. `Graph::new()` is a `const fn`, so the graph, its queues and its
node state sit in a `static`.

## What the chip build guarantees

- No allocator. `core/abi`, `core/types`, `core/value`, `core/graph`, `core/plan` and
  `core/runner` build for `thumbv7em-none-eabihf`, `thumbv6m-none-eabi` and
  `riscv32imac-unknown-none-elf` with none, and the gate checks it.
- Only the body that was chosen for each node is generated, so a node's other bodies cost no
  flash.
- A node body's error is a code and a short reason of fixed size, so a failure allocates
  nothing.

## Sizes

The five-node firmware's flash and RAM, by mode and target, are in [chips.md](chips.md) and are
checked by the gate (`scripts/ci.d/chip.sh`) and by the budget table
([`testing/budgets`](../testing/budgets/README.md)).

## Examples

- [`examples/chip_blink`](../examples/chip_blink): the three modes on one graph.
- [`examples/chip_board`](../examples/chip_board): a Raspberry Pi Pico firmware.
- [`examples/chip_shared`](../examples/chip_shared): one set of nodes for a host and a chip.
