# Documentation

Daedalus runs graphs of typed nodes. A node is a plain function or a shader, a graph connects
nodes by their ports, and a planner proves the graph valid before anything runs.

## Start here

- [design.md](design.md): the design, and how each part was built. It is the source of truth.
- [architecture.md](architecture.md): the same design as a longer walkthrough
  ([with diagrams](architecture.html)).
- [node-authoring.md](node-authoring.md): writing a node, its ports, constants, config and state.
- [host.md](host.md): planning a graph, loading it, and feed, step, take.
- [`examples/`](../examples): `00_quickstart`, `01_basics`, `02_host` and `03_metrics` are the
  shortest programs that do each of those.

## By subject

| You want to | Read |
|---|---|
| Write a node that runs on the CPU | [node-authoring.md](node-authoring.md) |
| Write a node that runs on a GPU | [gpu-nodes.md](gpu-nodes.md) |
| Run a graph in an application | [host.md](host.md) |
| Store a plan, or generate code from one | [plan-forms.md](plan-forms.md) |
| Run a graph on a microcontroller | [mcu.md](mcu.md), [chips.md](chips.md) |
| Ship nodes or a backend as a library loaded at run time | [plugins.md](plugins.md) |
| Write nodes in Python, JavaScript, Java, C or C++ | [ffi.md](ffi.md) |
| Pass a value Daedalus did not make | [foreign-values.md](foreign-values.md) |
| Run Daedalus as a daemon | [host.md](host.md#the-service) |
| Build, test and add to the repository | [development.md](development.md), [testing.md](testing.md) |
| Find where something on `dev` went | [migration-from-dev.md](migration-from-dev.md) |

## Reference

- [schema/](schema): the JSON schema of a graph document.
- [backend-matrix.md](backend-matrix.md): which kernel runs on which instruction set
  (generated).
- [chips.md](chips.md): firmware sizes by mode and target.
- [`testing/budgets/README.md`](../testing/budgets/README.md): the budgets and where each is
  checked.
- [`testing/capability-audit.md`](../testing/capability-audit.md): every capability of `dev` and
  what replaced it.
- [prs/](prs): the brief of each PR, each with a Status section that says how it was built.
