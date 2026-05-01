# daedalus-registry

Deterministic declaration registry for graph planning.

## Owns

- plugin manifests,
- transport type declarations,
- adapter declarations,
- boundary serializer declarations,
- device capability declarations,
- node declarations and port metadata,
- frozen capability snapshots for planner input.

The registry stores declarations and executable tables. It does not run graphs and does not own planner decisions.
