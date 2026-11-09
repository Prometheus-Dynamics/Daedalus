# daedalus-planner

Turns a graph into a proven, runnable plan: solve, ask the backends, assign, compile, stitch.

## Graph patches

A patch is a versioned list of edits. Applying one returns a report of what changed and what wasn't
found. A selector that matches nothing is reported, never ignored.

```json
{
  "version": 1,
  "ops": [
    { "type": "set_const", "node": { "id": "blur-1" }, "port": "radius", "value": { "type": "int", "value": 3 } },
    { "type": "replace_node_id", "node": { "index": 2 }, "new_id": "acme.blur_v2" },
    { "type": "delete_node", "node": { "index": 4 } }
  ]
}
```

A selector has `index` (the node's position), `id` (its UI id), or both, which must agree. Selectors
are resolved against the graph as it was before the patch, so deleting a node doesn't change what a
later index means. Deleting a node removes its edges and renumbers the rest.

The report:

```json
{
  "applied": [{ "op": 0, "node": 1 }],
  "not_found": [{ "op": 1, "selector": { "index": 9 } }],
  "removed_nodes": 0,
  "removed_edges": 0
}
```

`analyze` gives the selected nodes, everything downstream of them, and which of those are sinks,
without applying the patch.

## Port refresh

`refresh_ports` brings a stored graph's port lists up to the registry. Declared ports are added.
Undeclared ports are dropped unless an edge or a const still uses them, and those are reported as
kept. `RefreshPolicy { drop_stale_consts: true }` also drops ports only a const uses, and reports
each value. Nodes with no port lists, and nodes nothing declares, are left alone. Running it twice
changes nothing the second time.

```json
{
  "nodes": [
    {
      "node": 1,
      "id": "acme.map",
      "added_inputs": ["save"],
      "added_outputs": [],
      "removed_inputs": ["legacy"],
      "removed_outputs": [],
      "kept_inputs": [],
      "kept_outputs": ["debug"],
      "dropped_consts": []
    }
  ]
}
```
