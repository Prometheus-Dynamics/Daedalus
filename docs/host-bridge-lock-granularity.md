# Host Bridge Lock Granularity Review

This note records the release review of host bridge shared state in `crates/runtime/src/host_bridge.rs`.

## Current Shape

`HostBridgeShared` uses one `Mutex<HostBridgeBuffers>` plus one `Condvar`. The locked state currently includes:

- inbound queues
- outbound queues
- default and per-port input policies
- default and per-port output policies
- freshness tracking
- closed flags
- stats
- retained diagnostic events

This is simple and correct for the current host bridge contract. The lock is not held across node handler execution, and stream workers take the executor out of the shared `StreamGraph` before running nodes.

## Release Decision

Keep the single host bridge buffer lock for this release.

The current design favors correctness, deterministic policy application, and straightforward diagnostics. It is acceptable while host bridge traffic is expected to be moderate and while event retention is bounded by `HostBridgeConfig::event_limit`.

## Lock Ordering

Use this order when a future change must touch more than one shared runtime structure:

1. `HostBridgeManager` map/default locks, only long enough to clone the target `HostBridgeShared`.
2. A single `HostBridgeShared::buffers` lock for queue mutation, freshness checks, stats, and retained events.
3. `StreamGraph` lock for worker state, diagnostics, and executor ownership.
4. Executor edge queue locks, one queue at a time, while applying edge pressure policy or draining inputs.
5. State/resource locks, scoped to one state map or one node-resource bundle.

Do not hold host bridge, stream graph, executor queue, or state-resource locks while invoking a node
handler, polling GPU work, running host callbacks, or waiting on a condition variable. Stream workers
should take the executor out of `StreamGraph`, drop the stream lock, run the executor, then reacquire
the stream lock to publish diagnostics. Queue operations should not call into host bridge or state
resource code while an edge queue lock is held.

## Watch Points

Revisit this if profiling shows host bridge contention or if host IO becomes a hot path. The specific symptoms to look for are:

- high time in `feed_payload_ref`, `push_outbound_ref`, `try_pop_payload`, or `drain_payloads`
- many producer threads feeding the same host bridge
- large retained event limits
- high-frequency polling of pending counts or stats
- slow consumers holding outbound queues full under replacement/drop policies

## Candidate Split

If contention appears, split state in this order:

- Keep queue mutation and freshness tracking together per direction.
- Move retained events behind a separate bounded event buffer.
- Move stats to atomics or a separate stats lock.
- Consider per-port queue locks only after measuring contention, because per-port locks make policy updates and diagnostics more complex.

Any split should preserve bounded event retention, deterministic policy updates, and `Condvar` wakeups for inbound/outbound delivery.
