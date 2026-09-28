# Host Bridge Lock Granularity Review

This note records the release review of host bridge shared state in `crates/runtime/src/host_bridge.rs`.

## Current Shape

`HostBridgeShared` uses one `Mutex<HostBridgeBuffers>` plus one `Condvar`. The locked state currently includes:

- one `PortDirection` per direction (inbound, outbound), each holding the direction's default
  policies and a `HashMap<PortId, PortState>`; a `PortState` bundles the port's queue (a single
  slot for replace-style capacity-one policies, otherwise a FIFO), policy overrides, freshness
  watermarks, and close flag, so a push does one map lookup
- the bridge-wide closed flag
- stats
- retained diagnostic events (off by default)
- inbound waiter bookkeeping (wake epoch and async wakers)

This is simple and correct for the current host bridge contract. The lock is not held across node handler execution, and stream workers take the executor out of the shared `StreamGraph` before running nodes.

## Release Decision

Keep the single host bridge buffer lock for this release.

The current design favors correctness, deterministic policy application, and straightforward diagnostics. It is acceptable while host bridge traffic is expected to be moderate and while event retention is bounded by `HostBridgeConfig::event_limit`.

## Lock Ordering

All shared runtime locks are `parking_lot` locks (no poisoning). Acquire them in this order; a
path may skip levels but never takes a lower-numbered lock while holding a higher-numbered one:

1. `StreamGraph` lock (`SharedStreamGraph`) for worker state, diagnostics, and executor
   ownership. Stream methods and the worker loop read bridge counters (`pending_inbound`,
   `stats`, `config_snapshot`) while holding it.
2. `HostBridgeManager` map lock, then its defaults lock, only long enough to look up or create a
   `HostBridgeShared`. Manager-wide setters release both before locking each bridge.
3. One `HostBridgeShared::buffers` lock for queue mutation, freshness checks, stats, and retained
   events. Condvar waits (`recv_payload_timeout`, `InboundWaiter::wait`) release it while blocked.
4. Executor edge queue and direct-slot locks, one at a time. The executor's inbound drain
   (`inject_host_inputs` → `HostBridgeHandle::drain_inbound_port`) holds the bridge lock while it
   hands each payload to `fan_out`, so **bridge lock → edge queue lock** is the established order.
   The reverse never happens: output draining (`drain_host_outputs`) pops each payload with
   `pop_edge`/`pop_direct_edge`, which release the queue lock before `push_outbound_ref` takes
   the bridge lock.
5. Leaf locks taken inside a queue operation or node call: the warning de-duplication set, the
   data-size inspector registry, and state/resource locks (one state map or one node-resource
   bundle at a time).

Do not hold host bridge, stream graph, executor queue, or state-resource locks while invoking a node
handler, polling GPU work, running host callbacks, or waiting on a condition variable. Stream workers
take the executor out of `StreamGraph`, drop the stream lock, run the executor, then reacquire
the stream lock to publish diagnostics. Code running under an edge queue lock must not call into
the host bridge or the stream graph, and the `drain_inbound_port` sink must not call back into the
bridge it drains.

## Watch Points

Revisit this if profiling shows host bridge contention or if host IO becomes a hot path. The specific symptoms to look for are:

- high time in `feed_payload`, `push_outbound_ref`, `try_pop_payload`, or `drain_payloads`
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
