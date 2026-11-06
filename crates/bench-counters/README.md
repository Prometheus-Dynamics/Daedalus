# daedalus-bench-counters

Hardware performance counters around a closure, for benches (Linux `perf_event_open`).

Use it as a git dependency, pinned to the same commit as the rest of Daedalus:

```toml
[dev-dependencies]
daedalus-bench-counters = { git = "https://github.com/Prometheus-Dynamics/Daedalus", rev = "<sha>" }
```

```rust
let mut counters = daedalus_bench_counters::Counters::new(&Event::ALL)?;
let sample = counters.measure(iterations, || run_one_call())?;
let cycles = sample.per_call(Event::Cycles);   // None if the PMU lacks the event
for event in sample.missing() { eprintln!("not measured: {}", event.name()); }
if sample.multiplexed() { /* counts cover only part of the run */ }
```

Counters are per thread, user space only. Counting may be denied (`PermissionDenied`); the hint
names `/proc/sys/kernel/perf_event_paranoid`, which must be 2 or lower for your own threads. Off
Linux every call returns `Unsupported`, so skip the measurement there.
