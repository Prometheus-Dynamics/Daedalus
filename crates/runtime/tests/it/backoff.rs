//! Cross-tick backoff of failing nodes: a backed-off node is skipped without blocking the tick,
//! runs again once its delay passes, and its delay doubles per failure up to the cap. Uses the
//! `resilience` module's fixtures.

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use daedalus_runtime::executor::{Executor, OwnedExecutor};
use daedalus_runtime::sync::Mutex;
use daedalus_runtime::{
    NodeError, NodeHandler, NodeRecovery, RuntimeNode, SchedulerConfig, build_runtime,
};
use daedalus_transport::Payload;

use super::resilience::{Flaky, chain};

/// `src` pushes 1 each tick; `flaky` forwards its input +1 but fails its second call; `sink`
/// records every value it reads. Used by the cross-tick backoff tests.
struct SecondCallFails {
    calls: AtomicU32,
    seen: Arc<Mutex<Vec<i32>>>,
}

impl NodeHandler for SecondCallFails {
    fn run(
        &self,
        node: &RuntimeNode,
        _ctx: &daedalus_runtime::state::ExecutionContext,
        io: &mut daedalus_runtime::io::NodeIo,
    ) -> Result<(), NodeError> {
        match node.id.as_str() {
            "src" => io.push_payload("out", Payload::owned("test:i32", 1_i32)),
            "flaky" => {
                let value = io
                    .get_typed::<i32>("in")
                    .ok_or_else(|| NodeError::InvalidInput("flaky input".into()))?;
                if self.calls.fetch_add(1, Ordering::Relaxed) == 1 {
                    return Err(NodeError::Handler("flaky failed".into()));
                }
                io.push_payload("out", Payload::owned("test:i32", value + 1));
            }
            "sink" => {
                if let Some(value) = io.get_typed::<i32>("in") {
                    self.seen.lock().push(value);
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// A backoff never sleeps the tick: an in-tick retry with a long `backoff` returns at once.
#[test]
fn an_in_tick_retry_with_a_backoff_does_not_sleep_the_tick() {
    let rt = build_runtime(
        &chain(&["src", "flaky", "sink"]),
        &SchedulerConfig::default(),
    );
    let started = std::time::Instant::now();
    let telemetry = Executor::new(&rt, Flaky::new(5))
        .with_node_recovery(
            "flaky",
            NodeRecovery::Retry {
                max: 2,
                backoff: Duration::from_secs(5),
            },
        )
        .run()
        .expect("contained");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(telemetry.recovery().retries, 2);
}

/// Under a FIFO edge a backed-off producer leaves its consumer with nothing new: it sees no value
/// this tick, and nothing stale.
#[test]
fn a_fifo_consumer_sees_no_value_while_its_producer_is_backed_off() {
    let config = SchedulerConfig {
        default_policy: daedalus_runtime::RuntimeEdgePolicy::fifo(),
        ..SchedulerConfig::default()
    };
    let rt = Arc::new(build_runtime(&chain(&["src", "flaky", "sink"]), &config));
    let handler = SecondCallFails {
        calls: AtomicU32::new(0),
        seen: Arc::default(),
    };
    let seen = handler.seen.clone();
    let mut exec = OwnedExecutor::new(rt, handler)
        .with_backoff(Duration::from_secs(10), Duration::from_secs(10));
    exec.run_in_place().expect("tick 1");
    assert_eq!(*seen.lock(), vec![2]);
    exec.run_in_place().expect("tick 2: fails and backs off");
    exec.run_in_place().expect("tick 3: skipped");
    assert_eq!(*seen.lock(), vec![2], "no stale value under FIFO");
}

/// Under a Latest edge a backed-off producer's consumer keeps the last value: by definition the
/// edge is a latest-value slot, not a queue.
#[test]
fn a_latest_consumer_keeps_the_last_value_while_its_producer_is_backed_off() {
    let config = SchedulerConfig {
        default_policy: daedalus_runtime::RuntimeEdgePolicy::latest_only(),
        ..SchedulerConfig::default()
    };
    let rt = Arc::new(build_runtime(&chain(&["src", "flaky", "sink"]), &config));
    let handler = SecondCallFails {
        calls: AtomicU32::new(0),
        seen: Arc::default(),
    };
    let seen = handler.seen.clone();
    let mut exec = OwnedExecutor::new(rt, handler)
        .with_backoff(Duration::from_secs(10), Duration::from_secs(10));
    exec.run_in_place().expect("tick 1");
    exec.run_in_place().expect("tick 2: fails and backs off");
    exec.run_in_place().expect("tick 3: skipped");
    let values = seen.lock().clone();
    assert_eq!(values.first(), Some(&2));
    assert!(values.iter().all(|&value| value == 2), "{values:?}");
}

/// Backoff on an injected clock, so the delays are exact and nothing sleeps: a backed-off node is
/// skipped without blocking the tick, runs again once its delay has passed, and its delay doubles
/// per failure in a row up to the cap.
#[test]
fn backoff_on_an_injected_clock_skips_runs_again_and_caps_the_delay() {
    let nanos = Arc::new(AtomicU64::new(0));
    let clock = {
        let nanos = nanos.clone();
        daedalus_core::platform::Clock::new(move || {
            Duration::from_nanos(nanos.load(Ordering::Relaxed))
        })
    };
    let advance = |by: Duration| {
        nanos.fetch_add(by.as_nanos() as u64, Ordering::Relaxed);
    };
    let ms = Duration::from_millis;

    // Skipped, then runs again: the first failure waits the base delay (100 ms).
    let rt = Arc::new(build_runtime(
        &chain(&["src", "flaky", "sink"]),
        &SchedulerConfig::default(),
    ));
    let handler = Flaky::new(1);
    let seen = handler.seen.clone();
    let mut exec = OwnedExecutor::new(rt.clone(), handler)
        .with_clock(clock.clone())
        .with_backoff(ms(100), ms(400));
    let failed = exec.run_in_place().expect("contained");
    assert_eq!(failed.node_health[0].backoff_remaining, Some(ms(100)));

    advance(ms(50));
    let blocked = std::time::Instant::now();
    let skipped = exec.run_in_place().expect("skipped tick");
    assert!(
        blocked.elapsed() < ms(500),
        "a skipped node must not block the tick"
    );
    assert_eq!(skipped.recovery().backoff_skips, 1);
    assert!(skipped.errors.is_empty());
    assert!(seen.lock().is_empty(), "no output while backed off");

    advance(ms(50)); // now at the delay's end: the node runs again and succeeds
    exec.run_in_place().expect("recovered tick");
    assert_eq!(*seen.lock(), vec![2]);
    assert_eq!(exec.run_in_place().expect("steady").node_health.len(), 1);

    // Delays double per failure in a row and stop at the cap (400 ms): 100, 200, 400, 400.
    let mut exec = OwnedExecutor::new(rt, Flaky::new(u32::MAX))
        .with_clock(clock)
        .with_backoff(ms(100), ms(400));
    let mut remaining = Vec::new();
    for _ in 0..4 {
        let telemetry = exec.run_in_place().expect("contained");
        let report = &telemetry.node_health[0];
        remaining.push(report.backoff_remaining.expect("backed off"));
        advance(report.backoff_remaining.expect("backed off"));
    }
    assert_eq!(remaining, vec![ms(100), ms(200), ms(400), ms(400)]);
}

/// The configured cap is clamped to ten seconds, so no node is left out for longer than that.
#[test]
fn the_backoff_delay_never_exceeds_ten_seconds() {
    let nanos = Arc::new(AtomicU64::new(0));
    let clock = {
        let nanos = nanos.clone();
        daedalus_core::platform::Clock::new(move || {
            Duration::from_nanos(nanos.load(Ordering::Relaxed))
        })
    };
    let rt = Arc::new(build_runtime(
        &chain(&["src", "flaky", "sink"]),
        &SchedulerConfig::default(),
    ));
    let mut exec = OwnedExecutor::new(rt, Flaky::new(u32::MAX))
        .with_clock(clock)
        .with_backoff(Duration::from_secs(60), Duration::from_secs(60));
    let telemetry = exec.run_in_place().expect("contained");
    assert_eq!(
        telemetry.node_health[0].backoff_remaining,
        Some(daedalus_runtime::MAX_BACKOFF_CAP)
    );
}
