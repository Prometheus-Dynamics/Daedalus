//! Measures a known loop. Counters are often unavailable (CI, containers, VMs): then each test
//! returns early, and the unit tests cover the arithmetic.

use daedalus_bench_counters::{Counters, Error, Event};

fn busy(n: u64) -> u64 {
    (0..n).fold(0u64, |acc, i| std::hint::black_box(acc.wrapping_add(i)))
}

/// Opens every event, or returns `None` when the platform or the kernel says counting is off.
fn open_all() -> Option<Counters> {
    match Counters::new(&Event::ALL) {
        Ok(counters) => Some(counters),
        Err(Error::Unsupported(_) | Error::PermissionDenied { .. } | Error::NoSuchEvent(_)) => None,
        Err(err) => panic!("unexpected counter error: {err}"),
    }
}

#[test]
fn a_measured_loop_counts_its_calls() {
    let Some(mut counters) = open_all() else {
        return;
    };
    let sample = counters.measure(200, || {
        std::hint::black_box(busy(10_000));
    });
    let Ok(sample) = sample else { return };
    assert_eq!(sample.iterations(), 200);
    assert!(sample.elapsed().as_nanos() > 0);
    // Every opened event has a value; a missing one is listed, never absent without a reason.
    for event in Event::ALL {
        match sample.per_call(event) {
            Some(per_call) => assert!(per_call >= 0.0, "{}: {per_call}", event.name()),
            None => assert!(
                sample.missing().contains(&event),
                "{} not reported",
                event.name()
            ),
        }
    }
    if let Some(instructions) = sample.per_call(Event::Instructions) {
        // 10 000 additions take at least ten thousand instructions per call.
        assert!(
            instructions > 10_000.0,
            "instructions per call: {instructions}"
        );
    }
    assert!((0.0..=1.0).contains(&sample.running_fraction()));
}

#[test]
fn more_work_costs_more_instructions() {
    let Some(mut counters) = open_all() else {
        return;
    };
    let (Ok(small), Ok(large)) = (
        counters.measure(100, || {
            std::hint::black_box(busy(1_000));
        }),
        counters.measure(100, || {
            std::hint::black_box(busy(100_000));
        }),
    ) else {
        return;
    };
    if let (Some(a), Some(b)) = (
        small.per_call(Event::Instructions),
        large.per_call(Event::Instructions),
    ) {
        assert!(b > a * 10.0, "small {a}, large {b}");
    }
}

#[test]
fn an_empty_request_measures_nothing() {
    let Ok(mut counters) = Counters::new(&[]) else {
        return;
    };
    let sample = counters.measure(3, || {}).expect("an empty group reads");
    assert_eq!(sample.events().count(), 0);
    assert_eq!(sample.per_call(Event::Cycles), None);
}
