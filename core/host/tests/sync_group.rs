//! Grouping stamped values into whole ticks, and inputs that tick alone.

use std::collections::BTreeMap;
use std::sync::Arc;

use daedalus_host::sync_group::{
    GroupSink, HostSink, Independent, IndependentConfig, InputSet, PartialPolicy, Push, SyncConfig,
    Synchronized,
};
use daedalus_host::{Dir, Host, PortSpec, Ports};
use daedalus_runner::{Memory, Plan, Runner, SlotKind, SlotSpec};
use daedalus_types::{SpaceIndex, TypeIndex, TypeKey};
use daedalus_value::{Owner, Value};

const MS: u64 = 1_000_000;

/// The next tick's values: at most one waiting per input.
#[derive(Default)]
struct Ports3 {
    waiting: BTreeMap<usize, i64>,
}

impl GroupSink<i64> for Ports3 {
    fn deliver(&mut self, input: usize, value: i64) {
        self.waiting.insert(input, value);
    }

    fn clear(&mut self, input: usize) -> bool {
        self.waiting.remove(&input).is_some()
    }
}

impl Ports3 {
    fn tick(&mut self) -> Vec<(usize, i64)> {
        let taken = std::mem::take(&mut self.waiting);
        taken.into_iter().collect()
    }
}

fn config(partial: PartialPolicy) -> SyncConfig {
    SyncConfig {
        window: 8 * MS,
        timeout_ns: 50 * MS,
        partial,
        max_buffered: 4,
    }
}

fn group(count: usize, partial: PartialPolicy) -> (Synchronized<i64>, Ports3) {
    (Synchronized::new(count, config(partial)), Ports3::default())
}

#[test]
fn jittered_values_group_into_whole_ticks_in_any_arrival_order() {
    let (mut sync, mut sink) = group(3, PartialPolicy::Drop);
    let jitter = [0, 3, 5, 2, 7, 1];
    // Input 2 lags one value behind the others, so arrival is out of order across inputs.
    for n in 0..6_i64 {
        let stamp = |input: usize| (n as u64 * 33 + jitter[(n as usize + input) % 6]) * MS;
        assert_eq!(sync.push(0, n, stamp(0), 0, &mut sink), Push::Buffered);
        sync.push(1, n, stamp(1), 0, &mut sink);
        if n > 0 {
            let late = stamp(2) - 33 * MS;
            assert_eq!(sync.push(2, n - 1, late, 0, &mut sink), Push::Resolved);
            assert_eq!(sink.tick(), [(0, n - 1), (1, n - 1), (2, n - 1)]);
        }
        assert_eq!(sink.tick(), []);
    }
    let stats = sync.stats();
    assert_eq!((stats.complete_groups, stats.values), (5, 17));
    assert!(stats.max_skew <= 7 * MS);
    let last = stats.last_group.unwrap();
    assert!(last.complete && last.inputs == InputSet::all(3));
}

#[test]
fn a_stalled_input_times_out_into_the_partial_policy() {
    for partial in [
        PartialPolicy::Drop,
        PartialPolicy::TickPartial,
        PartialPolicy::HoldLast,
    ] {
        let (mut sync, mut sink) = group(3, partial);
        for input in 0..3 {
            sync.push(input, 0, 0, 0, &mut sink);
        }
        assert_eq!(sink.tick().len(), 3);
        // Input 2 stalls.
        let now = 33 * MS;
        sync.push(0, 1, 33 * MS, now, &mut sink);
        sync.push(1, 1, 34 * MS, now, &mut sink);
        assert_eq!(sync.next_deadline(), Some(now + 50 * MS));
        assert!(!sync.expire(82 * MS, &mut sink), "not due yet");
        assert!(sync.expire(83 * MS, &mut sink));
        assert_eq!(sync.next_deadline(), None);
        let stats = sync.stats();
        assert_eq!(stats.timeouts, 1);
        match partial {
            PartialPolicy::Drop => {
                assert_eq!(sink.tick(), []);
                assert_eq!((stats.dropped_groups, stats.dropped_values), (1, 2));
            }
            PartialPolicy::TickPartial => {
                assert_eq!(sink.tick(), [(0, 1), (1, 1)]);
                assert_eq!(stats.partial_groups, 1);
            }
            PartialPolicy::HoldLast => {
                assert_eq!(sink.tick(), [(0, 1), (1, 1), (2, 0)]);
                let last = stats.last_group.unwrap();
                assert_eq!((last.reused, last.complete), (InputSet::only(2), false));
            }
        }
        // The stalled input's late value is dropped.
        assert_eq!(sync.push(2, 1, 35 * MS, 90 * MS, &mut sink), Push::Late);
    }
}

#[test]
fn a_skipped_value_resolves_without_waiting_for_the_timeout() {
    let (mut sync, mut sink) = group(2, PartialPolicy::TickPartial);
    sync.push(0, 0, 0, 0, &mut sink);
    // Input 1 skipped value 0 and delivers value 1, so group 0 can't complete.
    assert_eq!(sync.push(1, 1, 33 * MS, 0, &mut sink), Push::Resolved);
    assert_eq!(sink.tick(), [(0, 0)]);
    assert_eq!(sync.stats().timeouts, 0);
    sync.push(0, 1, 34 * MS, 0, &mut sink);
    assert_eq!(sink.tick(), [(0, 1), (1, 1)]);
    assert_eq!(sync.stats().complete_groups, 1);
}

#[test]
fn out_of_order_values_of_one_input_are_sorted() {
    let (mut sync, mut sink) = group(2, PartialPolicy::Drop);
    sync.push(0, 1, 33 * MS, 0, &mut sink);
    sync.push(0, 0, 0, 0, &mut sink);
    assert_eq!(sync.buffered(0), 2);
    sync.push(1, 0, MS, 0, &mut sink);
    assert_eq!(sink.tick(), [(0, 0), (1, 0)]);
    sync.push(1, 1, 32 * MS, 0, &mut sink);
    assert_eq!(sink.tick(), [(0, 1), (1, 1)]);
}

#[test]
fn per_input_offsets_correct_clock_skew() {
    let (mut sync, mut sink) = group(2, PartialPolicy::Drop);
    // Input 1's clock runs 20 ms ahead, so without an offset no group completes.
    sync.push(0, 0, 100 * MS, 0, &mut sink);
    sync.push(1, 0, 120 * MS, 0, &mut sink);
    assert_eq!(sink.tick(), []);
    assert_eq!(sync.stats().dropped_groups, 1);
    sync.set_offset(1, -20 * MS as i64);
    for n in 1..4 {
        let at = 100 * MS + n as u64 * 33 * MS;
        sync.push(1, n, at + 20 * MS + MS, 0, &mut sink);
        sync.push(0, n, at, 0, &mut sink);
        assert_eq!(sink.tick(), [(0, n), (1, n)]);
    }
    assert_eq!(sync.stats().max_skew, MS);
}

#[test]
fn full_buffers_resolve_the_oldest_group_and_unread_groups_are_superseded() {
    let settings = SyncConfig {
        max_buffered: 2,
        ..config(PartialPolicy::TickPartial)
    };
    let mut sync = Synchronized::new(2, settings);
    let mut sink = Ports3::default();
    // Input 1 is silent, so input 0 piles up to the bound.
    for n in 0..5_i64 {
        sync.push(0, n, n as u64 * 33 * MS, 0, &mut sink);
        assert!(sync.buffered(0) <= 2);
    }
    let stats = sync.stats();
    assert_eq!(stats.overflows, 3);
    assert_eq!(stats.superseded_groups, 2, "no tick took groups 0 and 1");
    assert_eq!(sink.tick(), [(0, 2)], "ports hold only the latest group");
}

#[test]
fn an_unknown_input_is_refused() {
    let (mut sync, mut sink) = group(2, PartialPolicy::Drop);
    assert_eq!(sync.push(5, 1, 0, 0, &mut sink), Push::UnknownInput);
}

#[test]
fn clear_gives_up_what_is_buffered() {
    let (mut sync, mut sink) = group(2, PartialPolicy::Drop);
    sync.push(0, 1, 0, 0, &mut sink);
    sync.clear();
    assert_eq!(sync.buffered(0), 0);
    assert_eq!(sync.next_deadline(), None);
}

#[test]
fn independent_inputs_keep_the_latest_and_age_out() {
    let mut inputs = Independent::new(
        2,
        IndependentConfig {
            max_age_ns: Some(100),
        },
    );
    let mut sink = Ports3::default();
    assert!(inputs.push(0, 1, 10, &mut sink));
    assert!(inputs.push(0, 2, 20, &mut sink));
    assert!(inputs.push(1, 3, 40, &mut sink));
    assert_eq!(sink.waiting.get(&0), Some(&2), "a newer value replaces");
    assert_eq!(inputs.present(), InputSet::all(2));
    assert_eq!(inputs.arrived(0), Some(20));
    assert_eq!(inputs.next_deadline(), Some(120));

    assert!(!inputs.expire(119, &mut sink));
    assert!(inputs.expire(120, &mut sink));
    assert_eq!(inputs.present(), InputSet::only(1));
    assert_eq!(sink.waiting.get(&0), None);
    assert_eq!(inputs.stats().expired_values, 1);
    assert!(!inputs.push(9, 1, 0, &mut sink));
}

#[test]
fn independent_inputs_without_an_age_keep_their_value() {
    let mut inputs = Independent::new(1, IndependentConfig::default());
    let mut sink = Ports3::default();
    inputs.push(0, 1, 0, &mut sink);
    assert_eq!(inputs.next_deadline(), None);
    assert!(!inputs.expire(u64::MAX, &mut sink));
    inputs.clear::<i64>(0, &mut sink);
    assert_eq!(inputs.present(), InputSet::EMPTY);
}

fn leak<T>(values: Vec<T>) -> &'static mut [T] {
    Box::leak(values.into_boxed_slice())
}

fn two_inputs() -> Host<'static> {
    const TY: TypeIndex = TypeIndex(0);
    const SP: SpaceIndex = SpaceIndex(0);
    static KEY: TypeKey = TypeKey::new("t");
    static TYPES: [TypeKey; 1] = [TypeKey::new("t")];
    let spec = SlotSpec {
        cap: 4,
        kind: SlotKind::Fifo,
    };
    let memory = Memory {
        cells: leak(vec![daedalus_abi::Slot::EMPTY; 2]),
        queues: leak(vec![daedalus_runner::Queue::default(); 2]),
        storage: leak(vec![daedalus_abi::Slot::EMPTY; 8]),
        dirty: leak(vec![0; 1]),
        carry: leak(vec![0; 1]),
        always: leak(vec![0; 1]),
        pending: leak(Vec::new()),
        health: leak(Vec::new()),
        activity: leak(vec![0; 1]),
        inline: leak(vec![[0u8; 16]; 8]),
    };
    let plan = Plan {
        steps: leak(Vec::new()),
        meta: leak(Vec::new()),
        table: leak(Vec::new()),
        slots: leak(vec![spec; 2]),
    };
    let runner = Runner::load(plan, memory).unwrap();
    let port = |name, slot| PortSpec {
        name,
        dir: Dir::Input,
        slot,
        ty: TY,
        space: SP,
        key: &KEY,
        wake: &[],
    };
    let ports = leak(vec![port("a", 0), port("b", 1)]);
    Host::new(runner, Ports::new(ports).unwrap(), &TYPES)
}

fn value() -> Value {
    Value {
        ty: TypeIndex(0),
        space: SpaceIndex(0),
        owner: Owner::Static(b"x"),
    }
}

#[test]
fn the_host_sink_delivers_a_group_and_supersedes_the_last_one() {
    let mut host = two_inputs();
    let names = ["a", "b"];
    let mut sync: Synchronized<Arc<u32>> = Synchronized::new(2, config(PartialPolicy::Drop));
    let mut sink = HostSink::new(&mut host, &names);
    sync.push(0, Arc::new(1), 0, 0, &mut sink);
    sync.push(1, Arc::new(2), 0, 0, &mut sink);
    sync.push(0, Arc::new(3), 33 * MS, 0, &mut sink);
    sync.push(1, Arc::new(4), 33 * MS, 0, &mut sink);
    assert_eq!(sink.refused, 0);
    assert_eq!(sync.stats().superseded_groups, 1);
    let queued: Vec<u32> = host.port_infos().map(|port| port.queued).collect();
    assert_eq!(queued, [1, 1]);
}

#[test]
fn plain_values_reach_the_host_too() {
    let mut host = two_inputs();
    let names = ["a", "b"];
    let mut sink = HostSink::new(&mut host, &names);
    GroupSink::<Value>::deliver(&mut sink, 0, value());
    assert!(GroupSink::<Value>::clear(&mut sink, 0));
}
