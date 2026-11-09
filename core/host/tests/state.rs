//! Ports of the state, config cache and snapshot cases: typed state per node instance, managed
//! resources dropped once, config decoded only on change, and a JSON snapshot round trip.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use daedalus_host::{
    Codec, ConfigCache, SnapshotError, StateCodec, StateStore, restore_json, snapshot_json,
};
use daedalus_types::NodeId;

const COUNTER: NodeId = NodeId::new("state.counter");
const LABELS: NodeId = NodeId::new("state.labels");

#[derive(Debug, PartialEq, Eq)]
struct Count(u32);

impl StateCodec for Count {
    const TAG: &'static str = "count";
    fn encode(&self) -> String {
        self.0.to_string()
    }
    fn decode(text: &str) -> Option<Self> {
        text.parse().ok().map(Count)
    }
}

#[derive(Debug, PartialEq, Eq)]
struct Label(String);

impl StateCodec for Label {
    const TAG: &'static str = "label";
    fn encode(&self) -> String {
        self.0.clone()
    }
    fn decode(text: &str) -> Option<Self> {
        Some(Label(text.to_string()))
    }
}

/// Counts its own drops, so a test can see a value dropped exactly once.
struct Counted(Arc<AtomicUsize>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[test]
fn typed_state_is_read_and_replaced_by_type() {
    let mut store = StateStore::new();
    store.set(COUNTER, 0, 5u32);
    store.set(COUNTER, 0, String::from("name"));
    assert_eq!(store.get::<u32>(COUNTER, 0), Some(&5));
    assert_eq!(
        store.get::<String>(COUNTER, 0).map(String::as_str),
        Some("name")
    );
    *store.get_mut::<u32>(COUNTER, 0).unwrap() += 1;
    assert_eq!(store.get::<u32>(COUNTER, 0), Some(&6));
    store.set(COUNTER, 0, 9u32);
    assert_eq!(store.len(), 2, "a same-type set replaces, it does not add");
    assert_eq!(store.take::<u32>(COUNTER, 0), Some(9));
    assert_eq!(store.get::<u32>(COUNTER, 0), None);
}

#[test]
fn same_id_instances_count_independently() {
    let mut store = StateStore::new();
    store.set(COUNTER, 0, 1u32);
    store.set(COUNTER, 1, 10u32);
    *store.get_mut::<u32>(COUNTER, 0).unwrap() += 1;
    assert_eq!(store.get::<u32>(COUNTER, 0), Some(&2));
    assert_eq!(store.get::<u32>(COUNTER, 1), Some(&10));
    assert_eq!(store.remove_instance(COUNTER, 0), 1);
    assert_eq!(store.get::<u32>(COUNTER, 1), Some(&10));
}

#[test]
fn a_resource_is_dropped_exactly_once_across_a_reload() {
    let drops = Arc::new(AtomicUsize::new(0));
    let mut old = StateStore::new();
    old.set_resource(COUNTER, 0, Counted(drops.clone()));
    let mut fresh = StateStore::new();
    let moved = fresh.carry_resources(&mut old, &[COUNTER]);
    assert_eq!(moved, 1);
    assert!(old.is_empty());
    assert_eq!(drops.load(Ordering::SeqCst), 0, "carried, not dropped");
    fresh.set_resource(COUNTER, 0, Counted(drops.clone()));
    assert_eq!(
        drops.load(Ordering::SeqCst),
        1,
        "the replaced one dropped once"
    );
    drop(fresh);
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    drop(old);
    assert_eq!(drops.load(Ordering::SeqCst), 2, "nothing dropped twice");
}

#[test]
fn const_config_decodes_once() {
    let mut cache: ConfigCache<String, u32> = ConfigCache::new();
    let decode = |raw: &String| raw.parse::<u32>().map_err(|_| ());
    assert_eq!(cache.load("7".to_string(), decode), Ok(&7));
    assert_eq!(cache.load("7".to_string(), decode), Ok(&7));
    assert_eq!(cache.decodes(), 1);
}

#[test]
fn config_decodes_again_only_when_the_value_changes() {
    let mut cache: ConfigCache<String, u32> = ConfigCache::new();
    let decode = |raw: &String| raw.parse::<u32>().map_err(|_| ());
    assert_eq!(cache.update(1, "3".to_string(), decode), Ok(&3));
    // A re-emitted equal value on a new version is not decoded again.
    assert_eq!(cache.update(2, "3".to_string(), decode), Ok(&3));
    assert_eq!(cache.decodes(), 1);
    assert_eq!(cache.update(3, "4".to_string(), decode), Ok(&4));
    assert_eq!(cache.decodes(), 2);
    // A failed decode keeps the previous value.
    assert_eq!(cache.update(4, "x".to_string(), decode), Err(()));
    assert_eq!(cache.update(4, "4".to_string(), decode), Ok(&4));
    assert_eq!(cache.decodes(), 2);
}

#[test]
fn snapshot_round_trips_encoded_state() {
    let mut store = StateStore::new();
    store.set_encoded(COUNTER, 0, Count(42));
    store.set_encoded(COUNTER, 1, Count(7));
    store.set_encoded(LABELS, 0, Label("a \"quoted\"\nline, é".to_string()));
    store.set(LABELS, 0, 99u8);
    let text = snapshot_json(&store);
    let codecs = [Codec::of::<Count>(), Codec::of::<Label>()];

    let mut restored = StateStore::new();
    let count = restore_json(&mut restored, &text, &[COUNTER, LABELS], &codecs).unwrap();
    assert_eq!(count, 3, "plain state is not written");
    assert_eq!(restored.get::<Count>(COUNTER, 0), Some(&Count(42)));
    assert_eq!(restored.get::<Count>(COUNTER, 1), Some(&Count(7)));
    assert_eq!(
        restored.get::<Label>(LABELS, 0),
        Some(&Label("a \"quoted\"\nline, é".to_string()))
    );
    assert_eq!(snapshot_json(&restored), text);
}

#[test]
fn snapshot_refuses_unknown_nodes_and_tags() {
    let mut store = StateStore::new();
    store.set_encoded(COUNTER, 0, Count(1));
    let text = snapshot_json(&store);
    let codecs = [Codec::of::<Count>()];
    let mut restored = StateStore::new();
    assert_eq!(
        restore_json(&mut restored, &text, &[LABELS], &codecs),
        Err(SnapshotError::UnknownNode("state.counter".to_string()))
    );
    assert_eq!(
        restore_json(&mut restored, &text, &[COUNTER], &[]),
        Err(SnapshotError::UnknownTag("count".to_string()))
    );
    assert!(matches!(
        restore_json(&mut restored, "[{", &[COUNTER], &codecs),
        Err(SnapshotError::Syntax { .. })
    ));
}
