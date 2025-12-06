use daedalus_runtime::snapshot::SnapshotManager;
use daedalus_runtime::state::StateStore;
use serde_json::json;

#[test]
fn snapshot_round_trip() {
    let store = StateStore::default();
    let _ = store.set("k", json!(1));
    let mgr = SnapshotManager::new(store.clone());
    let snap = mgr.take();
    // mutate
    let _ = store.set("k", json!(2));
    mgr.restore(&snap);
    assert_eq!(store.get("k"), Some(json!(1)));
}
