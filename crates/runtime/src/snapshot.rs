//! Snapshot/restore scaffolding (feature-gated via `snapshots` when wiring real storage).
use crate::state::StateStore;

/// Represents a serialized snapshot of runtime state.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub state: String,
}

impl Snapshot {
    pub fn new(state: String) -> Self {
        Self { state }
    }
}

/// Snapshot manager; currently JSON-serializes the StateStore for deterministic tests.
pub struct SnapshotManager {
    store: StateStore,
}

impl SnapshotManager {
    pub fn new(store: StateStore) -> Self {
        Self { store }
    }

    pub fn take(&self) -> Snapshot {
        let state = self.store.dump_json().unwrap_or_else(|_| "{}".into());
        Snapshot::new(state)
    }

    pub fn restore(&self, snapshot: &Snapshot) {
        let _ = self.store.load_json(&snapshot.state);
    }
}
