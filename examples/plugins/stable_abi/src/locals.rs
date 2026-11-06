//! Plugin-local objects: a `Local<Tracker>` made once and kept in node state, carried by handle
//! along graph edges to other nodes of this plugin and read back as the same object (the facade's
//! `dylib_stable_local` test).

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use daedalus::macros::node;
use daedalus::runtime::NodeError;
use daedalus::transport::Local;

/// The port key of a `Local<Tracker>`.
pub const TRACKER_KEY: &str = "stable_abi:tracker";

/// Trackers of this build dropped so far.
static DROPS: AtomicI64 = AtomicI64::new(0);

/// An object only this plugin reads: an id and a hit count, changed in place.
#[derive(Debug)]
pub struct Tracker {
    pub id: i64,
    pub hits: AtomicU64,
}

impl Drop for Tracker {
    fn drop(&mut self) {
        DROPS.fetch_add(1, Ordering::Relaxed);
    }
}

/// The tracker `local_make` keeps.
#[derive(Default)]
pub struct Made(Option<Local<Tracker>>);

/// Makes a tracker on the first call, keeps it, and hands out the same object every tick.
#[node(
    id = "local_make",
    inputs("id"),
    outputs(port(name = "obj", type_key = "stable_abi:tracker")),
    state(Made)
)]
pub fn local_make(id: i64, state: &mut Made) -> Result<Local<Tracker>, NodeError> {
    Ok(state
        .0
        .get_or_insert_with(|| {
            Local::new(Tracker {
                id,
                hits: AtomicU64::new(0),
            })
        })
        .clone())
}

/// Counts a hit on the tracker and passes the same object on, with its address.
#[node(
    id = "local_touch",
    inputs(port(name = "obj", type_key = "stable_abi:tracker")),
    outputs(port(name = "obj", type_key = "stable_abi:tracker"), "ptr")
)]
pub fn local_touch(obj: &Local<Tracker>) -> Result<(Local<Tracker>, i64), NodeError> {
    obj.hits.fetch_add(1, Ordering::Relaxed);
    Ok((obj.clone(), obj.as_ptr() as i64))
}

/// The tracker's id, hits and address.
#[node(
    id = "local_read",
    inputs(port(name = "obj", type_key = "stable_abi:tracker")),
    outputs("id", "hits", "ptr")
)]
pub fn local_read(obj: &Local<Tracker>) -> Result<(i64, i64, i64), NodeError> {
    Ok((
        obj.id,
        obj.hits.load(Ordering::Relaxed) as i64,
        obj.as_ptr() as i64,
    ))
}

/// The trackers this build dropped so far.
#[node(id = "local_drops", inputs("x"), outputs("drops"))]
pub fn local_drops(x: i64) -> Result<i64, NodeError> {
    Ok(x + DROPS.load(Ordering::Relaxed))
}
