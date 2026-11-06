//! Plugin-local objects: a `Local<Tracker>` made once and kept in node state, carried by handle
//! along graph edges to other nodes of this plugin and read back as the same object (the facade's
//! `dylib_stable_local` test). Its codec ([`install_codecs`]) turns a tracker into a
//! `{ id, hits }` struct `Value` and back, and into 16 bytes and back, so a host converts one
//! without its Rust type (`dylib_stable_codecs`).

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use daedalus::data::model::{StructField, StructFieldValue, TypeExpr, Value, ValueType};
use daedalus::macros::node;
use daedalus::runtime::NodeError;
use daedalus::runtime::plugins::{PluginInstallContext, PluginResult, ValueCodec};
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

/// The `Value` type of a tracker: `{ id: Int, hits: Int }`.
pub fn tracker_schema() -> TypeExpr {
    let int = || TypeExpr::Scalar(ValueType::Int);
    TypeExpr::Struct(vec![
        StructField {
            name: "id".into(),
            ty: int(),
        },
        StructField {
            name: "hits".into(),
            ty: int(),
        },
    ])
}

fn tracker_to_value(tracker: &Tracker) -> Result<Value, String> {
    let field = |name: &str, value: i64| StructFieldValue {
        name: name.into(),
        value: Value::Int(value),
    };
    Ok(Value::Struct(vec![
        field("id", tracker.id),
        field("hits", tracker.hits.load(Ordering::Relaxed) as i64),
    ]))
}

fn tracker_from_value(value: &Value) -> Result<Tracker, String> {
    let Value::Struct(fields) = value else {
        return Err(format!("a tracker is a struct, found {value:?}"));
    };
    let field = |name: &str| match fields.iter().find(|field| field.name == name) {
        Some(StructFieldValue {
            value: Value::Int(value),
            ..
        }) => Ok(*value),
        _ => Err(format!("a tracker needs an int `{name}`")),
    };
    Ok(Tracker {
        id: field("id")?,
        hits: AtomicU64::new(field("hits")? as u64),
    })
}

fn tracker_to_bytes(tracker: &Tracker) -> Result<Vec<u8>, String> {
    let hits = tracker.hits.load(Ordering::Relaxed);
    Ok([tracker.id.to_le_bytes(), hits.to_le_bytes()].concat())
}

fn tracker_from_bytes(bytes: &[u8]) -> Result<Tracker, String> {
    let (id, hits) = bytes
        .split_first_chunk::<8>()
        .and_then(|(id, rest)| Some((id, rest.first_chunk::<8>()?)))
        .filter(|_| bytes.len() == 16)
        .ok_or_else(|| format!("a tracker is 16 bytes, found {}", bytes.len()))?;
    Ok(Tracker {
        id: i64::from_le_bytes(*id),
        hits: AtomicU64::new(u64::from_le_bytes(*hits)),
    })
}

/// The plugin's install hook: the tracker's codec.
pub fn install_codecs(registry: &mut PluginInstallContext<'_>) -> PluginResult<()> {
    registry.register_local_codec::<Tracker>(
        TRACKER_KEY,
        ValueCodec::new(tracker_schema())
            .to_value(tracker_to_value)
            .from_value(tracker_from_value)
            .to_bytes(tracker_to_bytes)
            .from_bytes(tracker_from_bytes),
    )
}
