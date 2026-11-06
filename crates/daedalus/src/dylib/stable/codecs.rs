//! Stable codecs (stable ABI 7): a plugin's per-type-key conversions to and from a
//! [`StableValue`] and bytes, exported as a table of C function pointers so a host can convert
//! the plugin's objects without their Rust types.
//!
//! **Plugin side.** [`export_plugin!`](crate::export_plugin) exports
//! [`STABLE_CODECS_SYMBOL`], a [`CodecTableFn`] returning this build's [`StableCodecTable`]: one
//! [`StableCodecEntry`] per codec the plugin registered
//! ([`PluginRegistry::register_value_codec`](crate::runtime::plugins::PluginRegistry::register_value_codec),
//! [`register_local_codec`](crate::runtime::plugins::PluginRegistry::register_local_codec)),
//! built once from the plugin's registry and kept for the library's life. Each entry's
//! [`CodecFn`]s read their input as the node path does (a `Local<T>` from this build's handle,
//! other handles as foreign payloads, anything else as a `Value`) and push one value to the
//! output sink: a `Value` (`to_value`), bytes (`to_bytes`), or the built object, lent by handle
//! for a `Local<T>` (`from_value`, `from_bytes`).
//!
//! **Host side.** The loader reads the table of a version-7 plugin (an older plugin has none);
//! `codec_host` registers each entry as a payload codec of the host
//! registry, so the planner inserts it as an adapter and the host bridge serializes with it.

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::OnceLock;

use super::plugin::{Failure, guarded};
use super::value::{Arena, StableValue, tag, to_value};
use super::{StableOutputSink, builtin, pod, status};
use crate::data::model::Value;
use crate::dylib::__support::LinkedDep;
use crate::dylib::{StrSink, StrView, extract};
use crate::runtime::plugins::{PayloadCodec, Plugin};
use crate::transport::{Payload, PayloadSlot, Residency, TypeKey};

/// The symbol of a plugin's [`CodecTableFn`] (exported since stable ABI 7).
pub const STABLE_CODECS_SYMBOL: &str = "daedalus_plugin_stable_codecs";

/// The first [`super::STABLE_ABI_VERSION`] with a codec table.
pub const CODECS_VERSION: u32 = 7;

/// One conversion: reads `input` (borrowed for the call) and pushes exactly one value to
/// `output` (port 0, borrowed for the push); returns a [`status`] code, writing a message to
/// `error` unless it is `OK`. `ctx` is the entry's.
pub type CodecFn = unsafe extern "C" fn(
    ctx: *const c_void,
    input: *const StableValue,
    output: StableOutputSink,
    error: StrSink,
) -> u32;

/// The codec of one type key.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StableCodecEntry {
    /// The type key whose payloads it converts.
    pub key: StrView,
    /// The JSON `TypeExpr` of the `Value`s it converts to and from.
    pub schema: StrView,
    /// Passed back to every function of the entry.
    pub ctx: *const c_void,
    /// The key's payload (a handle of it, or its value) to a `Value` of the schema.
    pub to_value: Option<CodecFn>,
    /// A `Value` of the schema to the key's payload.
    pub from_value: Option<CodecFn>,
    /// The key's payload to bytes (a [`tag::BYTES`] value).
    pub to_bytes: Option<CodecFn>,
    /// Bytes ([`tag::BYTES`]) to the key's payload.
    pub from_bytes: Option<CodecFn>,
}

/// A plugin's codecs, valid while its library is loaded.
#[repr(C)]
#[derive(Copy, Clone, Debug)]
pub struct StableCodecTable {
    /// [`super::STABLE_ABI_VERSION`] of the build that exports it.
    pub version: u32,
    pub entries: *const StableCodecEntry,
    pub len: usize,
    /// The build's mark ([`local_build_mark`](crate::transport::local_build_mark)): the host
    /// finds the build that made a plugin-local handle by it, and converts the handle there.
    pub build: *const c_void,
}

/// The function exported as [`STABLE_CODECS_SYMBOL`]: this build's table, or null when its
/// registry could not be built.
pub type CodecTableFn = unsafe extern "C" fn() -> *const StableCodecTable;

/// One exported codec: what an entry's `ctx` points to.
struct PluginCodec {
    key: TypeKey,
    codec: PayloadCodec,
}

/// The table and what it points into; never moved or changed once built.
struct BuiltTable {
    table: StableCodecTable,
    _entries: Vec<StableCodecEntry>,
    /// The entries' `ctx`s point into this buffer, which is never resized.
    _codecs: Vec<PluginCodec>,
}

// Safety: the pointers are into the table's own buffers and leaked strings, which it never
// changes.
unsafe impl Send for BuiltTable {}
// Safety: as above; shared reads only.
unsafe impl Sync for BuiltTable {}

/// Where [`export_plugin!`](crate::export_plugin) keeps a plugin's codec table: built on first
/// request and kept for the library's life (unlike the stable runtime, a reload never asks this
/// build again).
pub struct CodecTableCell(OnceLock<Option<BuiltTable>>);

impl CodecTableCell {
    pub const fn new() -> Self {
        Self(OnceLock::new())
    }
}

impl Default for CodecTableCell {
    fn default() -> Self {
        Self::new()
    }
}

fn leak(text: String) -> StrView {
    StrView::from_static(Box::leak(text.into_boxed_str()))
}

fn build<P: Plugin + Default>(deps: &[LinkedDep]) -> Result<BuiltTable, String> {
    let (registry, _) = extract::registry::<P>(deps, false)?;
    let codecs: Vec<PluginCodec> = registry
        .payload_codecs()
        .map(|(key, codec)| PluginCodec {
            key: key.clone(),
            codec: codec.clone(),
        })
        .collect();
    let mut entries = Vec::with_capacity(codecs.len());
    for codec in &codecs {
        let schema = serde_json::to_string(&codec.codec.schema)
            .map_err(|err| format!("codec of `{}`: {err}", codec.key))?;
        let pick = |present: bool, f: CodecFn| present.then_some(f);
        entries.push(StableCodecEntry {
            key: leak(codec.key.to_string()),
            schema: leak(schema),
            ctx: (codec as *const PluginCodec).cast(),
            to_value: pick(codec.codec.to_value.is_some(), to_value_entry),
            from_value: pick(codec.codec.from_value.is_some(), from_value_entry),
            to_bytes: pick(codec.codec.to_bytes.is_some(), to_bytes_entry),
            from_bytes: pick(codec.codec.from_bytes.is_some(), from_bytes_entry),
        });
    }
    Ok(BuiltTable {
        table: StableCodecTable {
            version: super::STABLE_ABI_VERSION,
            entries: entries.as_ptr(),
            len: entries.len(),
            build: crate::transport::local_build_mark(),
        },
        _entries: entries,
        _codecs: codecs,
    })
}

/// The codec table of plugin `P` (see [`CodecTableFn`]).
pub fn codec_table<P: Plugin + Default>(
    cell: &CodecTableCell,
    deps: &[LinkedDep],
) -> *const StableCodecTable {
    let built = catch_unwind(AssertUnwindSafe(|| {
        cell.0.get_or_init(|| build::<P>(deps).ok()).as_ref()
    }));
    match built {
        Ok(Some(built)) => &built.table,
        _ => std::ptr::null(),
    }
}

#[derive(Clone, Copy)]
enum Op {
    ToValue,
    FromValue,
    ToBytes,
    FromBytes,
}

unsafe extern "C" fn to_value_entry(
    ctx: *const c_void,
    input: *const StableValue,
    output: StableOutputSink,
    error: StrSink,
) -> u32 {
    // Safety: the host follows `CodecFn`'s contract.
    unsafe { run(Op::ToValue, ctx, input, output, error) }
}

unsafe extern "C" fn from_value_entry(
    ctx: *const c_void,
    input: *const StableValue,
    output: StableOutputSink,
    error: StrSink,
) -> u32 {
    // Safety: the host follows `CodecFn`'s contract.
    unsafe { run(Op::FromValue, ctx, input, output, error) }
}

unsafe extern "C" fn to_bytes_entry(
    ctx: *const c_void,
    input: *const StableValue,
    output: StableOutputSink,
    error: StrSink,
) -> u32 {
    // Safety: the host follows `CodecFn`'s contract.
    unsafe { run(Op::ToBytes, ctx, input, output, error) }
}

unsafe extern "C" fn from_bytes_entry(
    ctx: *const c_void,
    input: *const StableValue,
    output: StableOutputSink,
    error: StrSink,
) -> u32 {
    // Safety: the host follows `CodecFn`'s contract.
    unsafe { run(Op::FromBytes, ctx, input, output, error) }
}

fn failed(message: impl Into<String>) -> Failure {
    (status::FAILED, message.into())
}

fn missing(codec: &PluginCodec, what: &str) -> Failure {
    failed(format!("the codec of `{}` has no {what}", codec.key))
}

/// # Safety
/// The arguments must follow [`CodecFn`]'s contract, `ctx` being an entry's of this build.
unsafe fn run(
    op: Op,
    ctx: *const c_void,
    input: *const StableValue,
    output: StableOutputSink,
    error: StrSink,
) -> u32 {
    // Safety: forwarded from the caller.
    unsafe {
        guarded(error, || {
            // Safety: an entry's `ctx`, a `PluginCodec` the table keeps.
            let codec = ctx
                .cast::<PluginCodec>()
                .as_ref()
                .ok_or_else(|| failed("null codec"))?;
            let input = input.as_ref().ok_or_else(|| failed("null input"))?;
            let push = output.push.ok_or_else(|| failed("no output sink"))?;
            let handler = |message: String| (status::HANDLER_ERROR, message);
            let invalid = |message: String| (status::INVALID_INPUT, message);
            let key = codec.key.clone();
            let mut arena = Arena::default();
            let mut slot = PayloadSlot::default();
            // What the pushed value borrows, kept until the push returns.
            let bytes;
            let built;
            let value = match op {
                Op::ToValue => {
                    let f = codec.codec.to_value.as_ref();
                    let f = f.ok_or_else(|| missing(codec, "to_value"))?;
                    // Safety: the host's input, valid for the call.
                    let payload = decode(codec, &mut slot, input).map_err(invalid)?;
                    arena.encode_owned(f(&payload).map_err(handler)?)
                }
                Op::ToBytes => {
                    let f = codec.codec.to_bytes.as_ref();
                    let f = f.ok_or_else(|| missing(codec, "to_bytes"))?;
                    // Safety: as above.
                    let payload = decode(codec, &mut slot, input).map_err(invalid)?;
                    bytes = f(&payload).map_err(handler)?;
                    StableValue::bytes(&bytes)
                }
                Op::FromValue => {
                    let f = codec.codec.from_value.as_ref();
                    let f = f.ok_or_else(|| missing(codec, "from_value"))?;
                    // Safety: as above.
                    let value = to_value(input).map_err(invalid)?;
                    built = f(key, &value).map_err(handler)?;
                    encode(codec, &built, &mut arena).map_err(failed)?
                }
                Op::FromBytes => {
                    let f = codec.codec.from_bytes.as_ref();
                    let f = f.ok_or_else(|| missing(codec, "from_bytes"))?;
                    if input.tag != tag::BYTES {
                        return Err(invalid(format!("expected bytes, found tag {}", input.tag)));
                    }
                    // Safety: a bytes value of the host's, valid for the call.
                    built = f(key, input.as_bytes()).map_err(handler)?;
                    encode(codec, &built, &mut arena).map_err(failed)?
                }
            };
            // Safety: the host's sink for this call; the value borrows locals that outlive it.
            if !push(output.ctx, 0, &value) {
                return Err(failed("the host rejected the value"));
            }
            Ok(())
        })
    }
}

/// The payload `value` stands for: this build's `Local<T>` by handle, another handle as a
/// foreign payload, a `StablePod` view as a `PodValue`, anything else as a `Value`.
///
/// # Safety
/// Every pointer in `value` must be valid for the call.
unsafe fn decode(
    codec: &PluginCodec,
    slot: &mut PayloadSlot,
    value: &StableValue,
) -> Result<Payload, String> {
    let key = codec.key.clone();
    // Safety (throughout): forwarded from the caller.
    if let Some(handle) = unsafe { value.as_handle() } {
        if let Some(local) = &codec.codec.local {
            return (local.decode)(slot, key, handle).map_err(|err| err.to_string());
        }
        return Ok(slot.foreign(key, handle.clone(), value.residency(), false));
    }
    if value.tag == tag::POD {
        return unsafe { pod::decode_value(key, slot, value) };
    }
    Ok(Payload::owned(key, unsafe { to_value(value) }?))
}

/// `payload` for the host: a `Local<T>` lent by handle (the host keeps this build loaded while
/// it holds it), another foreign payload's handle, a `Value` or a builtin.
fn encode(
    codec: &PluginCodec,
    payload: &Payload,
    arena: &mut Arena,
) -> Result<StableValue, String> {
    let any = payload.value_any_sync();
    if let (Some(local), Some(any)) = (&codec.codec.local, any)
        && let Some(handle) = (local.to_handle)(any)
    {
        return Ok(arena.lend(handle, Residency::Cpu, true));
    }
    if let Some(value) = arena.encode_foreign(payload) {
        return Ok(value);
    }
    let any = any.ok_or_else(|| format!("the codec of `{}` built no value", codec.key))?;
    if let Some(value) = any.downcast_ref::<Value>() {
        return Ok(arena.encode(value));
    }
    builtin::encode(any).ok_or_else(|| {
        format!(
            "the codec of `{}` built `{}`, which has no stable encoding",
            codec.key,
            payload.storage_rust_type_name().unwrap_or("bytes")
        )
    })
}
