//! Host side of the stable codecs (see [`codecs`](super::codecs)): reading a plugin's table, and
//! registering each entry as a payload codec of the host registry
//! ([`PluginRegistry::register_payload_codec`](crate::runtime::plugins::PluginRegistry::register_payload_codec)),
//! which makes its `Value` conversions planner adapters and its `to_value` the host bridge's
//! serializer of the key.
//!
//! A conversion of a plugin-local object runs on the build that made it (found by the build mark
//! in the object's header, [`local_handle_build`], among the current and retired builds), so an
//! object from before a reload is still read by its own code; a conversion from a `Value`, bytes
//! or another payload runs on the current build.

use std::ffi::c_void;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use super::codecs::{
    CODECS_VERSION, CodecFn, CodecTableFn, STABLE_CODECS_SYMBOL, StableCodecTable,
};
use super::generation::{Generation, PluginSlot};
use super::host::string_sink;
use super::value::{Arena, StableValue, tag, to_value};
use super::{StableOutputSink, builtin, status};
use crate::data::model::{TypeExpr, Value};
use crate::dylib::library::LibraryHandle;
use crate::runtime::plugins::{
    CodecFromBytesFn, CodecFromValueFn, CodecToBytesFn, CodecToValueFn, PayloadCodec,
    PluginInstallContext, PluginResult,
};
use crate::transport::{
    LocalInterface, Payload, TypeKey, frame_handle_from_v2, local_handle_build,
};

/// One entry of a plugin's codec table, read by the host.
#[derive(Clone, Debug)]
pub struct HostCodec {
    pub(super) key: TypeKey,
    pub(super) schema: TypeExpr,
    ctx: *const c_void,
    /// The build's mark (the table's [`StableCodecTable::build`]).
    pub(super) build: *const c_void,
    to_value: Option<CodecFn>,
    from_value: Option<CodecFn>,
    to_bytes: Option<CodecFn>,
    from_bytes: Option<CodecFn>,
}

// Safety: `ctx` is the plugin's immutable entry data, and `build` an address only compared; its
// functions are callable from any thread.
unsafe impl Send for HostCodec {}
// Safety: as above.
unsafe impl Sync for HostCodec {}

impl HostCodec {
    /// What a reload must keep: the key, the schema and which conversions exist.
    pub(in crate::dylib) fn interface(&self) -> (TypeKey, TypeExpr, [bool; 4]) {
        (
            self.key.clone(),
            self.schema.clone(),
            [
                self.to_value.is_some(),
                self.from_value.is_some(),
                self.to_bytes.is_some(),
                self.from_bytes.is_some(),
            ],
        )
    }
}

/// The entries of `table`, a version-`version` plugin's.
///
/// # Safety
/// `table` must be null or the plugin's table, valid while its library is loaded.
pub(super) unsafe fn read_table(
    table: *const StableCodecTable,
    version: u32,
) -> Result<Vec<HostCodec>, String> {
    // Safety: forwarded from the caller.
    let Some(table) = (unsafe { table.as_ref() }) else {
        return Err("the plugin could not build its codec table".into());
    };
    if table.version != version {
        return Err(format!(
            "codec table of version {}, descriptor of version {version}",
            table.version
        ));
    }
    if table.len == 0 {
        return Ok(Vec::new());
    }
    if table.entries.is_null() {
        return Err("null codec entries".into());
    }
    // Safety: the plugin's table holds `len` entries for the library's life.
    let entries = unsafe { std::slice::from_raw_parts(table.entries, table.len) };
    entries
        .iter()
        .map(|entry| {
            let key = entry.key.as_str().ok_or("a codec without a key")?;
            let schema = entry.schema.as_str().ok_or("a codec without a schema")?;
            let schema: TypeExpr = serde_json::from_str(schema)
                .map_err(|err| format!("codec of `{key}`: schema: {err}"))?;
            Ok(HostCodec {
                key: TypeKey::new(key),
                schema,
                ctx: entry.ctx,
                build: table.build,
                to_value: entry.to_value,
                from_value: entry.from_value,
                to_bytes: entry.to_bytes,
                from_bytes: entry.from_bytes,
            })
        })
        .collect()
}

/// The codec table a version-`version` plugin library exports (none before version 7).
///
/// # Safety
/// `library` must be a loaded plugin library whose codec symbol, when present, is a
/// [`CodecTableFn`].
pub(in crate::dylib) unsafe fn load_codecs(
    library: &LibraryHandle,
    version: u32,
) -> Result<Vec<HostCodec>, String> {
    if !(CODECS_VERSION..=super::STABLE_ABI_VERSION).contains(&version) {
        return Ok(Vec::new());
    }
    // Safety: the symbol's signature is `CodecTableFn` (see above).
    let table = unsafe {
        library
            .library()
            .get::<CodecTableFn>(STABLE_CODECS_SYMBOL.as_bytes())
    };
    // The table is optional: a plugin exporting its symbols itself may have none.
    let Ok(table) = table.map(|sym| *sym) else {
        return Ok(Vec::new());
    };
    // Safety: the plugin's own function; its table lives as long as the library.
    unsafe { read_table(table(), version) }
}

/// Register the codecs of `slot`'s current build into the host registry.
pub(super) fn register(
    ctx: &mut PluginInstallContext<'_>,
    slot: &Arc<PluginSlot>,
) -> PluginResult<()> {
    let generation = slot.current();
    for codec in &generation.codecs {
        let mut erased = PayloadCodec::new(codec.schema.clone());
        let key = codec.key.clone();
        if codec.to_value.is_some() {
            let (slot, key) = (slot.clone(), key.clone());
            erased.to_value = Some(Arc::new(move |payload: &Payload| {
                let mut out = None;
                convert_payload(
                    &slot,
                    &key,
                    payload,
                    |c| c.to_value,
                    |value| {
                        // Safety: the plugin's value, valid for the push.
                        out = Some(unsafe { to_value(value) }?);
                        Ok(())
                    },
                )?;
                out.ok_or_else(|| "the codec pushed no value".to_string())
            }) as CodecToValueFn);
        }
        if codec.to_bytes.is_some() {
            let (slot, key) = (slot.clone(), key.clone());
            erased.to_bytes = Some(Arc::new(move |payload: &Payload| {
                let mut out = None;
                convert_payload(
                    &slot,
                    &key,
                    payload,
                    |c| c.to_bytes,
                    |value| {
                        if value.tag != tag::BYTES {
                            return Err(format!("expected bytes, found tag {}", value.tag));
                        }
                        // Safety: as above.
                        out = Some(unsafe { value.as_bytes() }.to_vec());
                        Ok(())
                    },
                )?;
                out.ok_or_else(|| "the codec pushed no bytes".to_string())
            }) as CodecToBytesFn);
        }
        if codec.from_value.is_some() {
            let slot = slot.clone();
            erased.from_value = Some(Arc::new(move |key: TypeKey, value: &Value| {
                let mut arena = Arena::default();
                let input = arena.encode(value);
                build(&slot, key, &input, |c| c.from_value)
            }) as CodecFromValueFn);
        }
        if codec.from_bytes.is_some() {
            let slot = slot.clone();
            erased.from_bytes = Some(Arc::new(move |key: TypeKey, bytes: &[u8]| {
                build(&slot, key, &StableValue::bytes(bytes), |c| c.from_bytes)
            }) as CodecFromBytesFn);
        }
        ctx.register_payload_codec(key, erased)?;
    }
    Ok(())
}

/// The build of `slot` that made `payload`, when it is one of its plugin-local objects: a
/// retired build stays loaded while any of its objects is alive, so the build is still there.
fn maker(slot: &PluginSlot, payload: &Payload) -> Option<Arc<Generation>> {
    let build = local_handle_build(payload.foreign_handle()?)?;
    slot.find(|generation| {
        generation
            .codecs
            .first()
            .is_some_and(|codec| std::ptr::addr_eq(codec.build, build))
    })
}

/// The `(ctx, function)` `pick` selects from `generation`'s codec of `key`.
fn function(
    generation: &Generation,
    key: &TypeKey,
    pick: impl Fn(&HostCodec) -> Option<CodecFn>,
) -> Result<(*const c_void, CodecFn), String> {
    let codec = generation
        .codecs
        .iter()
        .find(|codec| &codec.key == key)
        .ok_or_else(|| format!("build {} has no codec of `{key}`", generation.number))?;
    let f = pick(codec).ok_or_else(|| format!("the codec of `{key}` lacks this conversion"))?;
    Ok((codec.ctx, f))
}

/// Convert `payload` (of `key`) on the build that made it, else the current one.
fn convert_payload(
    slot: &PluginSlot,
    key: &TypeKey,
    payload: &Payload,
    pick: impl Fn(&HostCodec) -> Option<CodecFn>,
    take: impl FnMut(&StableValue) -> Result<(), String>,
) -> Result<(), String> {
    let generation = maker(slot, payload).unwrap_or_else(|| slot.current());
    let (ctx, f) = function(&generation, key, pick)?;
    let mut arena = Arena::default();
    let input = encode_payload(payload, &mut arena)?;
    call(f, ctx, &input, take)
}

/// Build a payload of `key` from `input` on the current build.
fn build(
    slot: &PluginSlot,
    key: TypeKey,
    input: &StableValue,
    pick: impl Fn(&HostCodec) -> Option<CodecFn>,
) -> Result<Payload, String> {
    let generation = slot.current();
    let (ctx, f) = function(&generation, &key, pick)?;
    let mut out = None;
    call(f, ctx, input, |value| {
        // Safety: the plugin's value, valid for the push.
        out = Some(unsafe { returned(&generation, key.clone(), value) }?);
        Ok(())
    })?;
    out.ok_or_else(|| "the codec pushed no value".to_string())
}

/// `payload` for a codec: its foreign handle (a plugin-local object as it is), else a `Value` or
/// a builtin.
fn encode_payload(payload: &Payload, arena: &mut Arena) -> Result<StableValue, String> {
    if let Some(value) = arena.encode_foreign(payload) {
        return Ok(value);
    }
    let any = payload.value_any_sync();
    if let Some(value) = any.and_then(|any| any.downcast_ref::<Value>()) {
        return Ok(arena.encode(value));
    }
    any.and_then(|any| builtin::encode(any)).ok_or_else(|| {
        format!(
            "a `{}` payload of `{}` cannot cross to the plugin's codec",
            payload.storage_rust_type_name().unwrap_or("bytes"),
            payload.type_key()
        )
    })
}

/// A payload of `key` from a value `generation` pushed: its handle (as a node's output: a
/// plugin-local object of a build that counts them is a plain handle, any other handle the
/// plugin made pins it), else a `Value`.
///
/// # Safety
/// Every pointer in `value` must be valid for the call.
unsafe fn returned(
    generation: &Arc<Generation>,
    key: TypeKey,
    value: &StableValue,
) -> Result<Payload, String> {
    // Safety (throughout): forwarded from the caller.
    let Some(handle) = (unsafe { value.as_handle() }) else {
        return Ok(Payload::owned(key, unsafe { to_value(value) }?));
    };
    if value.from_sender() {
        if generation.counts_objects() && handle.view::<LocalInterface>().is_ok() {
            // Its last release may run on any host thread (see `Generation::lend_local`).
            generation.lend_local();
        } else {
            generation.pin();
        }
    }
    Ok(Payload::foreign(
        key,
        frame_handle_from_v2(handle.clone()),
        value.residency(),
    ))
}

/// What the output sink of one codec call writes to.
struct Collect<'a> {
    take: &'a mut dyn FnMut(&StableValue) -> Result<(), String>,
    error: Option<String>,
}

unsafe extern "C" fn collect_push(ctx: *mut c_void, _port: u32, value: *const StableValue) -> bool {
    // Safety: `ctx` is the running call's `Collect`; the plugin passes a value valid for the push.
    let (collect, value) = unsafe { (&mut *ctx.cast::<Collect<'_>>(), value.as_ref()) };
    let result = catch_unwind(AssertUnwindSafe(|| {
        (collect.take)(value.ok_or("null codec value")?)
    }));
    let error = match result {
        Ok(Ok(())) => return true,
        Ok(Err(error)) => error,
        Err(_) => "reading a codec value panicked".to_string(),
    };
    collect.error.get_or_insert(error);
    false
}

/// Call `f` on `input`, handing the value it pushes to `take`.
fn call(
    f: CodecFn,
    ctx: *const c_void,
    input: &StableValue,
    mut take: impl FnMut(&StableValue) -> Result<(), String>,
) -> Result<(), String> {
    let mut collect = Collect {
        take: &mut take,
        error: None,
    };
    let sink = StableOutputSink {
        // Mutable provenance: the push callback writes through this pointer.
        ctx: (&mut collect as *mut Collect<'_>).cast::<c_void>(),
        push: Some(collect_push),
    };
    let mut message: Option<String> = None;
    // Safety: the entry's function and `ctx`, whose library the caller's generation keeps
    // loaded; `input` and the sink outlive the call.
    let code = unsafe { f(ctx, input, sink, string_sink(&mut message)) };
    if code == status::OK {
        return Ok(());
    }
    Err(collect
        .error
        .or(message)
        .unwrap_or_else(|| format!("codec failed with status {code}")))
}
