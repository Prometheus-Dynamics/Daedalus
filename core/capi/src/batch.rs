//! Many pushes in one call: all of them, or none.

use crate::Handle;
use crate::graph::GRAPHS;
use crate::guard::{fail, guard};
use crate::handles::lock;
use crate::io::{Port, Pushed, caller_bytes, check, push};
use crate::status::Status;

/// The kinds of value a batch entry holds.
pub const KIND_I64: u32 = 0;
pub const KIND_F64: u32 = 1;
pub const KIND_BOOL: u32 = 2;
pub const KIND_BYTES: u32 = 3;

/// Bytes a caller owns.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct ByteSpan {
    pub data: *const u8,
    pub len: usize,
}

/// One entry's value. Which member is read is the entry's `kind`.
#[repr(C)]
#[derive(Clone, Copy)]
pub union PushValue {
    pub i64_value: i64,
    pub f64_value: f64,
    pub bool_value: u8,
    pub bytes: ByteSpan,
}

/// One push of a batch: a port by its index, and a value.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Push {
    pub port: usize,
    pub kind: u32,
    pub value: PushValue,
}

/// What an entry pushes.
///
/// # Safety
/// The member `kind` names was written, and a `bytes` member's pointer is null or readable.
unsafe fn pushed<'a>(entry: &Push) -> Result<Pushed<'a>, Status> {
    // SAFETY: the caller wrote the member its `kind` names.
    unsafe {
        match entry.kind {
            KIND_I64 => Ok(Pushed::I64(entry.value.i64_value)),
            KIND_F64 => Ok(Pushed::F64(entry.value.f64_value)),
            KIND_BOOL => Ok(Pushed::Bool(entry.value.bool_value != 0)),
            KIND_BYTES => {
                caller_bytes(entry.value.bytes.data, entry.value.bytes.len).map(Pushed::Bytes)
            }
            _ => Err(Status::TYPE_MISMATCH),
        }
    }
}

/// Pushes `count` values, or none of them. Every entry is checked first: its port, its
/// direction, its type, its range. If one can't be pushed nothing is, the status is
/// `DAEDALUS_BAD_BATCH`, and `bad`, when not null, gets the first such entry's position.
///
/// # Safety
/// `pushes` points to `count` entries, each with the member its `kind` names written, and
/// `bad` is null or points to a `size_t`.
// SAFETY: the name is this library\'s own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_push_batch(
    graph: Handle,
    pushes: *const Push,
    count: usize,
    bad: *mut usize,
) -> Status {
    guard(|| {
        if count == 0 {
            return Status::OK;
        }
        if pushes.is_null() {
            return Status::NULL_POINTER;
        }
        // SAFETY: not null, and the caller says `count` entries are readable for the call.
        let pushes = unsafe { std::slice::from_raw_parts(pushes, count) };
        let entry = match GRAPHS.get(graph) {
            Ok(entry) => entry,
            Err(status) => return status,
        };
        let mut entry = lock(&entry);
        for (at, one) in pushes.iter().enumerate() {
            // SAFETY: the caller wrote each entry as its `kind` says.
            let checked = unsafe { pushed(one) }
                .and_then(|value| check(entry.loaded.host(), Port::Index(one.port), value));
            if let Err(status) = checked {
                if !bad.is_null() {
                    // SAFETY: not null, and the caller says it points to a `size_t`.
                    unsafe { bad.write(at) };
                }
                return fail(
                    Status::BAD_BATCH,
                    format!("entry {at}: {}", status.name().trim_end_matches('\0')),
                );
            }
        }
        for one in pushes {
            // SAFETY: checked above, under the same contract.
            let status = match unsafe { pushed(one) } {
                Ok(value) => push(&mut entry, Port::Index(one.port), value),
                Err(status) => status,
            };
            if status != Status::OK {
                return status;
            }
        }
        Status::OK
    })
}
