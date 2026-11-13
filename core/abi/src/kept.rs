//! Somewhere to keep what a step gives [`StepCx::detail`], for a caller that runs steps on a
//! context of its own and isn't allowed `unsafe`: a worker that serves a plugin's nodes, a test.
//! A host that runs a plan keeps details per step through the runner instead.

#![cfg(feature = "std")]

use std::boxed::Box;
use std::string::String;
use std::sync::{Mutex, MutexGuard};

use crate::{DetailSink, StepCx};

/// What the steps run on the contexts this is attached to have written.
pub struct KeptDetail(Mutex<String>);

impl KeptDetail {
    /// A place that is never freed, so it outlives every context it is attached to. A process
    /// makes one and uses it for as long as it runs.
    pub fn forever() -> &'static KeptDetail {
        Box::leak(Box::new(KeptDetail(Mutex::new(String::new()))))
    }

    /// Has the steps run on `cx` write here.
    pub fn attach(&'static self, cx: &mut StepCx) {
        cx.detail = DetailSink {
            context: core::ptr::from_ref(self).cast_mut().cast(),
            write: Some(write),
        };
    }

    fn held(&self) -> MutexGuard<'_, String> {
        match self.0.lock() {
            Ok(text) => text,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    /// Everything written so far, in order.
    pub fn text(&self) -> String {
        self.held().clone()
    }

    /// Everything written so far, in order, leaving nothing.
    pub fn take(&self) -> String {
        core::mem::take(&mut *self.held())
    }
}

extern "C" fn write(context: *mut core::ffi::c_void, _: u32, text: *const u8, len: usize) {
    if context.is_null() || text.is_null() {
        return;
    }
    // SAFETY: `context` is the `KeptDetail` that `attach` set, which is never freed, and `text`
    // is `len` bytes the caller holds for this call.
    let (kept, bytes) = unsafe {
        (
            &*context.cast::<KeptDetail>(),
            core::slice::from_raw_parts(text, len),
        )
    };
    kept.held().push_str(&String::from_utf8_lossy(bytes));
}

/// Gives `cx` a new place to keep what a step writes with [`StepCx::detail`], and hands it back
/// to read. It is never freed: a test's cost.
pub fn keep_detail(cx: &mut StepCx) -> &'static KeptDetail {
    let kept = KeptDetail::forever();
    kept.attach(cx);
    kept
}
