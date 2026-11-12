//! Merged chains, kept by content.
//!
//! A merged chain becomes a [`ShaderSpec`], whose text has to live as long as anything might
//! load it. Each distinct chain is generated once per process and kept, so compiling the same
//! chain again, on this device or after a lost one is replaced, finds it. The pipeline itself is
//! cached per device, under the spec's own key.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};

use super::{ElementSpec, Form, chain_key, generate, layout};
use crate::shader::{BindingSpec, Dispatch, ShaderSpec};

/// Chains this process has generated, by [`chain_key`].
static MERGED: Mutex<Option<HashMap<u64, &'static ShaderSpec>>> = Mutex::new(None);

/// The compute shader for `chain`, generated the first time this content is seen.
pub(crate) fn merged(chain: &[&ElementSpec]) -> &'static ShaderSpec {
    let key = chain_key(chain, Form::Compute);
    let mut kept = MERGED.lock().unwrap_or_else(PoisonError::into_inner);
    let kept = kept.get_or_insert_default();
    if let Some(found) = kept.get(&key) {
        return found;
    }
    let names: Vec<&str> = chain.iter().map(|spec| spec.name).collect();
    let bindings: Vec<BindingSpec> = layout(chain)
        .into_iter()
        .enumerate()
        .map(|(index, (_, kind, name))| BindingSpec {
            index: index as u32,
            name: name.leak(),
            kind,
        })
        .collect();
    let stride = match bindings.get(1).map(|binding| binding.kind) {
        Some(crate::shader::BindingKind::Storage { stride, .. }) => stride,
        _ => 0,
    };
    // Kept for the life of the process, once per distinct chain.
    let spec: &'static ShaderSpec = Box::leak(Box::new(ShaderSpec {
        name: format!("{}:{}", generate::form_name(Form::Compute), names.join("+")).leak(),
        entry: "main",
        source: generate::compute(chain).leak(),
        workgroup: [generate::WORKGROUP, 1, 1],
        bindings: bindings.leak(),
        dispatch: Dispatch::PerElement { binding: 1, stride },
        inputs: &[],
        outputs: &[],
    }));
    kept.insert(key, spec);
    spec
}
