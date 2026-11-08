//! The per-spec cache of a shader's bindings and workgroup size (inferred from the WGSL once).

use parking_lot::Mutex;
use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use super::{BindingSpec, ShaderSpec, infer_bindings, infer_workgroup_size};
use crate::GpuError;

#[derive(Clone)]
pub(crate) struct CachedSpec {
    pub bindings: Arc<[BindingSpec]>,
    pub workgroup_size: Option<[u32; 3]>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct ShaderSpecCacheKey {
    name: &'static str,
    src: &'static str,
    entry: &'static str,
    workgroup_size: Option<[u32; 3]>,
    bindings: Vec<BindingSpec>,
}

impl From<&ShaderSpec> for ShaderSpecCacheKey {
    fn from(spec: &ShaderSpec) -> Self {
        Self {
            name: spec.name,
            src: spec.src,
            entry: spec.entry,
            workgroup_size: spec.workgroup_size,
            bindings: spec.bindings.to_vec(),
        }
    }
}

pub(crate) fn cached_spec(spec: &ShaderSpec) -> Result<CachedSpec, GpuError> {
    static CACHE: OnceLock<Mutex<HashMap<ShaderSpecCacheKey, CachedSpec>>> = OnceLock::new();
    let key = ShaderSpecCacheKey::from(spec);
    let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    if let Some(cached) = cache.lock().get(&key).cloned() {
        return Ok(cached);
    }

    let bindings: Arc<[BindingSpec]> = if spec.bindings.is_empty() {
        Arc::from(
            infer_bindings(spec.src)
                .ok_or_else(|| GpuError::Internal("failed to infer bindings from shader".into()))?
                .into_boxed_slice(),
        )
    } else {
        Arc::from(spec.bindings)
    };
    let workgroup_size = spec
        .workgroup_size
        .or_else(|| infer_workgroup_size(spec.src));
    let cached = CachedSpec {
        bindings,
        workgroup_size,
    };
    cache.lock().insert(key, cached.clone());
    Ok(cached)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shader::BindingKind;

    const STORAGE_SHADER: &str = r#"
        @group(0) @binding(0)
        var<storage, read> input: array<f32>;

        @compute @workgroup_size(8)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            _ = input[id.x];
        }
    "#;

    const UNIFORM_SHADER: &str = r#"
        struct Params { value: f32 };

        @group(0) @binding(0)
        var<uniform> params: Params;

        @compute @workgroup_size(4)
        fn main(@builtin(global_invocation_id) id: vec3<u32>) {
            _ = params.value + f32(id.x);
        }
    "#;

    #[test]
    fn cached_spec_uses_shader_content_not_stack_address() {
        fn cached_binding_kind(src: &'static str) -> BindingKind {
            let spec = ShaderSpec {
                name: "stack-built",
                src,
                entry: "main",
                workgroup_size: None,
                bindings: &[],
            };
            cached_spec(&spec).expect("cached spec").bindings[0].kind
        }

        assert_eq!(cached_binding_kind(STORAGE_SHADER), BindingKind::Storage);
        assert_eq!(cached_binding_kind(UNIFORM_SHADER), BindingKind::Uniform);
        assert_eq!(cached_binding_kind(STORAGE_SHADER), BindingKind::Storage);
    }
}
