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
