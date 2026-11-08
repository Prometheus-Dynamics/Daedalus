use super::*;
use crate::shader::GpuState;

const SRC: &str = "@group(0) @binding(0) var<storage, read_write> out: array<u32>; \
    @compute @workgroup_size(1) fn main() { out[0] = 1u; }";

const SPEC: ShaderSpec = ShaderSpec {
    name: "no-context",
    src: SRC,
    entry: "main",
    workgroup_size: Some([1, 1, 1]),
    bindings: &[],
};

/// With no context nothing is created: the dispatch and the state refuse it. The device
/// count is compared before and after, as other tests may create devices concurrently.
#[test]
fn dispatch_and_state_without_a_context_create_no_device() {
    let before = crate::devices_created();
    let result = dispatch_shader_with_bindings(&SPEC, SRC, &[], None, None, None);
    assert_eq!(result.err(), Some(GpuError::NoContext));
    assert!(matches!(GpuState::new(0u32), Err(GpuError::NoContext)));
    assert_eq!(
        crate::devices_created(),
        before,
        "a device was created without a context"
    );
}
