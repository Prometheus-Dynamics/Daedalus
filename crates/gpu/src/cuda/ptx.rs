//! The kernels, as PTX text the driver JIT-compiles for the device it runs on.
//!
//! Hand-written, so the build needs no CUDA toolkit (`nvcc`, `nvrtc`). `.target sm_70` is the
//! lowest target a current driver still JITs for every NVIDIA GPU from Volta on, and the driver
//! compiles it for the actual device at load time.

/// The entry point of [`SCALE_F32_PTX`]: `data[i] *= factor` for `i < n`.
pub(crate) const SCALE_F32_ENTRY: &str = "scale_f32";

/// `scale_f32(u64 data, u32 n, f32 factor)`: one thread per element, 256 threads per block.
pub(crate) const SCALE_F32_PTX: &str = "\
.version 7.0
.target sm_70
.address_size 64

.visible .entry scale_f32(
    .param .u64 scale_f32_param_0,
    .param .u32 scale_f32_param_1,
    .param .f32 scale_f32_param_2
)
{
    .reg .pred  %p<2>;
    .reg .u32   %r<6>;
    .reg .f32   %f<4>;
    .reg .b64   %rd<5>;

    ld.param.u64    %rd1, [scale_f32_param_0];
    ld.param.u32    %r1, [scale_f32_param_1];
    ld.param.f32    %f1, [scale_f32_param_2];
    mov.u32         %r2, %ctaid.x;
    mov.u32         %r3, %ntid.x;
    mov.u32         %r4, %tid.x;
    mad.lo.u32      %r5, %r2, %r3, %r4;
    setp.ge.u32     %p1, %r5, %r1;
    @%p1 bra        DONE;
    cvta.to.global.u64  %rd2, %rd1;
    mul.wide.u32    %rd3, %r5, 4;
    add.s64         %rd4, %rd2, %rd3;
    ld.global.f32   %f2, [%rd4];
    mul.rn.f32      %f3, %f2, %f1;
    st.global.f32   [%rd4], %f3;
DONE:
    ret;
}
";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kernel_declares_the_entry_point_and_its_three_parameters() {
        assert!(SCALE_F32_PTX.contains(&format!(".visible .entry {SCALE_F32_ENTRY}(")));
        assert_eq!(SCALE_F32_PTX.matches(".param .").count(), 3);
        assert!(SCALE_F32_PTX.ends_with("}\n"));
        assert!(!SCALE_F32_PTX.contains('\0'), "PTX is passed as a C string");
    }
}
