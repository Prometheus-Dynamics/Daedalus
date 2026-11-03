use serde::{Deserialize, Serialize};

/// Backend kind identifiers.
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GpuBackendKind {
    Noop,
    Mock,
    Wgpu,
    /// NVIDIA CUDA (`gpu-cuda`): the driver API, with its own device memory.
    Cuda,
}

impl GpuBackendKind {
    /// Return a stable string representation.
    pub fn as_str(self) -> &'static str {
        match self {
            GpuBackendKind::Noop => "noop",
            GpuBackendKind::Mock => "mock",
            GpuBackendKind::Wgpu => "wgpu",
            GpuBackendKind::Cuda => "cuda",
        }
    }

    /// The suffix of the port type keys whose values live in this backend's device memory: CUDA
    /// device memory is `…@cuda`, and every other backend's (wgpu, and the mock and noop stand-
    /// ins for it) is `…@gpu`. A value of one residency is a different port type from the other,
    /// so an adapter from `test:f32` to `test:f32@cuda` never hands a wgpu buffer to a CUDA node.
    pub fn device_type_suffix(self) -> &'static str {
        match self {
            GpuBackendKind::Cuda => "@cuda",
            GpuBackendKind::Wgpu | GpuBackendKind::Mock | GpuBackendKind::Noop => "@gpu",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::GpuBackendKind;

    #[test]
    fn cuda_device_memory_has_its_own_port_type_suffix() {
        assert_eq!(GpuBackendKind::Cuda.as_str(), "cuda");
        assert_eq!(GpuBackendKind::Cuda.device_type_suffix(), "@cuda");
        for wgpu_like in [
            GpuBackendKind::Wgpu,
            GpuBackendKind::Mock,
            GpuBackendKind::Noop,
        ] {
            assert_eq!(wgpu_like.device_type_suffix(), "@gpu");
            assert_ne!(
                wgpu_like.device_type_suffix(),
                GpuBackendKind::Cuda.device_type_suffix()
            );
        }
    }
}
