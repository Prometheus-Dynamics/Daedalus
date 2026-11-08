//! Device loss as seen through a [`GpuContextHandle`]: whether the backend reports its device lost,
//! and why. A lost context is replaced by its source's next request.

use crate::GpuContextHandle;

impl GpuContextHandle {
    /// Whether the device has been lost ([`crate::GpuBackend::is_lost`]).
    pub fn is_lost(&self) -> bool {
        self.backend.is_lost()
    }

    /// The loss's message, when the device is lost ([`crate::GpuBackend::lost_reason`]).
    pub fn lost_reason(&self) -> Option<String> {
        self.backend.lost_reason()
    }
}
