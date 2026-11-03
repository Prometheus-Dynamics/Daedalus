//! The VeriSilicon VGLite 2.5D GPU (GCNanoUltraV) on the Kendryte K230 and K230D, through the
//! vendor's `libvg_lite.so`, loaded at run time (`dlopen`; nothing links it): resize, crop,
//! flip and quarter turns, fill, blend, palette expansion and one-pixel lines, between byte
//! buffers or dmabufs. The fixed-function `vglite` variant of the `daedalus.image.*` ops.
//!
//! ```no_run
//! # #[cfg(target_os = "linux")] {
//! use daedalus_vglite::{Filter, Format, Image, Vglite};
//! let gpu = Vglite::load()?;
//! let (src, dst) = (Image::packed(Format::L8, 640, 480), Image::packed(Format::L8, 320, 240));
//! let (from, mut to) = (vec![0u8; src.frame_len()], vec![0u8; dst.frame_len()]);
//! let cost = gpu.resize_bytes(src, &from, dst, &mut to, Filter::Area)?;
//! eprintln!("{} bytes staged", cost.staged_bytes);
//! # }
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! Status: written, untested on board. The declarations follow `vg_lite.h` of the K230 Linux
//! SDK v0.6.1; the unit tests run this code against software stand-ins for the library.
//!
//! - **Strict:** what the unit cannot do as asked is [`std::io::ErrorKind::Unsupported`]: area
//!   resizes other than 2x (VGLite has no averaging filter), premultiplied blends, formats the
//!   K230's build leaves out (24-bit RGB, planar YUV input). No colour key and no ROP exist on
//!   this GPU.
//! - **Memory:** the kernel driver maps dmabufs only, and only physically contiguous ones
//!   correctly. Byte buffers, [`Backing::Scattered`] dmabufs and rows whose stride VGLite cannot
//!   take go through a contiguous staging buffer; every call returns the bytes it copied
//!   ([`Transfer`]).
//! - **Capabilities:** `daedalus_rga::caps` lists the K230D's VGLite route, each operation
//!   `Unmeasured` until a board run.

mod ffi;
// The shared checks serve the Linux routes only.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
mod plan;

pub use daedalus_rga::{Filter, Transform};
pub use plan::{BlendMode, Format, Image};

#[cfg(target_os = "linux")]
mod gpu;
#[cfg(target_os = "linux")]
mod surface;
#[cfg(all(test, target_os = "linux"))]
mod tests;

#[cfg(target_os = "linux")]
pub use gpu::Vglite;
#[cfg(target_os = "linux")]
pub use surface::{Backing, Frame, Transfer};
