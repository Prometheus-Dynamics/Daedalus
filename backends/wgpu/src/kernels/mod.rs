//! The data-parallel kernel ops, on data that is already on the device.
//!
//! These are library functions, not nodes: a node calls them from its recording body, or a
//! program calls them on buffers it holds. Each gives, bit for bit, what the CPU kernel of the
//! same name gives. Whether one beats the CPU depends on the device and on where the data is:
//! use them when the bytes are already in a [`Storage`](crate::Storage) and would otherwise be
//! read back just to be converted.
//!
//! The per-value conversions are also [`PixelSpec`]s, [`HALF_TO_F32`] and [`F32_TO_UNORM8`], so
//! a chain of pixel bodies that includes one is still one shader.

mod codec;
mod crc;
mod ops;

use std::fmt;

pub use codec::Decoding;
pub use crc::Checksum;
pub use ops::Kernels;

use crate::buffer::StorageError;
use crate::context::OpenError;
use crate::merge::{PixelSpec, source_hash};
use crate::shader::ShaderError;
use crate::wait::WaitError;

/// One op. `n` in what follows is the op's count of input elements.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Kernel {
    /// `n` little-endian f32, as f32.
    F32FromLe,
    /// `n` binary16 values to f32.
    F16ToF32,
    /// `n` f32 to unorm8: clamped, scaled, rounded half away from zero, NaN to 0.
    F32ToUnorm8,
    /// `n` bytes, copied by a shader. [`Kernels::copy_with_engine`] is the copy command.
    Copy,
    /// `n` bytes to lowercase hex.
    HexLower,
    /// `n` bytes to standard base64, padded.
    Base64Encode,
    /// `n` base64 symbols to bytes. Run it with [`Kernels::base64_decode`], which reports the
    /// length and invalid input.
    Base64Decode,
    /// The CRC-32 of each 4 KiB chunk of `n` bytes. [`Kernels::crc32`] combines them.
    Crc32Chunks,
}

/// Bytes in one chunk of [`Kernel::Crc32Chunks`].
pub const CRC_CHUNK: u64 = 4096;

impl Kernel {
    pub const ALL: [Kernel; 8] = [
        Kernel::F32FromLe,
        Kernel::F16ToF32,
        Kernel::F32ToUnorm8,
        Kernel::Copy,
        Kernel::HexLower,
        Kernel::Base64Encode,
        Kernel::Base64Decode,
        Kernel::Crc32Chunks,
    ];

    pub(crate) const fn index(self) -> usize {
        self as usize
    }

    /// Input bytes for `n` elements.
    pub const fn input_len(self, n: u64) -> u64 {
        match self {
            Kernel::F32FromLe | Kernel::F32ToUnorm8 => 4 * n,
            Kernel::F16ToF32 => 2 * n,
            Kernel::Copy
            | Kernel::HexLower
            | Kernel::Base64Encode
            | Kernel::Base64Decode
            | Kernel::Crc32Chunks => n,
        }
    }

    /// Output bytes for `n` elements. For base64 decode this is the most, before padding is
    /// taken off.
    pub const fn output_len(self, n: u64) -> u64 {
        match self {
            Kernel::F32ToUnorm8 | Kernel::Copy => n,
            Kernel::F32FromLe | Kernel::F16ToF32 => 4 * n,
            Kernel::HexLower => 2 * n,
            Kernel::Base64Encode => n.div_ceil(3) * 4,
            Kernel::Base64Decode => n / 4 * 3,
            Kernel::Crc32Chunks => n.div_ceil(CRC_CHUNK) * 4,
        }
    }

    /// Invocations that write for `n` elements: one for each output word.
    pub(crate) const fn threads(self, n: u64) -> u64 {
        match self {
            Kernel::F32FromLe | Kernel::F16ToF32 => n,
            Kernel::Base64Encode => n.div_ceil(3),
            _ => self.output_len(n).div_ceil(4),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KernelError {
    Open(OpenError),
    Shader(ShaderError),
    Storage(StorageError),
    Wait(WaitError),
    /// A count or a buffer that doesn't fit the op. The text says which.
    Size(String),
    /// Base64 input with a byte that is no symbol, or bits in its last symbol that no byte
    /// takes, at this offset.
    InvalidSymbol {
        at: u64,
    },
}

impl fmt::Display for KernelError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KernelError::Open(error) => error.fmt(f),
            KernelError::Shader(error) => error.fmt(f),
            KernelError::Storage(error) => error.fmt(f),
            KernelError::Wait(error) => error.fmt(f),
            KernelError::Size(text) => f.write_str(text),
            KernelError::InvalidSymbol { at } => {
                write!(f, "invalid base64 symbol at offset {at}")
            }
        }
    }
}

impl std::error::Error for KernelError {}

/// The functions of one value, as the kernel shaders have them.
const COMMON: &str = include_str!("common.wgsl");

/// Every top-level name in `common.wgsl`, and `apply`.
const NAMES: &[&str] = &[
    "half_to_f32_bits",
    "unorm8",
    "hex_digit",
    "b64_symbol",
    "b64_value",
    "apply",
];

const fn pixel(name: &'static str, source: &'static str) -> PixelSpec {
    PixelSpec {
        name,
        source,
        names: NAMES,
        input: "u32",
        output: "u32",
        input_stride: 4,
        output_stride: 4,
        config: None,
        sides: &[],
        params: &["c"],
        inputs: &[0],
        source_hash: source_hash(source),
    }
}

const HALF_SOURCE: &str = concat!(
    include_str!("common.wgsl"),
    "fn apply(c: u32) -> u32 { return half_to_f32_bits(c & 0xffffu); }\n"
);
const UNORM_SOURCE: &str = concat!(
    include_str!("common.wgsl"),
    "fn apply(c: u32) -> u32 { return unorm8(c); }\n"
);

/// A pixel body: the binary16 value in the low half of a `u32`, to the bits of the f32 it
/// converts to. The same function [`Kernel::F16ToF32`] runs.
pub const HALF_TO_F32: PixelSpec = pixel("kernels.half_to_f32", HALF_SOURCE);

/// A pixel body: the bits of an f32, to its unorm8 in a `u32`. The same function
/// [`Kernel::F32ToUnorm8`] runs, which packs four results to a word.
pub const F32_TO_UNORM8: PixelSpec = pixel("kernels.f32_to_unorm8", UNORM_SOURCE);

const _: () = assert!(COMMON.len() < HALF_SOURCE.len());
