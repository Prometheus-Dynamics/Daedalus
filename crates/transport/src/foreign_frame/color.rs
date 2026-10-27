//! `daedalus:frame` v3 colorimetry and metadata keys.

/// How a frame's values map to color: ITU-T H.273 ("CICP") code points for the primaries,
/// transfer characteristics and matrix coefficients, plus the quantization range.
///
/// `#[repr(C)]`, four bytes, returned by value from the v3 `colorimetry` accessor. Unknown
/// codes are passed through unchanged: consumers that do not know one treat it as unspecified.
/// The associated constants name the codes Daedalus users meet; any other H.273 value is valid.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FrameColorimetry {
    /// H.273 `ColourPrimaries`.
    pub primaries: u8,
    /// H.273 `TransferCharacteristics`.
    pub transfer: u8,
    /// H.273 `MatrixCoefficients` (YCbCr to RGB; [`Self::MATRIX_IDENTITY`] for RGB data).
    pub matrix: u8,
    /// [`ColorRange`] as `u8`.
    pub range: u8,
}

impl FrameColorimetry {
    pub const PRIMARIES_BT709: u8 = 1;
    pub const PRIMARIES_UNSPECIFIED: u8 = 2;
    pub const PRIMARIES_BT470BG: u8 = 5;
    pub const PRIMARIES_SMPTE170M: u8 = 6;
    pub const PRIMARIES_BT2020: u8 = 9;
    pub const PRIMARIES_P3_D65: u8 = 12;

    pub const TRANSFER_BT709: u8 = 1;
    pub const TRANSFER_UNSPECIFIED: u8 = 2;
    pub const TRANSFER_SMPTE170M: u8 = 6;
    pub const TRANSFER_LINEAR: u8 = 8;
    pub const TRANSFER_SRGB: u8 = 13;
    pub const TRANSFER_PQ: u8 = 16;
    pub const TRANSFER_HLG: u8 = 18;

    pub const MATRIX_IDENTITY: u8 = 0;
    pub const MATRIX_BT709: u8 = 1;
    pub const MATRIX_UNSPECIFIED: u8 = 2;
    pub const MATRIX_BT470BG: u8 = 5;
    pub const MATRIX_SMPTE170M: u8 = 6;
    pub const MATRIX_BT2020_NCL: u8 = 9;

    /// Nothing known (what v2 frames report through the v3 interface).
    pub const UNSPECIFIED: Self = Self::new(
        Self::PRIMARIES_UNSPECIFIED,
        Self::TRANSFER_UNSPECIFIED,
        Self::MATRIX_UNSPECIFIED,
        ColorRange::Unknown,
    );
    /// sRGB RGB data (libcamera `Srgb` for RGB formats).
    pub const SRGB: Self = Self::new(
        Self::PRIMARIES_BT709,
        Self::TRANSFER_SRGB,
        Self::MATRIX_IDENTITY,
        ColorRange::Full,
    );
    /// sYCC / JPEG: BT.709 primaries, sRGB transfer, BT.601 matrix, full range (libcamera
    /// `Sycc`; what most USB cameras' MJPEG and many ISPs' YUV output use).
    pub const SYCC: Self = Self::new(
        Self::PRIMARIES_BT709,
        Self::TRANSFER_SRGB,
        Self::MATRIX_SMPTE170M,
        ColorRange::Full,
    );
    /// SMPTE 170M (BT.601 SD video), limited range (libcamera `Smpte170m`).
    pub const SMPTE170M: Self = Self::new(
        Self::PRIMARIES_SMPTE170M,
        Self::TRANSFER_BT709,
        Self::MATRIX_SMPTE170M,
        ColorRange::Limited,
    );
    /// BT.709 HD video, limited range (libcamera `Rec709`).
    pub const BT709: Self = Self::new(
        Self::PRIMARIES_BT709,
        Self::TRANSFER_BT709,
        Self::MATRIX_BT709,
        ColorRange::Limited,
    );
    /// BT.2020 (non-constant luminance), limited range (libcamera `Rec2020`).
    pub const BT2020: Self = Self::new(
        Self::PRIMARIES_BT2020,
        Self::TRANSFER_BT709,
        Self::MATRIX_BT2020_NCL,
        ColorRange::Limited,
    );

    pub const fn new(primaries: u8, transfer: u8, matrix: u8, range: ColorRange) -> Self {
        Self {
            primaries,
            transfer,
            matrix,
            range: range as u8,
        }
    }

    /// The decoded [`Self::range`].
    pub fn color_range(&self) -> ColorRange {
        ColorRange::from_raw(self.range)
    }

    /// The same colorimetry with another range.
    pub const fn with_range(mut self, range: ColorRange) -> Self {
        self.range = range as u8;
        self
    }
}

impl Default for FrameColorimetry {
    fn default() -> Self {
        Self::UNSPECIFIED
    }
}

/// Quantization range of a frame's values.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum ColorRange {
    /// Not stated.
    #[default]
    Unknown = 0,
    /// Limited ("video", "TV") range: 16..=235 luma, 16..=240 chroma at 8 bits.
    Limited = 1,
    /// Full ("PC", "JPEG") range.
    Full = 2,
}

impl ColorRange {
    /// Decode the vtable value; unknown values are treated as [`Self::Unknown`].
    pub fn from_raw(raw: u8) -> Self {
        match raw {
            1 => Self::Limited,
            2 => Self::Full,
            _ => Self::Unknown,
        }
    }
}

/// Metadata keys of `daedalus:frame` v3 (`metadata_u64` / `metadata_bytes`).
///
/// One key space: each key is either numeric or bytes, as documented on it. A provider answers
/// the keys it knows and returns nothing for the rest, so new keys never change the vtable.
/// Keys below [`VENDOR_BASE`] are assigned here; keys from [`VENDOR_BASE`] up are private to a
/// provider (name them in its documentation; consumers check the frame's source type first).
pub mod frame_meta {
    /// Numeric: exposure time in nanoseconds.
    pub const EXPOSURE_NS: u32 = 1;
    /// Numeric: analogue gain as `f64` bits (`f64::to_bits`; 1.0 = no gain).
    pub const ANALOGUE_GAIN: u32 = 2;
    /// Numeric: digital gain as `f64` bits.
    pub const DIGITAL_GAIN: u32 = 3;
    /// Numeric: correlated color temperature in kelvin.
    pub const COLOR_TEMPERATURE_K: u32 = 4;
    /// Numeric: frame duration (time from this frame's start to the next's) in nanoseconds.
    pub const FRAME_DURATION_NS: u32 = 5;
    /// Bytes: UTF-8 identifier of the sensor or camera (e.g. a libcamera camera id).
    pub const SENSOR_ID: u32 = 64;
    /// First provider-private key.
    pub const VENDOR_BASE: u32 = 0x8000_0000;
}
