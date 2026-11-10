//! How an image's rows get into a texture when they don't start 256 bytes apart.
//!
//! A copy from a buffer to a texture wants each row to start a multiple of 256 bytes after the
//! last. A stride that is one copies straight across. One that isn't is folded: `k` rows of the
//! image, taken together, are `k * stride` bytes, and for the right `k` that is a multiple of
//! 256. So a single copy lays `k` image rows side by side as one row of a wider texture, and a
//! pass over the real texture reads each pixel from where the fold put it.

/// The alignment a row has to have for a direct copy.
pub(crate) const ROW_ALIGNMENT: u64 = 256;
/// The most rows folded together. Past this the wide texture gets too wide to be worth it.
const MAX_FOLD: u32 = 8;

/// How an image gets from a buffer into a texture.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Plan {
    /// One copy, straight across.
    Direct,
    /// One copy into a texture `rows` times as wide, which holds `rows` image rows in each of
    /// its own, and then a pass that unfolds it.
    Folded { rows: u32 },
    /// One copy for each row. Slow, and only for strides nothing else fits.
    Rows,
}

/// Picks how to copy an image of `width` x `height` pixels of `pixel` bytes each, whose rows
/// are `stride` bytes apart, into a texture on a device whose textures are at most `max_side`
/// wide.
pub fn plan(stride: u64, width: u32, height: u32, pixel: u32, max_side: u32) -> Plan {
    if stride.is_multiple_of(ROW_ALIGNMENT) {
        return Plan::Direct;
    }
    // A folded row is whole pixels, so the stride has to be.
    if !stride.is_multiple_of(u64::from(pixel)) {
        return Plan::Rows;
    }
    let across = stride / u64::from(pixel);
    let fits = |rows: u32| {
        (stride * u64::from(rows)).is_multiple_of(ROW_ALIGNMENT)
            && height.is_multiple_of(rows)
            && across * u64::from(rows) <= u64::from(max_side)
            && u64::from(width) <= across
    };
    match (2..=MAX_FOLD).find(|&rows| fits(rows)) {
        Some(rows) => Plan::Folded { rows },
        None => Plan::Rows,
    }
}

/// Where the pixel at `(x, y)` of the image sits in the folded texture: `rows` image rows side
/// by side, each `across` pixels wide including its padding.
pub fn source_of(x: u32, y: u32, rows: u32, across: u32) -> (u32, u32) {
    (x + (y % rows) * across, y / rows)
}

#[cfg(test)]
mod tests {
    use super::{Plan, plan, source_of};

    #[test]
    fn a_stride_of_640_folds_two_rows_and_every_pixel_maps_back_to_its_byte() {
        // 640-byte GREY rows: two of them are 1280 bytes, which is five times 256.
        let (width, height, stride) = (640u32, 480u32, 640u64);
        assert_eq!(
            plan(stride, width, height, 1, 8192),
            Plan::Folded { rows: 2 }
        );
        // Every pixel's place in the folded texture is, read as bytes of the copy, exactly the
        // byte it has in the image's own buffer.
        for y in [0, 1, 2, 3, 478, 479] {
            for x in [0, 1, 319, 638, 639] {
                let (fx, fy) = source_of(x, y, 2, stride as u32);
                let byte_in_copy = u64::from(fy) * 2 * stride + u64::from(fx);
                let byte_in_image = u64::from(y) * stride + u64::from(x);
                assert_eq!(byte_in_copy, byte_in_image, "({x}, {y})");
                assert!(fx < 1280 && fy < 240);
            }
        }
    }

    #[test]
    fn the_plan_is_direct_when_it_can_be_and_row_by_row_when_nothing_else_fits() {
        // Rows already 256 apart, padded or not.
        assert_eq!(plan(256, 64, 4, 4, 8192), Plan::Direct);
        assert_eq!(plan(2560, 640, 480, 4, 8192), Plan::Direct);
        assert_eq!(plan(1024, 200, 3, 4, 8192), Plan::Direct);
        // 320-byte rows of four-byte pixels: four rows are 1280.
        assert_eq!(plan(320, 80, 8, 4, 8192), Plan::Folded { rows: 4 });
        // A stride with padding folds the padding too: 648-byte rows hold 640 pixels.
        assert_eq!(
            plan(648, 640, 480, 1, 16384),
            Plan::Rows,
            "648 * k is never a multiple of 256 for k <= 8"
        );
        assert_eq!(plan(384, 380, 480, 1, 8192), Plan::Folded { rows: 2 });
        // What can't fold: an odd height, a texture too wide, a stride that splits a pixel, or
        // one that needs more rows than are folded.
        assert_eq!(plan(640, 640, 481, 1, 8192), Plan::Rows);
        assert_eq!(plan(640, 640, 480, 1, 1024), Plan::Rows);
        assert_eq!(plan(642, 160, 480, 4, 8192), Plan::Rows);
        assert_eq!(plan(100, 90, 64, 1, 8192), Plan::Rows);
        // Folded pixels of several bytes map by pixel, not by byte.
        assert_eq!(source_of(5, 7, 4, 80), (5 + 3 * 80, 1));
    }
}
