//! Crop rectangles and quarter-turn transforms, checked before any device call.

use std::io;

/// A flip or a quarter turn. Turns are clockwise (`daedalus.image.rotate`); which way each unit
/// turns is a board measurement (see [`crate::caps`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Transform {
    FlipHorizontal,
    FlipVertical,
    Rotate90,
    /// Both flips.
    Rotate180,
    Rotate270,
}

impl Transform {
    /// The output size for a `width` by `height` input.
    pub fn output_size(self, width: u32, height: u32) -> (u32, u32) {
        match self {
            Transform::Rotate90 | Transform::Rotate270 => (height, width),
            _ => (width, height),
        }
    }
}

/// `rect` (left, top, width, height) is non-empty and inside a `width` by `height` frame.
pub(crate) fn check_crop(rect: (u32, u32, u32, u32), width: u32, height: u32) -> io::Result<()> {
    let (left, top, w, h) = rect;
    let inside = w > 0
        && h > 0
        && left.checked_add(w).is_some_and(|r| r <= width)
        && top.checked_add(h).is_some_and(|b| b <= height);
    if inside {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("crop {rect:?} is not inside a {width}x{height} frame"),
        ))
    }
}

/// The output has the size `want`, or the call would be a resize.
pub(crate) fn check_size(got: (u32, u32), want: (u32, u32)) -> io::Result<()> {
    if got == want {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "a {}x{} output for a {}x{} result; that is a resize",
                got.0, got.1, want.0, want.1
            ),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crops_stay_inside_and_turns_swap_the_sides() {
        assert!(check_crop((0, 0, 4, 2), 4, 2).is_ok());
        assert!(check_crop((1, 1, 3, 1), 4, 2).is_ok());
        for bad in [
            (0, 0, 0, 1),
            (1, 0, 4, 2),
            (0, 1, 4, 2),
            (u32::MAX, 0, 2, 1),
        ] {
            assert!(check_crop(bad, 4, 2).is_err(), "{bad:?}");
        }
        assert_eq!(Transform::Rotate90.output_size(4, 2), (2, 4));
        assert_eq!(Transform::Rotate180.output_size(4, 2), (4, 2));
        assert!(check_size((2, 4), (2, 4)).is_ok());
        assert!(check_size((4, 2), (2, 4)).is_err());
    }
}
