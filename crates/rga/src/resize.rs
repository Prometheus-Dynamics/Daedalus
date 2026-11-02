//! What a resize asks of the engine, checked before any device call: an area downscale by an
//! integer factor, or a bilinear resize the route can do. Anything else is
//! [`io::ErrorKind::Unsupported`], never a substitute filter.

use std::io;

use crate::caps::Route;

/// A resize filter. See [`crate::caps`] for what each unit and route does with it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Filter {
    /// Downscale by an integer factor, each output pixel the mean of its block
    /// (`daedalus.image.resize_area`). The output is `max(1, src / factor)` per axis; a
    /// trailing partial block is cropped away.
    Area,
    /// Bilinear (`daedalus.image.resize_bilinear`).
    Bilinear,
}

/// The source size to read (from the top-left corner) for a resize of `src` to `dst`.
pub(crate) fn source_size(
    src: (u32, u32),
    dst: (u32, u32),
    filter: Filter,
    route: Route,
) -> io::Result<(u32, u32)> {
    let unsupported = |why: String| Err(io::Error::new(io::ErrorKind::Unsupported, why));
    if src.0 == 0 || src.1 == 0 || dst.0 == 0 || dst.1 == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("empty frame: {src:?} to {dst:?}"),
        ));
    }
    match filter {
        Filter::Area => {
            let factor = (2..=src.0.max(src.1))
                .find(|f| (src.0 / f).max(1) == dst.0 && (src.1 / f).max(1) == dst.1);
            let Some(f) = factor else {
                return unsupported(format!(
                    "area resize of {src:?} to {dst:?} is not an integer downscale"
                ));
            };
            Ok(((dst.0 * f).min(src.0), (dst.1 * f).min(src.1)))
        }
        Filter::Bilinear => {
            let up = dst.0 >= src.0 && dst.1 >= src.1 && dst != src;
            if route == Route::V4l2 && !up {
                return unsupported(format!(
                    "bilinear {src:?} to {dst:?}: the mainline driver averages when it \
                     downscales and has no filter control; use librga"
                ));
            }
            if dst == src {
                return unsupported("bilinear resize to the same size".into());
            }
            Ok(src)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn area_needs_an_integer_downscale_and_crops_the_remainder() {
        let area = |s, d| source_size(s, d, Filter::Area, Route::V4l2);
        assert_eq!(area((640, 480), (320, 240)).unwrap(), (640, 480));
        assert_eq!(area((641, 481), (320, 240)).unwrap(), (640, 480));
        assert_eq!(area((640, 480), (160, 120)).unwrap(), (640, 480));
        assert_eq!(area((1, 3), (1, 1)).unwrap(), (1, 2));
        for bad in [(321, 240), (320, 120), (640, 480)] {
            let err = area((640, 480), bad).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::Unsupported, "{bad:?}");
        }
    }

    #[test]
    fn bilinear_downscale_is_librga_only() {
        let err = source_size((64, 64), (32, 32), Filter::Bilinear, Route::V4l2).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        assert!(source_size((64, 64), (32, 32), Filter::Bilinear, Route::Librga).is_ok());
        assert!(source_size((32, 32), (64, 48), Filter::Bilinear, Route::V4l2).is_ok());
        assert!(source_size((32, 32), (32, 32), Filter::Bilinear, Route::Librga).is_err());
    }
}
