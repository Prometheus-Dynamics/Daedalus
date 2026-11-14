//! Little-endian numbers in place: a byte range viewed as a slice of numbers, with no copy, when
//! that is what its bytes are on this machine.
//!
//! Values on the wire are little-endian. On a little-endian machine, aligned bytes of a list of
//! numbers already are a slice of those numbers, so a reader can view them where they are.

mod sealed {
    pub trait Sealed {}
}

/// A number whose little-endian bytes, aligned, are the number on a little-endian machine, and
/// every bit pattern of which is a valid value.
pub trait LeItem: sealed::Sealed + Copy + 'static {}

macro_rules! le_items {
    ($($t:ty),*) => {$(
        impl sealed::Sealed for $t {}
        impl LeItem for $t {}
    )*};
}

le_items!(u8, i8, u16, i16, u32, i32, u64, i64, f32, f64);

/// `bytes` as a slice of `T`, with no copy. None on a big-endian machine, or when `bytes` is not
/// aligned for `T` or not a whole number of them: then the reader decodes them.
pub fn le_items<T: LeItem>(bytes: &[u8]) -> Option<&[T]> {
    let size = core::mem::size_of::<T>();
    if cfg!(target_endian = "big")
        || !bytes.len().is_multiple_of(size)
        || bytes.as_ptr().align_offset(core::mem::align_of::<T>()) != 0
    {
        return None;
    }
    // SAFETY: the bytes are aligned for `T` and a whole number of them, `T` is a plain number
    // for which any bit pattern is valid (`LeItem` is sealed to those), and on a little-endian
    // machine their wire bytes are their memory. The slice borrows `bytes`, so it lives no longer.
    Some(unsafe { core::slice::from_raw_parts(bytes.as_ptr().cast::<T>(), bytes.len() / size) })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn aligned_bytes_are_viewed_and_others_are_not() {
        let numbers = [1.0f32, -2.5, 3.25, 0.0];
        let mut buf = [0u8; 64];
        let at = buf.as_ptr().align_offset(core::mem::align_of::<f32>());
        for (slot, number) in buf[at..].as_chunks_mut::<4>().0.iter_mut().zip(numbers) {
            *slot = number.to_le_bytes();
        }
        let bytes = &buf[at..at + 16];
        if cfg!(target_endian = "little") {
            assert_eq!(le_items::<f32>(bytes), Some(&numbers[..]));
        }
        assert_eq!(le_items::<f32>(&buf[at + 1..at + 5]), None, "unaligned");
        assert_eq!(le_items::<f32>(&bytes[..6]), None, "not whole");
    }
}
