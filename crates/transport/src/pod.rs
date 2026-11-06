//! Plain-old-data values that cross the stable plugin boundary as typed byte views.
//!
//! A [`StablePod`] type is a `#[repr(C)]` struct of scalars, fixed arrays and other `StablePod`
//! structs (derive it with `#[derive(StablePod)]`), or one of the scalars and arrays below. Its
//! [`PodLayout`] names it and hashes its layout (field names, offsets, sizes and alignments, the
//! fields' own layouts), so two separately built sides that each define the type check they agree
//! before either reads the other's bytes: a different layout is a typed error
//! ([`PodMismatch`]), never a misread.
//!
//! A port carrying `T` or `Vec<T>` ([`PodPort`]) crosses as a view of the sender's elements
//! (pointer, element count, element size, layout hash and name), which the receiver copies once
//! into its own `T` or `Vec<T>`; nothing goes through `Value`. A side that does not have the Rust
//! type keeps the elements as a [`PodValue`], which passes them on unchanged and can be read as
//! any `T` whose layout matches.

use alloc::string::String;
use alloc::vec::Vec;
use core::any::Any;
use core::fmt;

/// The largest alignment a [`StablePod`] type may have ([`PodValue`] stores elements in
/// 8-byte words).
pub const POD_MAX_ALIGN: usize = 8;

/// The name and layout of a [`StablePod`] type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PodLayout {
    /// The type's name: a struct's own name (no module path), a scalar's Rust name, `array`.
    pub name: &'static str,
    pub size: usize,
    pub align: usize,
    /// FNV-1a over the layout: field names, offsets and field layout hashes, then size and
    /// alignment (see [`PodLayout::structure`]).
    pub hash: u64,
    /// A struct ([`PodLayout::structure`]) rather than a scalar or array. Only struct ports
    /// cross as views: scalars and their `Vec`s keep their builtin encodings.
    pub structure: bool,
}

/// One field of a derived [`StablePod`] struct (see [`PodLayout::structure`]).
#[derive(Clone, Copy, Debug)]
pub struct PodField {
    pub name: &'static str,
    pub offset: usize,
    /// The field type's [`PodLayout::hash`].
    pub hash: u64,
}

const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

const fn fnv_bytes(mut hash: u64, bytes: &[u8]) -> u64 {
    let mut i = 0;
    while i < bytes.len() {
        hash = (hash ^ bytes[i] as u64).wrapping_mul(FNV_PRIME);
        i += 1;
    }
    hash
}

const fn fnv_word(hash: u64, word: u64) -> u64 {
    fnv_bytes(hash, &word.to_le_bytes())
}

impl PodLayout {
    /// A scalar's layout: hashed from its name, size and alignment.
    pub const fn scalar(name: &'static str, size: usize, align: usize) -> Self {
        let hash = fnv_bytes(FNV_OFFSET, name.as_bytes());
        let hash = fnv_word(fnv_word(hash, size as u64), align as u64);
        Self {
            name,
            size,
            align,
            hash,
            structure: false,
        }
    }

    /// The layout of `[elem; len]`.
    pub const fn array(elem: &PodLayout, len: usize) -> Self {
        let hash = fnv_bytes(FNV_OFFSET, b"array");
        let hash = fnv_word(fnv_word(hash, elem.hash), len as u64);
        Self {
            name: "array",
            size: elem.size * len,
            align: elem.align,
            hash,
            structure: false,
        }
    }

    /// A struct's layout (what `#[derive(StablePod)]` builds): `name` is the struct's name and is
    /// not part of the hash, which covers each field's name, offset and layout hash, in
    /// declaration order, then the size and alignment.
    pub const fn structure(
        name: &'static str,
        size: usize,
        align: usize,
        fields: &[PodField],
    ) -> Self {
        let mut hash = fnv_bytes(FNV_OFFSET, b"struct");
        let mut i = 0;
        while i < fields.len() {
            hash = fnv_bytes(hash, fields[i].name.as_bytes());
            hash = fnv_word(hash, fields[i].offset as u64);
            hash = fnv_word(hash, fields[i].hash);
            i += 1;
        }
        let hash = fnv_word(fnv_word(hash, size as u64), align as u64);
        Self {
            name,
            size,
            align,
            hash,
            structure: true,
        }
    }

    /// Whether `self` (the receiver's type) can read elements described by `name`, `hash`,
    /// `size` and `align` (the sender's): same name, hash, size and alignment.
    pub fn check(
        &self,
        name: &str,
        hash: u64,
        size: usize,
        align: usize,
    ) -> Result<(), PodMismatch> {
        if self.name == name && self.hash == hash && self.size == size && self.align == align {
            return Ok(());
        }
        Err(PodMismatch {
            expected: *self,
            found_name: String::from(name),
            found_hash: hash,
            found_size: size,
            found_align: align,
        })
    }
}

/// A [`StablePod`] layout check failed: the sender's type is not the receiver's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PodMismatch {
    pub expected: PodLayout,
    pub found_name: String,
    pub found_hash: u64,
    pub found_size: usize,
    pub found_align: usize,
}

impl fmt::Display for PodMismatch {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "plain data layout mismatch: expected `{}` (layout {:016x}, size {}, align {}), found \
             `{}` (layout {:016x}, size {}, align {})",
            self.expected.name,
            self.expected.hash,
            self.expected.size,
            self.expected.align,
            self.found_name,
            self.found_hash,
            self.found_size,
            self.found_align
        )
    }
}

impl core::error::Error for PodMismatch {}

/// A plain-old-data type: `Copy`, no pointers, valid for every bit pattern of its fields, with
/// a [`PodLayout`] describing it. Derive it for a `#[repr(C)]` struct with
/// `#[derive(StablePod)]`.
///
/// # Safety
/// `LAYOUT` must describe `Self` exactly (size, alignment at most [`POD_MAX_ALIGN`]), and every
/// byte pattern a sender with the same layout hash can produce must be a valid `Self` (so no
/// `bool`, `char`, enums, references or pointers).
pub unsafe trait StablePod: Copy + Send + Sync + 'static {
    const LAYOUT: PodLayout;
}

macro_rules! scalar_pods {
    ($($ty:ty),* $(,)?) => {$(
        // Safety: a number is valid for every bit pattern and its layout is its own.
        unsafe impl StablePod for $ty {
            const LAYOUT: PodLayout = PodLayout::scalar(
                stringify!($ty),
                core::mem::size_of::<$ty>(),
                core::mem::align_of::<$ty>(),
            );
        }
    )*};
}

scalar_pods!(u8, u16, u32, u64, i8, i16, i32, i64, f32, f64);

// Safety: an array of `StablePod` elements has no padding between them and is valid for every
// bit pattern its elements are.
unsafe impl<T: StablePod, const N: usize> StablePod for [T; N] {
    const LAYOUT: PodLayout = PodLayout::array(&T::LAYOUT, N);
}

/// A port value that crosses as a [`StablePod`] view: `T` itself (one element) or `Vec<T>`.
pub trait PodPort: Send + Sync + 'static {
    type Elem: StablePod;
    /// `Vec<T>`: any number of elements; `T`: exactly one.
    const SLICE: bool;
    fn elems(&self) -> &[Self::Elem];
    /// A value of `elems` (the first one for a single value; `elems` is never empty then).
    fn make(elems: &[Self::Elem]) -> Self;
    /// Replace `self`'s contents with `elems`, reusing its allocation.
    fn assign(&mut self, elems: &[Self::Elem]);
}

impl<T: StablePod> PodPort for T {
    type Elem = T;
    const SLICE: bool = false;
    fn elems(&self) -> &[T] {
        core::slice::from_ref(self)
    }
    fn make(elems: &[T]) -> Self {
        elems[0]
    }
    fn assign(&mut self, elems: &[T]) {
        if let Some(first) = elems.first() {
            *self = *first;
        }
    }
}

impl<T: StablePod> PodPort for Vec<T> {
    type Elem = T;
    const SLICE: bool = true;
    fn elems(&self) -> &[T] {
        self
    }
    fn make(elems: &[T]) -> Self {
        elems.to_vec()
    }
    fn assign(&mut self, elems: &[T]) {
        self.clear();
        self.extend_from_slice(elems);
    }
}

/// The port codec of a [`PodPort`] type, as plain function pointers (see [`PodCodec::of`]).
#[derive(Clone, Copy)]
pub struct PodCodec {
    pub layout: PodLayout,
    /// The port takes `Vec<T>` (any element count) rather than one `T`.
    pub slice: bool,
    pub type_id: core::any::TypeId,
    pub rust_type: &'static str,
    /// The elements of a port value (`None` when `value` is not the port's type): pointer and
    /// element count.
    pub elems: fn(&(dyn Any + Send + Sync)) -> Option<(*const u8, usize)>,
    /// Builds a payload of the port's type from `count` elements at `data` (checked by the
    /// caller against `layout`), in `slot`'s allocation when it is free.
    ///
    /// # Safety
    /// `data` must point to `count` valid, aligned elements of the layout (at least one for a
    /// single value).
    pub build: unsafe fn(
        slot: &mut crate::PayloadSlot,
        key: crate::TypeKey,
        data: *const u8,
        count: usize,
    ) -> crate::Payload,
}

impl fmt::Debug for PodCodec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PodCodec")
            .field("layout", &self.layout)
            .field("slice", &self.slice)
            .field("rust_type", &self.rust_type)
            .finish_non_exhaustive()
    }
}

impl PodCodec {
    /// The codec of `P` (`T` or `Vec<T>`).
    pub fn of<P: PodPort + Reusable>() -> Self {
        fn elems<P: PodPort>(value: &(dyn Any + Send + Sync)) -> Option<(*const u8, usize)> {
            let elems = value.downcast_ref::<P>()?.elems();
            Some((elems.as_ptr().cast(), elems.len()))
        }
        unsafe fn build<P: PodPort + Reusable>(
            slot: &mut crate::PayloadSlot,
            key: crate::TypeKey,
            data: *const u8,
            count: usize,
        ) -> crate::Payload {
            // Safety: forwarded from the caller.
            let elems = unsafe { pod_slice::<P::Elem>(data, count) };
            slot.owned_reused::<P>(key, || P::make(elems), |value| value.assign(elems))
        }
        Self {
            layout: P::Elem::LAYOUT,
            slice: P::SLICE,
            type_id: core::any::TypeId::of::<P>(),
            rust_type: core::any::type_name::<P>(),
            elems: elems::<P>,
            build: build::<P>,
        }
    }
}

/// `count` elements at `data`.
///
/// # Safety
/// `data` must point to `count` valid, aligned `T`s for `'a` (or `count` is 0).
pub unsafe fn pod_slice<'a, T: StablePod>(data: *const u8, count: usize) -> &'a [T] {
    if count == 0 || data.is_null() {
        return &[];
    }
    // Safety: forwarded from the caller.
    unsafe { core::slice::from_raw_parts(data.cast::<T>(), count) }
}

/// Plain-old-data elements held without their Rust type: a copy of a sender's [`StablePod`]
/// elements with their layout's name and hash, read back as any `T` with the same layout.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PodValue {
    name: String,
    hash: u64,
    size: usize,
    align: usize,
    count: usize,
    slice: bool,
    /// The elements' bytes, in 8-byte words so any [`StablePod`] alignment holds.
    words: Vec<u64>,
}

impl PodValue {
    /// A copy of `elems`.
    pub fn from_slice<T: StablePod>(elems: &[T]) -> Self {
        let mut value = Self::default();
        // Safety: `elems` is a valid slice of `T`, described by `T::LAYOUT`.
        unsafe {
            value.assign(&T::LAYOUT, true, elems.as_ptr().cast(), elems.len());
        }
        value
    }

    /// A copy of `elem`, as a single value.
    pub fn from_value<T: StablePod>(elem: &T) -> Self {
        let mut value = Self::default();
        // Safety: as above.
        unsafe { value.assign(&T::LAYOUT, false, (elem as *const T).cast(), 1) };
        value
    }

    /// Replace the contents with `count` elements of `layout` at `data`, reusing the buffers.
    ///
    /// # Safety
    /// `data` must be valid for `count * layout.size` bytes.
    pub unsafe fn assign(
        &mut self,
        layout: &PodLayout,
        slice: bool,
        data: *const u8,
        count: usize,
    ) {
        // Safety: forwarded from the caller.
        unsafe {
            self.assign_raw(
                layout.name,
                layout.hash,
                layout.size,
                layout.align,
                slice,
                data,
                count,
            )
        }
    }

    /// [`Self::assign`] from a layout's parts (a sender's view, whose name is borrowed).
    ///
    /// # Safety
    /// `data` must be valid for `count * size` bytes.
    #[allow(clippy::too_many_arguments)]
    pub unsafe fn assign_raw(
        &mut self,
        name: &str,
        hash: u64,
        size: usize,
        align: usize,
        slice: bool,
        data: *const u8,
        count: usize,
    ) {
        self.name.clear();
        self.name.push_str(name);
        self.hash = hash;
        self.size = size;
        self.align = align;
        self.count = count;
        self.slice = slice;
        let bytes = count * size;
        self.words.clear();
        self.words.resize(bytes.div_ceil(8), 0);
        if bytes > 0 {
            // Safety: forwarded from the caller; `words` holds at least `bytes` bytes.
            unsafe {
                core::ptr::copy_nonoverlapping(data, self.words.as_mut_ptr().cast::<u8>(), bytes)
            };
        }
    }

    /// The layout's name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The layout's hash.
    pub fn layout_hash(&self) -> u64 {
        self.hash
    }

    /// Element size and alignment.
    pub fn elem_size(&self) -> usize {
        self.size
    }

    pub fn align(&self) -> usize {
        self.align
    }

    /// Number of elements.
    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Whether the value is a slice (`Vec<T>`) rather than one `T`.
    pub fn is_slice(&self) -> bool {
        self.slice
    }

    /// The elements' bytes.
    pub fn bytes(&self) -> &[u8] {
        // Safety: `words` holds at least `count * size` initialized bytes.
        unsafe {
            core::slice::from_raw_parts(self.words.as_ptr().cast::<u8>(), self.count * self.size)
        }
    }

    /// Pointer to the first element.
    pub fn as_ptr(&self) -> *const u8 {
        self.words.as_ptr().cast()
    }

    /// The elements as `T`, when `T`'s layout is the stored one.
    pub fn as_slice<T: StablePod>(&self) -> Result<&[T], PodMismatch> {
        T::LAYOUT.check(&self.name, self.hash, self.size, self.align)?;
        // Safety: the layout matches, so the words hold `count` valid `T`s, aligned (`T`'s
        // alignment is at most 8).
        Ok(unsafe { pod_slice(self.as_ptr(), self.count) })
    }

    /// The single element as `T`.
    pub fn get<T: StablePod>(&self) -> Result<&T, PodMismatch> {
        self.as_slice::<T>()?.first().ok_or_else(|| PodMismatch {
            expected: T::LAYOUT,
            found_name: self.name.clone(),
            found_hash: self.hash,
            found_size: self.size,
            found_align: self.align,
        })
    }
}

/// A value a [`crate::PayloadSlot`] can refill in place ([`crate::PayloadSlot::owned_reused`]):
/// `reset` lets go of what it holds but keeps its allocation.
pub trait Reusable: Send + Sync + 'static {
    fn reset(&mut self);
}

impl<T: StablePod> Reusable for T {
    fn reset(&mut self) {}
}

impl<T: Send + Sync + 'static> Reusable for Vec<T> {
    fn reset(&mut self) {
        self.clear();
    }
}

impl Reusable for PodValue {
    fn reset(&mut self) {
        self.count = 0;
        self.words.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Clone, Copy, Debug, PartialEq, Default)]
    #[repr(C)]
    struct Point {
        x: i32,
        y: f32,
    }

    // Safety: two 4-byte numbers, no padding.
    unsafe impl StablePod for Point {
        const LAYOUT: PodLayout = PodLayout::structure(
            "Point",
            8,
            4,
            &[
                PodField {
                    name: "x",
                    offset: 0,
                    hash: i32::LAYOUT.hash,
                },
                PodField {
                    name: "y",
                    offset: 4,
                    hash: f32::LAYOUT.hash,
                },
            ],
        );
    }

    #[test]
    fn layouts_differ_by_field_name_type_and_order() {
        let swapped = PodLayout::structure(
            "Point",
            8,
            4,
            &[
                PodField {
                    name: "y",
                    offset: 0,
                    hash: f32::LAYOUT.hash,
                },
                PodField {
                    name: "x",
                    offset: 4,
                    hash: i32::LAYOUT.hash,
                },
            ],
        );
        assert_ne!(swapped.hash, Point::LAYOUT.hash);
        assert_ne!(<[i32; 2]>::LAYOUT.hash, <[f32; 2]>::LAYOUT.hash);
        assert_ne!(<[i32; 2]>::LAYOUT.hash, <[i32; 3]>::LAYOUT.hash);
        assert_eq!(<[Point; 4]>::LAYOUT.size, 32);
    }

    #[test]
    fn a_pod_value_reads_back_only_as_its_layout() {
        let points = [Point { x: 1, y: 2.5 }, Point { x: -3, y: 0.25 }];
        let value = PodValue::from_slice(&points);
        assert_eq!(value.as_slice::<Point>().unwrap(), &points);
        assert_eq!(value.bytes().len(), 16);
        let err = value.as_slice::<[i32; 2]>().unwrap_err();
        assert_eq!(err.found_name, "Point");
        assert!(PodValue::from_value(&points[0]).get::<Point>().is_ok());
    }
}
