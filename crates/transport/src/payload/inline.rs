//! Where a payload's value lives: shared storage in an `Arc`, or a value held in the payload.

use crate::portable::{Arc, arc_dyn};
use core::any::{Any, TypeId};
use core::fmt;
use core::marker::PhantomData;
use core::mem::{MaybeUninit, align_of, size_of};

use super::foreign::ForeignStorage;
use super::storage::PayloadStorage;
use crate::Residency;

/// A payload's value: type-erased storage shared through an `Arc`, a value held inline, or a
/// foreign handle held inline.
///
/// An inline value is unique: it is held by its payload alone, and cloning the payload copies the
/// value. Only the types of [`InlineValue`] are held inline, so the copy is always the value
/// itself, never a second handle to shared state.
///
/// A foreign handle ([`Payload::foreign`](super::Payload::foreign)) is held in the payload too,
/// so carrying a frame or another handle allocates nothing. Cloning the payload retains the
/// handle (the owner's reference count, as an `Arc` clone would), and the last payload to drop
/// releases it: nothing outlives the payloads, so a frame's buffers go back to their owner as
/// soon as the graph lets go of them.
#[derive(Clone)]
pub(super) enum Body {
    Shared(Arc<dyn PayloadStorage>, Residency),
    Inline(InlineValue, Residency),
    Foreign(ForeignStorage<false>, Residency),
    /// A handle this build created ([`Payload::foreign_local`](super::Payload::foreign_local)).
    LocalForeign(ForeignStorage<true>, Residency),
}

impl Body {
    /// The payload's residency, which the body holds: its tag word has the spare bytes, so the
    /// payload stays 208 bytes with a handle held inline.
    pub(super) fn residency(&self) -> Residency {
        match self {
            Self::Shared(_, residency)
            | Self::Inline(_, residency)
            | Self::Foreign(_, residency)
            | Self::LocalForeign(_, residency) => *residency,
        }
    }

    /// The storage, whichever form the value takes.
    pub(super) fn get(&self) -> &dyn PayloadStorage {
        match self {
            Self::Shared(storage, _) => storage.as_ref(),
            Self::Inline(value, _) => value,
            Self::Foreign(value, _) => value,
            Self::LocalForeign(value, _) => value,
        }
    }

    /// Mutable access to the storage: `None` while shared storage has other holders.
    pub(super) fn get_mut(&mut self) -> Option<&mut (dyn PayloadStorage + '_)> {
        match self {
            Self::Shared(storage, _) => Arc::get_mut(storage).map(|storage| storage as _),
            Self::Inline(value, _) => Some(value),
            Self::Foreign(value, _) => Some(value),
            Self::LocalForeign(value, _) => Some(value),
        }
    }

    /// Strong references to the shared storage. An inline value or handle has one: its payload.
    pub(super) fn strong_count(&self) -> usize {
        match self {
            Self::Shared(storage, _) => Arc::strong_count(storage),
            Self::Inline(_, _) | Self::Foreign(_, _) | Self::LocalForeign(_, _) => 1,
        }
    }

    /// The storage's address, for identity checks against a reference into it.
    pub(super) fn addr(&self) -> *const (dyn PayloadStorage + '_) {
        match self {
            Self::Shared(storage, _) => Arc::as_ptr(storage),
            Self::Inline(value, _) => value as *const (dyn PayloadStorage + '_),
            Self::Foreign(value, _) => value as *const (dyn PayloadStorage + '_),
            Self::LocalForeign(value, _) => value as *const (dyn PayloadStorage + '_),
        }
    }

    /// The storage as an `Arc`: an inline value or handle is moved into a new allocation (only
    /// the owner-retyping paths need this, and they never see either).
    pub(super) fn into_arc(self) -> Arc<dyn PayloadStorage> {
        match self {
            Self::Shared(storage, _) => storage,
            Self::Inline(value, _) => arc_dyn!(value),
            Self::Foreign(value, _) => arc_dyn!(value),
            Self::LocalForeign(value, _) => arc_dyn!(value),
        }
    }

    /// Whether two bodies hold one shared storage, equal inline values, or one foreign value
    /// (same data and owner) (see
    /// [`Payload::shares_storage`](super::Payload::shares_storage)).
    pub(super) fn shares_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Shared(a, _), Self::Shared(b, _)) => {
                core::ptr::addr_eq(Arc::as_ptr(a), Arc::as_ptr(b))
            }
            (Self::Inline(a, _), Self::Inline(b, _)) => a.same_value(b),
            (Self::Foreign(a, _), Self::Foreign(b, _)) => a.same_value(b),
            (Self::LocalForeign(a, _), Self::LocalForeign(b, _)) => a.same_value(b),
            _ => false,
        }
    }
}

/// Generates [`InlineValue`] from the list of inline primitive types: its variants, and the typed
/// accessors that the variants' payloads answer. [`InlineValue::Plain`] is the one variant outside
/// the list.
macro_rules! inline_values {
    ($($variant:ident($ty:ty)),* $(,)?) => {
        /// A value a payload holds instead of an `Arc`: a primitive that
        /// [`Payload::owned`](super::Payload::owned) keeps inline (each type is `Copy` with no drop
        /// glue, so a copy of the value is as good as the value itself), or a plain value of
        /// another `Copy` type that fits ([`Payload::plain`](super::Payload::plain)).
        #[derive(Clone, Copy, Debug, PartialEq)]
        pub(super) enum InlineValue {
            $($variant($ty),)*
            Plain(PlainValue),
        }

        impl InlineValue {
            /// The inline value holding `value`, when `T` is one of the inline primitives.
            pub(super) fn of<T: Send + Sync + 'static>(value: &T) -> Option<Self> {
                let any: &dyn Any = value;
                $(
                    if let Some(v) = any.downcast_ref::<$ty>() {
                        return Some(Self::$variant(*v));
                    }
                )*
                None
            }

            fn any(&self) -> &dyn Any {
                match self {
                    $(Self::$variant(v) => v,)*
                    Self::Plain(v) => v.any(),
                }
            }

            fn any_mut(&mut self) -> &mut dyn Any {
                match self {
                    $(Self::$variant(v) => v,)*
                    Self::Plain(v) => v.any_mut(),
                }
            }

            fn any_sync(&self) -> &(dyn Any + Send + Sync) {
                match self {
                    $(Self::$variant(v) => v,)*
                    Self::Plain(v) => v.any_sync(),
                }
            }

            fn type_id(&self) -> TypeId {
                match self {
                    $(Self::$variant(_) => TypeId::of::<$ty>(),)*
                    Self::Plain(v) => v.type_id(),
                }
            }

            fn type_name(&self) -> &'static str {
                match self {
                    $(Self::$variant(_) => core::any::type_name::<$ty>(),)*
                    Self::Plain(v) => v.type_name(),
                }
            }
        }

        impl PayloadStorage for InlineValue {
            fn as_any(&self) -> &dyn Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn Any {
                self
            }

            fn value_any(&self) -> Option<&dyn Any> {
                Some(self.any())
            }

            fn value_any_sync(&self) -> Option<&(dyn Any + Send + Sync)> {
                Some(self.any_sync())
            }

            fn rust_type_name(&self) -> Option<&'static str> {
                Some(self.type_name())
            }

            fn rust_type_id(&self) -> Option<TypeId> {
                Some(self.type_id())
            }
        }
    };
}

inline_values! {
    Unit(()),
    Bool(bool),
    Char(char),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    Usize(usize),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    Isize(isize),
    F32(f32),
    F64(f64),
}

impl InlineValue {
    /// The value as `T`, when it is one.
    pub(super) fn value_ref<T: 'static>(&self) -> Option<&T> {
        self.any().downcast_ref::<T>()
    }

    /// The value as `&mut T`, when it is one.
    pub(super) fn value_mut<T: 'static>(&mut self) -> Option<&mut T> {
        self.any_mut().downcast_mut::<T>()
    }

    /// The value as an owned `T`, when it is one.
    pub(super) fn take<T: 'static>(self) -> Option<T> {
        let value = self.value_ref::<T>()?;
        // Safety: `value` refers to a `T` (the downcast checked the type), and every inline type
        // is `Copy`, so a bitwise copy is a valid, independent `T`; `self` is consumed, so the
        // source is never used again.
        Some(unsafe { core::ptr::read(value) })
    }

    /// The inline value holding a `Copy` `value`: a primitive as [`Self::of`], or a plain value
    /// when `T` fits ([`PlainValue::fits`]).
    pub(super) fn copied<T: Copy + Send + Sync + 'static>(value: &T) -> Option<Self> {
        Self::of(value)
            .or_else(|| PlainValue::fits::<T>().then(|| Self::Plain(PlainValue::new(*value))))
    }

    /// Whether `self` and `other` hold the same value of the same type. Floats compare by bits,
    /// so a NaN equals itself. A plain value equals nothing (see [`PlainValue`]).
    pub(super) fn same_value(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::F32(a), Self::F32(b)) => a.to_bits() == b.to_bits(),
            (Self::F64(a), Self::F64(b)) => a.to_bits() == b.to_bits(),
            _ => self == other,
        }
    }
}

/// Words a [`PlainValue`] holds: 32 bytes, the largest output the detector graph passes
/// (`Histogram`), aligned for any value that fits ([`PlainValue::fits`]).
const PLAIN_WORDS: usize = 4;
type PlainWords = [MaybeUninit<u64>; PLAIN_WORDS];

/// A `Copy` value of at most `PLAIN_WORDS * 8` bytes and alignment at most 8, held in its own
/// bytes: a payload of it allocates nothing.
///
/// The value's type is described by a static [`PlainVtable`], which views the bytes as the type
/// through `Any`. A plain value is unique (a clone copies its bytes), and it equals no other:
/// its bytes may hold padding, which is not compared, so
/// [`Payload::shares_storage`](super::Payload::shares_storage) is false for it.
#[derive(Clone, Copy)]
pub(super) struct PlainValue {
    vtable: &'static PlainVtable,
    words: PlainWords,
}

/// What a plain value's type provides: its name, and the held bytes viewed as the type.
struct PlainVtable {
    name: fn() -> &'static str,
    any: fn(&PlainWords) -> &dyn Any,
    any_sync: fn(&PlainWords) -> &(dyn Any + Send + Sync),
    any_mut: fn(&mut PlainWords) -> &mut dyn Any,
}

/// The vtable of `T`: one static per type, which every plain value of `T` points at.
struct PlainOf<T>(PhantomData<fn() -> T>);

impl<T: Copy + Send + Sync + 'static> PlainOf<T> {
    const VTABLE: PlainVtable = PlainVtable {
        name: core::any::type_name::<T>,
        any: view::<T>,
        any_sync: view_sync::<T>,
        any_mut: view_mut::<T>,
    };
}

/// `T` in `words`: the bytes [`PlainValue::new`] wrote for it.
fn words_ref<T>(words: &PlainWords) -> &T {
    // Safety: a `PlainValue` of `T` wrote a `T` at the start of `words` (its vtable is `T`'s),
    // and the words are aligned for `T` (`PlainValue::fits`).
    unsafe { &*words.as_ptr().cast::<T>() }
}

/// `T` in `words`, mutably: see [`words_ref`].
fn words_mut<T>(words: &mut PlainWords) -> &mut T {
    // Safety: as [`words_ref`].
    unsafe { &mut *words.as_mut_ptr().cast::<T>() }
}

fn view<T: Any>(words: &PlainWords) -> &dyn Any {
    words_ref::<T>(words)
}

fn view_sync<T: Any + Send + Sync>(words: &PlainWords) -> &(dyn Any + Send + Sync) {
    words_ref::<T>(words)
}

fn view_mut<T: Any>(words: &mut PlainWords) -> &mut dyn Any {
    words_mut::<T>(words)
}

impl PlainValue {
    /// Whether a `T` fits in a plain value: at most the words' bytes, aligned no more than them.
    pub(super) const fn fits<T>() -> bool {
        size_of::<T>() <= PLAIN_WORDS * size_of::<u64>() && align_of::<T>() <= align_of::<u64>()
    }

    /// A plain value holding `value`, which must fit ([`Self::fits`]).
    pub(super) fn new<T: Copy + Send + Sync + 'static>(value: T) -> Self {
        assert!(Self::fits::<T>(), "a plain value must fit its words");
        let mut words = [MaybeUninit::<u64>::uninit(); PLAIN_WORDS];
        // Safety: `fits` holds, so the value's bytes fill the start of `words`, and the words are
        // aligned for `T`.
        unsafe { words.as_mut_ptr().cast::<T>().write(value) };
        Self {
            vtable: &PlainOf::<T>::VTABLE,
            words,
        }
    }

    fn any(&self) -> &dyn Any {
        (self.vtable.any)(&self.words)
    }

    fn any_sync(&self) -> &(dyn Any + Send + Sync) {
        (self.vtable.any_sync)(&self.words)
    }

    fn any_mut(&mut self) -> &mut dyn Any {
        (self.vtable.any_mut)(&mut self.words)
    }

    fn type_id(&self) -> TypeId {
        Any::type_id(self.any())
    }

    fn type_name(&self) -> &'static str {
        (self.vtable.name)()
    }
}

/// A plain value equals nothing, not even itself (see [`PlainValue`]): comparing its bytes would
/// read padding.
impl PartialEq for PlainValue {
    fn eq(&self, _other: &Self) -> bool {
        false
    }
}

impl fmt::Debug for PlainValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlainValue")
            .field("type", &self.type_name())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::Payload;

    #[test]
    fn primitives_are_held_inline_and_read_back_as_their_type() {
        let payload = Payload::owned("test:i64", -7i64);
        assert_eq!(payload.get_ref::<i64>(), Some(&-7));
        assert_eq!(payload.get_ref::<u64>(), None);
        assert_eq!(
            payload.storage_rust_type_id(),
            Some(core::any::TypeId::of::<i64>())
        );
        assert_eq!(payload.storage_rust_type_name(), Some("i64"));
        assert_eq!(
            payload.value_any().and_then(|v| v.downcast_ref::<i64>()),
            Some(&-7)
        );
        assert_eq!(
            payload
                .value_any_sync()
                .and_then(|v| v.downcast_ref::<i64>()),
            Some(&-7)
        );
        assert_eq!(payload.bytes_estimate(), None);
        assert!(payload.is_storage_unique());
        assert_eq!(payload.typed_strong_count::<i64>(), Some(1));
        assert!(payload.is_typed_unique::<i64>());
        assert!(payload.get_bytes().is_none());
        assert!(payload.foreign_handle().is_none());
        assert!(payload.boundary_contract().is_none());
    }

    #[test]
    fn every_inline_type_round_trips_through_its_accessors() {
        macro_rules! round_trip {
            ($($ty:ty: $value:expr),* $(,)?) => {$(
                let value: $ty = $value;
                let mut payload = Payload::owned("test:prim", value);
                assert_eq!(payload.get_ref::<$ty>(), Some(&value));
                assert_eq!(payload.get_mut::<$ty>().map(|v| *v), Some(value));
                assert_eq!(payload.try_into_owned::<$ty>().ok(), Some(value));
            )*};
        }
        round_trip!(
            bool: true,
            u8: b'x',
            u16: 513,
            u32: 70_000,
            u64: 5_000_000_000,
            usize: 9,
            i8: -3,
            i16: -300,
            i32: -70_000,
            i64: -5_000_000_000,
            isize: -9,
            f32: 1.5,
            f64: 2.25,
            char: 'λ',
        );
        let unit = Payload::owned("test:unit", ());
        assert_eq!(unit.get_ref::<()>(), Some(&()));
        assert_eq!(unit.try_into_owned::<()>().ok(), Some(()));
    }

    #[test]
    fn a_clone_is_an_independent_copy_of_the_value() {
        let mut original = Payload::owned("test:i64", 41i64);
        let copy = original.clone();
        assert!(original.shares_storage(&copy), "equal values share");
        assert!(
            original.is_typed_unique::<i64>(),
            "inline values are unique"
        );
        *original.get_mut::<i64>().unwrap() += 1;
        assert_eq!(original.get_ref::<i64>(), Some(&42));
        assert_eq!(
            copy.get_ref::<i64>(),
            Some(&41),
            "the clone keeps its value"
        );
        assert!(
            !original.shares_storage(&copy),
            "a changed value no longer shares"
        );
    }

    #[test]
    fn equal_inline_values_share_and_floats_compare_by_bits() {
        let a = Payload::owned("test:f64", f64::NAN);
        let b = Payload::owned("test:f64", f64::NAN);
        assert!(a.shares_storage(&b));
        assert!(!a.shares_storage(&Payload::owned("test:f64", 0.0f64)));
        assert!(!a.shares_storage(&Payload::owned("test:u64", f64::NAN.to_bits())));
        let zero = Payload::owned("test:f32", 0.0f32);
        assert!(!zero.shares_storage(&Payload::owned("test:f32", -0.0f32)));
    }

    #[test]
    fn inline_payloads_never_share_with_arc_storage() {
        let inline = Payload::owned("test:i64", 5i64);
        let shared = Payload::shared("test:i64", crate::portable::Arc::new(5i64));
        assert!(!inline.shares_storage(&shared));
        assert!(!shared.shares_storage(&inline));
    }

    #[test]
    fn get_arc_hands_out_a_fresh_allocation_of_the_value() {
        let payload = Payload::owned("test:i64", 8i64);
        let arc = payload.get_arc::<i64>().expect("the value");
        assert_eq!(*arc, 8);
        assert_eq!(
            payload.get_ref::<i64>(),
            Some(&8),
            "the payload keeps the value"
        );
    }

    #[test]
    fn a_non_primitive_takes_the_arc_path() {
        let payload = Payload::owned("test:string", String::from("hi"));
        assert!(matches!(payload.storage, super::Body::Shared(..)));
        assert_eq!(payload.get_ref::<String>().map(String::as_str), Some("hi"));
        assert_eq!(
            payload.try_into_owned::<String>().ok().as_deref(),
            Some("hi")
        );
        let pair = Payload::owned("test:pair", (1u8, 2u8));
        assert!(matches!(pair.storage, super::Body::Shared(..)));
    }

    #[test]
    fn a_take_of_the_wrong_type_hands_the_payload_back() {
        let payload = Payload::owned("test:i64", 3i64);
        let back = payload.try_into_owned::<u64>().expect_err("not a u64");
        assert_eq!(back.get_ref::<i64>(), Some(&3));
    }

    #[test]
    fn inline_payloads_are_small() {
        // The storage is an `Arc<dyn PayloadStorage>`, an inline value (a plain value's 32 bytes
        // and its vtable pointer) or a foreign handle (48 bytes), with the residency in the tag
        // word. Guard against further growth.
        let size = core::mem::size_of::<Payload>();
        assert!(size <= SIZE_BOUND, "a payload is {size} bytes");
    }

    /// The largest `Payload`: 208 bytes (184 before plain values). A foreign handle held inline
    /// would make it 216, but the residency sits in the body's tag word.
    const SIZE_BOUND: usize = 208;

    #[derive(Clone, Copy, Debug, PartialEq)]
    #[allow(dead_code, reason = "the fields are compared, not read")]
    struct Roi {
        x: u32,
        y: u32,
        w: u32,
        h: u32,
    }

    #[derive(Clone, Copy, Debug, PartialEq)]
    #[allow(dead_code, reason = "the bins are compared, not read")]
    struct Histogram([u32; 8]);

    #[test]
    fn a_plain_value_is_held_inline_and_read_back_as_its_type() {
        let roi = Roi {
            x: 1,
            y: 2,
            w: 3,
            h: 4,
        };
        let payload = Payload::plain("test:roi", roi);
        assert!(matches!(payload.storage, super::Body::Inline(..)));
        assert_eq!(payload.get_ref::<Roi>(), Some(&roi));
        assert_eq!(payload.get_ref::<u32>(), None);
        assert_eq!(payload.typed_strong_count::<Roi>(), Some(1));
        assert!(payload.is_typed_unique::<Roi>());
        assert!(payload.bytes_estimate().is_none());
        assert_eq!(
            payload.storage_rust_type_name(),
            Some(core::any::type_name::<Roi>())
        );
        assert_eq!(
            payload.try_into_owned::<Roi>().ok(),
            Some(roi),
            "the owned value comes back whole"
        );
    }

    #[test]
    fn the_detector_outputs_fit_inline() {
        // The largest detector output is `Histogram` (32 bytes), and `Report` is 24.
        assert!(super::PlainValue::fits::<Histogram>());
        assert!(super::PlainValue::fits::<[u64; 4]>());
        assert!(!super::PlainValue::fits::<[u64; 5]>());
        assert!(!super::PlainValue::fits::<[u8; 33]>());
    }

    #[test]
    fn a_plain_clone_is_an_independent_copy() {
        let mut original = Payload::plain(
            "test:roi",
            Roi {
                x: 9,
                y: 0,
                w: 0,
                h: 0,
            },
        );
        let copy = original.clone();
        assert!(!original.shares_storage(&copy), "plain values never share");
        assert!(original.is_typed_unique::<Roi>());
        original.get_mut::<Roi>().unwrap().x = 10;
        assert_eq!(original.get_ref::<Roi>().map(|r| r.x), Some(10));
        assert_eq!(copy.get_ref::<Roi>().map(|r| r.x), Some(9));
    }

    #[test]
    fn a_plain_value_too_large_takes_the_arc_path() {
        let payload = Payload::plain("test:wide", [7u64; 5]);
        assert!(matches!(payload.storage, super::Body::Shared(..)));
        assert_eq!(payload.get_ref::<[u64; 5]>(), Some(&[7u64; 5]));
    }

    #[test]
    fn a_plain_value_over_aligned_takes_the_arc_path() {
        #[derive(Clone, Copy, Debug, PartialEq)]
        #[repr(align(16))]
        #[allow(dead_code, reason = "the value is compared, not read")]
        struct Aligned(u8);
        let payload = Payload::plain("test:aligned", Aligned(3));
        assert!(matches!(payload.storage, super::Body::Shared(..)));
        assert_eq!(payload.get_ref::<Aligned>(), Some(&Aligned(3)));
    }

    #[test]
    fn a_probe_holds_a_copy_inline_and_moves_any_other_value() {
        use crate::{CopyOutput as _, OutputProbe, OwnedOutput as _};
        let copied = OutputProbe::new(Histogram([1; 8])).output_payload("test:hist".into());
        assert!(matches!(copied.storage, super::Body::Inline(..)));
        assert_eq!(copied.get_ref::<Histogram>(), Some(&Histogram([1; 8])));

        let moved = OutputProbe::new(String::from("owned")).output_payload("test:string".into());
        assert!(matches!(moved.storage, super::Body::Shared(..)));
        assert_eq!(moved.get_ref::<String>().map(String::as_str), Some("owned"));
    }

    #[test]
    fn residency_and_lineage_survive_an_inline_payload() {
        let payload = Payload::owned("test:i64", 6i64).with_lineage(Default::default());
        let cached = payload
            .clone()
            .with_cached_resident(Payload::owned("test:i64", 6i64));
        assert!(cached.has_resident(&"test:i64".into(), payload.residency(), None));
        assert_eq!(
            cached.resident_ref::<i64>(&"test:i64".into(), payload.residency(), None),
            Some(&6)
        );
    }
}
