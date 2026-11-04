//! Where a payload's value lives: shared storage in an `Arc`, or a primitive held in the payload.

use crate::portable::{Arc, arc_dyn};
use core::any::{Any, TypeId};

use super::storage::PayloadStorage;

/// A payload's value: type-erased storage shared through an `Arc`, or a primitive held inline.
///
/// An inline value is unique: it is held by its payload alone, and cloning the payload copies the
/// value. Only the types of [`InlineValue`] are held inline, so the copy is always the value
/// itself, never a second handle to shared state.
#[derive(Clone)]
pub(super) enum Body {
    Shared(Arc<dyn PayloadStorage>),
    Inline(InlineValue),
}

impl Body {
    /// The storage, whichever form the value takes.
    pub(super) fn get(&self) -> &dyn PayloadStorage {
        match self {
            Self::Shared(storage) => storage.as_ref(),
            Self::Inline(value) => value,
        }
    }

    /// Mutable access to the storage: `None` while shared storage has other holders.
    pub(super) fn get_mut(&mut self) -> Option<&mut (dyn PayloadStorage + '_)> {
        match self {
            Self::Shared(storage) => Arc::get_mut(storage).map(|storage| storage as _),
            Self::Inline(value) => Some(value),
        }
    }

    /// Strong references to the shared storage. An inline value has one: its payload.
    pub(super) fn strong_count(&self) -> usize {
        match self {
            Self::Shared(storage) => Arc::strong_count(storage),
            Self::Inline(_) => 1,
        }
    }

    /// The storage's address, for identity checks against a reference into it.
    pub(super) fn addr(&self) -> *const (dyn PayloadStorage + '_) {
        match self {
            Self::Shared(storage) => Arc::as_ptr(storage),
            Self::Inline(value) => value as *const (dyn PayloadStorage + '_),
        }
    }

    /// The storage as an `Arc`: an inline value is moved into a new allocation (only the
    /// foreign paths need this, as the inline types are never lent through them).
    pub(super) fn into_arc(self) -> Arc<dyn PayloadStorage> {
        match self {
            Self::Shared(storage) => storage,
            Self::Inline(value) => arc_dyn!(value),
        }
    }

    /// Whether two bodies hold one shared storage, or equal inline values (see
    /// [`Payload::shares_storage`](super::Payload::shares_storage)).
    pub(super) fn shares_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Shared(a), Self::Shared(b)) => {
                core::ptr::addr_eq(Arc::as_ptr(a), Arc::as_ptr(b))
            }
            (Self::Inline(a), Self::Inline(b)) => a.same_value(b),
            _ => false,
        }
    }
}

/// Generates [`InlineValue`] from the list of inline types: its variants, and the typed
/// accessors that the variants' payloads answer.
macro_rules! inline_values {
    ($($variant:ident($ty:ty)),* $(,)?) => {
        /// A primitive Rust value held in a payload instead of an `Arc`: what
        /// [`Payload::owned`](super::Payload::owned) keeps inline. Each type is `Copy` with no
        /// drop glue, so a copy of the value is as good as the value itself.
        #[derive(Clone, Copy, Debug, PartialEq)]
        pub(super) enum InlineValue {
            $($variant($ty)),*
        }

        impl InlineValue {
            /// The inline value holding `value`, when `T` is one of the inline types.
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
                    $(Self::$variant(v) => v),*
                }
            }

            fn any_mut(&mut self) -> &mut dyn Any {
                match self {
                    $(Self::$variant(v) => v),*
                }
            }

            fn any_sync(&self) -> &(dyn Any + Send + Sync) {
                match self {
                    $(Self::$variant(v) => v),*
                }
            }

            fn type_id(&self) -> TypeId {
                match self {
                    $(Self::$variant(_) => TypeId::of::<$ty>()),*
                }
            }

            fn type_name(&self) -> &'static str {
                match self {
                    $(Self::$variant(_) => core::any::type_name::<$ty>()),*
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

    /// Whether `self` and `other` hold the same value of the same type. Floats compare by bits,
    /// so a NaN equals itself.
    pub(super) fn same_value(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::F32(a), Self::F32(b)) => a.to_bits() == b.to_bits(),
            (Self::F64(a), Self::F64(b)) => a.to_bits() == b.to_bits(),
            _ => self == other,
        }
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
        assert!(matches!(payload.storage, super::Body::Shared(_)));
        assert_eq!(payload.get_ref::<String>().map(String::as_str), Some("hi"));
        assert_eq!(
            payload.try_into_owned::<String>().ok().as_deref(),
            Some("hi")
        );
        let pair = Payload::owned("test:pair", (1u8, 2u8));
        assert!(matches!(pair.storage, super::Body::Shared(_)));
    }

    #[test]
    fn a_take_of_the_wrong_type_hands_the_payload_back() {
        let payload = Payload::owned("test:i64", 3i64);
        let back = payload.try_into_owned::<u64>().expect_err("not a u64");
        assert_eq!(back.get_ref::<i64>(), Some(&3));
    }

    #[test]
    fn inline_payloads_are_small() {
        // The storage is an `Arc<dyn PayloadStorage>` or an inline value: the enum adds 8 bytes
        // to the 176 bytes a payload took with the `Arc` alone. Guard against further growth.
        let size = core::mem::size_of::<Payload>();
        assert!(size <= 184, "a payload is {size} bytes");
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
