//! Plain-old-data values ([`StablePod`](crate::transport::StablePod) types and their `Vec`s) on
//! the stable path: lent as a [`StablePodView`] of the sender's elements, checked against the
//! receiver's layout and copied once into the receiver's own type (or a [`PodValue`] when it has
//! no Rust type for them). Nothing goes through `Value`.

use std::any::Any;
use std::ffi::c_void;

use super::value::{Arena, StablePodView, StableValue, tag};
use crate::transport::{POD_MAX_ALIGN, Payload, PayloadSlot, PodCodec, PodValue, TypeKey};

/// Lend `any`'s elements when it is `codec`'s port type.
pub fn encode(
    arena: &mut Arena,
    codec: &PodCodec,
    any: &(dyn Any + Send + Sync),
) -> Option<StableValue> {
    let (data, count) = (codec.elems)(any)?;
    let layout = &codec.layout;
    let view = StablePodView {
        hash: layout.hash,
        size: layout.size,
        align: layout.align,
        count,
        data: data.cast::<c_void>(),
        name_ptr: layout.name.as_ptr(),
        name_len: layout.name.len(),
    };
    Some(arena.pod(view, codec.slice))
}

/// Lend a [`PodValue`]'s elements.
pub fn encode_value(arena: &mut Arena, value: &PodValue) -> StableValue {
    let view = StablePodView {
        hash: value.layout_hash(),
        size: value.elem_size(),
        align: value.align(),
        count: value.len(),
        data: value.as_ptr().cast::<c_void>(),
        name_ptr: value.name().as_ptr(),
        name_len: value.name().len(),
    };
    arena.pod(view, value.is_slice())
}

/// The view of a [`tag::POD`] value, with its name, checked to be readable: a known alignment,
/// aligned data, a byte length that fits.
///
/// # Safety
/// Every pointer in `value` must be valid for the call.
unsafe fn view<'a>(value: &StableValue) -> Result<(&'a StablePodView, &'a str), String> {
    // Safety: forwarded from the caller.
    let view = unsafe { value.ptr.cast::<StablePodView>().as_ref() }
        .filter(|_| value.tag == tag::POD)
        .ok_or("not a plain-old-data value")?;
    let name = if view.name_len == 0 || view.name_ptr.is_null() {
        ""
    } else {
        // Safety: forwarded from the caller.
        std::str::from_utf8(unsafe { std::slice::from_raw_parts(view.name_ptr, view.name_len) })
            .map_err(|err| err.to_string())?
    };
    let aligned = view.align.is_power_of_two()
        && view.align <= POD_MAX_ALIGN
        && (view.data as usize).is_multiple_of(view.align);
    if !aligned || view.count != value.len || view.size.checked_mul(view.count).is_none() {
        return Err(format!("malformed plain-old-data view of `{name}`"));
    }
    if view.count > 0 && view.data.is_null() {
        return Err(format!("plain-old-data view of `{name}` without data"));
    }
    Ok((view, name))
}

/// Build `codec`'s port type from a [`tag::POD`] value, after checking its layout.
///
/// # Safety
/// Every pointer in `value` must be valid for the call.
pub unsafe fn decode(
    codec: &PodCodec,
    key: TypeKey,
    slot: &mut PayloadSlot,
    value: &StableValue,
) -> Result<Payload, String> {
    // Safety: forwarded from the caller.
    let (view, name) = unsafe { view(value) }?;
    codec
        .layout
        .check(name, view.hash, view.size, view.align)
        .map_err(|err| err.to_string())?;
    if !codec.slice && view.count != 1 {
        return Err(format!(
            "expected one `{}`, got {} elements",
            codec.layout.name, view.count
        ));
    }
    // Safety: the layout matches the receiver's type, and `data` holds `count` aligned elements
    // for the call.
    Ok(unsafe { (codec.build)(slot, key, view.data.cast(), view.count) })
}

/// A [`PodValue`] copy of a [`tag::POD`] value, for a side without its Rust type.
///
/// # Safety
/// Every pointer in `value` must be valid for the call.
pub unsafe fn decode_value(
    key: TypeKey,
    slot: &mut PayloadSlot,
    value: &StableValue,
) -> Result<Payload, String> {
    // Safety: forwarded from the caller.
    let (view, name) = unsafe { view(value) }?;
    let slice = value.scalar != 0;
    let fill = |pod: &mut PodValue| {
        // Safety: `view` checked that `data` holds `count * size` bytes for the call.
        unsafe {
            pod.assign_raw(
                name,
                view.hash,
                view.size,
                view.align,
                slice,
                view.data.cast(),
                view.count,
            );
        }
    };
    Ok(slot.owned_reused::<PodValue>(
        key,
        || {
            let mut pod = PodValue::default();
            fill(&mut pod);
            pod
        },
        |pod| fill(pod),
    ))
}
