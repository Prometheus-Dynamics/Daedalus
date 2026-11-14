//! A run's values for a worker that agreed values by reference, and values in a buffer by
//! descriptor (a dma-buf) for any worker.
//!
//! To a worker that agreed values by reference, a port's whole value has a form byte when
//! `daedalus_wire::shared::has_form` says so: a `bytes` or opaque value, and with `ANY_TYPE` any
//! value but a fixed-size scalar. Its bytes follow, or it is a place
//! (`daedalus_wire::shared::Place`). Whether it is a place is decided by its size alone. A large
//! input is written into the host's lent buffer as its cell holds it (a compound value's cell
//! holds its wire form) and sent as its place, with an array's items on an aligned address; a
//! value whose cell holds a type
//! registered with [`crate::ProcessBackend::with_dmabuf`] is sent as its buffer's descriptor,
//! once, and by slot after, so the worker reads the buffer itself and nothing is copied. An output
//! the worker wrote into a value buffer is held by the output cell as it is.
//!
//! A worker on the plain path is sent a dma-buf value's bytes: the host maps the buffer, read-only,
//! and copies them into the packet, inside the kernel's CPU access brackets.

use daedalus_abi::{SharedBytes, Slot};
use daedalus_wire::WireType;
use daedalus_wire::codec::{Reader, Writer};
use daedalus_wire::shared::{Place, form};

use crate::child::Worker;

/// A type whose values are a buffer by descriptor, registered with the backend.
#[cfg(target_os = "linux")]
#[derive(Clone, Copy)]
pub(crate) struct DmaBufType {
    /// The buffer behind a cell of this type, the value's offset in it and its length. None when
    /// the cell holds another type, or a value with no buffer.
    pub fd_of: for<'a> fn(&'a Slot) -> Option<(std::os::fd::BorrowedFd<'a>, u64, u64)>,
}

#[cfg(target_os = "linux")]
pub(crate) fn fd_of<T: daedalus_dmabuf::DmaBufBacked>(
    slot: &Slot,
) -> Option<(std::os::fd::BorrowedFd<'_>, u64, u64)> {
    let value = daedalus_runner::typed_view::<T>(slot)?;
    let (fd, len) = value.dmabuf()?;
    Some((fd, value.offset(), len))
}

/// Whether a port's whole value can be a place, for a worker that agreed `features`.
pub(crate) fn by_place(features: u32, ty: &WireType) -> bool {
    daedalus_wire::shared::has_form(features, ty)
}

/// Whether a value by descriptor can stand for a port's value: a `bytes` or opaque port.
#[cfg(target_os = "linux")]
fn by_descriptor(ty: &WireType) -> bool {
    matches!(
        ty,
        WireType::Scalar(daedalus_types::Scalar::Bytes) | WireType::Opaque(_)
    )
}

/// One input cell's value, as a run sends it.
pub(crate) enum Input<'a> {
    Bytes(&'a [u8]),
    #[cfg(target_os = "linux")]
    Fd(std::os::fd::BorrowedFd<'a>, u64, u64),
}

/// What an input cell holds that a run can send, if anything.
pub(crate) fn input_of<'a>(
    #[cfg(target_os = "linux")] types: &[DmaBufType],
    ty: &WireType,
    cell: &'a Slot,
) -> Option<Input<'a>> {
    #[cfg(target_os = "linux")]
    if by_descriptor(ty)
        && let Some((fd, offset, len)) = types.iter().find_map(|kind| (kind.fd_of)(cell))
    {
        return Some(Input::Fd(fd, offset, len));
    }
    #[cfg(not(target_os = "linux"))]
    let _ = ty;
    cell.bytes().map(Input::Bytes)
}

/// Writes an input's form for a worker that agreed values by reference: its place when it is
/// large and fits, or it is by descriptor, and otherwise the form that says its bytes follow,
/// which the caller then writes. Returns whether it went as a place.
pub(crate) fn put_placed(
    worker: &mut Worker,
    ty: &WireType,
    input: &Input<'_>,
    out: &mut Writer,
) -> Result<bool, String> {
    match input {
        Input::Bytes(bytes) => {
            let items_at = ty.layout().items_at().unwrap_or(0);
            if bytes.len() >= worker.shared_from()
                && let Some(place) = worker.place(bytes, items_at)
            {
                place.encode(out);
                return Ok(true);
            }
            out.u8(form::INLINE);
            Ok(false)
        }
        #[cfg(target_os = "linux")]
        Input::Fd(fd, offset, len) => {
            worker.fd_place(*fd, *offset, *len)?.encode(out);
            Ok(true)
        }
    }
}

/// The bytes of a value by descriptor, for a worker that takes no places: copied out of the
/// buffer through a read-only mapping, inside the kernel's CPU access brackets.
#[cfg(target_os = "linux")]
pub(crate) fn copy_out(
    fd: std::os::fd::BorrowedFd<'_>,
    offset: u64,
    len: u64,
    into: &mut Vec<u8>,
) -> Result<(), String> {
    use daedalus_dma_sys::{Access, DmaBufRef};
    let buf = DmaBufRef::from_fd(fd);
    let len = usize::try_from(len).map_err(|_| "a value longer than memory".to_owned())?;
    let mapping = buf
        .map_read_only()
        .map_err(|error| format!("mapping a dma-buf input: {error:?}"))?;
    into.clear();
    into.resize(len, 0);
    let bracketed = buf.begin_cpu_access(Access::Read);
    let read = mapping.read(offset, into);
    if bracketed.is_ok() {
        let _ = buf.end_cpu_access(Access::Read);
    }
    read.map_err(|error| format!("reading a dma-buf input: {error:?}"))
}

/// What a `bytes` or opaque output came as, from a worker that agreed values by reference.
pub(crate) enum Output {
    /// Its bytes follow in the packet, as without the agreement.
    Inline,
    /// It is in a value buffer, now held for the output cell.
    Held(SharedBytes),
}

/// Reads an output's form byte, and adopts the value it names when it is a place.
pub(crate) fn read_placed(
    worker: &Worker,
    input: &mut Reader<'_>,
) -> Result<Output, daedalus_wire::WireError> {
    match input.u8()? {
        form::INLINE => Ok(Output::Inline),
        tag => {
            let place = Place::decode(tag, input)?;
            worker
                .adopt(&place)
                .map(Output::Held)
                .ok_or(daedalus_wire::WireError::BadTag {
                    what: "output place outside the value buffers",
                    tag: u32::from(tag),
                })
        }
    }
}
