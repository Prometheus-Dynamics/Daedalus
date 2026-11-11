//! Which memory is whose, and what moves between.
//!
//! Each device has its own memory spaces: `wgpu.buffer` on the default device is not
//! `wgpu.b.buffer` on the device named `b`. Every move a backend lists is between the host and
//! its own device. Nothing moves a value straight from one device to another, so the only way
//! across is the one the planner can see and cost: read back on one, hand to the host, upload on
//! the other. A value with no way to the host, such as a texture, has no way across, and a
//! graph that asks for one fails to plan with the port and both spaces named.

use std::borrow::Cow;

use daedalus_abi::Step;
use daedalus_backend::{CompileCx, MemorySpace, Refusal, SpaceKind, Transfer, TransferCost};

use crate::devices::Named;
use crate::memory::{
    SPACE_DEVICE, SPACE_DMA_HEAP, SPACE_HOST_CACHED, SPACE_HOST_MAPPED, SPACE_TEXTURE,
};
use crate::transfer::{
    HOST_SPACE, MAP, MAP_CACHED, MAP_HEAP, READBACK, READBACK_HEAP, UPLOAD, UPLOAD_MAPPED, intern,
};

/// The words a space or a transfer of this backend ends in. A device can't be named one.
pub(crate) const KINDS: [&str; 8] = [
    "buffer", "mapped", "cached", "dmaheap", "texture", "upload", "readback", "map",
];

/// The id `of`, a space or transfer of the default device such as `wgpu.buffer`, has on the
/// device called `device`: `wgpu.<device>.buffer`. With no name it is `of`.
pub fn on_device(device: Option<&str>, of: &'static str) -> Cow<'static, str> {
    match (device, of.strip_prefix("wgpu.")) {
        (Some(device), Some(rest)) => Cow::Owned(format!("wgpu.{device}.{rest}")),
        _ => Cow::Borrowed(of),
    }
}

/// Whose a memory space is: `None` when it isn't this backend's, `Some(None)` for the default
/// device's, `Some(Some(name))` for a named device's.
pub fn device_of(space: &str) -> Option<Option<&str>> {
    let rest = space.strip_prefix("wgpu.")?;
    match rest.split_once('.') {
        Some((device, _)) if !KINDS.contains(&device) => Some(Some(device)),
        _ => Some(None),
    }
}

/// Whether both spaces are this backend's, on different devices.
pub fn crosses_devices(from: &str, to: &str) -> bool {
    matches!((device_of(from), device_of(to)), (Some(a), Some(b)) if a != b)
}

fn name(named: Option<&Named>) -> Option<&str> {
    named.map(|named| &*named.name)
}

/// A backend's memory. With `host` the last entry is the host's own space, which no shader
/// reads: it tells the planner that a host port's value lives on the host, so the way to and
/// from this backend's nodes is an upload and a readback.
pub(crate) fn spaces(named: Option<&Named>, host: bool) -> Vec<MemorySpace> {
    let space = |id, kind| MemorySpace {
        id: on_device(name(named), id),
        kind,
        readable: true,
    };
    let mut spaces = vec![
        space(SPACE_DEVICE, SpaceKind::Device),
        space(SPACE_HOST_MAPPED, SpaceKind::Shared),
        space(SPACE_HOST_CACHED, SpaceKind::Shared),
        space(SPACE_DMA_HEAP, SpaceKind::Shared),
        space(SPACE_TEXTURE, SpaceKind::Device),
    ];
    if host {
        spaces.push(MemorySpace {
            id: Cow::Borrowed(HOST_SPACE),
            kind: SpaceKind::Host,
            readable: false,
        });
    }
    spaces
}

type Make = fn(&CompileCx<'_>) -> Result<Step, Refusal>;

/// Every move a backend offers, all for any type and all between the host and its own device.
/// `heap` says the device imports dma-bufs and the system heap opens, which offers the readback
/// into dma-heap memory beside the host-mapped one.
pub(crate) fn transfers(named: Option<&Named>, heap: bool) -> Vec<Transfer> {
    use TransferCost::{Copy, CopyCached, Import};
    let slot = named.map_or(0, |named| named.slot);
    let device = name(named);
    let entry = |id: &'static str, from: &'static str, to: &'static str, cost, make: Make| {
        Transfer {
            // A transfer's id is a static string: a named device's are made once per name.
            id: match on_device(device, id) {
                Cow::Borrowed(id) => id,
                Cow::Owned(id) => intern(id),
            },
            key: None,
            from: on_device(device, from),
            to: on_device(device, to),
            cost,
            make,
        }
    };
    let (upload, upload_mapped) = crate::upload::makes(slot);
    let map: Make = crate::readback::make_map;
    let mut table = vec![
        entry(UPLOAD, HOST_SPACE, SPACE_DEVICE, Copy, upload),
        entry(
            UPLOAD_MAPPED,
            HOST_SPACE,
            SPACE_HOST_MAPPED,
            Copy,
            upload_mapped,
        ),
        entry(
            READBACK,
            SPACE_DEVICE,
            SPACE_HOST_MAPPED,
            Copy,
            crate::readback::make,
        ),
        entry(MAP, SPACE_HOST_MAPPED, HOST_SPACE, Import, map),
        entry(MAP_CACHED, SPACE_HOST_CACHED, HOST_SPACE, Import, map),
        entry(MAP_HEAP, SPACE_DMA_HEAP, HOST_SPACE, Import, map),
    ];
    if heap {
        table.push(entry(
            READBACK_HEAP,
            SPACE_DEVICE,
            SPACE_DMA_HEAP,
            CopyCached,
            crate::readback::make_heap,
        ));
    }
    table
}

#[cfg(test)]
mod tests {
    use super::{crosses_devices, device_of, on_device};

    #[test]
    fn a_space_says_whose_it_is() {
        assert_eq!(on_device(None, "wgpu.buffer"), "wgpu.buffer");
        assert_eq!(on_device(Some("b"), "wgpu.buffer"), "wgpu.b.buffer");
        assert_eq!(on_device(Some("b"), "wgpu.map.cached"), "wgpu.b.map.cached");
        assert_eq!(
            on_device(Some("b"), "cpu.host"),
            "cpu.host",
            "not ours to rename"
        );

        assert_eq!(device_of("wgpu.buffer"), Some(None));
        assert_eq!(device_of("wgpu.map.cached"), Some(None));
        assert_eq!(device_of("wgpu.b.buffer"), Some(Some("b")));
        assert_eq!(device_of("wgpu.b.map.cached"), Some(Some("b")));
        assert_eq!(device_of("cpu.host"), None);

        assert!(crosses_devices("wgpu.buffer", "wgpu.b.buffer"));
        assert!(crosses_devices("wgpu.a.texture", "wgpu.b.texture"));
        assert!(!crosses_devices("wgpu.b.buffer", "wgpu.b.mapped"));
        assert!(!crosses_devices("wgpu.buffer", "cpu.host"));
    }

    #[test]
    fn the_heap_readback_is_offered_only_when_asked_for() {
        use daedalus_backend::TransferCost;

        use super::super::memory::{SPACE_DEVICE, SPACE_DMA_HEAP};
        use super::super::transfer::{READBACK, READBACK_HEAP};
        use super::transfers;

        let plain = transfers(None, false);
        assert!(plain.iter().all(|t| t.id != READBACK_HEAP));
        assert!(plain.iter().any(|t| t.id == READBACK));
        let heap = transfers(None, true);
        let entry = heap
            .iter()
            .find(|t| t.id == READBACK_HEAP)
            .expect("offered");
        assert_eq!(
            (&*entry.from, &*entry.to, entry.cost),
            (SPACE_DEVICE, SPACE_DMA_HEAP, TransferCost::CopyCached)
        );
        // Dearer than an import, cheaper than the plain copy: the planner takes it over READBACK.
        assert!(TransferCost::Import < entry.cost && entry.cost < TransferCost::Copy);
        // A named device's move is named for that device, in the same spaces.
        let named = crate::devices::Named {
            slot: 1,
            name: "gpu2".into(),
            id: "wgpu.gpu2".into(),
        };
        let named_heap = transfers(Some(&named), true);
        let entry = named_heap
            .iter()
            .find(|t| t.id.starts_with("wgpu.gpu2.readback.heap"))
            .expect("a named device offers its own");
        assert_eq!(&*entry.to, "wgpu.gpu2.dmaheap");
    }
}
