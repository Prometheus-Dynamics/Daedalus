//! The alignment a dma-buf's value has to start at to be imported in place, and what a value at
//! another offset is told: that it is copied, and why, with its offset and the alignment named.
//! Needs no dma-heap: the device's answer is given by `assume_dmabuf_import` where it matters.
#![cfg(target_os = "linux")]
#![allow(clippy::print_stderr)] // what each adapter answered is this test's record

use daedalus_wgpu::{DmaBufStatus, Options, WgpuBackend, WhyCopied};

#[test]
fn the_alignment_is_a_power_of_two_and_none_with_no_device() {
    let closed = WgpuBackend::open(Options {
        index: Some(0),
        ..Options::default()
    });
    assert_eq!(closed.dmabuf_offset_alignment(), None);
    assert_eq!(closed.dmabuf_offset_status(0, false), DmaBufStatus::NotOpen);

    let mut opened = 0;
    for index in 0..8 {
        let backend = WgpuBackend::open(Options {
            index: Some(index),
            ..Options::default()
        });
        if backend.start().is_err() {
            break;
        }
        opened += 1;
        let name = backend.caps().expect("open").adapter.name.clone();
        match backend.dmabuf_offset_alignment() {
            Some(alignment) => {
                assert!(alignment.is_power_of_two(), "{name}: {alignment}");
                eprintln!("offset alignment on {name}: {alignment}");
            }
            None => {
                // Only a device that imports no dma-buf of either kind has no alignment to give.
                assert_eq!(
                    (backend.dmabuf_import(false), backend.dmabuf_import(true)),
                    (Some(false), Some(false)),
                    "{name}"
                );
                eprintln!("offset alignment on {name}: none, it imports no dma-buf");
            }
        }
    }
    if opened == 0 {
        assert!(
            std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
            "this lane needs an adapter"
        );
        eprintln!("no adapter here: the alignment was not checked");
    }
}

#[test]
fn a_value_off_the_alignment_is_copied_and_its_status_names_the_offset_and_alignment() {
    let mut checked = 0;
    for index in 0..8 {
        let backend = WgpuBackend::open(Options {
            index: Some(index),
            ..Options::default()
        });
        if backend.start().is_err() {
            break;
        }
        let name = backend.caps().expect("open").adapter.name.clone();
        // Said to import, so that only the offset can make a value a copy.
        backend.assume_dmabuf_import(false, true);
        backend.assume_dmabuf_import(true, true);
        let Some(alignment) = backend.dmabuf_offset_alignment() else {
            eprintln!("offset status on {name}: no alignment, no dma-buf import to check");
            continue;
        };
        checked += 1;

        assert_eq!(
            backend.dmabuf_offset_status(0, false),
            DmaBufStatus::Imports,
            "{name}: offset 0 is a multiple"
        );
        assert_eq!(
            backend.dmabuf_offset_status(alignment, false),
            DmaBufStatus::Imports,
            "{name}: the alignment itself is a multiple"
        );
        if alignment == 1 {
            eprintln!("offset status on {name}: alignment 1, every offset is imported");
            continue;
        }
        let at = 1;
        let status = backend.dmabuf_offset_status(at, false);
        assert_eq!(
            status,
            DmaBufStatus::Copies(WhyCopied::Misaligned {
                offset: at,
                alignment
            }),
            "{name}"
        );
        let said = status.to_string();
        assert!(
            said.contains(&format!("byte {at} of its dma-buf")),
            "{said}"
        );
        assert!(said.contains(&format!("multiple of {alignment}")), "{said}");
        eprintln!("offset status on {name}: offset {at} of alignment {alignment}: {said}");

        // A device that refuses the kind says so first: the offset doesn't change that.
        backend.assume_dmabuf_import(false, false);
        assert_eq!(
            backend.dmabuf_offset_status(at, false),
            backend.dmabuf_status(false),
            "{name}: the device's answer comes first"
        );
    }
    if checked == 0 {
        assert!(
            std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none(),
            "this lane needs an adapter"
        );
        eprintln!("no adapter here: no offset was checked");
    }
}
