//! The machine's own adapter is recognised, and its limits are read from the device.

#![allow(clippy::print_stderr)] // which adapter opened is this test's record

mod common;

#[test]
fn this_adapter_classifies_and_reports_its_limits() {
    let Some(record) = common::adapter() else {
        return;
    };
    eprintln!("adapter: {}", record.adapter.name);
    assert_ne!(
        record.profile.family,
        daedalus_wgpu::Family::Unknown,
        "{record:?}"
    );
    assert!(record.compute.fits_workgroup([8, 8, 1]), "{record:?}");
    assert!(record.compute.max_storage_buffers_per_stage >= 4);
}
