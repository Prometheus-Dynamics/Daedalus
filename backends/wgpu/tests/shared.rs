//! One device per process: every backend opened without naming an adapter gets the device the
//! first one opened, and it closes with its last holder.

mod common;

use daedalus_wgpu::{Options, WgpuBackend, devices_created};

#[test]
fn shared_context_opens_one_device_per_process() {
    let first = WgpuBackend::open(Options::default());
    assert_eq!(first.caps(), None, "nothing is open yet");
    let Ok(record) = first.start() else {
        assert!(std::env::var_os("DAEDALUS_GPU_REQUIRED").is_none());
        return;
    };
    let opened = devices_created();
    assert_eq!(opened, 1);

    let second = WgpuBackend::open(Options::default());
    assert_eq!(second.caps(), Some(record.clone()), "known without opening");
    assert_eq!(second.start(), Ok(record.clone()));
    assert_eq!(devices_created(), opened, "no second device");

    // Naming an adapter is how a second device opens.
    let named = WgpuBackend::open(Options {
        adapter: Some(record.adapter.name.clone()),
        ..Options::default()
    });
    assert_eq!(named.caps(), None, "a private device isn't the shared one");
    assert_eq!(
        named.start().map(|own| own.adapter),
        Ok(record.adapter.clone())
    );
    assert_eq!(devices_created(), opened + 1);

    // The shared device closes with its last holder, and the next use opens a new one.
    first.stop();
    second.stop();
    assert_eq!(WgpuBackend::open(Options::default()).caps(), None);
    assert!(first.start().is_ok());
    assert_eq!(devices_created(), opened + 2);

    // An adapter that doesn't exist is an error, and is tried again next time.
    let missing = WgpuBackend::open(Options {
        adapter: Some("no such adapter".into()),
        ..Options::default()
    });
    assert_eq!(missing.start(), Err(daedalus_wgpu::OpenError::NoAdapter));
    assert_eq!(missing.start(), Err(daedalus_wgpu::OpenError::NoAdapter));
}
