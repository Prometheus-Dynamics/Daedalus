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
    let started = std::time::Instant::now();
    assert_eq!(missing.start(), Err(daedalus_wgpu::OpenError::NoAdapter));
    // The attempt failed, so the next is held back: 250 ms at first, doubling to 8 s, for ever.
    // The wait counts from the failure, which came after `started`, so a call made less than the
    // first wait after `started` must be held back. A later call may be a new attempt, if the
    // machine was slow to get here.
    let again = missing.start();
    if started.elapsed() < daedalus_wgpu::RETRY_FIRST {
        assert!(matches!(again, Err(daedalus_wgpu::OpenError::Backoff(_))));
    } else {
        assert!(matches!(
            again,
            Err(daedalus_wgpu::OpenError::Backoff(_) | daedalus_wgpu::OpenError::NoAdapter)
        ));
    }
    assert!(
        missing
            .retry_in()
            .is_some_and(|wait| wait <= daedalus_wgpu::RETRY_FIRST)
    );
}

/// When no adapter is found and the user is refused the GPU's nodes, the error names them: the
/// group a process needs isn't left to a driver's stderr.
#[test]
fn a_refused_device_node_is_named_in_the_error() {
    let error = daedalus_wgpu::OpenError::NoAccess("/dev/dri/card0, /dev/dri/card1".into());
    let text = error.to_string();
    assert!(text.contains("/dev/dri/card0, /dev/dri/card1"), "{text}");
    assert!(text.contains("group"), "{text}");
}
