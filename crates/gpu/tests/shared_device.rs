//! One wgpu device per process: every `shared_context` caller gets the device the first one
//! opened, `process_context` finds it without opening one, and it goes with its last handle.
//! Skips without a GPU adapter.
#![allow(clippy::print_stderr)] // the skip reason

use daedalus_gpu::{
    GpuBackendKind, GpuOptions, devices_created, process_context, select_backend, shared_context,
};

fn wgpu_options() -> GpuOptions {
    GpuOptions {
        preferred_backend: Some(GpuBackendKind::Wgpu),
        allow_software: true,
        ..Default::default()
    }
}

#[test]
fn shared_context_opens_one_device_per_process() {
    assert!(process_context().is_none(), "nothing open yet");
    let first = match shared_context(&wgpu_options()) {
        Ok(handle) if handle.backend_kind() == GpuBackendKind::Wgpu => handle,
        _ => {
            eprintln!("skipped: no wgpu adapter");
            return;
        }
    };
    let opened = devices_created();
    let second = shared_context(&GpuOptions {
        allow_software: false,
        ..wgpu_options()
    })
    .expect("the open device");
    let found = process_context().expect("open while handles live");
    assert_eq!(devices_created(), opened, "no second device");
    let device = |handle: &daedalus_gpu::GpuContextHandle| handle.wgpu().expect("wgpu").0.clone();
    assert!(device(&first) == device(&second) && device(&first) == device(&found));

    // The mock is never the process's device.
    let mock = shared_context(&GpuOptions {
        preferred_backend: Some(GpuBackendKind::Mock),
        ..Default::default()
    });
    if let Ok(mock) = mock {
        assert_ne!(mock.backend_kind(), GpuBackendKind::Wgpu);
    }

    drop((first, second, found));
    assert!(process_context().is_none(), "closed with its last handle");
    // `select_backend` still opens a private device.
    if select_backend(&wgpu_options()).is_ok() {
        assert_eq!(devices_created(), opened + 1);
    }
}
