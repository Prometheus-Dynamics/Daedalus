//! `DAEDALUS_GPU_ADAPTERS` on the adapters this machine has. Run under the lavapipe lane, the
//! only adapter is the software one. Unpinned, a real GPU may be there too, and these tests never
//! open it: enumerating opens no device, and a hardware adapter is opened only when the policy
//! has nothing to hide.

#![allow(clippy::print_stderr)] // which adapter opened is this test's record

use daedalus_vk_sys::adapters::Policy;

use crate::adapter::{DeviceKind, Options};
use crate::context::Context;
use crate::driver::{LockedInstance, driver_lock};

/// The adapters of every API that `policy` keeps, and how many it hid.
fn enumerate(policy: Policy) -> (Vec<wgpu::Adapter>, usize) {
    let held = driver_lock();
    let instance = LockedInstance::new(&held, wgpu::Backends::all());
    instance.adapters(wgpu::Backends::all(), policy)
}

/// Whether this machine has a software adapter at all, and the environment says tests must fail
/// without an adapter.
fn required() -> bool {
    std::env::var_os("DAEDALUS_GPU_REQUIRED").is_some()
}

#[test]
fn software_opens_a_cpu_device_when_there_is_one() {
    let (software, _) = enumerate(Policy::Software);
    if software.is_empty() {
        assert!(!required(), "this lane needs a software adapter");
        return;
    }
    drop(software);
    let context = match Context::open_under(&Options::default(), Policy::Software) {
        Ok(context) => context,
        Err(error) => panic!("the software adapter did not open: {error}"),
    };
    assert_eq!(context.record.adapter.kind, DeviceKind::Cpu);
    eprintln!("adapter: {}", context.record.adapter.name);
}

#[test]
fn hardware_hides_every_software_adapter_and_says_so() {
    let (software, _) = enumerate(Policy::Software);
    let software_count = software.len();
    drop(software);
    let (hardware, hidden) = enumerate(Policy::Hardware);
    assert_eq!(
        hidden, software_count,
        "hardware hides exactly the CPU adapters"
    );
    assert!(
        hardware
            .iter()
            .all(|adapter| adapter.get_info().device_type != wgpu::DeviceType::Cpu),
        "a CPU adapter was kept under hardware"
    );
    if software_count == 0 || !hardware.is_empty() {
        // Nothing software to hide, or a hardware adapter that is not ours to open here.
        return;
    }
    drop(hardware);
    match Context::open_under(&Options::default(), Policy::Hardware) {
        Ok(_) => panic!("a device opened under hardware with only software adapters present"),
        Err(error) => {
            let text = error.to_string();
            assert!(
                text.contains(&format!(
                    "{software_count} adapter(s) hidden by DAEDALUS_GPU_ADAPTERS=hardware"
                )),
                "{text}"
            );
        }
    }
}
