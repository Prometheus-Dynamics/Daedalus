//! A Vulkan call that is wrong on purpose, for the lane that runs under the validation layer.
//!
//! The lane fails on a validation message it doesn't know. That only means something if the
//! layer's messages reach the place the lane reads, so the lane also runs this test, alone,
//! with `DAEDALUS_PLANT_VALIDATION_ERROR=1`, and fails unless the layer's log names the rule
//! this breaks. Without that variable the test does nothing.

use ash::vk;

use crate::device::{Raw, test_device};

/// The rule the planted call breaks: a buffer's size is greater than zero.
pub(crate) const PLANTED_RULE: &str = "VUID-VkBufferCreateInfo-size-00912";

#[test]
fn a_planted_invalid_call_is_made_when_asked_for() {
    if std::env::var_os("DAEDALUS_PLANT_VALIDATION_ERROR").is_none() {
        return;
    }
    assert!(PLANTED_RULE.starts_with("VUID-"));
    let Some((device, _queue)) = test_device(false) else {
        panic!("the planted call needs a device");
    };
    let raw = Raw::of(&device).expect("a Vulkan device");
    let info = vk::BufferCreateInfo::default()
        .size(0)
        .usage(vk::BufferUsageFlags::STORAGE_BUFFER)
        .sharing_mode(vk::SharingMode::EXCLUSIVE);
    // SAFETY: the create info is valid in form, and wrong only in its size, which a driver
    // takes as an empty buffer. Whatever comes back is destroyed at once and never bound or
    // used.
    unsafe {
        if let Ok(buffer) = raw.device.create_buffer(&info, None) {
            raw.device.destroy_buffer(buffer, None);
        }
    }
}
