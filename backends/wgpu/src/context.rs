//! An open device. Nothing outside this crate gets one.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use crate::adapter::{Options, choose};
use crate::caps::{CapsRecord, OPTIONAL_FEATURES};
use crate::driver::{LockedInstance, driver_lock, open_device};

/// Why no device opened. Opening is tried again the next time one is needed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OpenError {
    /// No adapter passed the options.
    NoAdapter,
    /// Adapters passed, and none gave a device. The text is the last one's reason.
    Device(String),
}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OpenError::NoAdapter => f.write_str("no adapter matches"),
            OpenError::Device(reason) => write!(f, "no adapter gave a device: {reason}"),
        }
    }
}

impl std::error::Error for OpenError {}

pub(crate) struct Context {
    #[expect(dead_code, reason = "read from E2 on")]
    pub(crate) device: wgpu::Device,
    #[expect(dead_code, reason = "read from E2 on")]
    pub(crate) queue: wgpu::Queue,
    pub(crate) record: CapsRecord,
}

/// The device the process shares. It closes with its last holder.
static SHARED: Mutex<Weak<Context>> = Mutex::new(Weak::new());

impl Context {
    /// The shared device, opened if nobody holds it. A named adapter gets a device of its own.
    pub(crate) fn open(options: &Options) -> Result<Arc<Context>, OpenError> {
        if options.adapter.is_some() {
            return Context::open_private(options).map(Arc::new);
        }
        let mut shared = SHARED.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(open) = shared.upgrade() {
            return Ok(open);
        }
        let opened = Arc::new(Context::open_private(options)?);
        *shared = Arc::downgrade(&opened);
        Ok(opened)
    }

    /// The shared device's record, if the device is open. Opens nothing.
    pub(crate) fn shared_record() -> Option<CapsRecord> {
        let shared = SHARED.lock().unwrap_or_else(PoisonError::into_inner);
        shared.upgrade().map(|context| context.record.clone())
    }

    pub(crate) fn adopted(
        device: wgpu::Device,
        queue: wgpu::Queue,
        info: &wgpu::AdapterInfo,
    ) -> Context {
        let record = CapsRecord::new(info, &device.limits(), device.features());
        Context {
            device,
            queue,
            record,
        }
    }

    fn open_private(options: &Options) -> Result<Context, OpenError> {
        let held = driver_lock();
        let wanted = options.apis.to_wgpu();
        let instance = LockedInstance::new(&held, wanted);
        let mut adapters = instance.adapters(wanted);
        if adapters.is_empty() && wanted != wgpu::Backends::all() {
            adapters = instance.adapters(wgpu::Backends::all());
        }
        let mut last = OpenError::NoAdapter;
        for adapter in choose(adapters, options) {
            let desc = wgpu::DeviceDescriptor {
                label: None,
                required_features: adapter.features() & OPTIONAL_FEATURES,
                required_limits: adapter.limits(),
                ..wgpu::DeviceDescriptor::default()
            };
            match open_device(&held, &adapter, &desc) {
                Ok((device, queue)) => {
                    let record =
                        CapsRecord::new(&adapter.get_info(), &device.limits(), device.features());
                    return Ok(Context {
                        device,
                        queue,
                        record,
                    });
                }
                Err(error) => last = OpenError::Device(error.to_string()),
            }
        }
        Err(last)
    }
}
