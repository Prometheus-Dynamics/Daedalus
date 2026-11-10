//! More than one device in a process.
//!
//! By default there is one device, which every [`WgpuBackend::open`] shares. A second exists
//! only when someone names it: [`WgpuBackend::open_named`] gives a backend a device of its own,
//! picked by the options' vendor, adapter name or index. A named backend is another backend to
//! the planner, `wgpu.<name>`, with memory spaces of its own, so a value on one device is never
//! taken for a value on another.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError, Weak};

use crate::adapter::Options;
use crate::backend::{Link, WgpuBackend};
use crate::context::{Context, OpenError};

/// How many named devices a process can have at once, beside the default one.
pub const MAX_NAMED: usize = 3;

/// A named device: which of the process's places it has, and what it is called.
pub(crate) struct Named {
    /// 1 to [`MAX_NAMED`]. 0 is the default device.
    pub(crate) slot: usize,
    pub(crate) name: Arc<str>,
    /// The backend's id, `wgpu.<name>`.
    pub(crate) id: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeviceError {
    /// Not a name: empty, not lowercase letters, digits, `_` and `-`, or one of the words a
    /// memory space ends in.
    Name(String),
    /// A backend of this name is alive.
    Taken(String),
    /// [`MAX_NAMED`] named devices are alive.
    Full,
}

impl fmt::Display for DeviceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeviceError::Name(name) => write!(f, "`{name}` can't name a device"),
            DeviceError::Taken(name) => write!(f, "a device named `{name}` is already open"),
            DeviceError::Full => write!(f, "{MAX_NAMED} named devices are already open"),
        }
    }
}

impl std::error::Error for DeviceError {}

type Place = Option<(Arc<str>, Weak<Link>)>;

/// The named devices alive in this process, by slot less one.
static NAMED: Mutex<[Place; MAX_NAMED]> = Mutex::new([const { None }; MAX_NAMED]);

fn valid(name: &str) -> bool {
    let plain = name
        .bytes()
        .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-".contains(&byte));
    !name.is_empty() && plain && !crate::handoff::KINDS.contains(&name)
}

/// The backend with the named device in `slot`, if it is alive.
pub(crate) fn link(slot: usize) -> Option<Arc<Link>> {
    let named = NAMED.lock().unwrap_or_else(PoisonError::into_inner);
    let (_, link) = named.get(slot.checked_sub(1)?)?.as_ref()?;
    link.upgrade()
}

/// Which named device has the open device `key`, or 0 when none does.
pub(crate) fn slot_of(key: u64) -> usize {
    (1..=MAX_NAMED)
        .find(|&slot| link(slot).is_some_and(|link| link.open_key() == Some(key)))
        .unwrap_or(0)
}

/// The device a transfer of `slot` goes to: the named one, or for slot 0 the device a backend
/// opened most recently, or the one the process shares.
pub(crate) fn context(slot: usize) -> Result<Arc<Context>, OpenError> {
    if slot == 0 {
        return Context::latest()
            .filter(|latest| !latest.is_lost())
            .map_or_else(|| Context::open(&Options::default()), Ok);
    }
    link(slot)
        .ok_or_else(|| OpenError::Device("the named device's backend is gone".into()))?
        .context()
}

impl WgpuBackend {
    /// A backend with a device of its own, called `name`. Nothing opens until first use. The
    /// planner sees it as the backend `wgpu.<name>`, whose memory is `wgpu.<name>.buffer` and
    /// so on. The name is free again when every clone of what this returns is dropped.
    pub fn open_named(name: &str, options: Options) -> Result<WgpuBackend, DeviceError> {
        if !valid(name) {
            return Err(DeviceError::Name(name.to_string()));
        }
        let mut named = NAMED.lock().unwrap_or_else(PoisonError::into_inner);
        let alive = |place: &Place| {
            place
                .as_ref()
                .is_some_and(|(_, link)| link.strong_count() > 0)
        };
        if named
            .iter()
            .any(|place| alive(place) && place.as_ref().is_some_and(|(have, _)| &**have == name))
        {
            return Err(DeviceError::Taken(name.to_string()));
        }
        let free = named
            .iter()
            .position(|place| !alive(place))
            .ok_or(DeviceError::Full)?;
        let name: Arc<str> = Arc::from(name);
        let link = Arc::new(Link::named(
            options,
            Named {
                slot: free + 1,
                id: format!("wgpu.{name}"),
                name: Arc::clone(&name),
            },
        ));
        named[free] = Some((name, Arc::downgrade(&link)));
        Ok(WgpuBackend::of(link))
    }

    /// What this backend's device is called, or `None` for the default device.
    pub fn device_name(&self) -> Option<&str> {
        self.link().named_as().map(|named| &*named.name)
    }

    /// Which device is open, as a key: buffers, textures and shaders with the same key are on
    /// it. A device opened in place of a lost one has a new key. `None` while none is open.
    pub fn device_key(&self) -> Option<u64> {
        self.open_key()
    }
}

#[cfg(test)]
mod tests {
    use super::valid;

    #[test]
    fn a_name_is_plain_and_isnt_a_kind_of_memory() {
        for good in ["b", "igpu", "gpu-1", "left_eye"] {
            assert!(valid(good), "{good}");
        }
        for bad in [
            "", "GPU", "a.b", "a b", "buffer", "texture", "mapped", "upload",
        ] {
            assert!(!valid(bad), "{bad}");
        }
    }
}
