//! Typed host takes: pop or drain outputs and read them as `T`.

use crate::portable::Arc;
use crate::prelude::*;
use daedalus_transport::Payload;

use super::HostBridgeHandle;

impl HostBridgeHandle {
    pub fn try_pop<T>(&self, port: impl AsRef<str>) -> Option<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.shared.io_timing.timed_take(|| {
            self.pop_payload(port.as_ref())
                .and_then(|payload| payload.get_ref::<T>().cloned())
        })
    }
    pub fn try_pop_owned<T>(&self, port: impl AsRef<str>) -> Result<Option<T>, Box<Payload>>
    where
        T: Send + Sync + 'static,
    {
        self.shared.io_timing.timed_take(|| {
            let Some(payload) = self.pop_payload(port.as_ref()) else {
                return Ok(None);
            };
            payload.try_into_owned::<T>().map(Some)
        })
    }
    pub fn try_pop_arc<T>(&self, port: impl AsRef<str>) -> Option<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.shared.io_timing.timed_take(|| {
            self.pop_payload(port.as_ref())
                .and_then(|payload| payload.get_arc::<T>())
        })
    }
    pub fn drain<T>(&self, port: impl AsRef<str>) -> Vec<T>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.shared.io_timing.timed_take(|| {
            self.drain_port(port.as_ref())
                .into_iter()
                .filter_map(|payload| payload.get_ref::<T>().cloned())
                .collect()
        })
    }
    pub fn drain_arcs<T>(&self, port: impl AsRef<str>) -> Vec<Arc<T>>
    where
        T: Send + Sync + 'static,
    {
        self.shared.io_timing.timed_take(|| {
            self.drain_port(port.as_ref())
                .into_iter()
                .filter_map(|payload| payload.get_arc::<T>())
                .collect()
        })
    }
}
