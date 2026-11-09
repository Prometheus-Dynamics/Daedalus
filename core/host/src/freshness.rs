//! Freshness checks on host inputs, before a tick. A value past its port's limit is dropped.

use crate::io::Host;

impl Host<'_> {
    /// Drops the values on every input whose freshness rule says they are too old. Time comes from
    /// [`Host::set_clock`].
    pub(crate) fn expire_stale(&mut self) {
        for index in 0..self.inbound.len() {
            self.expire_port(index);
        }
    }

    fn expire_port(&mut self, index: usize) {
        let Some(spec) = self.ports.at(index) else {
            return;
        };
        let now = self.now_ns;
        loop {
            let entry = &self.inbound[index];
            let Some(queue) = self.runner.queue(spec.slot) else {
                return;
            };
            if queue.len == 0 || entry.arrivals.is_empty() {
                return;
            }
            let span = entry.arrivals.len() as u64;
            let oldest = entry.pushed.saturating_sub(u64::from(queue.len));
            let arrived = entry.arrivals[(oldest % span) as usize];
            let Some(reason) = entry
                .policy
                .freshness
                .expired(arrived, now, entry.newest_ns)
            else {
                return;
            };
            self.inbound[index].drops.count(reason);
            if self.runner.drop_oldest(spec.slot).is_err() {
                return;
            }
        }
    }
}
