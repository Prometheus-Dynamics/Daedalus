//! Buffers a readback is mapped from, kept between readbacks.

use std::collections::BTreeMap;

/// Limits of a device's staging pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StagingConfig {
    pub max_size_classes: usize,
    pub max_buffers_per_size: usize,
    pub max_bytes: u64,
}

impl Default for StagingConfig {
    fn default() -> Self {
        StagingConfig {
            max_size_classes: 16,
            max_buffers_per_size: 2,
            max_bytes: 64 << 20,
        }
    }
}

impl StagingConfig {
    /// A count of zero becomes one.
    pub fn new(max_size_classes: usize, max_buffers_per_size: usize, max_bytes: u64) -> Self {
        StagingConfig {
            max_size_classes: max_size_classes.max(1),
            max_buffers_per_size: max_buffers_per_size.max(1),
            max_bytes,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StagingStats {
    pub size_classes: usize,
    pub pooled_buffers: usize,
    pub pooled_bytes: u64,
    /// Readbacks that reused a buffer.
    pub hits: u64,
    /// Readbacks that had to make one.
    pub misses: u64,
    /// Buffers kept after a readback.
    pub returned: u64,
    /// Buffers dropped because keeping them would pass a limit.
    pub evicted: u64,
}

/// A request for `size` bytes reuses a buffer of `size..=2 * size`, so a small readback never
/// maps a much larger buffer.
fn reuse_range(size: u64) -> std::ops::RangeInclusive<u64> {
    size..=size.saturating_mul(2)
}

/// Mapped ranges are multiples of this.
const MAP_ALIGNMENT: u64 = 8;

/// The size of a staging buffer that holds `size` bytes.
pub(crate) fn staging_size(size: u64) -> u64 {
    size.next_multiple_of(MAP_ALIGNMENT)
}

/// Idle buffers by size. Generic over the buffer so the bookkeeping is testable without a
/// device.
pub(crate) struct StagingPool<B> {
    buffers: BTreeMap<u64, Vec<B>>,
    config: StagingConfig,
    stats: StagingStats,
}

impl<B> StagingPool<B> {
    pub(crate) fn new(config: StagingConfig) -> Self {
        StagingPool {
            buffers: BTreeMap::new(),
            config: StagingConfig::new(
                config.max_size_classes,
                config.max_buffers_per_size,
                config.max_bytes,
            ),
            stats: StagingStats::default(),
        }
    }

    pub(crate) fn take(&mut self, size: u64) -> Option<B> {
        let class = self
            .buffers
            .range(reuse_range(size))
            .find_map(|(class, list)| (!list.is_empty()).then_some(*class));
        let Some(class) = class else {
            self.stats.misses += 1;
            return None;
        };
        let list = self.buffers.get_mut(&class)?;
        let buffer = list.pop()?;
        if list.is_empty() {
            self.buffers.remove(&class);
        }
        self.stats.hits += 1;
        self.stats.pooled_bytes -= class;
        Some(buffer)
    }

    /// Keeps a buffer of `size` bytes, or drops it when that would pass a limit.
    pub(crate) fn put(&mut self, size: u64, buffer: B) {
        let fits = |pool: &Self| {
            size <= pool.config.max_bytes
                && pool.stats.pooled_bytes.saturating_add(size) <= pool.config.max_bytes
        };
        if !fits(self) {
            self.stats.evicted += 1;
            return;
        }
        if !self.buffers.contains_key(&size) && self.buffers.len() >= self.config.max_size_classes {
            // A new class pushes the largest one out.
            if let Some((largest, dropped)) = self.buffers.pop_last() {
                let count = dropped.len() as u64;
                self.stats.pooled_bytes -= largest.saturating_mul(count);
                self.stats.evicted += count;
            }
        }
        let list = self.buffers.entry(size).or_default();
        if list.len() >= self.config.max_buffers_per_size {
            self.stats.evicted += 1;
            return;
        }
        list.push(buffer);
        self.stats.returned += 1;
        self.stats.pooled_bytes += size;
    }

    pub(crate) fn stats(&self) -> StagingStats {
        StagingStats {
            size_classes: self.buffers.len(),
            pooled_buffers: self.buffers.values().map(Vec::len).sum(),
            ..self.stats
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{StagingConfig, StagingPool, staging_size};

    #[test]
    fn staging_pool_config_normalizes_zero_counts() {
        let config = StagingConfig::new(0, 0, 1024);
        assert_eq!(
            (config.max_size_classes, config.max_buffers_per_size),
            (1, 1)
        );
        let raw = StagingConfig {
            max_size_classes: 0,
            max_buffers_per_size: 0,
            max_bytes: 1024,
        };
        let mut pool = StagingPool::new(raw);
        pool.put(64, ());
        assert_eq!(pool.stats().pooled_buffers, 1, "zero counts act as one");
    }

    #[test]
    fn a_small_readback_never_takes_a_much_larger_buffer() {
        let mut pool = StagingPool::new(StagingConfig::default());
        pool.put(16 << 10, 'a');
        assert_eq!(pool.take(256), None);
        assert_eq!(pool.take(8 << 10), Some('a'));
        assert_eq!(pool.take(8 << 10), None);
        let stats = pool.stats();
        assert_eq!((stats.hits, stats.misses, stats.pooled_bytes), (1, 2, 0));
    }

    #[test]
    fn the_limits_evict() {
        let mut pool = StagingPool::new(StagingConfig::new(2, 1, 1000));
        pool.put(100, 1);
        pool.put(100, 2);
        assert_eq!(pool.stats().evicted, 1, "one buffer per size");
        pool.put(200, 3);
        pool.put(300, 4);
        let stats = pool.stats();
        assert_eq!(
            (stats.size_classes, stats.evicted),
            (2, 2),
            "the largest class went"
        );
        assert_eq!(stats.pooled_bytes, 400);
        pool.put(700, 5);
        assert_eq!(pool.stats().evicted, 3, "over the byte limit");
        assert_eq!(staging_size(13), 16);
        assert_eq!(staging_size(8), 8);
    }
}
