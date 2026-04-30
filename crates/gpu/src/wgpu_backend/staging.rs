use std::collections::HashMap;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct WgpuStagingPoolStats {
    pub size_classes: usize,
    pub pooled_buffers: usize,
    pub pooled_bytes: u64,
    pub max_size_classes: usize,
    pub max_buffers_per_size: usize,
    pub max_bytes: u64,
    pub hits: u64,
    pub misses: u64,
    pub returned: u64,
    pub evicted: u64,
}

const DEFAULT_STAGING_POOL_MAX_SIZE_CLASSES: usize = 16;
const DEFAULT_STAGING_POOL_MAX_BUFFERS_PER_SIZE: usize = 2;
const DEFAULT_STAGING_POOL_MAX_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WgpuStagingPoolConfig {
    pub max_size_classes: usize,
    pub max_buffers_per_size: usize,
    pub max_bytes: u64,
}

impl Default for WgpuStagingPoolConfig {
    fn default() -> Self {
        Self {
            max_size_classes: DEFAULT_STAGING_POOL_MAX_SIZE_CLASSES,
            max_buffers_per_size: DEFAULT_STAGING_POOL_MAX_BUFFERS_PER_SIZE,
            max_bytes: DEFAULT_STAGING_POOL_MAX_BYTES,
        }
    }
}

impl WgpuStagingPoolConfig {
    pub fn new(max_size_classes: usize, max_buffers_per_size: usize, max_bytes: u64) -> Self {
        Self {
            max_size_classes: max_size_classes.max(1),
            max_buffers_per_size: max_buffers_per_size.max(1),
            max_bytes,
        }
    }

    pub fn from_env() -> Result<Self, String> {
        let defaults = Self::default();
        Ok(Self::new(
            read_env_usize(
                "DAEDALUS_WGPU_STAGING_MAX_SIZE_CLASSES",
                defaults.max_size_classes,
            )?,
            read_env_usize(
                "DAEDALUS_WGPU_STAGING_MAX_BUFFERS_PER_SIZE",
                defaults.max_buffers_per_size,
            )?,
            read_env_u64("DAEDALUS_WGPU_STAGING_MAX_BYTES", defaults.max_bytes)?,
        ))
    }
}

fn read_env_usize(var: &'static str, default: usize) -> Result<usize, String> {
    match std::env::var(var) {
        Ok(raw) => raw.parse().map_err(|_| format!("invalid {var} '{raw}'")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(format!("error reading {var}: {err}")),
    }
}

fn read_env_u64(var: &'static str, default: u64) -> Result<u64, String> {
    match std::env::var(var) {
        Ok(raw) => raw.parse().map_err(|_| format!("invalid {var} '{raw}'")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(err) => Err(format!("error reading {var}: {err}")),
    }
}

#[derive(Debug)]
pub(super) struct StagingPool {
    buffers: HashMap<u64, Vec<wgpu::Buffer>>,
    #[cfg(feature = "gpu-async")]
    max_size_classes: usize,
    #[cfg(feature = "gpu-async")]
    max_buffers_per_size: usize,
    #[cfg(feature = "gpu-async")]
    max_bytes: u64,
    pooled_bytes: u64,
    hits: u64,
    misses: u64,
    returned: u64,
    evicted: u64,
}

impl Default for StagingPool {
    fn default() -> Self {
        Self::with_config(WgpuStagingPoolConfig::default())
    }
}

impl StagingPool {
    pub(super) fn with_config(config: WgpuStagingPoolConfig) -> Self {
        let _ = config;
        Self {
            buffers: HashMap::new(),
            #[cfg(feature = "gpu-async")]
            max_size_classes: config.max_size_classes.max(1),
            #[cfg(feature = "gpu-async")]
            max_buffers_per_size: config.max_buffers_per_size.max(1),
            #[cfg(feature = "gpu-async")]
            max_bytes: config.max_bytes,
            pooled_bytes: 0,
            hits: 0,
            misses: 0,
            returned: 0,
            evicted: 0,
        }
    }

    #[cfg(feature = "gpu-async")]
    pub(super) fn take(&mut self, size: u64) -> Option<wgpu::Buffer> {
        let buffer = self.buffers.get_mut(&size).and_then(Vec::pop);
        if let Some(buffer) = buffer {
            self.hits = self.hits.saturating_add(1);
            self.pooled_bytes = self.pooled_bytes.saturating_sub(size);
            if self.buffers.get(&size).is_some_and(Vec::is_empty) {
                self.buffers.remove(&size);
            }
            Some(buffer)
        } else {
            self.misses = self.misses.saturating_add(1);
            None
        }
    }

    #[cfg(feature = "gpu-async")]
    pub(super) fn put(&mut self, size: u64, buffer: wgpu::Buffer) {
        if !self.can_store(size) {
            self.evicted = self.evicted.saturating_add(1);
            return;
        }
        self.make_room_for_size_class(size);
        if !self.can_store(size) {
            self.evicted = self.evicted.saturating_add(1);
            return;
        }
        let list = self.buffers.entry(size).or_default();
        if list.len() >= self.max_buffers_per_size {
            self.evicted = self.evicted.saturating_add(1);
            return;
        }
        list.push(buffer);
        self.returned = self.returned.saturating_add(1);
        self.pooled_bytes = self.pooled_bytes.saturating_add(size);
    }

    #[cfg(feature = "gpu-async")]
    fn can_store(&self, size: u64) -> bool {
        size <= self.max_bytes && self.pooled_bytes.saturating_add(size) <= self.max_bytes
    }

    #[cfg(feature = "gpu-async")]
    fn make_room_for_size_class(&mut self, size: u64) {
        if self.buffers.contains_key(&size) || self.buffers.len() < self.max_size_classes {
            return;
        }
        let Some(victim_size) = self.buffers.keys().copied().max() else {
            return;
        };
        if let Some(victim) = self.buffers.remove(&victim_size) {
            let removed = victim_size.saturating_mul(victim.len() as u64);
            self.pooled_bytes = self.pooled_bytes.saturating_sub(removed);
            self.evicted = self.evicted.saturating_add(victim.len() as u64);
        }
    }

    pub(super) fn stats(&self) -> WgpuStagingPoolStats {
        WgpuStagingPoolStats {
            size_classes: self.buffers.len(),
            pooled_buffers: self.buffers.values().map(Vec::len).sum(),
            pooled_bytes: self.pooled_bytes,
            max_size_classes: self.max_size_classes(),
            max_buffers_per_size: self.max_buffers_per_size(),
            max_bytes: self.max_bytes(),
            hits: self.hits,
            misses: self.misses,
            returned: self.returned,
            evicted: self.evicted,
        }
    }

    fn max_size_classes(&self) -> usize {
        #[cfg(feature = "gpu-async")]
        {
            self.max_size_classes
        }
        #[cfg(not(feature = "gpu-async"))]
        {
            0
        }
    }

    fn max_buffers_per_size(&self) -> usize {
        #[cfg(feature = "gpu-async")]
        {
            self.max_buffers_per_size
        }
        #[cfg(not(feature = "gpu-async"))]
        {
            0
        }
    }

    fn max_bytes(&self) -> u64 {
        #[cfg(feature = "gpu-async")]
        {
            self.max_bytes
        }
        #[cfg(not(feature = "gpu-async"))]
        {
            0
        }
    }
}
