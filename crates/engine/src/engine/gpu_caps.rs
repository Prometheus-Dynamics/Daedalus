//! The GPU caps the planner reads, decided without opening a device. A Mock engine's caps are the
//! mock backend's own. A Device engine's caps are unknown (`Probe`) when this machine has a DRM
//! render node, which is read from sysfs with no driver loaded; the first dispatch then checks the
//! real device against the plan. A machine with no render node has no GPU (`Absent`).

use daedalus_planner::{GpuCaps, GpuPlanCaps};

use crate::prelude::*;

use crate::config::{EngineConfig, GpuBackend};

/// The mock backend's caps (`daedalus-gpu`, `mock.rs`): one queue, no transfer queue, and
/// `Rgba8Unorm` sampleable.
const MOCK_CAPS: GpuCaps = GpuCaps {
    queue_count: 1,
    has_transfer_queue: false,
    rgba8_unorm_sampleable: true,
};

/// Board facts: the caps measured on a known board, keyed by the device tree's model string. Each
/// entry is added once the board has been measured with a real enumeration.
#[cfg(feature = "std")]
const BOARD_CAPS: &[(&str, GpuCaps)] = &[];

/// The caps the planner reads for `config`'s GPU backend.
pub(crate) fn plan_gpu_caps(config: &EngineConfig) -> GpuPlanCaps {
    match config.gpu {
        GpuBackend::Cpu => GpuPlanCaps::Absent,
        GpuBackend::Mock => GpuPlanCaps::Known(MOCK_CAPS),
        GpuBackend::Device => device_caps(),
    }
}

/// The caps for a Device engine: `Absent` with no render node, else the known caps or `Probe`.
#[cfg(feature = "std")]
fn device_caps() -> GpuPlanCaps {
    if !render_node_present() {
        return GpuPlanCaps::Absent;
    }
    match known_device_caps() {
        Some(caps) => GpuPlanCaps::Known(caps),
        None => GpuPlanCaps::Probe,
    }
}

/// Without `std` there is no device to probe (the `gpu` feature requires `std`).
#[cfg(not(feature = "std"))]
fn device_caps() -> GpuPlanCaps {
    GpuPlanCaps::Absent
}

/// The caps this machine's device is known to have: a board fact for this board, else the cache
/// of an earlier real enumeration with the same driver and ICD identity.
#[cfg(feature = "std")]
fn known_device_caps() -> Option<GpuCaps> {
    if let Some(model) = board_model()
        && let Some((_, caps)) = BOARD_CAPS.iter().find(|(name, _)| *name == model)
    {
        return Some(*caps);
    }
    cache_key().and_then(|key| {
        read_cache()
            .into_iter()
            .find(|(k, _)| *k == key)
            .map(|(_, c)| c)
    })
}

/// Record the caps of a real enumeration: written to the cache for this driver and ICD identity.
/// A cached entry that disagrees with the real caps is replaced (the stale entry is invalidated).
#[cfg(feature = "gpu")]
pub(crate) fn record_real_caps(real: GpuCaps) {
    let Some(key) = cache_key() else { return };
    let mut entries = read_cache();
    match entries.iter_mut().find(|(k, _)| *k == key) {
        Some((_, cached)) if *cached == real => return,
        Some((_, cached)) => *cached = real,
        None => entries.push((key, real)),
    }
    write_cache(&entries);
}

/// The device tree's model string (`/sys/firmware/devicetree/base/model`), trimmed.
#[cfg(feature = "std")]
fn board_model() -> Option<String> {
    let raw = std::fs::read("/sys/firmware/devicetree/base/model").ok()?;
    let model = String::from_utf8_lossy(&raw)
        .trim_end_matches('\0')
        .trim()
        .to_string();
    (!model.is_empty()).then_some(model)
}

/// The cache key: the first render node's kernel driver and the Vulkan ICD files (name, size and
/// modification time). A driver or Mesa upgrade changes the key, so its entry no longer applies.
#[cfg(feature = "std")]
fn cache_key() -> Option<String> {
    let node = first_render_node()?;
    let driver = std::fs::read_link(format!("/sys/class/drm/{node}/device/driver")).ok()?;
    let driver = driver.file_name()?.to_string_lossy().into_owned();
    let mut icds = Vec::new();
    for dir in ["/usr/share/vulkan/icd.d", "/etc/vulkan/icd.d"] {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(meta) = entry.metadata() else { continue };
            let mtime = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map_or(0, |d| d.as_secs());
            icds.push(format!(
                "{}:{}:{mtime}",
                entry.file_name().to_string_lossy(),
                meta.len()
            ));
        }
    }
    icds.sort();
    Some(format!("{driver}|{}", icds.join(",")))
}

/// The cache file: `$XDG_CACHE_HOME/daedalus/gpu-caps` (or `~/.cache/daedalus/gpu-caps`).
#[cfg(feature = "std")]
fn cache_path() -> Option<std::path::PathBuf> {
    let base = std::env::var_os("XDG_CACHE_HOME")
        .map(std::path::PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME").map(|home| std::path::PathBuf::from(home).join(".cache"))
        })?;
    Some(base.join("daedalus").join("gpu-caps"))
}

/// One cache line: `key<TAB>queue_count<TAB>transfer<TAB>rgba8` (1 or 0 for the flags).
#[cfg(feature = "std")]
fn read_cache() -> Vec<(String, GpuCaps)> {
    let Some(text) = cache_path().and_then(|path| std::fs::read_to_string(path).ok()) else {
        return Vec::new();
    };
    text.lines()
        .filter_map(|line| {
            let mut fields = line.split('\t');
            let key = fields.next()?.to_string();
            let queue_count = fields.next()?.parse().ok()?;
            let has_transfer_queue = fields.next()? == "1";
            let rgba8_unorm_sampleable = fields.next()? == "1";
            Some((
                key,
                GpuCaps {
                    queue_count,
                    has_transfer_queue,
                    rgba8_unorm_sampleable,
                },
            ))
        })
        .collect()
}

#[cfg(feature = "gpu")]
fn write_cache(entries: &[(String, GpuCaps)]) {
    let Some(path) = cache_path() else { return };
    if let Some(dir) = path.parent() {
        let _ = std::fs::create_dir_all(dir);
    }
    let text: String = entries
        .iter()
        .map(|(key, c)| {
            format!(
                "{key}\t{}\t{}\t{}\n",
                c.queue_count,
                u8::from(c.has_transfer_queue),
                u8::from(c.rgba8_unorm_sampleable)
            )
        })
        .collect();
    let _ = std::fs::write(path, text);
}

#[cfg(feature = "std")]
fn first_render_node() -> Option<String> {
    let mut nodes: Vec<String> = std::fs::read_dir("/sys/class/drm")
        .ok()?
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("renderD"))
        .collect();
    nodes.sort();
    nodes.into_iter().next()
}

/// Whether this machine has a DRM render node (`/sys/class/drm/renderD*`). Nothing is opened.
#[cfg(feature = "std")]
fn render_node_present() -> bool {
    first_render_node().is_some()
}
