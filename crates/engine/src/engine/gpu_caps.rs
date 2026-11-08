//! The GPU caps the planner reads, decided without opening a device. A Mock engine's caps are the
//! mock backend's own. A Device engine's caps are unknown (`Probe`) when this machine has a DRM
//! render node, which is read from sysfs with no driver loaded; the first dispatch then checks the
//! real device against the plan. A machine with no render node has no GPU (`Absent`).
//!
//! The cache (`gpu-caps` in the caps directory) holds the caps of an earlier real enumeration,
//! keyed by the render node's driver and the Vulkan ICD files. Any failure to read or write it
//! (a missing or read-only directory, a corrupt file, no home directory) is simply no cache: the
//! planner probes, and at most one debug line is logged for the process.

use daedalus_planner::{GpuCaps, GpuPlanCaps};

use crate::config::{EngineConfig, GpuBackend};
use crate::prelude::*;
#[cfg(feature = "std")]
use crate::trace::debug;
#[cfg(feature = "std")]
use std::path::{Path, PathBuf};
#[cfg(feature = "std")]
use std::sync::atomic::{AtomicBool, Ordering};

/// The mock backend's caps (`daedalus-gpu`, `mock.rs`): one queue, no transfer queue, and
/// `Rgba8Unorm` sampleable.
const MOCK_CAPS: GpuCaps = GpuCaps {
    queue_count: 1,
    has_transfer_queue: false,
    rgba8_unorm_sampleable: true,
};

/// Environment variable naming the caps cache directory (overridden by
/// [`CacheSection::gpu_caps_dir`](crate::CacheSection::gpu_caps_dir)).
#[cfg(feature = "std")]
pub const GPU_CAPS_CACHE_ENV: &str = "DAEDALUS_GPU_CAPS_CACHE";

/// The cache file in the caps directory.
#[cfg(any(feature = "std", test))]
const CACHE_FILE: &str = "gpu-caps";

/// Board facts: the caps measured on a known board, keyed by the device tree's model string. The
/// table is empty: the caps of a board come from the host facts (Lemnos), not from this table.
#[cfg(feature = "std")]
const BOARD_CAPS: &[(&str, GpuCaps)] = &[];

/// The directory of the caps cache: the config's `gpu_caps_dir`, else the environment variable,
/// else `$XDG_CACHE_HOME/daedalus` or `$HOME/.cache/daedalus`. None when none of them is set.
#[cfg(feature = "std")]
pub(crate) fn cache_dir(config: &EngineConfig) -> Option<PathBuf> {
    config
        .cache
        .gpu_caps_dir
        .as_ref()
        .map(PathBuf::from)
        .or_else(|| std::env::var_os(GPU_CAPS_CACHE_ENV).map(PathBuf::from))
        .or_else(|| {
            std::env::var_os("XDG_CACHE_HOME")
                .map(|base| PathBuf::from(base).join("daedalus"))
                .or_else(|| {
                    std::env::var_os("HOME")
                        .map(|home| PathBuf::from(home).join(".cache").join("daedalus"))
                })
        })
}

/// The caps the planner reads for `config`'s GPU backend.
pub(crate) fn plan_gpu_caps(config: &EngineConfig) -> GpuPlanCaps {
    match config.gpu {
        GpuBackend::Cpu => GpuPlanCaps::Absent,
        GpuBackend::Mock => GpuPlanCaps::Known(MOCK_CAPS),
        GpuBackend::Device => device_caps(config),
    }
}

/// The caps for a Device engine: `Absent` with no render node, else the known caps or `Probe`.
#[cfg(feature = "std")]
fn device_caps(config: &EngineConfig) -> GpuPlanCaps {
    if first_render_node().is_none() {
        return GpuPlanCaps::Absent;
    }
    match known_device_caps(cache_dir(config).as_deref()) {
        Some(caps) => GpuPlanCaps::Known(caps),
        None => GpuPlanCaps::Probe,
    }
}

/// Without `std` there is no device to probe (the `gpu` feature requires `std`).
#[cfg(not(feature = "std"))]
fn device_caps(_config: &EngineConfig) -> GpuPlanCaps {
    GpuPlanCaps::Absent
}

/// The caps this machine's device is known to have: a board fact for this board, else the cache
/// entry for this driver and ICD identity.
#[cfg(feature = "std")]
fn known_device_caps(dir: Option<&Path>) -> Option<GpuCaps> {
    if let Some(model) = board_model()
        && let Some((_, caps)) = BOARD_CAPS.iter().find(|(name, _)| *name == model)
    {
        return Some(*caps);
    }
    let key = cache_key()?;
    lookup(dir?, &key)
}

/// Record the caps of a real enumeration in the cache, replacing a stale entry for the same
/// driver and ICD identity. A failure is no cache.
#[cfg(feature = "gpu")]
pub(crate) fn record_real_caps(dir: Option<&Path>, real: GpuCaps) {
    let (Some(dir), Some(key)) = (dir, cache_key()) else {
        return;
    };
    let mut entries = read_entries(dir);
    match entries.iter_mut().find(|(k, _)| *k == key) {
        Some((_, cached)) if *cached == real => return,
        Some((_, cached)) => *cached = real,
        None => entries.push((key, real)),
    }
    write_entries(dir, &entries);
}

/// The cache entry for `key`, if the file reads and holds one.
#[cfg(feature = "std")]
fn lookup(dir: &Path, key: &str) -> Option<GpuCaps> {
    read_entries(dir)
        .into_iter()
        .find(|(k, _)| k == key)
        .map(|(_, caps)| caps)
}

/// The cache entries of `dir`. A missing, unreadable or corrupt file is no entries; corrupt lines
/// are skipped.
#[cfg(feature = "std")]
fn read_entries(dir: &Path) -> Vec<(String, GpuCaps)> {
    let text = match std::fs::read_to_string(dir.join(CACHE_FILE)) {
        Ok(text) => text,
        Err(err) => {
            note("read", &err);
            return Vec::new();
        }
    };
    text.lines().filter_map(parse_line).collect()
}

/// One line: `key<TAB>queue_count<TAB>transfer<TAB>rgba8` (the flags are 1 or 0).
#[cfg(feature = "std")]
fn parse_line(line: &str) -> Option<(String, GpuCaps)> {
    let mut fields = line.split('\t');
    let key = fields.next()?.to_string();
    let queue_count = fields.next()?.parse().ok()?;
    let has_transfer_queue = flag(fields.next()?)?;
    let rgba8_unorm_sampleable = flag(fields.next()?)?;
    if fields.next().is_some() || key.is_empty() {
        return None;
    }
    Some((
        key,
        GpuCaps {
            queue_count,
            has_transfer_queue,
            rgba8_unorm_sampleable,
        },
    ))
}

#[cfg(feature = "std")]
fn flag(field: &str) -> Option<bool> {
    match field {
        "1" => Some(true),
        "0" => Some(false),
        _ => None,
    }
}

/// Write `entries` to the cache file, creating the directory. A failure is no cache.
#[cfg(any(feature = "gpu", test))]
fn write_entries(dir: &Path, entries: &[(String, GpuCaps)]) {
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
    let written =
        std::fs::create_dir_all(dir).and_then(|()| std::fs::write(dir.join(CACHE_FILE), text));
    if let Err(err) = written {
        note("write", &err);
    }
}

/// Log the first cache failure of the process, at debug level (no further lines, no retries).
#[cfg(feature = "std")]
fn note(what: &str, err: &std::io::Error) {
    static LOGGED: AtomicBool = AtomicBool::new(false);
    let _ = (what, err);
    if !LOGGED.swap(true, Ordering::Relaxed) {
        debug!("gpu caps cache {what} failed, continuing without it: {err}");
    }
}

/// Whether this machine has a DRM render node, the first in name order (`renderD*`).
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
            let Ok(meta) = entry.metadata() else {
                continue;
            };
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

#[cfg(all(test, feature = "std"))]
mod tests {
    use super::*;

    /// A fresh directory under the system temp dir, unique to this test.
    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("daedalus-gpu-caps-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    const CAPS: GpuCaps = GpuCaps {
        queue_count: 2,
        has_transfer_queue: true,
        rgba8_unorm_sampleable: true,
    };

    #[test]
    fn the_config_directory_beats_the_environment_variable() {
        let config = EngineConfig::default().with_gpu_caps_dir("/from/config");
        // SAFETY-free: only this test reads the variable's value back through `cache_dir`.
        let env_dir = scratch("env");
        // The environment is process-wide, so set it only around the check.
        unsafe { std::env::set_var(GPU_CAPS_CACHE_ENV, &env_dir) };
        let from_config = cache_dir(&config);
        let from_env = cache_dir(&EngineConfig::default());
        unsafe { std::env::remove_var(GPU_CAPS_CACHE_ENV) };
        assert_eq!(from_config, Some(PathBuf::from("/from/config")));
        assert_eq!(from_env, Some(env_dir));
    }

    #[test]
    fn a_written_entry_reads_back_and_a_second_write_replaces_it() {
        let dir = scratch("roundtrip");
        write_entries(&dir, &[("drv|icd".to_string(), CAPS)]);
        assert_eq!(lookup(&dir, "drv|icd"), Some(CAPS));
        let other = GpuCaps {
            queue_count: 1,
            ..CAPS
        };
        write_entries(&dir, &[("drv|icd".to_string(), other)]);
        assert_eq!(lookup(&dir, "drv|icd"), Some(other));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn an_unwritable_directory_is_no_cache() {
        // A regular file where the directory should be: creating it fails, and that is no cache.
        let blocker = scratch("blocker");
        std::fs::write(&blocker, "x").expect("write blocker");
        let dir = blocker.join("sub");
        write_entries(&dir, &[("drv|icd".to_string(), CAPS)]);
        assert!(read_entries(&dir).is_empty());
        assert_eq!(lookup(&dir, "drv|icd"), None);
        let _ = std::fs::remove_file(&blocker);
    }

    #[test]
    fn a_corrupt_file_is_no_entries_and_its_good_lines_still_read() {
        let dir = scratch("corrupt");
        std::fs::create_dir_all(&dir).expect("create dir");
        std::fs::write(
            dir.join(CACHE_FILE),
            "not a line\nkey-a\tx\t1\t1\nkey-b\t3\t1\t0\ttrailing\nkey-c\t2\t1\t1\n\t1\t1\t1\n",
        )
        .expect("write corrupt");
        let entries = read_entries(&dir);
        assert_eq!(
            entries,
            vec![(
                "key-c".to_string(),
                GpuCaps {
                    queue_count: 2,
                    has_transfer_queue: true,
                    rgba8_unorm_sampleable: true,
                }
            )]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_binary_file_is_no_entries() {
        let dir = scratch("binary");
        std::fs::create_dir_all(&dir).expect("create dir");
        std::fs::write(dir.join(CACHE_FILE), [0xff_u8, 0x00, 0x9c, 0x01]).expect("write binary");
        assert!(read_entries(&dir).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
