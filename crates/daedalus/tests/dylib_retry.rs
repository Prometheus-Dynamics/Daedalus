//! A plugin's load and reload are retried off the tick path: a missing or broken plugin is
//! retried with a capped backoff, a failed reload leaves the loaded library serving, and stopping
//! and resuming are manual. Time comes from an injected clock, so nothing sleeps.

#[path = "support/cdylib.rs"]
mod cdylib;

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use daedalus::dylib::{PluginRetry, PluginRetryState};
use daedalus::runtime::plugins::PluginRegistry;
use daedalus_core::platform::Clock;

struct Time(Arc<AtomicU64>);

impl Time {
    fn new() -> (Self, Clock) {
        let nanos = Arc::new(AtomicU64::new(0));
        let clock = {
            let nanos = nanos.clone();
            Clock::new(move || Duration::from_nanos(nanos.load(Ordering::Relaxed)))
        };
        (Self(nanos), clock)
    }

    fn advance(&self, by: Duration) {
        self.0.fetch_add(by.as_nanos() as u64, Ordering::Relaxed);
    }
}

/// A fresh, empty directory for one test's plugin file.
fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("daedalus-retry-{}-{name}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join(format!("libfixture{}", std::env::consts::DLL_SUFFIX))
}

fn poll(retry: &mut PluginRetry, registry: &mut PluginRegistry) -> PluginRetryState {
    // Safety: the fixture is the workspace's own test plugin, or the file is absent or broken.
    unsafe { retry.poll(registry) }
}

#[test]
fn a_missing_plugin_is_retried_with_a_growing_delay_until_it_loads() {
    let (time, clock) = Time::new();
    let path = scratch("missing");
    let mut retry = PluginRetry::new(&path, clock);
    let mut registry = PluginRegistry::new();

    assert_eq!(poll(&mut retry, &mut registry), PluginRetryState::Retrying);
    let status = retry.status();
    assert_eq!(status.consecutive_failures, 1);
    assert!(status.last_error.is_some());
    assert_eq!(status.retry_in, Some(Duration::from_millis(250)));

    // Not due yet: no attempt is made.
    time.advance(Duration::from_millis(100));
    poll(&mut retry, &mut registry);
    assert_eq!(retry.status().attempts, 1);

    // Due: the next attempt fails too, and the delay doubles.
    time.advance(Duration::from_millis(150));
    poll(&mut retry, &mut registry);
    let status = retry.status();
    assert_eq!(status.attempts, 2);
    assert_eq!(status.retry_in, Some(Duration::from_millis(500)));

    // The plugin appears: the next due attempt loads it.
    std::fs::copy(cdylib::stable_abi_cdylib(), &path).unwrap();
    time.advance(Duration::from_millis(500));
    assert_eq!(poll(&mut retry, &mut registry), PluginRetryState::Loaded);
    let status = retry.status();
    assert_eq!(status.generation, Some(1));
    assert_eq!(status.consecutive_failures, 0);
    assert_eq!(status.last_error, None);
}

#[test]
fn a_failed_reload_leaves_the_loaded_library_serving_and_is_retried() {
    let (time, clock) = Time::new();
    let path = scratch("reload");
    let good = cdylib::stable_abi_cdylib();
    std::fs::copy(good, &path).unwrap();
    let mut retry = PluginRetry::new(&path, clock);
    let mut registry = PluginRegistry::new();
    assert_eq!(poll(&mut retry, &mut registry), PluginRetryState::Loaded);
    assert_eq!(retry.status().generation, Some(1));

    // The new build is broken: the reload fails, and the loaded library keeps serving.
    std::fs::write(&path, b"not a plugin").unwrap();
    retry.request_reload();
    assert_eq!(poll(&mut retry, &mut registry), PluginRetryState::Loaded);
    let status = retry.status();
    assert!(status.reload_pending);
    assert!(status.last_error.is_some());
    assert_eq!(status.generation, Some(1));
    assert!(retry.library().is_some(), "the old library still serves");

    // The build is fixed: the retry after its delay reloads it.
    std::fs::copy(good, &path).unwrap();
    time.advance(Duration::from_millis(250));
    poll(&mut retry, &mut registry);
    let status = retry.status();
    assert!(!status.reload_pending);
    assert_eq!(status.generation, Some(2));
    assert_eq!(status.last_error, None);
}

#[test]
fn stopping_by_hand_halts_attempts_until_resumed() {
    let (time, clock) = Time::new();
    let path = scratch("stop");
    let mut retry = PluginRetry::new(&path, clock);
    let mut registry = PluginRegistry::new();
    poll(&mut retry, &mut registry);
    assert_eq!(retry.status().attempts, 1);

    retry.stop_retrying();
    time.advance(Duration::from_secs(3600));
    assert_eq!(poll(&mut retry, &mut registry), PluginRetryState::Stopped);
    assert_eq!(retry.status().attempts, 1, "no attempt while stopped");
    assert_eq!(retry.status().retry_in, None);

    retry.resume_retrying();
    assert_eq!(poll(&mut retry, &mut registry), PluginRetryState::Retrying);
    assert_eq!(retry.status().attempts, 2, "resumed: due at once");
}

#[test]
fn the_retry_delay_never_exceeds_the_cap() {
    let (time, clock) = Time::new();
    let path: &Path = &scratch("cap");
    let mut retry = PluginRetry::new(path, clock)
        .with_backoff(Duration::from_millis(250), Duration::from_secs(3600));
    let mut registry = PluginRegistry::new();
    for _ in 0..12 {
        poll(&mut retry, &mut registry);
        let wait = retry.status().retry_in.expect("retry scheduled");
        assert!(wait <= Duration::from_secs(10), "{wait:?} past the cap");
        time.advance(wait);
    }
    poll(&mut retry, &mut registry);
    assert_eq!(retry.status().retry_in, Some(Duration::from_secs(10)));
}
