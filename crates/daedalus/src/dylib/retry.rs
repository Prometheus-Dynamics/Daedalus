//! Retrying a plugin library's load or reload, off the tick path. A load that fails (the file is
//! missing, unreadable, or not a plugin yet) is retried with a capped backoff, and never given up.
//! A reload that fails leaves the loaded library serving (a reload is atomic), and is retried the
//! same way. The host drives [`PluginRetry::poll`] between ticks; stopping and resuming are manual.

use std::path::{Path, PathBuf};

use daedalus_core::platform::{Clock, Instant};

use super::{InstallPath, PluginLibrary};
use crate::runtime::plugins::PluginRegistry;

/// The first retry delay after a failure (`PluginRetry::with_backoff`).
pub const DEFAULT_RETRY_BASE: core::time::Duration = core::time::Duration::from_millis(250);
/// The cap on a retry delay (`PluginRetry::with_backoff`).
pub const DEFAULT_RETRY_CAP: core::time::Duration = core::time::Duration::from_secs(5);
/// The largest cap `with_backoff` accepts.
pub const MAX_RETRY_CAP: core::time::Duration = core::time::Duration::from_secs(10);

/// Where a plugin is in its retries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PluginRetryState {
    /// A library is loaded and serving (a reload may still be pending or failing).
    Loaded,
    /// No library is loaded yet: the load is being retried.
    Retrying,
    /// Retrying is stopped by hand ([`PluginRetry::stop_retrying`]).
    Stopped,
}

/// A plugin's retry status, for the host's telemetry. Field names are stable.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PluginRetryStatus {
    pub path: PathBuf,
    pub state: PluginRetryState,
    /// A reload is requested and not yet done.
    pub reload_pending: bool,
    /// Attempts (loads and reloads) made so far.
    pub attempts: u64,
    /// Failures in a row since the last success (drives the next delay).
    pub consecutive_failures: u32,
    /// Time until the next attempt, measured from the moment [`PluginRetry::status`] is called;
    /// `None` when an attempt is due now.
    pub retry_in: Option<core::time::Duration>,
    /// The most recent failure's message; `None` after a success.
    pub last_error: Option<String>,
    /// The loaded library's generation (1 on load, one more per reload).
    pub generation: Option<u64>,
}

/// One plugin library's load and reload, retried. See the module docs.
pub struct PluginRetry {
    path: PathBuf,
    clock: Clock,
    base: core::time::Duration,
    cap: core::time::Duration,
    library: Option<PluginLibrary>,
    reload_pending: bool,
    stopped: bool,
    /// The next attempt is due at this time; `None` means due now.
    next_attempt: Option<Instant>,
    attempts: u64,
    consecutive: u32,
    last_error: Option<String>,
}

impl PluginRetry {
    /// Retry the load of the plugin at `path`, reading time from `clock`.
    pub fn new(path: impl AsRef<Path>, clock: Clock) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
            clock,
            base: DEFAULT_RETRY_BASE,
            cap: DEFAULT_RETRY_CAP,
            library: None,
            reload_pending: false,
            stopped: false,
            next_attempt: None,
            attempts: 0,
            consecutive: 0,
            last_error: None,
        }
    }

    /// The first delay after a failure, and the cap on it (clamped to [`MAX_RETRY_CAP`]).
    pub fn with_backoff(mut self, base: core::time::Duration, cap: core::time::Duration) -> Self {
        self.cap = cap.min(MAX_RETRY_CAP);
        self.base = base.min(self.cap);
        self
    }

    /// Try the pending work if it is due: the load while no library is loaded, else the reload
    /// when one is requested. Off the tick path: the host calls this between ticks, and it never
    /// sleeps. A failed attempt is recorded and retried after its delay.
    ///
    /// # Safety
    /// As [`PluginLibrary::load`] and [`PluginLibrary::reload`]: the file at the path must be a
    /// plugin the host trusts.
    pub unsafe fn poll(&mut self, registry: &mut PluginRegistry) -> PluginRetryState {
        if self.stopped || !self.due() {
            return self.state();
        }
        self.attempts += 1;
        // Safety: forwarded from the caller.
        let result = unsafe {
            match &mut self.library {
                None => self.try_load(registry),
                Some(library) if self.reload_pending => library
                    .reload()
                    .map(|_| {
                        self.reload_pending = false;
                    })
                    .map_err(|err| err.to_string()),
                Some(_) => return self.state(),
            }
        };
        match result {
            Ok(()) => {
                self.consecutive = 0;
                self.last_error = None;
                self.next_attempt = None;
            }
            Err(message) => {
                self.consecutive = self.consecutive.saturating_add(1);
                self.last_error = Some(message);
                let delay = backoff_delay(self.consecutive, self.base, self.cap);
                self.next_attempt = Some(self.clock.now() + delay);
            }
        }
        self.state()
    }

    /// Request a reload of the loaded library (a new build is on disk). It runs at the next
    /// [`poll`](Self::poll); until it succeeds, the loaded library keeps serving.
    pub fn request_reload(&mut self) {
        if self.library.is_some() {
            self.reload_pending = true;
            self.next_attempt = None;
            self.consecutive = 0;
        }
    }

    /// Stop retrying by hand: no attempt runs until [`resume_retrying`](Self::resume_retrying).
    pub fn stop_retrying(&mut self) {
        self.stopped = true;
    }

    /// Resume retrying after [`stop_retrying`](Self::stop_retrying); the next attempt is due now.
    pub fn resume_retrying(&mut self) {
        self.stopped = false;
        self.next_attempt = None;
    }

    /// The loaded library, if any.
    pub fn library(&self) -> Option<&PluginLibrary> {
        self.library.as_ref()
    }

    /// The plugin's retry status now.
    pub fn status(&self) -> PluginRetryStatus {
        let now = self.clock.now();
        PluginRetryStatus {
            path: self.path.clone(),
            state: self.state(),
            reload_pending: self.reload_pending,
            attempts: self.attempts,
            consecutive_failures: self.consecutive,
            retry_in: self
                .next_attempt
                .filter(|_| !self.stopped)
                .map(|at| at.duration_since(now)),
            last_error: self.last_error.clone(),
            generation: self.library.as_ref().map(PluginLibrary::generation),
        }
    }

    fn state(&self) -> PluginRetryState {
        if self.stopped {
            PluginRetryState::Stopped
        } else if self.library.is_some() {
            PluginRetryState::Loaded
        } else {
            PluginRetryState::Retrying
        }
    }

    fn due(&self) -> bool {
        self.next_attempt.is_none_or(|at| self.clock.now() >= at)
    }

    /// # Safety
    /// As [`PluginLibrary::load_reloadable`].
    unsafe fn try_load(&mut self, registry: &mut PluginRegistry) -> Result<(), String> {
        // Safety: forwarded from the caller.
        let library =
            unsafe { PluginLibrary::load_reloadable(&self.path) }.map_err(|err| err.to_string())?;
        library
            .install_into_as(registry, InstallPath::Stable)
            .map_err(|err| err.to_string())?;
        self.library = Some(library);
        Ok(())
    }
}

/// The delay after the `consecutive`-th failure in a row: `base` doubled per extra failure, at
/// most `cap`.
fn backoff_delay(
    consecutive: u32,
    base: core::time::Duration,
    cap: core::time::Duration,
) -> core::time::Duration {
    let shift = consecutive.saturating_sub(1).min(31);
    base.saturating_mul(1u32 << shift).min(cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_doubles_from_base_and_stops_at_cap() {
        let base = core::time::Duration::from_millis(250);
        let cap = core::time::Duration::from_secs(5);
        let delays: Vec<_> = (1..=7)
            .map(|n| backoff_delay(n, base, cap).as_millis())
            .collect();
        assert_eq!(delays, vec![250, 500, 1000, 2000, 4000, 5000, 5000]);
        assert_eq!(backoff_delay(u32::MAX, base, cap), cap);
    }
}
