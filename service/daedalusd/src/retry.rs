//! The retry machinery for everything the daemon starts that can fail to come up: a plugin's
//! library, a worker program. Each one that failed is tried again with its own backoff, capped at
//! 8 s. Nothing is given up on by the daemon itself: a thing that says it is gone (a plugin whose
//! file was removed) stops being tried, and an operator stops a worker by removing it from the
//! config and starting the daemon again.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::thread::JoinHandle;
use std::time::Instant;

use daedalus_host::listen::ServiceHost;
use daedalus_loader::Retry;

use crate::logging::{debug, info, warning};

/// One thing the daemon starts, which is tried again while it does not come up.
pub(crate) trait Attempt: Send {
    /// What it is, for the log: `plugin` or `worker`.
    fn kind(&self) -> &'static str;
    /// Its name, which is what it is kept under and logged as.
    fn name(&self) -> String;
    /// The phrase a failed try is logged with, after the name.
    fn down_word(&self) -> &'static str {
        "is refused"
    }
    /// True when there is no point trying again. Nothing is tried for it after this says so.
    fn gone(&self) -> bool {
        false
    }
    /// One more try. The line it returns is logged at info; the error is the reason it failed.
    fn retry(&mut self, host: &ServiceHost) -> Result<String, String>;
}

/// What the loader tells the retry thread.
pub(crate) enum Note {
    /// A library the integrity check refused. It is tried again, as a library that failed at start
    /// is, until its file is removed.
    Refused {
        attempt: Box<dyn Attempt>,
        reason: String,
    },
    /// A library loaded, by a request or a retry. Nothing is tried again for it. The key is its
    /// [`Attempt::name`].
    Loaded(String),
    /// The daemon is stopping, and the retry thread ends.
    Stop,
}

/// Something waiting to be tried again.
struct Waiting {
    attempt: Box<dyn Attempt>,
    retry: Retry,
    due: Instant,
    /// The last reason it did not come up. A repeat of it is logged at debug, a new one at warning.
    reason: String,
}

/// Keeps trying what failed to come up, until each one does, is gone, or the daemon stops. The
/// thread waits on `notes` between tries, so it costs nothing while there is nothing to try.
///
/// `starting` are tried at once, before anything has failed: the workers, which the daemon starts
/// on this thread so that it answers while one waits for its hello. `failed` are the things that
/// already failed, and wait their first backoff.
pub(crate) fn retry_in_background(
    host: Arc<ServiceHost>,
    failed: Vec<Box<dyn Attempt>>,
    starting: Vec<Box<dyn Attempt>>,
    notes: Receiver<Note>,
) -> Option<JoinHandle<()>> {
    let body = move || {
        let mut waiting: BTreeMap<String, Waiting> = BTreeMap::new();
        for attempt in starting {
            waiting.insert(
                attempt.name(),
                Waiting {
                    attempt,
                    retry: Retry::new(),
                    due: Instant::now(),
                    reason: String::new(),
                },
            );
        }
        for attempt in failed {
            let mut retry = Retry::new();
            let due = Instant::now() + retry.failed();
            waiting.insert(
                attempt.name(),
                Waiting {
                    attempt,
                    retry,
                    due,
                    reason: String::new(),
                },
            );
        }
        loop {
            // Wait for the next due try, or for a note, whichever is first.
            let note = match waiting.values().map(|waiting| waiting.due).min() {
                None => match notes.recv() {
                    Ok(note) => Some(note),
                    Err(_) => return,
                },
                Some(due) => {
                    match notes.recv_timeout(due.saturating_duration_since(Instant::now())) {
                        Ok(note) => Some(note),
                        Err(RecvTimeoutError::Timeout) => None,
                        Err(RecvTimeoutError::Disconnected) => return,
                    }
                }
            };
            match note {
                Some(Note::Stop) => return,
                Some(Note::Loaded(name)) => {
                    waiting.remove(&name);
                }
                // A refusal the thread already waits on is its own failed try coming back.
                Some(Note::Refused { attempt, reason })
                    if !waiting.contains_key(&attempt.name()) =>
                {
                    let mut retry = Retry::new();
                    let delay = retry.failed();
                    warning!(
                        "{} {} {}: {reason}; what is loaded keeps running; next try in {delay:?}",
                        attempt.kind(),
                        attempt.name(),
                        attempt.down_word()
                    );
                    waiting.insert(
                        attempt.name(),
                        Waiting {
                            attempt,
                            retry,
                            due: Instant::now() + delay,
                            reason,
                        },
                    );
                }
                None | Some(Note::Refused { .. }) => {}
            }
            let now = Instant::now();
            let due: Vec<String> = waiting
                .iter()
                .filter(|(_, waiting)| waiting.due <= now)
                .map(|(name, _)| name.clone())
                .collect();
            for name in due {
                let Some(entry) = waiting.get_mut(&name) else {
                    continue;
                };
                if entry.attempt.gone() {
                    info!(
                        "{} {name} was removed: no longer retried",
                        entry.attempt.kind()
                    );
                    waiting.remove(&name);
                    continue;
                }
                match entry.attempt.retry(&host) {
                    Ok(line) => {
                        info!("{line}");
                        waiting.remove(&name);
                    }
                    Err(reason) => {
                        let delay = entry.retry.failed();
                        entry.due = Instant::now() + delay;
                        if entry.reason == reason {
                            debug!(
                                "{} {name} still {} ({reason}), next try in {delay:?}",
                                entry.attempt.kind(),
                                entry.attempt.down_word()
                            );
                        } else {
                            warning!(
                                "{} {name} {}: {reason}; what is loaded keeps running; next try in {delay:?}",
                                entry.attempt.kind(),
                                entry.attempt.down_word()
                            );
                        }
                        entry.reason = reason;
                    }
                }
            }
        }
    };
    std::thread::Builder::new()
        .name("daedalusd-retry".to_owned())
        .spawn(body)
        .ok()
}
