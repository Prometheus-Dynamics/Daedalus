//! Pollable inbound readiness (Linux, std): an eventfd a host waits on next to its own
//! descriptors, instead of blocking in the waiter. The protocol has no lost wakeups: wait until
//! readable, clear, take the pending input, then rearm. A push signals only when the fd isn't
//! already signalled.

#![cfg(all(feature = "std", target_os = "linux"))]

use core::sync::atomic::{AtomicBool, Ordering};
use std::io;
use std::os::fd::{AsFd, BorrowedFd};
use std::sync::Arc;

use daedalus_runner::event_fd::EventFd;

use crate::InboundSignal;

/// The eventfd behind [`InboundFd`] and whether it is signalled.
pub(crate) struct Readiness {
    fd: EventFd,
    signalled: AtomicBool,
}

impl Readiness {
    fn new() -> io::Result<Readiness> {
        Ok(Readiness {
            fd: EventFd::new()?,
            signalled: AtomicBool::new(false),
        })
    }

    pub(crate) fn signal(&self) {
        if !self.signalled.swap(true, Ordering::AcqRel) {
            self.fd.signal();
        }
    }

    fn clear(&self) {
        self.signalled.store(false, Ordering::Release);
        self.fd.clear();
    }
}

/// A descriptor that is readable while input is pending.
#[derive(Clone)]
pub struct InboundFd {
    signal: Arc<InboundSignal>,
}

impl InboundFd {
    /// The descriptor of `signal`, created on the first call.
    pub fn new(signal: Arc<InboundSignal>) -> io::Result<InboundFd> {
        if signal.fd.get().is_none() {
            let readiness = Readiness::new()?;
            let _ = signal.fd.set(readiness);
        }
        Ok(InboundFd { signal })
    }

    fn readiness(&self) -> &Readiness {
        self.signal.fd.get().expect("set in new")
    }

    /// Clears the descriptor. Input fed after this signals it again.
    pub fn clear(&self) {
        self.readiness().clear();
    }

    /// After a drain, signals the descriptor again if input is still pending.
    pub fn rearm(&self) {
        if self.signal.is_pending() {
            self.readiness().signal();
        }
    }
}

impl AsFd for InboundFd {
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.readiness().fd.as_fd()
    }
}
