//! Who closes the fd of an import the driver refused.
//!
//! Vulkan says a failed import leaves the fd with the caller. NVIDIA's driver closes it anyway.
//! Closing it again aborts in a debug build, and in a release build closes whatever the number
//! has been reused for since. So a refused fd is settled through here, exactly once.

/// What became of a refused fd.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Settled {
    /// It was still the caller's, and was closed.
    Closed,
    /// The driver had closed it already, so it was let go of without closing.
    LeftToDriver,
}

/// What to do with a refused fd on a driver that does or doesn't close it itself.
pub const fn settle(driver_closes_refused_fds: bool) -> Settled {
    if driver_closes_refused_fds {
        Settled::LeftToDriver
    } else {
        Settled::Closed
    }
}

/// An fd handed to an import that may be refused. Settling consumes it, so it can't be settled
/// twice, and dropping it unsettled closes it, which is right for an import that never reached
/// the driver.
#[cfg(target_os = "linux")]
#[derive(Debug)]
pub(crate) struct Refusable(std::os::fd::OwnedFd);

#[cfg(target_os = "linux")]
impl Refusable {
    pub fn new(fd: std::os::fd::OwnedFd) -> Refusable {
        Refusable(fd)
    }

    /// Settles a refused fd, once.
    pub fn refused(self, driver_closes_refused_fds: bool) -> Settled {
        let settled = settle(driver_closes_refused_fds);
        match settled {
            Settled::Closed => drop(self.0),
            // The number isn't ours any more. Forgetting it is the only safe thing.
            Settled::LeftToDriver => {
                let _ = std::os::fd::IntoRawFd::into_raw_fd(self.0);
            }
        }
        settled
    }
}

#[cfg(test)]
mod tests {
    use super::{Settled, settle};

    #[test]
    fn a_refused_fd_is_closed_by_exactly_one_side() {
        assert_eq!(settle(false), Settled::Closed);
        assert_eq!(settle(true), Settled::LeftToDriver);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn settling_closes_or_forgets_and_can_only_happen_once() {
        use std::os::fd::{AsRawFd, OwnedFd};

        use super::Refusable;
        // `refused` takes the guard by value, so a second settle doesn't compile.
        let file = std::fs::File::open("/dev/null").expect("/dev/null");
        let twin = file.try_clone().expect("a second fd");
        let guard = Refusable::new(OwnedFd::from(file));
        assert_eq!(guard.refused(false), Settled::Closed);
        // Left to the driver: forgotten, not closed.
        let number = twin.as_raw_fd();
        let guard = Refusable::new(OwnedFd::from(twin));
        assert_eq!(guard.refused(true), Settled::LeftToDriver);
        let still_open = std::fs::read_link(format!("/proc/self/fd/{number}")).is_ok();
        assert!(still_open, "a forgotten fd isn't closed");
        // It stays open for the rest of this test process: no driver closed it here.
    }
}
