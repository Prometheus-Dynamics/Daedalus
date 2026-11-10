//! SIGTERM and SIGINT ask the daemon to stop. The handler only sets a flag, and the main thread
//! does the stopping.

use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use signal_hook::consts::{SIGINT, SIGTERM};

/// The stop request. It can also be set from inside the daemon, as the end of a stdio session
/// does.
#[derive(Clone)]
pub struct Stop {
    asked: Arc<AtomicBool>,
}

impl Stop {
    /// Installs the handlers.
    pub fn install() -> io::Result<Stop> {
        let asked = Arc::new(AtomicBool::new(false));
        for signal in [SIGTERM, SIGINT] {
            signal_hook::flag::register(signal, Arc::clone(&asked))?;
        }
        Ok(Stop { asked })
    }

    pub fn asked(&self) -> bool {
        self.asked.load(Ordering::SeqCst)
    }

    pub fn ask(&self) {
        self.asked.store(true, Ordering::SeqCst);
    }

    /// The flag itself, for a thread that only needs to see it.
    pub fn flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.asked)
    }
}
