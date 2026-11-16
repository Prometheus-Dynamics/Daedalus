//! Runs a command with a time bound, and optionally on one core: the board scripts' stand-in for
//! `timeout` and `taskset`, which a board's image may not have.
//!
//! ```text
//! bounded [-c CORE] SECONDS CMD...
//! ```
//!
//! The exit status is the command's, or 128 plus the signal that ended it. Past the bound the
//! command gets SIGTERM, then SIGKILL ten seconds later, and the status is 124. Exit 125 is a
//! usage error or a core that cannot be set, 126 is a command that is not executable, and 127 is
//! a command that is not there. Only the command this program started is ever signalled.

#![allow(clippy::print_stderr)] // a command-line tool: its errors go to stderr

#[cfg(not(target_os = "linux"))]
fn main() -> std::process::ExitCode {
    eprintln!("bounded needs Linux");
    std::process::ExitCode::from(125)
}

#[cfg(target_os = "linux")]
fn main() -> std::process::ExitCode {
    std::process::ExitCode::from(linux::run())
}

#[cfg(target_os = "linux")]
mod linux {
    use std::io::ErrorKind;
    use std::os::unix::process::{CommandExt, ExitStatusExt};
    use std::process::Command;
    use std::sync::{Arc, Condvar, Mutex};
    use std::time::Duration;

    const TIMED_OUT: u8 = 124;
    const USAGE: u8 = 125;
    const GRACE: Duration = Duration::from_secs(10);

    /// What the command's watcher and its waiter share: whether the command has ended, and
    /// whether the bound ran out first.
    #[derive(Default)]
    struct Watch {
        ended: bool,
        timed_out: bool,
    }

    fn usage() -> u8 {
        eprintln!("usage: bounded [-c CORE] SECONDS CMD...");
        USAGE
    }

    pub fn run() -> u8 {
        let mut args: Vec<String> = std::env::args().skip(1).collect();
        let mut core = None;
        if args.first().map(String::as_str) == Some("-c") {
            let Some(Ok(number)) = args.get(1).map(|text| text.parse::<usize>()) else {
                return usage();
            };
            core = Some(number);
            args.drain(..2);
        }
        let Some(Ok(seconds)) = args.first().map(|text| text.parse::<u64>()) else {
            return usage();
        };
        let Some(program) = args.get(1) else {
            return usage();
        };
        let mut command = Command::new(program);
        command.args(&args[2..]);
        if let Some(core) = core {
            if core >= libc::CPU_SETSIZE as usize {
                eprintln!("bounded: core {core} is out of range");
                return USAGE;
            }
            // SAFETY: the closure runs in the forked child before exec and calls only
            // sched_setaffinity, which is async-signal-safe, on a set built on its own stack.
            unsafe {
                command.pre_exec(move || {
                    let mut set: libc::cpu_set_t = std::mem::zeroed();
                    libc::CPU_SET(core, &mut set);
                    match libc::sched_setaffinity(0, size_of::<libc::cpu_set_t>(), &set) {
                        0 => Ok(()),
                        _ => Err(std::io::Error::last_os_error()),
                    }
                });
            }
        }
        let mut child = match command.spawn() {
            Ok(child) => child,
            Err(error) => {
                eprintln!("bounded: {program}: {error}");
                return match error.kind() {
                    ErrorKind::NotFound => 127,
                    ErrorKind::PermissionDenied => 126,
                    // A core this machine does not have, or any other failed start.
                    _ => USAGE,
                };
            }
        };
        let pid = child.id() as libc::pid_t;
        let watch = Arc::new((Mutex::new(Watch::default()), Condvar::new()));
        let watcher = {
            let watch = Arc::clone(&watch);
            std::thread::spawn(move || signal_past(pid, Duration::from_secs(seconds), &watch))
        };
        // Wait without reaping, so the pid stays this command's until `ended` is set under the
        // lock: the watcher can then never signal a pid the kernel has handed to someone else.
        // SAFETY: waitid writes into the zeroed siginfo it is given, and the pid is this
        // program's own child.
        unsafe {
            let mut info: libc::siginfo_t = std::mem::zeroed();
            while libc::waitid(
                libc::P_PID,
                pid as libc::id_t,
                &mut info,
                libc::WEXITED | libc::WNOWAIT,
            ) != 0
                && std::io::Error::last_os_error().kind() == ErrorKind::Interrupted
            {}
        }
        let timed_out = {
            let (lock, wake) = &*watch;
            let mut state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
            state.ended = true;
            wake.notify_all();
            state.timed_out
        };
        let _ = watcher.join();
        let status = child.wait();
        if timed_out {
            eprintln!("bounded: {program} ran past {seconds} s and was stopped");
            return TIMED_OUT;
        }
        match status {
            Ok(status) => match (status.code(), status.signal()) {
                (Some(code), _) => code as u8,
                (None, Some(signal)) => 128u8.saturating_add(signal as u8),
                (None, None) => 1,
            },
            Err(error) => {
                eprintln!("bounded: waiting for {program}: {error}");
                USAGE
            }
        }
    }

    /// Signals the command when the bound runs out before it ends: SIGTERM, then SIGKILL after
    /// the grace time. The lock is held across each signal.
    fn signal_past(pid: libc::pid_t, bound: Duration, watch: &(Mutex<Watch>, Condvar)) {
        let (lock, wake) = watch;
        let state = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
        let (mut state, _) = wake
            .wait_timeout_while(state, bound, |state| !state.ended)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.ended {
            return;
        }
        state.timed_out = true;
        // SAFETY: the command has not been reaped (`ended` is false under the lock), so the pid
        // is still the child this program started.
        unsafe { libc::kill(pid, libc::SIGTERM) };
        let (state, _) = wake
            .wait_timeout_while(state, GRACE, |state| !state.ended)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if !state.ended {
            // SAFETY: as above.
            unsafe { libc::kill(pid, libc::SIGKILL) };
        }
    }
}
