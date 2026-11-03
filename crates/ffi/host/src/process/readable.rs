//! Waiting for a worker's stdout to have data, with a timeout: `poll(2)` on Unix, the pipe's
//! available bytes (`PeekNamedPipe`) polled on Windows, where anonymous pipes cannot be waited on.

use std::io::BufReader;
use std::process::ChildStdout;
use std::time::Duration;

use crate::RunnerPoolError;

/// Whether `stdout` has data (or hit end of file / an error) within `timeout`.
pub(super) fn wait_stdout_readable(
    stdout: &BufReader<ChildStdout>,
    timeout: Duration,
) -> Result<bool, RunnerPoolError> {
    if !stdout.buffer().is_empty() {
        return Ok(true);
    }
    wait_pipe(stdout.get_ref(), timeout)
}

#[cfg(unix)]
fn wait_pipe(stdout: &ChildStdout, timeout: Duration) -> Result<bool, RunnerPoolError> {
    use std::os::fd::AsRawFd;

    let timeout_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
    let mut fd = libc::pollfd {
        fd: stdout.as_raw_fd(),
        events: libc::POLLIN | libc::POLLHUP | libc::POLLERR,
        revents: 0,
    };
    loop {
        // SAFETY: `fd` contains a valid descriptor owned by `ChildStdout`, and the pointer/count
        // describe exactly one initialized `pollfd` for the duration of this call.
        let result = unsafe { libc::poll(&mut fd, 1, timeout_ms) };
        if result > 0 {
            return Ok(true);
        }
        if result == 0 {
            return Ok(false);
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EINTR) {
            continue;
        }
        return Err(RunnerPoolError::Runner(format!(
            "failed to wait for worker stdout: {err}"
        )));
    }
}

#[cfg(windows)]
fn wait_pipe(stdout: &ChildStdout, timeout: Duration) -> Result<bool, RunnerPoolError> {
    use std::os::windows::io::AsRawHandle;
    use std::time::Instant;
    use windows_sys::Win32::Foundation::{ERROR_BROKEN_PIPE, GetLastError};
    use windows_sys::Win32::System::Pipes::PeekNamedPipe;

    // Short sleeps between peeks: a worker's reply usually lands within the first few.
    const STEP: Duration = Duration::from_millis(1);
    let deadline = Instant::now() + timeout;
    loop {
        let mut available = 0u32;
        // SAFETY: the handle is the open pipe `ChildStdout` owns; only the available-bytes count
        // is written, into a local.
        let ok = unsafe {
            PeekNamedPipe(
                stdout.as_raw_handle(),
                core::ptr::null_mut(),
                0,
                core::ptr::null_mut(),
                &mut available,
                core::ptr::null_mut(),
            )
        };
        if ok == 0 {
            // SAFETY: reads the calling thread's last error code.
            if unsafe { GetLastError() } == ERROR_BROKEN_PIPE {
                // The worker closed stdout: the next read returns end of file.
                return Ok(true);
            }
            return Err(RunnerPoolError::Runner(format!(
                "failed to wait for worker stdout: {}",
                std::io::Error::last_os_error()
            )));
        }
        if available > 0 {
            return Ok(true);
        }
        let now = Instant::now();
        if now >= deadline {
            return Ok(false);
        }
        std::thread::sleep(STEP.min(deadline - now));
    }
}
