//! Pin the benchmark thread to one CPU from inside the binary, for devices without `taskset`.

/// Environment variable naming the CPU to pin the calling thread to (e.g. `FRAME_BENCH_CPU=2`).
pub const PIN_CPU_ENV: &str = "FRAME_BENCH_CPU";

/// Pins the calling thread to the CPU named by [`PIN_CPU_ENV`], if set.
///
/// Returns the CPU pinned to, `None` when the variable is unset, or an error when it is not a
/// CPU index or the kernel refuses the affinity. Only Linux supports pinning; elsewhere a set
/// variable is an error.
pub fn pin_from_env() -> std::io::Result<Option<usize>> {
    let Ok(value) = std::env::var(PIN_CPU_ENV) else {
        return Ok(None);
    };
    let cpu: usize = value.trim().parse().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("{PIN_CPU_ENV}={value:?} is not a CPU index"),
        )
    })?;
    pin_to_cpu(cpu).map(|()| Some(cpu))
}

#[cfg(target_os = "linux")]
fn pin_to_cpu(cpu: usize) -> std::io::Result<()> {
    // SAFETY: `cpu_set_t` is plain data; zeroed is the empty set, and the libc macros only
    // touch the set we own. `sched_setaffinity(0, ..)` targets the calling thread.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        if cpu >= 8 * std::mem::size_of::<libc::cpu_set_t>() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("CPU index {cpu} is out of range"),
            ));
        }
        libc::CPU_SET(cpu, &mut set);
        if libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) != 0 {
            return Err(std::io::Error::last_os_error());
        }
    }
    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn pin_to_cpu(_cpu: usize) -> std::io::Result<()> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        format!("{PIN_CPU_ENV} is only supported on Linux"),
    ))
}
