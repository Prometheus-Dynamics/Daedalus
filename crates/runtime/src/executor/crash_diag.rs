use crate::plan::RuntimeNode;
use std::ffi::CString;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};
use std::sync::OnceLock;

static INSTALLED: OnceLock<()> = OnceLock::new();

static CURRENT_MSG: AtomicPtr<libc::c_char> = AtomicPtr::new(std::ptr::null_mut());
static CURRENT_IDX: AtomicUsize = AtomicUsize::new(usize::MAX);
static MSG_TABLE_PTR: AtomicPtr<*const libc::c_char> = AtomicPtr::new(std::ptr::null_mut());
static MSG_TABLE_LEN: AtomicUsize = AtomicUsize::new(0);
static MSG_TABLE_PLAN_HASH: AtomicU64 = AtomicU64::new(0);

/// Install a SIGSEGV/SIGABRT handler (best-effort) that prints the last node being executed.
///
/// This is always enabled on Unix. (On non-Unix targets it is a no-op.)
pub fn install_if_enabled(nodes: &[RuntimeNode]) {
    // Install handler only once per process.
    let _ = INSTALLED.get_or_init(|| {
        #[cfg(unix)]
        unsafe {
            install_signal_handler(libc::SIGSEGV);
            install_signal_handler(libc::SIGABRT);
        }
    });

    // The executor calls this on every run. Rebuilding the same table every frame creates a real
    // leak because the signal handler needs stable pointers and the previous implementation
    // intentionally leaked each published table. Cache by plan fingerprint so steady-state graph
    // execution only publishes once per distinct plan.
    let plan_hash = plan_fingerprint(nodes);
    let published_hash = MSG_TABLE_PLAN_HASH.load(Ordering::Relaxed);
    let published_len = MSG_TABLE_LEN.load(Ordering::Relaxed);
    if published_hash == plan_hash && published_len == nodes.len() {
        return;
    }

    // Build a message table for this plan and publish it for the signal handler.
    // This is debug-only; we intentionally leak allocations.
    let mut ptrs: Vec<*const libc::c_char> = Vec::with_capacity(nodes.len());
    for (idx, n) in nodes.iter().enumerate() {
        let label = n.label.as_deref().unwrap_or("-");
        let msg = format!(
            "daedalus-runtime: crash in node idx={idx} id={} label={label}\n",
            n.id
        );
        let c = CString::new(msg).unwrap_or_else(|_| {
            CString::new("daedalus-runtime: crash in node (invalid utf8)\n").unwrap()
        });
        ptrs.push(Box::leak(c.into_boxed_c_str()).as_ptr());
    }
    let boxed: Box<[*const libc::c_char]> = ptrs.into_boxed_slice();
    let len = boxed.len();
    let base = Box::leak(boxed).as_ptr();
    MSG_TABLE_PTR.store(base as *mut *const libc::c_char, Ordering::Relaxed);
    MSG_TABLE_LEN.store(len, Ordering::Relaxed);
    MSG_TABLE_PLAN_HASH.store(plan_hash, Ordering::Relaxed);
}

pub fn set_current_node(idx: usize) {
    CURRENT_IDX.store(idx, Ordering::Relaxed);
    let base = MSG_TABLE_PTR.load(Ordering::Relaxed);
    let len = MSG_TABLE_LEN.load(Ordering::Relaxed);
    if base.is_null() || idx >= len {
        return;
    }
    // SAFETY: base points to a leaked boxed slice of pointers, published by `install_if_enabled`.
    let msg_ptr = unsafe { *base.add(idx) };
    CURRENT_MSG.store(msg_ptr as *mut libc::c_char, Ordering::Relaxed);
}

#[cfg(unix)]
unsafe fn install_signal_handler(sig: libc::c_int) {
    let mut sa: libc::sigaction = unsafe { std::mem::zeroed() };
    sa.sa_flags = libc::SA_SIGINFO | libc::SA_RESETHAND;
    sa.sa_sigaction = handler_siginfo as *const () as usize;
    unsafe { libc::sigemptyset(&mut sa.sa_mask) };
    unsafe { libc::sigaction(sig, &sa, std::ptr::null_mut()) };
}

#[cfg(unix)]
extern "C" fn handler_siginfo(
    sig: libc::c_int,
    info: *mut libc::siginfo_t,
    _uctx: *mut libc::c_void,
) {
    unsafe {
        let _ = write_str("daedalus-runtime: fatal signal received\n");
        if sig == libc::SIGSEGV {
            if let Some(addr) = info.as_ref().map(|i| i.si_addr()) {
                let mut buf = [0u8; 64];
                let n = write_hex_line(&mut buf, "fault_addr=0x", addr as usize);
                let _ = libc::write(libc::STDERR_FILENO, buf.as_ptr() as *const _, n);
            }
            #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
            {
                if let Some(rip) = linux_x86_64_rip(_uctx) {
                    let mut buf = [0u8; 64];
                    let n = write_hex_line(&mut buf, "rip=0x", rip);
                    let _ = libc::write(libc::STDERR_FILENO, buf.as_ptr() as *const _, n);
                }
            }
        }
        if let Some(msg) = current_msg_ptr() {
            libc::write(libc::STDERR_FILENO, msg as *const _, c_strlen(msg));
        } else {
            let idx = CURRENT_IDX.load(Ordering::Relaxed);
            let _ = write_str("daedalus-runtime: no current node recorded\n");
            if idx != usize::MAX {
                let _ = write_str("daedalus-runtime: current node index recorded\n");
            }
        }
        let _ = write_str("daedalus-runtime: set DAEDALUS_TRACE_NODES=1 for per-node logs\n");
        libc::_exit(128 + sig);
    }
}

#[cfg(unix)]
unsafe fn current_msg_ptr() -> Option<*const libc::c_char> {
    let p = CURRENT_MSG.load(Ordering::Relaxed);
    (!p.is_null()).then_some(p as *const libc::c_char)
}

#[cfg(unix)]
unsafe fn write_str(s: &str) -> isize {
    unsafe { libc::write(libc::STDERR_FILENO, s.as_ptr() as *const _, s.len()) }
}

#[cfg(unix)]
unsafe fn c_strlen(mut s: *const libc::c_char) -> usize {
    let mut n = 0usize;
    while unsafe { *s } != 0 {
        n += 1;
        s = unsafe { s.add(1) };
    }
    n
}

fn plan_fingerprint(nodes: &[RuntimeNode]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;

    fn write_bytes(hash: &mut u64, bytes: &[u8]) {
        for &byte in bytes {
            *hash ^= byte as u64;
            *hash = hash.wrapping_mul(FNV_PRIME);
        }
        *hash ^= 0xff;
        *hash = hash.wrapping_mul(FNV_PRIME);
    }

    let mut hash = FNV_OFFSET;
    for node in nodes {
        write_bytes(&mut hash, node.id.as_bytes());
        if let Some(label) = node.label.as_deref() {
            write_bytes(&mut hash, label.as_bytes());
        } else {
            write_bytes(&mut hash, b"-");
        }
    }
    hash
}

#[cfg(unix)]
fn write_hex_line(buf: &mut [u8], prefix: &str, val: usize) -> usize {
    let mut i = 0usize;
    for &b in prefix.as_bytes() {
        if i < buf.len() {
            buf[i] = b;
            i += 1;
        }
    }
    // hex without allocation
    let hex = b"0123456789abcdef";
    let mut started = false;
    for shift in (0..(usize::BITS as usize)).step_by(4).rev() {
        let nib = (val >> shift) & 0xF;
        if nib != 0 || started || shift == 0 {
            started = true;
            if i < buf.len() {
                buf[i] = hex[nib];
                i += 1;
            }
        }
    }
    if i < buf.len() {
        buf[i] = b'\n';
        i += 1;
    }
    i
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
unsafe fn linux_x86_64_rip(uctx: *mut libc::c_void) -> Option<usize> {
    if uctx.is_null() {
        return None;
    }
    let uctx = uctx as *const libc::ucontext_t;
    // SAFETY: only reads from the ucontext provided by the kernel.
    let mcontext = unsafe { &(*uctx).uc_mcontext };
    // REG_RIP is provided by libc on linux/x86_64.
    #[allow(clippy::useless_conversion)]
    let rip = mcontext.gregs[libc::REG_RIP as usize] as usize;
    Some(rip)
}
