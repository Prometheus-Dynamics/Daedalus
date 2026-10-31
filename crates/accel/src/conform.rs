//! Conformance checks that record CI evidence.

use std::io::Write as _;

use crate::Backend;
use crate::dump::json_str;

/// What a conformance check ran: the op, the backend and, for GPU kernels, the variant and
/// adapter.
#[derive(Clone, Copy, Debug)]
pub struct Evidence<'a> {
    pub op: &'a str,
    pub backend: Backend,
    pub variant: Option<&'a str>,
    /// The GPU adapter (name and driver) for [`Backend::Gpu`].
    pub adapter: Option<&'a str>,
}

impl<'a> Evidence<'a> {
    pub fn new(op: &'a str, backend: Backend) -> Self {
        Self {
            op,
            backend,
            variant: None,
            adapter: None,
        }
    }

    pub fn variant(mut self, variant: &'a str) -> Self {
        self.variant = Some(variant);
        self
    }

    pub fn adapter(mut self, adapter: &'a str) -> Self {
        self.adapter = Some(adapter);
        self
    }
}

/// Check `op` on `backend` against its scalar reference: `check` returns whether the kernel's
/// output equals scalar's. Records the result when `ACCEL_RESULTS_DIR` is set, then panics on a
/// mismatch. Returns `false` without running `check` when this process cannot run `backend`
/// ([`detected_backends`]; GPU, portable SIMD and fixed-function checks always run).
pub fn conform(op: &str, backend: Backend, check: impl FnOnce() -> bool) -> bool {
    conform_with(Evidence::new(op, backend), || {
        if check() {
            Ok(())
        } else {
            Err("output differs from scalar".into())
        }
    })
}

/// [`conform`] with a variant or adapter, and a mismatch description.
pub fn conform_with(evidence: Evidence<'_>, check: impl FnOnce() -> Result<(), String>) -> bool {
    let cpu = !matches!(
        evidence.backend,
        Backend::Gpu | Backend::PortableSimd | Backend::FixedFunction
    );
    if cpu && !detected_backends().contains(&evidence.backend) {
        return false;
    }
    let result = check();
    record(&evidence, &result);
    if let Err(err) = result {
        panic!(
            "{} on {}{}: {err}",
            evidence.op,
            evidence.backend.name(),
            evidence
                .variant
                .map(|v| format!(" ({v})"))
                .unwrap_or_default()
        );
    }
    true
}

/// The CPU backends this process can run: scalar, plus what the target was built with or the
/// CPU reports at runtime. ARMv7 NEON and RVV count only when compiled in
/// (`-C target-feature=+neon` / `+v`): stable Rust cannot detect them at runtime.
pub fn detected_backends() -> Vec<Backend> {
    let mut found = vec![Backend::Scalar];
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        if std::arch::is_x86_feature_detected!("sse2") {
            found.push(Backend::X86Sse2);
        }
        if std::arch::is_x86_feature_detected!("avx2") {
            found.push(Backend::X86Avx2);
        }
    }
    #[cfg(target_arch = "aarch64")]
    if std::arch::is_aarch64_feature_detected!("neon") {
        found.push(Backend::Neon64);
    }
    #[cfg(all(target_arch = "arm", target_feature = "neon"))]
    found.push(Backend::Neon32);
    #[cfg(all(
        any(target_arch = "riscv64", target_arch = "riscv32"),
        target_feature = "v"
    ))]
    found.push(Backend::Rvv);
    found
}

/// The RISC-V vector register length in bits, when the build has the V extension.
pub fn rvv_vlen() -> Option<u32> {
    #[cfg(all(
        any(target_arch = "riscv64", target_arch = "riscv32"),
        target_feature = "v"
    ))]
    {
        let vlenb: usize;
        // SAFETY: reading the read-only `vlenb` CSR, which exists with the V extension.
        unsafe {
            core::arch::asm!("csrr {}, vlenb", out(reg) vlenb, options(nomem, nostack));
        }
        return Some((vlenb * 8) as u32);
    }
    #[allow(unreachable_code)]
    None
}

fn record(evidence: &Evidence<'_>, result: &Result<(), String>) {
    let Some(dir) = std::env::var_os("ACCEL_RESULTS_DIR") else {
        return;
    };
    let env = |name: &str| std::env::var(name).ok();
    let target = env("ACCEL_TARGET")
        .unwrap_or_else(|| format!("{}-{}", std::env::consts::ARCH, std::env::consts::OS));
    let runner = env("ACCEL_RUNNER").unwrap_or_else(|| "native".into());
    let rev = env("ACCEL_REV")
        .or_else(|| env("GITHUB_SHA"))
        .unwrap_or_default();
    let opt = |v: Option<&str>| v.map_or_else(|| "null".into(), json_str);
    let vlen = if evidence.backend == Backend::Rvv {
        rvv_vlen().map_or_else(|| "null".into(), |v| v.to_string())
    } else {
        "null".into()
    };
    let line = format!(
        "{{\"kind\":\"test\",\"op\":{},\"backend\":{},\"variant\":{},\"adapter\":{},\"target\":{},\"runner\":{},\"vlen\":{vlen},\"rev\":{},\"result\":{},\"detail\":{}}}\n",
        json_str(evidence.op),
        json_str(evidence.backend.name()),
        opt(evidence.variant),
        opt(evidence.adapter),
        json_str(&target),
        json_str(&runner),
        json_str(&rev),
        json_str(if result.is_ok() { "pass" } else { "fail" }),
        opt(result.as_ref().err().map(String::as_str)),
    );
    let dir = std::path::PathBuf::from(dir);
    let _ = std::fs::create_dir_all(&dir);
    let path = dir.join(format!("{}.jsonl", std::process::id()));
    // One write per line, in append mode: lines from concurrent tests do not interleave.
    if let Ok(mut file) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
    {
        let _ = file.write_all(line.as_bytes());
    }
}
