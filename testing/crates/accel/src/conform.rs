//! Conformance checks that record CI evidence.

use std::io::Write as _;

use crate::dump::json_str;
use crate::{Backend, Ext};

/// What a conformance check ran: the op, the backend and, for GPU kernels, the variant and
/// adapter.
#[derive(Clone, Copy, Debug)]
pub struct Evidence<'a> {
    pub op: &'a str,
    pub backend: Backend,
    pub variant: Option<&'a str>,
    /// The GPU adapter (name and driver) for [`Backend::Gpu`].
    pub adapter: Option<&'a str>,
    /// Extensions the kernel used; the check runs only when the process has them all.
    pub extensions: &'a [Ext],
    /// A measurement or remark for the evidence column ("gpu 0.41 ms vs cpu 0.12 ms").
    pub note: Option<&'a str>,
}

impl<'a> Evidence<'a> {
    pub fn new(op: &'a str, backend: Backend) -> Self {
        Self {
            op,
            backend,
            variant: None,
            adapter: None,
            extensions: &[],
            note: None,
        }
    }

    pub fn note(mut self, note: &'a str) -> Self {
        self.note = Some(note);
        self
    }

    pub fn extensions(mut self, extensions: &'a [Ext]) -> Self {
        self.extensions = extensions;
        self
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
/// ([`detected_backends`]; GPU and portable SIMD checks always run).
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
    let cpu = !matches!(evidence.backend, Backend::Gpu | Backend::PortableSimd);
    if cpu && !detected_backends().contains(&evidence.backend) {
        return false;
    }
    let have = detected_extensions();
    if !evidence.extensions.iter().all(|e| have.contains(e)) {
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
    #[cfg(daedalus_neon32)]
    found.push(Backend::Neon32);
    #[cfg(daedalus_rvv)]
    found.push(Backend::Rvv);
    found
}

/// The ISA extensions this process can use: detected at runtime on x86 and AArch64, and on
/// RISC-V through the kernel's `riscv_hwprobe` (or compiled in with `-C target-feature`).
pub fn detected_extensions() -> Vec<Ext> {
    #[allow(unused_mut)]
    let mut found = Vec::new();
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        use std::arch::is_x86_feature_detected as has;
        for (on, ext) in [
            (has!("popcnt"), Ext::Popcnt),
            (has!("bmi2"), Ext::Bmi2),
            (has!("fma"), Ext::Fma),
            (has!("f16c"), Ext::F16c),
            (has!("pclmulqdq"), Ext::Pclmulqdq),
            (has!("aes"), Ext::Aes),
            (has!("sha"), Ext::Sha),
            (has!("avx512f"), Ext::Avx512f),
            (has!("avx512bw"), Ext::Avx512bw),
            (has!("avx512vnni"), Ext::Avx512vnni),
        ] {
            if on {
                found.push(ext);
            }
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        use std::arch::is_aarch64_feature_detected as has;
        for (on, ext) in [
            (has!("dotprod"), Ext::Dotprod),
            (has!("fp16"), Ext::Fp16),
            (has!("i8mm"), Ext::I8mm),
            (has!("crc"), Ext::Crc32),
            (has!("aes"), Ext::Aes),
            (has!("sha2"), Ext::Sha2),
            (has!("sha3"), Ext::Sha3),
            (has!("sve"), Ext::Sve),
            (has!("sve2"), Ext::Sve2),
        ] {
            if on {
                found.push(ext);
            }
        }
    }
    #[cfg(any(target_arch = "riscv64", target_arch = "riscv32"))]
    let isa = riscv_isa_names();
    #[cfg(any(target_arch = "riscv64", target_arch = "riscv32"))]
    let named = |name: &str| isa.iter().any(|item| item == name);
    #[cfg(any(target_arch = "riscv64", target_arch = "riscv32"))]
    for (on, ext) in [
        (cfg!(daedalus_ext_zba) || named("zba"), Ext::Zba),
        (cfg!(daedalus_ext_zbb) || named("zbb"), Ext::Zbb),
        (cfg!(daedalus_ext_zbs) || named("zbs"), Ext::Zbs),
        (cfg!(daedalus_ext_zvbb) || named("zvbb"), Ext::Zvbb),
        (cfg!(daedalus_ext_zvbc) || named("zvbc"), Ext::Zvbc),
    ] {
        if on {
            found.push(ext);
        }
    }
    found
}

/// The RISC-V extensions named in the kernel's `isa` string (`/proc/cpuinfo`), for
/// [`detected_extensions`]. Empty off Linux or without the line.
#[cfg(any(target_arch = "riscv64", target_arch = "riscv32"))]
fn riscv_isa_names() -> Vec<String> {
    std::fs::read_to_string("/proc/cpuinfo")
        .unwrap_or_default()
        .lines()
        .find(|line| line.starts_with("isa"))
        .and_then(|line| line.split_once(':'))
        .map(|(_, isa)| isa.trim().split('_').map(str::to_owned).collect())
        .unwrap_or_default()
}

/// The RISC-V vector register length in bits: the `ACCEL_RVV_VLEN` the runner sets for the
/// vector length it runs at (qemu `vlen=`, or the board). This crate forbids `unsafe`, so it
/// cannot read the `vlenb` CSR itself; without the variable a vector pass has no evidence.
pub fn rvv_vlen() -> Option<u32> {
    if !cfg!(daedalus_rvv) {
        return None;
    }
    std::env::var("ACCEL_RVV_VLEN").ok()?.trim().parse().ok()
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
    let extensions: Vec<String> = evidence
        .extensions
        .iter()
        .map(|e| json_str(e.name()))
        .collect();
    let line = format!(
        "{{\"kind\":\"test\",\"op\":{},\"backend\":{},\"extensions\":[{}],\"variant\":{},\"adapter\":{},\"target\":{},\"runner\":{},\"vlen\":{vlen},\"rev\":{},\"result\":{},\"detail\":{},\"note\":{}}}\n",
        json_str(evidence.op),
        json_str(evidence.backend.name()),
        extensions.join(","),
        opt(evidence.variant),
        opt(evidence.adapter),
        json_str(&target),
        json_str(&runner),
        json_str(&rev),
        json_str(if result.is_ok() { "pass" } else { "fail" }),
        opt(result.as_ref().err().map(String::as_str)),
        opt(evidence.note),
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Runs in a child process with `ACCEL_RESULTS_DIR` set (see the parent test below).
    #[test]
    fn child_records_one_check() {
        conform("daedalus.test.record", Backend::Scalar, || true);
    }

    #[test]
    fn conform_records_a_json_line_with_results_dir() {
        let dir = std::env::temp_dir().join(format!("accel-record-{}", std::process::id()));
        let status = std::process::Command::new(std::env::current_exe().expect("test binary"))
            .args([
                "--exact",
                "conform::tests::child_records_one_check",
                "--test-threads=1",
            ])
            .env("ACCEL_RESULTS_DIR", &dir)
            .status()
            .expect("run the child test");
        assert!(status.success());
        let mut lines = String::new();
        for entry in std::fs::read_dir(&dir).expect("results dir") {
            let path = entry.expect("entry").path();
            lines.push_str(&std::fs::read_to_string(path).expect("results file"));
        }
        std::fs::remove_dir_all(&dir).ok();
        assert!(lines.contains("\"op\":\"daedalus.test.record\""), "{lines}");
        assert!(lines.contains("\"backend\":\"scalar\""), "{lines}");
        assert!(lines.contains("\"result\":\"pass\""), "{lines}");
    }

    #[test]
    fn unavailable_backend_is_skipped_without_running_the_check() {
        if detected_backends().contains(&Backend::Neon32) {
            return;
        }
        let mut ran = false;
        let ok = conform("daedalus.test.skip", Backend::Neon32, || {
            ran = true;
            true
        });
        assert!(!ok);
        assert!(!ran);
    }
}
