//! Acceleration inventory shared by Daedalus, Eidos and Styx.
//!
//! Every data-parallel op names its scalar reference ([`Op`]) and lists, per [`Backend`], a
//! [`Kernel`]: implemented (with the conformance test that checks it against scalar), a gap, or
//! impossible with a reason. Crates export these as const [`Registry`] tables, which nest, so the
//! inventory works without `std`, linker sections or a dependency on Daedalus.
//!
//! A kernel is never "pass" because its table says so. Conformance tests call [`conform`]
//! (`std`), which records evidence (target, runner, RVV vector length, GPU adapter, result)
//! when `ACCEL_RESULTS_DIR` is set; the `accel-matrix` generator (feature `generator`) combines
//! the registry dumps ([`dump`]) with that evidence into `docs/backend-matrix.{md,csv}` and gates
//! releases on it. See the crate README for the rules.
#![cfg_attr(not(feature = "std"), no_std)]

#[cfg(feature = "std")]
mod conform;
#[cfg(feature = "std")]
mod dump;
#[cfg(feature = "generator")]
pub mod matrix;

#[cfg(feature = "std")]
pub use conform::{
    Evidence, conform, conform_with, detected_backends, detected_extensions, rvv_vlen,
};
#[cfg(feature = "std")]
pub use dump::{dump, dump_json};

/// Where a kernel runs. Columns of the backend matrix, in this order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Backend {
    /// The reference implementation every other backend is checked against.
    Scalar,
    /// Portable SIMD (`core::simd` or a portable-SIMD crate), independent of the ISA.
    PortableSimd,
    /// x86 / x86_64 SSE2 (the x86_64 baseline), including SSE3-SSE4.2 paths.
    X86Sse2,
    /// x86_64 AVX2 (runtime-dispatched).
    X86Avx2,
    /// AArch64 Advanced SIMD.
    Neon64,
    /// ARMv7 NEON (a build with `-C target-feature=+neon`, or intrinsic/asm shims).
    Neon32,
    /// RISC-V Vector 1.0 (a build with `-C target-feature=+v`, or asm). Pass needs evidence
    /// at VLEN 128, 256 and 512.
    Rvv,
    /// A GPU through wgpu (compute or fragment; see [`Kernel::variant`]).
    Gpu,
    /// A fixed-function unit (texture formatting unit, ISP, video or JPEG block, DMA engine).
    FixedFunction,
}

impl Backend {
    /// Every backend, in matrix column order.
    pub const ALL: [Backend; 9] = [
        Backend::Scalar,
        Backend::PortableSimd,
        Backend::X86Sse2,
        Backend::X86Avx2,
        Backend::Neon64,
        Backend::Neon32,
        Backend::Rvv,
        Backend::Gpu,
        Backend::FixedFunction,
    ];

    /// The stable name used in dumps, evidence and the CSV.
    pub const fn name(self) -> &'static str {
        match self {
            Backend::Scalar => "scalar",
            Backend::PortableSimd => "portable_simd",
            Backend::X86Sse2 => "x86_sse2",
            Backend::X86Avx2 => "x86_avx2",
            Backend::Neon64 => "neon64",
            Backend::Neon32 => "neon32",
            Backend::Rvv => "rvv",
            Backend::Gpu => "gpu",
            Backend::FixedFunction => "fixed_function",
        }
    }

    /// Inverse of [`name`](Self::name).
    pub fn from_name(name: &str) -> Option<Backend> {
        Backend::ALL.into_iter().find(|b| b.name() == name)
    }

    /// Vector lengths (bits) a pass needs evidence at: RVV 128, 256 and 512; other backends any
    /// one run.
    pub const fn required_vlens(self) -> &'static [u32] {
        match self {
            Backend::Rvv => &[128, 256, 512],
            _ => &[],
        }
    }
}

/// An ISA extension a kernel uses on top of its backend's base vector unit. Matrix columns
/// split per extension set (`neon64+dotprod`); a base kernel's pass does not count for one.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[non_exhaustive]
pub enum Ext {
    // AArch64
    Dotprod,
    Fp16,
    I8mm,
    Crc32,
    Aes,
    Sha2,
    Sha3,
    Sve,
    Sve2,
    // RISC-V
    Zba,
    Zbb,
    Zbs,
    Zvbb,
    Zvbc,
    // x86
    Popcnt,
    Bmi2,
    Fma,
    F16c,
    Pclmulqdq,
    Sha,
    Avx512f,
    Avx512bw,
    Avx512vnni,
}

impl Ext {
    pub const ALL: [Ext; 23] = [
        Ext::Dotprod,
        Ext::Fp16,
        Ext::I8mm,
        Ext::Crc32,
        Ext::Aes,
        Ext::Sha2,
        Ext::Sha3,
        Ext::Sve,
        Ext::Sve2,
        Ext::Zba,
        Ext::Zbb,
        Ext::Zbs,
        Ext::Zvbb,
        Ext::Zvbc,
        Ext::Popcnt,
        Ext::Bmi2,
        Ext::Fma,
        Ext::F16c,
        Ext::Pclmulqdq,
        Ext::Sha,
        Ext::Avx512f,
        Ext::Avx512bw,
        Ext::Avx512vnni,
    ];

    /// The stable name (the target-feature name where there is one).
    pub const fn name(self) -> &'static str {
        match self {
            Ext::Dotprod => "dotprod",
            Ext::Fp16 => "fp16",
            Ext::I8mm => "i8mm",
            Ext::Crc32 => "crc",
            Ext::Aes => "aes",
            Ext::Sha2 => "sha2",
            Ext::Sha3 => "sha3",
            Ext::Sve => "sve",
            Ext::Sve2 => "sve2",
            Ext::Zba => "zba",
            Ext::Zbb => "zbb",
            Ext::Zbs => "zbs",
            Ext::Zvbb => "zvbb",
            Ext::Zvbc => "zvbc",
            Ext::Popcnt => "popcnt",
            Ext::Bmi2 => "bmi2",
            Ext::Fma => "fma",
            Ext::F16c => "f16c",
            Ext::Pclmulqdq => "pclmulqdq",
            Ext::Sha => "sha",
            Ext::Avx512f => "avx512f",
            Ext::Avx512bw => "avx512bw",
            Ext::Avx512vnni => "avx512vnni",
        }
    }

    pub fn from_name(name: &str) -> Option<Ext> {
        Ext::ALL.into_iter().find(|e| e.name() == name)
    }
}

/// What a backend's entry for an op says. A backend with no entry is a gap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Implemented; the matrix shows pass only with CI evidence for [`Kernel::test`].
    Implemented,
    /// Known missing, with the reason or plan.
    Gap(&'static str),
    /// Cannot be done on this backend, with the reason (an instruction the ISA lacks, a
    /// toolchain limit).
    Impossible(&'static str),
    /// The op is not data-parallel in a way this backend can use (a serial dependency chain,
    /// per-element heap values). On [`Backend::Gpu`] for an op marked
    /// [`Op::per_element`], the gate lists it for review.
    NotDataParallel(&'static str),
}

/// A data-parallel operation and its scalar reference.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Op {
    /// `<repo>.<area>.<op>`, unique across repos.
    pub id: &'static str,
    /// One line: what it computes.
    pub summary: &'static str,
    /// Path of the scalar reference function.
    pub scalar: &'static str,
    /// Per-pixel or per-element work: never impossible on the GPU without review.
    pub per_element: bool,
    /// Extension variants the op could use (`(Neon64, &[Ext::Dotprod])`): without a kernel
    /// for one, the matrix shows a gap in that column.
    pub candidates: &'static [(Backend, &'static [Ext])],
    /// Where the op runs in the product: the matrix's `wired` column checks that each site
    /// calls the kernel and holds none of the old scalar code. No sites is a gap.
    pub call_sites: &'static [CallSite],
}

/// A place that must run an op through its kernel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CallSite {
    /// `path/from/repo/root.rs`.
    pub path: &'static str,
    /// Text the file must contain: the kernel call (`daedalus_kernels::rgb_to_rgba(`).
    pub calls: &'static str,
    /// Text it must not contain: the scalar code the kernel replaced (`half::f16`).
    pub forbid: &'static [&'static str],
}

impl CallSite {
    pub const fn new(path: &'static str, calls: &'static str) -> Self {
        Self {
            path,
            calls,
            forbid: &[],
        }
    }

    pub const fn forbid(mut self, forbid: &'static [&'static str]) -> Self {
        self.forbid = forbid;
        self
    }
}

impl Op {
    /// A per-element op with no extension candidates.
    pub const fn new(id: &'static str, summary: &'static str, scalar: &'static str) -> Self {
        Self {
            id,
            summary,
            scalar,
            per_element: true,
            candidates: &[],
            call_sites: &[],
        }
    }

    /// The call sites that must use the kernel.
    pub const fn wired_at(mut self, call_sites: &'static [CallSite]) -> Self {
        self.call_sites = call_sites;
        self
    }

    /// Not per-element work (a hash chain, setup code).
    pub const fn not_per_element(mut self) -> Self {
        self.per_element = false;
        self
    }

    /// Extension variants the op could use.
    pub const fn could_use(mut self, candidates: &'static [(Backend, &'static [Ext])]) -> Self {
        self.candidates = candidates;
        self
    }
}

/// One backend's entry for an op.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Kernel {
    /// The [`Op::id`].
    pub op: &'static str,
    pub backend: Backend,
    /// A variant within the backend ("fragment", "compute", "avx2+fma"); `None` for the
    /// backend's only kernel. Evidence carries the same string.
    pub variant: Option<&'static str>,
    /// Extensions the kernel needs beyond the backend's base (sorted or not; compared as a set).
    pub extensions: &'static [Ext],
    /// `path/from/repo/root.rs:line` of the kernel; the gate matches marker files by path.
    pub source: &'static str,
    /// The conformance test (`module::path::test_name`) that checks it against scalar.
    pub test: &'static str,
    /// For kernels the compiler vectorizes (or asm shims): a substring of the symbol whose
    /// disassembly must contain this backend's vector instructions.
    pub symbol: Option<&'static str>,
    pub status: Status,
}

impl Kernel {
    /// An implemented kernel.
    pub const fn implemented(
        op: &'static str,
        backend: Backend,
        source: &'static str,
        test: &'static str,
    ) -> Self {
        Self {
            op,
            backend,
            variant: None,
            extensions: &[],
            source,
            test,
            symbol: None,
            status: Status::Implemented,
        }
    }

    /// An entry that is not implemented: `Gap`, `Impossible` or `NotDataParallel`.
    pub const fn missing(op: &'static str, backend: Backend, status: Status) -> Self {
        Self {
            op,
            backend,
            variant: None,
            extensions: &[],
            source: "",
            test: "",
            symbol: None,
            status,
        }
    }

    pub const fn with_variant(mut self, variant: &'static str) -> Self {
        self.variant = Some(variant);
        self
    }

    pub const fn with_extensions(mut self, extensions: &'static [Ext]) -> Self {
        self.extensions = extensions;
        self
    }

    pub const fn with_symbol(mut self, symbol: &'static str) -> Self {
        self.symbol = Some(symbol);
        self
    }
}

/// A source file with SIMD or GPU markers that holds no kernel (pipeline plumbing, feature
/// detection), with the reason. The gate accepts it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Exempt {
    pub path: &'static str,
    pub reason: &'static str,
}

/// A crate's (or module's) table. Nest module tables with `children`.
#[derive(Clone, Copy, Debug)]
pub struct Registry {
    /// The repository: "daedalus", "eidos", "styx".
    pub repo: &'static str,
    /// The crate or module the table covers.
    pub name: &'static str,
    pub ops: &'static [Op],
    pub kernels: &'static [Kernel],
    pub exempt: &'static [Exempt],
    pub children: &'static [&'static Registry],
}

impl Registry {
    /// An empty table, for `..Registry::EMPTY`.
    pub const EMPTY: Registry = Registry {
        repo: "",
        name: "",
        ops: &[],
        kernels: &[],
        exempt: &[],
        children: &[],
    };

    /// Visit this table and every nested one, depth first.
    pub fn walk(&self, visit: &mut dyn FnMut(&Registry)) {
        visit(self);
        for child in self.children {
            child.walk(visit);
        }
    }
}
