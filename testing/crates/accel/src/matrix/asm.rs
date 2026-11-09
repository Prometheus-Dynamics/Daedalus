//! Recognizing vector code in `llvm-objdump -d --demangle --no-show-raw-insn` listings.

use crate::Backend;

/// Vector instructions for `backend` in an `llvm-objdump -d` listing of one function: packed
/// operations only (scalar SSE `*ss`/`*sd`, VFP scalar `.f64` and plain moves do not count).
pub fn vector_instructions(backend: Backend, asm: &str) -> usize {
    vector_or_extension_instructions(backend, &[], asm)
}

/// Mnemonic prefixes of extension instructions that are not vector operations (AArch64
/// `crc32x`, x86 `popcnt`, BMI2 `pdep`), so a kernel built on them passes the asm check.
fn extension_mnemonics(ext: &str) -> &'static [&'static str] {
    match ext {
        "crc" => &["crc32"],
        "pclmulqdq" => &["pclmul", "vpclmul"],
        "aes" => &["aes", "vaes"],
        "sha2" => &["sha256", "sha1"],
        "sha3" => &["sha512", "eor3", "rax1", "xar", "bcax"],
        "sha" => &["sha1", "sha256"],
        "popcnt" => &["popcnt"],
        "bmi2" => &[
            "pdep", "pext", "shlx", "shrx", "sarx", "bzhi", "mulx", "rorx",
        ],
        "f16c" => &["vcvtph2ps", "vcvtps2ph"],
        "dotprod" => &["sdot", "udot"],
        "i8mm" => &["smmla", "ummla", "usmmla", "usdot", "sudot"],
        "zba" => &["sh1add", "sh2add", "sh3add", "add.uw", "slli.uw"],
        "zbb" => &[
            "andn", "orn", "xnor", "clz", "ctz", "cpop", "max", "min", "rev8", "rol", "ror",
            "orc.b",
        ],
        "zbs" => &["bclr", "bext", "binv", "bset"],
        "zvbc" => &["vclmul"],
        "zvbb" => &[
            "vandn", "vbrev", "vclz", "vctz", "vcpop", "vrol", "vror", "vwsll",
        ],
        _ => &[],
    }
}

/// [`vector_instructions`], also counting the instructions of `extensions` (names as
/// [`crate::Ext::name`]).
pub fn vector_or_extension_instructions(
    backend: Backend,
    extensions: &[String],
    asm: &str,
) -> usize {
    let ext: Vec<&str> = extensions
        .iter()
        .flat_map(|e| extension_mnemonics(e).iter().copied())
        .collect();
    let base = base_vector_instructions(backend, asm);
    let extra = asm
        .lines()
        .filter(|line| {
            let m = line.split_whitespace().next().unwrap_or("");
            ext.iter().any(|p| m.starts_with(p))
        })
        .count();
    base + extra
}

fn base_vector_instructions(backend: Backend, asm: &str) -> usize {
    asm.lines()
        .filter(|line| {
            let line = line.trim();
            let (mnemonic, operands) = line.split_once(char::is_whitespace).unwrap_or((line, ""));
            let operands = operands.trim();
            match backend {
                Backend::X86Sse2 | Backend::X86Avx2 => {
                    let reg = if backend == Backend::X86Avx2 {
                        "ymm"
                    } else {
                        "xmm"
                    };
                    operands.contains(reg) && x86_packed(mnemonic, operands)
                }
                Backend::Neon64 => [".16b", ".8b", ".8h", ".4h", ".4s", ".2s", ".2d"]
                    .iter()
                    .any(|t| operands.contains(t)),
                Backend::Neon32 => neon32_vector(mnemonic, operands),
                Backend::Rvv => {
                    !mnemonic.starts_with("vset")
                        && mnemonic.starts_with('v')
                        && operands.split(',').any(|o| {
                            let o = o.trim().trim_start_matches('(');
                            o.len() >= 2
                                && o.starts_with('v')
                                && o[1..].chars().all(|c| c.is_ascii_digit())
                        })
                }
                _ => false,
            }
        })
        .count()
}

/// A packed SSE/AVX operation: integer `p*` ops, `*ps`/`*pd` float ops and vector moves, not
/// scalar `*ss`/`*sd` ones or a register-zeroing `xor`.
fn x86_packed(mnemonic: &str, operands: &str) -> bool {
    let m = mnemonic.strip_prefix('v').unwrap_or(mnemonic);
    let ops: Vec<&str> = operands.split(',').map(str::trim).collect();
    let zeroing =
        (m.starts_with("xor") || m.starts_with("pxor")) && ops.windows(2).all(|w| w[0] == w[1]);
    if zeroing {
        return false;
    }
    m.starts_with('p')
        || m.ends_with("ps")
        || m.ends_with("pd")
        || m.ends_with("dq")
        || m.starts_with("movdq")
        || m.starts_with("movup")
        || m.starts_with("movap")
        || m.starts_with("perm")
        || m.starts_with("broadcast")
        || m.starts_with("insert")
        || m.starts_with("extract")
        || m.starts_with("unpck")
        || m.starts_with("shufp")
        || m.starts_with("blend")
}

/// A NEON (Advanced SIMD) operation on ARMv7: on q registers, or on d registers with an
/// integer or f32 lane type, excluding VFP scalar doubles, single-register loads and moves.
fn neon32_vector(mnemonic: &str, operands: &str) -> bool {
    if !mnemonic.starts_with('v') || mnemonic.contains(".f64") {
        return false;
    }
    let base = mnemonic.split('.').next().unwrap_or(mnemonic);
    if matches!(
        base,
        "vldr" | "vstr" | "vmov" | "vpush" | "vpop" | "vmrs" | "vmsr" | "vcmp" | "vcmpe"
    ) {
        return false;
    }
    let reg = |prefix: char| {
        operands.split([',', ' ', '{', '}', '[', ']']).any(|o| {
            o.len() >= 2 && o.starts_with(prefix) && o[1..].chars().all(|c| c.is_ascii_digit())
        })
    };
    reg('q') || (reg('d') && mnemonic.contains('.'))
}

/// Functions (name, body) in an `llvm-objdump -d --demangle` listing.
pub fn functions(listing: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    for line in listing.lines() {
        if let Some(name) = line
            .trim_end()
            .strip_suffix(">:")
            .and_then(|l| l.split_once(" <"))
            .map(|(_, name)| name.to_string())
        {
            out.push((name, String::new()));
        } else if let Some((_, body)) = out.last_mut()
            && let Some((_, insn)) = line.split_once(":\t").or_else(|| line.split_once(": "))
        {
            body.push_str(insn.trim());
            body.push('\n');
        }
    }
    out
}
