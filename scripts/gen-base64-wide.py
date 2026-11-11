#!/usr/bin/env python3
"""Writes backends/wgpu/src/kernels/wide_base64.wgsl: the base64 kernels on vectors, as straight-line
WGSL. Each value is a named local, so no array is indexed by a loop variable (a local array indexed
dynamically is slow on the device). Run from the repository root: python3 scripts/gen-base64-wide.py"""
import pathlib

LANES = "xyzw"


def byte_of_vector(q, k):
    """Byte k (0..15) of input vector q: word k // 4 of the vector, byte k % 4 of that word."""
    return f"((v{q}.{LANES[(k % 16) // 4]} >> {8 * (k % 4)}u) & 0xffu)"


def encode():
    out = [
        "// Four output vectors for forty-eight input bytes: three input vectors, sixteen groups of",
        "// three bytes, four symbols each. Counts that are multiples of 48 have no padded group.",
        "@compute @workgroup_size(64)",
        "fn base64_encode_wide(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {",
        "    let t = gid.x + gid.y * groups.x * 64u;",
        "    if (t >= params.threads) { return; }",
        "    let v0 = src[3u * t];",
        "    let v1 = src[3u * t + 1u];",
        "    let v2 = src[3u * t + 2u];",
    ]
    for k in range(48):
        out.append(f"    let b{k} = {byte_of_vector(k // 16, k)};")
    for g in range(16):
        out.append(f"    let x{g} = (b{3 * g} << 16u) | (b{3 * g + 1} << 8u) | b{3 * g + 2};")
        syms = [f"b64_symbol((x{g} >> {18 - 6 * k}u) & 63u) << {8 * k}u" for k in range(4)]
        out.append(f"    let w{g} = {' | '.join(syms)};")
    for q in range(4):
        out.append(f"    dst[4u * t + {q}u] = vec4<u32>(w{4 * q}, w{4 * q + 1}, w{4 * q + 2}, w{4 * q + 3});")
    out.append("}")
    return "\n".join(out)


def decode():
    out = [
        "// Three output vectors for sixty-four input symbols: sixteen groups of four symbols, three",
        "// bytes each. A group that holds an invalid symbol, or a padded last group with bits no byte",
        "// takes, lowers the status to the index of its first such symbol; the status is written",
        "// once an invocation, and only when it found one. Output bytes past the length are zero.",
        "fn byte_w(i: u32) -> u32 {",
        "    return (src[i / 16u][(i % 16u) / 4u] >> ((i % 4u) * 8u)) & 0xffu;",
        "}",
        "",
        "@compute @workgroup_size(64)",
        "fn base64_decode_wide(@builtin(global_invocation_id) gid: vec3<u32>, @builtin(num_workgroups) groups: vec3<u32>) {",
        "    let t = gid.x + gid.y * groups.x * 64u;",
        "    if (t >= params.threads) { return; }",
        "    let n = params.n;",
        "    let last = n / 4u - 1u;",
        "    let pad = select(0u, select(1u, 2u, byte_w(n - 2u) == 61u), byte_w(n - 1u) == 61u);",
        "    let out_len = (last + 1u) * 3u - pad;",
        "    if (t == 0u) {",
        "        atomicStore(&status[1], out_len);",
        "    }",
        "    let v0 = src[4u * t];",
        "    let v1 = src[4u * t + 1u];",
        "    let v2 = src[4u * t + 2u];",
        "    let v3 = src[4u * t + 3u];",
        "    var first = 0xffffffffu;",
    ]
    words = ["v0.x", "v0.y", "v0.z", "v0.w", "v1.x", "v1.y", "v1.z", "v1.w",
             "v2.x", "v2.y", "v2.z", "v2.w", "v3.x", "v3.y", "v3.z", "v3.w"]
    # The twelve output words, accumulated group by group: each group is decoded in its own block,
    # so only the accumulators and the group's own values are live at once.
    for w in range(12):
        out.append(f"    var o{w} = 0u;")
    for j in range(16):
        out.append("    {")
        out.append(f"        let s = {words[j]};")
        out.append(f"        let g = 16u * t + {j}u;")
        out.append(f"        let used = select(4u, 4u - pad, g == last);")
        # Four lookups: each entry is the six bits, or the six bits' invalid flag (bit 8). A symbol
        # past the end of a padded group counts as zero, so it can't fail the group.
        for k in range(4):
            out.append(f"        let e{k} = dec_table[(s >> {8 * k}u) & 0xffu];")
            out.append(f"        let q{k} = select(0u, e{k}, {k}u < used);")
        out.append("        let bad = (q0 | q1 | q2 | q3) & 0x100u;")
        out.append("        if (bad != 0u) {")
        out.append("            // The failing path only: the lowest invalid symbol of the group.")
        out.append("            var at = 0xffffffffu;")
        for k in reversed(range(4)):
            out.append(f"            if ((q{k} & 0x100u) != 0u) {{ at = 4u * g + {k}u; }}")
        out.append("            first = min(first, at);")
        out.append("        }")
        parts = [f"((q{k} & 63u) << {18 - 6 * k}u)" for k in range(4)]
        out.append(f"        let x = {' | '.join(parts)};")
        out.append("        if (g == last && pad > 0u) {")
        out.append("            let stray = x & select(0xffu, 0xffffu, pad == 2u);")
        out.append("            first = min(first, select(0xffffffffu, 4u * g + used - 1u, stray != 0u));")
        out.append("        }")
        for m, shift in enumerate((16, 8, 0)):
            b = 3 * j + m
            w, k = b // 4, b % 4
            out.append(
                f"        o{w} = o{w} | (select(0u, (x >> {shift}u) & 0xffu, 3u * g + {m}u < out_len) << {8 * k}u);")
        out.append("    }")
    for q in range(3):
        out.append(f"    dst[3u * t + {q}u] = vec4<u32>(o{4 * q}, o{4 * q + 1}, o{4 * q + 2}, o{4 * q + 3});")
    out.append("    if (first != 0xffffffffu) {")
    out.append("        atomicMin(&status[0], first);")
    out.append("    }")
    out.append("}")
    return "\n".join(out)


def main():
    root = pathlib.Path(__file__).resolve().parent.parent
    path = root / "backends/wgpu/src/kernels/wide_base64.wgsl"
    header = [
        "// The base64 kernels on vectors. Written by scripts/gen-base64-wide.py: edit the generator,",
        "// not this file. `common.wgsl` comes before this text.",
        "",
        "struct Params {",
        "    // Input symbols (decode) or bytes (encode).",
        "    n: u32,",
        "    // Invocations that write.",
        "    threads: u32,",
        "    // Output bytes.",
        "    out_len: u32,",
        "    unused: u32,",
        "}",
        "",
        "@group(0) @binding(0) var<storage, read> src: array<vec4<u32>>;",
        "@group(0) @binding(1) var<storage, read_write> dst: array<vec4<u32>>;",
        "@group(0) @binding(2) var<storage, read> params: Params;",
        "// decode: [offset of the first invalid symbol, or 0xffffffff; decoded length].",
        "@group(0) @binding(3) var<storage, read_write> status: array<atomic<u32>>;",
        "// decode: the six bits of each byte, or 0xff for a byte that is no symbol.",
        "@group(0) @binding(4) var<storage, read> dec_table: array<u32>;",
        "",
    ]
    text = "\n".join(header) + "\n" + encode() + "\n\n" + decode() + "\n"
    path.write_text(text)
    print(f"wrote {path} ({len(text)} bytes)")


if __name__ == "__main__":
    main()
