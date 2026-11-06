//! Keeps `include/daedalus_surface.h` in sync with the Rust exports and the `lemnos-abi` layouts:
//! the same function names, return types and parameter types (normalized), the same constants
//! and struct sizes, and the header compiles as C11 when a C compiler is on the path.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use daedalus_surface::{FEATURE_ALLOC_COUNT, FEATURE_GRAPHS, FEATURE_PLUGINS, FEATURE_TELEMETRY};
use daedalus_surface::{PUSH_BOOL, PUSH_BYTES, PUSH_F64, PUSH_I64, PushEntry};
use daedalus_surface::{SURFACE_MAJOR, SURFACE_MINOR};
use lemnos_abi::{AbiVersion, BufferDesc, FenceDesc, LAYOUT_HASH, Status};

const HEADER: &str = include_str!("../include/daedalus_surface.h");

/// Exported for the test build only: not part of the header.
const TEST_ONLY: &[&str] = &["daedalus_surface_allocations"];

/// A signature: normalized return type and parameter types, in order.
#[derive(Debug, PartialEq)]
struct Signature {
    ret: String,
    params: Vec<String>,
}

/// Rust type to the normalized form. `const`/`mut` drop out; pointers keep a `*`.
fn norm_rust(ty: &str) -> String {
    let ty = ty.trim();
    if let Some(inner) = ty
        .strip_prefix("*const ")
        .or_else(|| ty.strip_prefix("*mut "))
    {
        return format!("*{}", norm_rust(inner));
    }
    match ty {
        "Handle" => "u64".into(),
        "Status" => "Status".into(),
        other => other.into(),
    }
}

/// C type to the normalized form, the same vocabulary as [`norm_rust`].
fn norm_c(ty: &str) -> String {
    let words: Vec<&str> = ty.split_whitespace().filter(|w| *w != "const").collect();
    let joined = words.join(" ");
    if let Some(inner) = joined.strip_suffix('*') {
        return format!("*{}", norm_c(inner.trim()));
    }
    match joined.as_str() {
        "daedalus_status" => "Status".into(),
        "daedalus_handle" | "uint64_t" => "u64".into(),
        "uint8_t" => "u8".into(),
        "uint32_t" => "u32".into(),
        "int32_t" => "i32".into(),
        "int64_t" => "i64".into(),
        "double" => "f64".into(),
        "size_t" => "usize".into(),
        "daedalus_abi_version" => "AbiVersion".into(),
        "daedalus_buffer_desc" => "BufferDesc".into(),
        "daedalus_fence_desc" => "FenceDesc".into(),
        "daedalus_message_sink" => "MessageSink".into(),
        "daedalus_push_entry" => "PushEntry".into(),
        other => other.into(),
    }
}

/// Splits a parameter list, dropping each parameter's name (the trailing identifier).
fn split_params(list: &str) -> Vec<String> {
    list.split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty() && *p != "void")
        .map(|p| {
            p.trim_end_matches(|c: char| c.is_alphanumeric() || c == '_')
                .to_string()
        })
        .collect()
}

/// Every `daedalus_surface_*` function the Rust sources export, with its signature.
fn rust_exports() -> BTreeMap<String, Signature> {
    let mut sources = Vec::new();
    collect_rs(
        &Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut sources,
    );
    let mut exports = BTreeMap::new();
    for path in sources {
        let text = std::fs::read_to_string(&path).unwrap();
        let mut rest = text.as_str();
        while let Some(at) = rest.find("extern \"C\" fn daedalus_surface_") {
            let after = &rest[at + "extern \"C\" fn ".len()..];
            let name_end = after.find('(').unwrap();
            let name = after[..name_end].to_string();
            let close = matching_paren(after, name_end);
            let params_src = &after[name_end + 1..close];
            let tail = &after[close + 1..];
            let ret = tail
                .trim_start()
                .strip_prefix("->")
                .map(|t| {
                    t.trim_start()
                        .split(|c: char| c == '{' || c.is_whitespace())
                        .next()
                        .unwrap_or("")
                        .to_string()
                })
                .unwrap_or_else(|| "()".into());
            let params = params_src
                .split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(|p| norm_rust(p.split_once(':').map_or(p, |(_, ty)| ty).trim()))
                .collect();
            exports.insert(
                name,
                Signature {
                    ret: norm_rust(&ret),
                    params,
                },
            );
            rest = &after[close..];
        }
    }
    for name in TEST_ONLY {
        exports.remove(*name);
    }
    exports
}

fn collect_rs(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_rs(&path, out);
        } else if path.extension().is_some_and(|e| e == "rs") {
            out.push(path);
        }
    }
}

/// The index of the `)` closing the `(` at `open` in `s`.
fn matching_paren(s: &str, open: usize) -> usize {
    let mut depth = 0;
    for (i, c) in s.char_indices().skip(open) {
        match c {
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return i;
                }
            }
            _ => {}
        }
    }
    panic!("unbalanced parentheses");
}

/// Every `daedalus_surface_*` prototype the header declares.
fn header_decls() -> BTreeMap<String, Signature> {
    let mut decls = BTreeMap::new();
    let mut rest = HEADER;
    while let Some(at) = rest
        .find("daedalus_status daedalus_surface_")
        .or_else(|| rest.find("\ndaedalus_status ").map(|i| i + 1))
    {
        let line = &rest[at..];
        let ret_end = line.find(' ').unwrap();
        let after = &line[ret_end + 1..];
        let name_end = after.find('(').unwrap();
        let name = after[..name_end].to_string();
        let close = after.find(')').unwrap();
        let params = split_params(&after[name_end + 1..close]);
        decls.insert(
            name,
            Signature {
                ret: norm_c(&line[..ret_end]),
                params: params.iter().map(|p| norm_c(p)).collect(),
            },
        );
        rest = &after[close..];
    }
    decls
}

/// The `#define NAME value` constants the header declares, as integers (hex or decimal).
fn header_define(name: &str) -> i128 {
    let line = HEADER
        .lines()
        .find(|l| l.trim_start().starts_with(&format!("#define {name} ")))
        .unwrap_or_else(|| panic!("the header has no #define {name}"));
    let value = line[line.find(name).unwrap() + name.len()..].trim();
    let value = value.replace("ULL", "").replace("UL", "").replace("U", "");
    let value = value.trim_start_matches('(').trim_end_matches(')');
    let value = value.trim();
    if let Some((base, shift)) = value.split_once("<<") {
        return integer(base.trim()) << integer(shift.trim());
    }
    integer(value)
}

/// A decimal (optionally negative) or hexadecimal literal, suffixes already stripped.
fn integer(value: &str) -> i128 {
    if let Some(hex) = value.strip_prefix("0x") {
        return i128::from_str_radix(hex, 16).unwrap();
    }
    if let Some(inner) = value.strip_prefix('-') {
        return -integer(inner.trim());
    }
    value
        .parse::<i128>()
        .unwrap_or_else(|_| panic!("not an integer: {value}"))
}

#[test]
fn header_declares_exactly_the_rust_exports_with_the_same_signatures() {
    let rust = rust_exports();
    let c = header_decls();
    assert_eq!(rust.len(), 34, "the surface exports 34 functions");
    for (name, sig) in &rust {
        let declared = c
            .get(name)
            .unwrap_or_else(|| panic!("{name} is exported but not declared in the header"));
        assert_eq!(
            declared, sig,
            "{name}: the header and the Rust signature differ"
        );
    }
    for name in c.keys() {
        assert!(
            rust.contains_key(name),
            "{name} is declared but not exported"
        );
    }
}

#[test]
fn header_constants_match_lemnos_abi_and_the_build() {
    assert_eq!(
        header_define("DAEDALUS_LAYOUT_HASH"),
        i128::from(LAYOUT_HASH),
        "LAYOUT_HASH"
    );
    assert_eq!(
        header_define("DAEDALUS_SURFACE_ABI_MAJOR"),
        i128::from(SURFACE_MAJOR)
    );
    assert_eq!(
        header_define("DAEDALUS_SURFACE_ABI_MINOR"),
        i128::from(SURFACE_MINOR)
    );
    assert_eq!(
        header_define("DAEDALUS_ABI_VERSION_SIZE"),
        std::mem::size_of::<AbiVersion>() as i128
    );
    assert_eq!(
        header_define("DAEDALUS_BUFFER_DESC_SIZE"),
        std::mem::size_of::<BufferDesc>() as i128
    );
    assert_eq!(
        header_define("DAEDALUS_FENCE_DESC_SIZE"),
        std::mem::size_of::<FenceDesc>() as i128
    );
    assert_eq!(header_define("DAEDALUS_PLANE_DESC_SIZE"), 32);
    assert_eq!(
        header_define("DAEDALUS_MAX_PLANES"),
        lemnos_abi::MAX_PLANES as i128
    );
    assert_eq!(
        header_define("DAEDALUS_FEATURE_PLUGINS"),
        i128::from(FEATURE_PLUGINS)
    );
    assert_eq!(
        header_define("DAEDALUS_FEATURE_GRAPHS"),
        i128::from(FEATURE_GRAPHS)
    );
    assert_eq!(
        header_define("DAEDALUS_FEATURE_TELEMETRY"),
        i128::from(FEATURE_TELEMETRY)
    );
    assert_eq!(
        header_define("DAEDALUS_FEATURE_ALLOC_COUNT"),
        i128::from(FEATURE_ALLOC_COUNT)
    );
    assert_eq!(
        header_define("DAEDALUS_BUFFER_OWNED"),
        i128::from(BufferDesc::OWNED)
    );
    assert_eq!(
        header_define("DAEDALUS_FENCE_NONE"),
        i128::from(FenceDesc::NONE)
    );
    assert_eq!(
        header_define("DAEDALUS_FENCE_SYNC_FILE"),
        i128::from(FenceDesc::SYNC_FILE)
    );
    assert_eq!(
        header_define("DAEDALUS_FENCE_SYNCOBJ"),
        i128::from(FenceDesc::SYNCOBJ)
    );
    for (name, kind) in [
        ("DAEDALUS_PUSH_I64", PUSH_I64),
        ("DAEDALUS_PUSH_F64", PUSH_F64),
        ("DAEDALUS_PUSH_BOOL", PUSH_BOOL),
        ("DAEDALUS_PUSH_BYTES", PUSH_BYTES),
    ] {
        assert_eq!(header_define(name), i128::from(kind), "{name}");
    }
    // The C struct's size is asserted in the header against the same formula.
    assert_eq!(
        std::mem::size_of::<PushEntry>(),
        24 + std::mem::size_of::<*const u8>() + std::mem::size_of::<usize>()
    );
    let statuses = [
        ("DAEDALUS_OK", Status::OK),
        ("DAEDALUS_IO", Status::IO),
        ("DAEDALUS_NOT_FOUND", Status::NOT_FOUND),
        ("DAEDALUS_AGAIN", Status::AGAIN),
        ("DAEDALUS_NO_MEMORY", Status::NO_MEMORY),
        ("DAEDALUS_BUSY", Status::BUSY),
        ("DAEDALUS_INVALID_ARGUMENT", Status::INVALID_ARGUMENT),
        ("DAEDALUS_NOT_SUPPORTED", Status::NOT_SUPPORTED),
        ("DAEDALUS_STALE_HANDLE", Status::STALE_HANDLE),
        ("DAEDALUS_VERSION_MISMATCH", Status::VERSION_MISMATCH),
        ("DAEDALUS_BUFFER_TOO_SMALL", Status::BUFFER_TOO_SMALL),
        ("DAEDALUS_PANIC", Status::PANIC),
    ];
    for (name, status) in statuses {
        assert_eq!(header_define(name), i128::from(status.0), "{name}");
    }
}

/// The header is valid C11 (its `_Static_assert`s included). Skipped without a C compiler.
#[test]
fn header_compiles_as_c11() {
    let dir = std::env::temp_dir().join(format!("daedalus-surface-header-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let source = dir.join("check.c");
    std::fs::write(
        &source,
        "#include \"daedalus_surface.h\"\nint main(void) { return 0; }\n",
    )
    .unwrap();
    let include = Path::new(env!("CARGO_MANIFEST_DIR")).join("include");
    let compiler = std::env::var("CC").unwrap_or_else(|_| "cc".to_string());
    let result = Command::new(compiler)
        .args([
            "-std=c11",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-pedantic",
            "-fsyntax-only",
        ])
        .arg("-I")
        .arg(&include)
        .arg(&source)
        .output();
    let _ = std::fs::remove_dir_all(&dir);
    // No C compiler on this machine: the check is skipped, the other tests still run.
    if let Ok(out) = result {
        assert!(
            out.status.success(),
            "the header does not compile as C11:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
