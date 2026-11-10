//! The header is written by hand, so this holds it to the library three ways: every function
//! here has the Rust type the table gives it, the header declares it with the C prototype the
//! table gives it and declares nothing else, and the built library exports exactly these. And
//! the header compiles as C11 with every warning an error.

mod support;

use std::path::Path;
#[cfg(unix)]
use std::process::Command;

use daedalus_capi::*;

type H = Handle;
type S = Status;
type Name = *const u8;

/// Checks a function against the Rust type it is declared with, and gives its C prototype.
macro_rules! surface {
    ($($name:ident: $ty:ty => $c:literal;)*) => {
        fn table() -> Vec<(&'static str, &'static str)> {
            $(let _: $ty = $name;)*
            vec![$((stringify!($name), $c),)*]
        }
    };
}

surface! {
    daedalus_version: unsafe extern "C" fn(*mut Version) -> S
        => "daedalus_status_t daedalus_version(daedalus_version_t *out);";
    daedalus_status_name: extern "C" fn(S) -> *const std::ffi::c_char
        => "const char *daedalus_status_name(daedalus_status_t status);";
    daedalus_last_error: unsafe extern "C" fn(*mut u8, usize, *mut usize) -> S
        => "daedalus_status_t daedalus_last_error(uint8_t *buf, size_t cap, size_t *len);";
    daedalus_selftest_panic: extern "C" fn() -> S
        => "daedalus_status_t daedalus_selftest_panic(void);";
    daedalus_host_create: unsafe extern "C" fn(*mut H) -> S
        => "daedalus_status_t daedalus_host_create(daedalus_handle_t *out);";
    daedalus_host_release: extern "C" fn(H) -> S
        => "daedalus_status_t daedalus_host_release(daedalus_handle_t host);";
    daedalus_host_install: unsafe extern "C" fn(H, Name, usize) -> S
        => "daedalus_status_t daedalus_host_install(daedalus_handle_t host, const uint8_t *path, size_t len);";
    daedalus_host_schemas: unsafe extern "C" fn(H, *mut u8, usize, *mut usize) -> S
        => "daedalus_status_t daedalus_host_schemas(daedalus_handle_t host, uint8_t *buf, size_t cap, size_t *len);";
    daedalus_graph_compile: unsafe extern "C" fn(H, Name, usize, *mut H) -> S
        => "daedalus_status_t daedalus_graph_compile(daedalus_handle_t host, const uint8_t *json, size_t len, daedalus_handle_t *out);";
    daedalus_graph_release: extern "C" fn(H) -> S
        => "daedalus_status_t daedalus_graph_release(daedalus_handle_t graph);";
    daedalus_graph_port_count: unsafe extern "C" fn(H, *mut usize) -> S
        => "daedalus_status_t daedalus_graph_port_count(daedalus_handle_t graph, size_t *count);";
    daedalus_graph_port: unsafe extern "C" fn(H, usize, *mut PortInfo) -> S
        => "daedalus_status_t daedalus_graph_port(daedalus_handle_t graph, size_t index, daedalus_port_t *out);";
    daedalus_graph_port_index: unsafe extern "C" fn(H, Name, usize, *mut usize) -> S
        => "daedalus_status_t daedalus_graph_port_index(daedalus_handle_t graph, const uint8_t *name, size_t len, size_t *index);";
    daedalus_graph_explain: unsafe extern "C" fn(H, *mut u8, usize, *mut usize) -> S
        => "daedalus_status_t daedalus_graph_explain(daedalus_handle_t graph, uint8_t *buf, size_t cap, size_t *len);";
    daedalus_graph_telemetry: unsafe extern "C" fn(H, *mut Telemetry) -> S
        => "daedalus_status_t daedalus_graph_telemetry(daedalus_handle_t graph, daedalus_telemetry_t *out);";
    daedalus_tick: unsafe extern "C" fn(H, *mut TickInfo) -> S
        => "daedalus_status_t daedalus_tick(daedalus_handle_t graph, daedalus_tick_t *out);";
    daedalus_push_i64: unsafe extern "C" fn(H, Name, usize, i64) -> S
        => "daedalus_status_t daedalus_push_i64(daedalus_handle_t graph, const uint8_t *name, size_t len, int64_t value);";
    daedalus_push_i64_at: extern "C" fn(H, usize, i64) -> S
        => "daedalus_status_t daedalus_push_i64_at(daedalus_handle_t graph, size_t index, int64_t value);";
    daedalus_push_f64: unsafe extern "C" fn(H, Name, usize, f64) -> S
        => "daedalus_status_t daedalus_push_f64(daedalus_handle_t graph, const uint8_t *name, size_t len, double value);";
    daedalus_push_f64_at: extern "C" fn(H, usize, f64) -> S
        => "daedalus_status_t daedalus_push_f64_at(daedalus_handle_t graph, size_t index, double value);";
    daedalus_push_bool: unsafe extern "C" fn(H, Name, usize, u8) -> S
        => "daedalus_status_t daedalus_push_bool(daedalus_handle_t graph, const uint8_t *name, size_t len, uint8_t value);";
    daedalus_push_bool_at: extern "C" fn(H, usize, u8) -> S
        => "daedalus_status_t daedalus_push_bool_at(daedalus_handle_t graph, size_t index, uint8_t value);";
    daedalus_push_bytes: unsafe extern "C" fn(H, Name, usize, *const u8, usize) -> S
        => "daedalus_status_t daedalus_push_bytes(daedalus_handle_t graph, const uint8_t *name, size_t name_len, const uint8_t *bytes, size_t len);";
    daedalus_push_bytes_at: unsafe extern "C" fn(H, usize, *const u8, usize) -> S
        => "daedalus_status_t daedalus_push_bytes_at(daedalus_handle_t graph, size_t index, const uint8_t *bytes, size_t len);";
    daedalus_push_batch: unsafe extern "C" fn(H, *const Push, usize, *mut usize) -> S
        => "daedalus_status_t daedalus_push_batch(daedalus_handle_t graph, const daedalus_push_t *pushes, size_t count, size_t *bad);";
    daedalus_take_i64: unsafe extern "C" fn(H, Name, usize, *mut i64, *mut u8) -> S
        => "daedalus_status_t daedalus_take_i64(daedalus_handle_t graph, const uint8_t *name, size_t len, int64_t *out, uint8_t *present);";
    daedalus_take_i64_at: unsafe extern "C" fn(H, usize, *mut i64, *mut u8) -> S
        => "daedalus_status_t daedalus_take_i64_at(daedalus_handle_t graph, size_t index, int64_t *out, uint8_t *present);";
    daedalus_take_f64: unsafe extern "C" fn(H, Name, usize, *mut f64, *mut u8) -> S
        => "daedalus_status_t daedalus_take_f64(daedalus_handle_t graph, const uint8_t *name, size_t len, double *out, uint8_t *present);";
    daedalus_take_f64_at: unsafe extern "C" fn(H, usize, *mut f64, *mut u8) -> S
        => "daedalus_status_t daedalus_take_f64_at(daedalus_handle_t graph, size_t index, double *out, uint8_t *present);";
    daedalus_take_bool: unsafe extern "C" fn(H, Name, usize, *mut u8, *mut u8) -> S
        => "daedalus_status_t daedalus_take_bool(daedalus_handle_t graph, const uint8_t *name, size_t len, uint8_t *out, uint8_t *present);";
    daedalus_take_bool_at: unsafe extern "C" fn(H, usize, *mut u8, *mut u8) -> S
        => "daedalus_status_t daedalus_take_bool_at(daedalus_handle_t graph, size_t index, uint8_t *out, uint8_t *present);";
    daedalus_take_bytes: unsafe extern "C" fn(H, Name, usize, *mut u8, usize, *mut usize, *mut u8) -> S
        => "daedalus_status_t daedalus_take_bytes(daedalus_handle_t graph, const uint8_t *name, size_t name_len, uint8_t *buf, size_t cap, size_t *len, uint8_t *present);";
    daedalus_take_bytes_at: unsafe extern "C" fn(H, usize, *mut u8, usize, *mut usize, *mut u8) -> S
        => "daedalus_status_t daedalus_take_bytes_at(daedalus_handle_t graph, size_t index, uint8_t *buf, size_t cap, size_t *len, uint8_t *present);";
}

fn header() -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("include/daedalus.h"))
        .expect("the header")
}

/// Every line of the header that declares a function.
fn declared(header: &str) -> Vec<&str> {
    header
        .lines()
        .filter(|line| {
            line.ends_with(");") && line.contains(" daedalus_") || line.contains("*daedalus_")
        })
        .filter(|line| !line.starts_with("typedef") && !line.starts_with(' '))
        .collect()
}

#[test]
fn the_header_declares_every_function_as_it_is_and_nothing_else() {
    let header = header();
    let table = table();
    let declared = declared(&header);
    for (name, prototype) in &table {
        assert!(
            declared.contains(prototype),
            "the header doesn't declare `{name}` as `{prototype}`"
        );
    }
    assert_eq!(
        declared.len(),
        table.len(),
        "the header declares something that isn't in the library"
    );
}

#[test]
fn the_header_names_every_status_with_its_number() {
    let header = header();
    for (status, name) in Status::ALL {
        let line = format!("#define {name} {}", status.0);
        assert!(
            header.lines().any(|have| have == line),
            "the header lacks `{line}`"
        );
    }
    let defined = header
        .lines()
        .filter(|line| {
            line.starts_with("#define DAEDALUS_") && !line.contains("KIND") && !line.ends_with("_H")
        })
        .count();
    assert_eq!(
        defined,
        Status::ALL.len(),
        "a status the library doesn't have"
    );
    for (name, kind) in [
        ("I64", KIND_I64),
        ("F64", KIND_F64),
        ("BOOL", KIND_BOOL),
        ("BYTES", KIND_BYTES),
    ] {
        let line = format!("#define DAEDALUS_KIND_{name} {kind}");
        assert!(
            header.lines().any(|have| have == line),
            "the header lacks `{line}`"
        );
    }
}

/// The C compiler, or `None` on a machine with none, where these checks can't run.
#[cfg(unix)]
fn cc() -> Option<&'static str> {
    ["cc", "gcc", "clang"].into_iter().find(|cc| {
        Command::new(cc)
            .arg("--version")
            .output()
            .is_ok_and(|out| out.status.success())
    })
}

#[cfg(unix)]
#[test]
fn the_header_compiles_as_c11_with_every_warning_an_error() {
    let cc = cc().expect("a C compiler: this check needs one");
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("header");
    std::fs::create_dir_all(&dir).expect("a directory");
    // The header alone, and included twice, with the structs' sizes as Rust has them.
    let source = dir.join("header.c");
    let sizes = format!(
        "#include \"daedalus.h\"\n#include \"daedalus.h\"\n\
         _Static_assert(sizeof(daedalus_version_t) == {}, \"version\");\n\
         _Static_assert(sizeof(daedalus_port_t) == {}, \"port\");\n\
         _Static_assert(sizeof(daedalus_telemetry_t) == {}, \"telemetry\");\n\
         _Static_assert(sizeof(daedalus_tick_t) == {}, \"tick\");\n\
         _Static_assert(sizeof(daedalus_push_t) == {}, \"push\");\n\
         _Static_assert(sizeof(daedalus_push_value_t) == {}, \"push value\");\n\
         _Static_assert(sizeof(daedalus_bytes_t) == {}, \"bytes\");\n\
         int main(void) {{ return 0; }}\n",
        size_of::<Version>(),
        size_of::<PortInfo>(),
        size_of::<Telemetry>(),
        size_of::<TickInfo>(),
        size_of::<Push>(),
        size_of::<PushValue>(),
        size_of::<ByteSpan>(),
    );
    std::fs::write(&source, sizes).expect("the source");
    let include = Path::new(env!("CARGO_MANIFEST_DIR")).join("include");
    let output = Command::new(cc)
        .args([
            "-std=c11",
            "-Wall",
            "-Wextra",
            "-Werror",
            "-pedantic",
            "-fsyntax-only",
            "-I",
        ])
        .arg(&include)
        .arg(&source)
        .output()
        .expect("the compiler runs");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(target_os = "linux")]
#[test]
fn the_library_exports_exactly_these() {
    let library = support::surface_library();
    let output = Command::new("nm")
        .args(["-D", "--defined-only"])
        .arg(&library)
        .output()
        .expect("nm runs: this check needs binutils");
    assert!(output.status.success());
    let listing = String::from_utf8_lossy(&output.stdout);
    let exported: std::collections::BTreeSet<&str> = listing
        .lines()
        .filter_map(|line| line.split_whitespace().last())
        .filter(|symbol| symbol.starts_with("daedalus_"))
        .collect();
    let table = table();
    let wanted: std::collections::BTreeSet<&str> = table.iter().map(|(name, _)| *name).collect();
    assert_eq!(exported, wanted);
}
