//! The header is written by hand, so this holds it to the library three ways: every function here
//! has the Rust type the table gives it, the header declares it with the C prototype the table
//! gives it and declares nothing else, and the built library exports exactly these. And the header
//! compiles as C11 with every warning an error, beside core/capi's header in one program.
//!
//! The baseline is the surface as it was first published. Growth is allowed: a function may be
//! added, and then the table, the header and the library all gain it. A baseline function that is
//! removed, renamed or given another prototype fails the baseline test.

mod support;

use daedalus_client_capi::*;
use std::ffi::c_char;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::path::PathBuf;
#[cfg(unix)]
use std::process::Command;

type H = Handle;
type S = Status;

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
    daedalus_client_version: unsafe extern "C" fn(*mut Version) -> S
        => "daedalus_client_status_t daedalus_client_version(daedalus_client_version_t *out);";
    daedalus_client_status_name: extern "C" fn(S) -> *const c_char
        => "const char *daedalus_client_status_name(daedalus_client_status_t status);";
    daedalus_client_last_error: unsafe extern "C" fn(*mut u8, usize, *mut usize) -> S
        => "daedalus_client_status_t daedalus_client_last_error(uint8_t *buf, size_t cap, size_t *len);";
    daedalus_client_last_error_code: unsafe extern "C" fn(*mut u32) -> S
        => "daedalus_client_status_t daedalus_client_last_error_code(uint32_t *code);";
    daedalus_client_selftest_panic: extern "C" fn() -> S
        => "daedalus_client_status_t daedalus_client_selftest_panic(void);";
    daedalus_client_connect: unsafe extern "C" fn(*const u8, usize, u8, *mut H) -> S
        => "daedalus_client_status_t daedalus_client_connect(const uint8_t *endpoint, size_t len, uint8_t reconnect, daedalus_client_handle_t *out);";
    daedalus_client_close: extern "C" fn(H) -> S
        => "daedalus_client_status_t daedalus_client_close(daedalus_client_handle_t client);";
    daedalus_client_in_session: unsafe extern "C" fn(H, *const u8, usize, *mut H) -> S
        => "daedalus_client_status_t daedalus_client_in_session(daedalus_client_handle_t client, const uint8_t *name, size_t len, daedalus_client_handle_t *out);";
    daedalus_client_ping: unsafe extern "C" fn(H, *mut u32) -> S
        => "daedalus_client_status_t daedalus_client_ping(daedalus_client_handle_t client, uint32_t *version);";
    daedalus_client_load_plugin: unsafe extern "C" fn(H, *const u8, usize) -> S
        => "daedalus_client_status_t daedalus_client_load_plugin(daedalus_client_handle_t client, const uint8_t *path, size_t len);";
    daedalus_client_put_graph: unsafe extern "C" fn(H, *const u8, usize, *const u8, usize) -> S
        => "daedalus_client_status_t daedalus_client_put_graph(daedalus_client_handle_t client, const uint8_t *name, size_t name_len, const uint8_t *json, size_t json_len);";
    daedalus_client_patch_graph: unsafe extern "C" fn(H, *const u8, usize, *const u8, usize) -> S
        => "daedalus_client_status_t daedalus_client_patch_graph(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const uint8_t *patch, size_t patch_len);";
    daedalus_client_plan: unsafe extern "C" fn(H, *const u8, usize, *const PlanOptions, *mut u8, usize, *mut usize) -> S
        => "daedalus_client_status_t daedalus_client_plan(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const daedalus_client_plan_options_t *options, uint8_t *buf, size_t cap, size_t *len);";
    daedalus_client_make_plan: unsafe extern "C" fn(H, *const u8, usize, *const Config, *mut u64, *mut u8, usize, *mut usize) -> S
        => "daedalus_client_status_t daedalus_client_make_plan(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const daedalus_client_config_t *config, uint64_t *hash, uint8_t *buf, size_t cap, size_t *len);";
    daedalus_client_put_plan: unsafe extern "C" fn(H, *const u8, usize, *const u8, usize) -> S
        => "daedalus_client_status_t daedalus_client_put_plan(daedalus_client_handle_t client, const uint8_t *name, size_t name_len, const uint8_t *bytes, size_t len);";
    daedalus_client_start: unsafe extern "C" fn(H, *const u8, usize, *const Config, *mut u64) -> S
        => "daedalus_client_status_t daedalus_client_start(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const daedalus_client_config_t *config, uint64_t *run);";
    daedalus_client_step: unsafe extern "C" fn(H, u64, u32, *mut Step) -> S
        => "daedalus_client_status_t daedalus_client_step(daedalus_client_handle_t client, uint64_t run, uint32_t count, daedalus_client_step_t *out);";
    daedalus_client_cancel: extern "C" fn(H, u64) -> S
        => "daedalus_client_status_t daedalus_client_cancel(daedalus_client_handle_t client, uint64_t run);";
    daedalus_client_stop: unsafe extern "C" fn(H, u64, u32, *mut Stopped) -> S
        => "daedalus_client_status_t daedalus_client_stop(daedalus_client_handle_t client, uint64_t run, uint32_t mode, daedalus_client_stopped_t *out);";
    daedalus_client_reload: unsafe extern "C" fn(H, u64, *const u8, usize, *mut Carry) -> S
        => "daedalus_client_status_t daedalus_client_reload(daedalus_client_handle_t client, uint64_t run, const uint8_t *graph, size_t graph_len, daedalus_client_carry_t *out);";
    daedalus_client_inspect_run: unsafe extern "C" fn(H, u64, *mut u8, usize, *mut usize) -> S
        => "daedalus_client_status_t daedalus_client_inspect_run(daedalus_client_handle_t client, uint64_t run, uint8_t *buf, size_t cap, size_t *len);";
    daedalus_client_inspect_graph: unsafe extern "C" fn(H, *const u8, usize, *mut u8, usize, *mut usize) -> S
        => "daedalus_client_status_t daedalus_client_inspect_graph(daedalus_client_handle_t client, const uint8_t *name, size_t name_len, uint8_t *buf, size_t cap, size_t *len);";
    daedalus_client_inspect_cache: unsafe extern "C" fn(H, *mut u8, usize, *mut usize) -> S
        => "daedalus_client_status_t daedalus_client_inspect_cache(daedalus_client_handle_t client, uint8_t *buf, size_t cap, size_t *len);";
    daedalus_client_inspect_telemetry: unsafe extern "C" fn(H, u64, *mut u8, usize, *mut usize) -> S
        => "daedalus_client_status_t daedalus_client_inspect_telemetry(daedalus_client_handle_t client, uint64_t run, uint8_t *buf, size_t cap, size_t *len);";
    daedalus_client_inspect_host: unsafe extern "C" fn(H, *mut u8, usize, *mut usize) -> S
        => "daedalus_client_status_t daedalus_client_inspect_host(daedalus_client_handle_t client, uint8_t *buf, size_t cap, size_t *len);";
    daedalus_client_cache_clear: extern "C" fn(H) -> S
        => "daedalus_client_status_t daedalus_client_cache_clear(daedalus_client_handle_t client);";
    daedalus_client_trace: unsafe extern "C" fn(H, u32, *mut u8, usize, *mut usize) -> S
        => "daedalus_client_status_t daedalus_client_trace(daedalus_client_handle_t client, uint32_t limit, uint8_t *buf, size_t cap, size_t *len);";
    daedalus_client_feed_i64: unsafe extern "C" fn(H, u64, *const u8, usize, i64, *mut Fed) -> S
        => "daedalus_client_status_t daedalus_client_feed_i64(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, int64_t value, daedalus_client_fed_t *out);";
    daedalus_client_feed_f64: unsafe extern "C" fn(H, u64, *const u8, usize, f64, *mut Fed) -> S
        => "daedalus_client_status_t daedalus_client_feed_f64(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, double value, daedalus_client_fed_t *out);";
    daedalus_client_feed_bool: unsafe extern "C" fn(H, u64, *const u8, usize, u8, *mut Fed) -> S
        => "daedalus_client_status_t daedalus_client_feed_bool(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, uint8_t value, daedalus_client_fed_t *out);";
    daedalus_client_feed_bytes: unsafe extern "C" fn(H, u64, *const u8, usize, *const u8, usize, *mut Fed) -> S
        => "daedalus_client_status_t daedalus_client_feed_bytes(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, const uint8_t *bytes, size_t len, daedalus_client_fed_t *out);";
    daedalus_client_feed_str: unsafe extern "C" fn(H, u64, *const u8, usize, *const u8, usize, *mut Fed) -> S
        => "daedalus_client_status_t daedalus_client_feed_str(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, const uint8_t *text, size_t len, daedalus_client_fed_t *out);";
    daedalus_client_feed_wire: unsafe extern "C" fn(H, u64, *const u8, usize, *const u8, usize, *mut Fed) -> S
        => "daedalus_client_status_t daedalus_client_feed_wire(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, const uint8_t *bytes, size_t len, daedalus_client_fed_t *out);";
    daedalus_client_take_i64: unsafe extern "C" fn(H, u64, *const u8, usize, *mut i64, *mut u8) -> S
        => "daedalus_client_status_t daedalus_client_take_i64(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, int64_t *out, uint8_t *present);";
    daedalus_client_take_f64: unsafe extern "C" fn(H, u64, *const u8, usize, *mut f64, *mut u8) -> S
        => "daedalus_client_status_t daedalus_client_take_f64(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, double *out, uint8_t *present);";
    daedalus_client_take_bool: unsafe extern "C" fn(H, u64, *const u8, usize, *mut u8, *mut u8) -> S
        => "daedalus_client_status_t daedalus_client_take_bool(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, uint8_t *out, uint8_t *present);";
    daedalus_client_take_bytes: unsafe extern "C" fn(H, u64, *const u8, usize, *mut u8, usize, *mut usize, *mut u8) -> S
        => "daedalus_client_status_t daedalus_client_take_bytes(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, uint8_t *buf, size_t cap, size_t *len, uint8_t *present);";
    daedalus_client_take_wire: unsafe extern "C" fn(H, u64, *const u8, usize, *mut u8, usize, *mut usize, *mut u8) -> S
        => "daedalus_client_status_t daedalus_client_take_wire(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, uint8_t *buf, size_t cap, size_t *len, uint8_t *present);";
    daedalus_client_watch_open: unsafe extern "C" fn(H, u64, u32, *mut H) -> S
        => "daedalus_client_status_t daedalus_client_watch_open(daedalus_client_handle_t client, uint64_t run, uint32_t kinds, daedalus_client_handle_t *out);";
    daedalus_client_watch_next: unsafe extern "C" fn(H, u32, *mut u8, usize, *mut usize, *mut u32) -> S
        => "daedalus_client_status_t daedalus_client_watch_next(daedalus_client_handle_t watch, uint32_t timeout_ms, uint8_t *buf, size_t cap, size_t *len, uint32_t *state);";
    daedalus_client_watch_close: extern "C" fn(H) -> S
        => "daedalus_client_status_t daedalus_client_watch_close(daedalus_client_handle_t watch);";
}

/// The surface's first prototypes, frozen. A later library keeps every one of them.
const BASELINE: &[&str] = &[
    "daedalus_client_status_t daedalus_client_version(daedalus_client_version_t *out);",
    "const char *daedalus_client_status_name(daedalus_client_status_t status);",
    "daedalus_client_status_t daedalus_client_last_error(uint8_t *buf, size_t cap, size_t *len);",
    "daedalus_client_status_t daedalus_client_last_error_code(uint32_t *code);",
    "daedalus_client_status_t daedalus_client_selftest_panic(void);",
    "daedalus_client_status_t daedalus_client_connect(const uint8_t *endpoint, size_t len, uint8_t reconnect, daedalus_client_handle_t *out);",
    "daedalus_client_status_t daedalus_client_close(daedalus_client_handle_t client);",
    "daedalus_client_status_t daedalus_client_in_session(daedalus_client_handle_t client, const uint8_t *name, size_t len, daedalus_client_handle_t *out);",
    "daedalus_client_status_t daedalus_client_ping(daedalus_client_handle_t client, uint32_t *version);",
    "daedalus_client_status_t daedalus_client_load_plugin(daedalus_client_handle_t client, const uint8_t *path, size_t len);",
    "daedalus_client_status_t daedalus_client_put_graph(daedalus_client_handle_t client, const uint8_t *name, size_t name_len, const uint8_t *json, size_t json_len);",
    "daedalus_client_status_t daedalus_client_patch_graph(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const uint8_t *patch, size_t patch_len);",
    "daedalus_client_status_t daedalus_client_plan(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const daedalus_client_plan_options_t *options, uint8_t *buf, size_t cap, size_t *len);",
    "daedalus_client_status_t daedalus_client_make_plan(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const daedalus_client_config_t *config, uint64_t *hash, uint8_t *buf, size_t cap, size_t *len);",
    "daedalus_client_status_t daedalus_client_put_plan(daedalus_client_handle_t client, const uint8_t *name, size_t name_len, const uint8_t *bytes, size_t len);",
    "daedalus_client_status_t daedalus_client_start(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const daedalus_client_config_t *config, uint64_t *run);",
    "daedalus_client_status_t daedalus_client_step(daedalus_client_handle_t client, uint64_t run, uint32_t count, daedalus_client_step_t *out);",
    "daedalus_client_status_t daedalus_client_cancel(daedalus_client_handle_t client, uint64_t run);",
    "daedalus_client_status_t daedalus_client_stop(daedalus_client_handle_t client, uint64_t run, uint32_t mode, daedalus_client_stopped_t *out);",
    "daedalus_client_status_t daedalus_client_reload(daedalus_client_handle_t client, uint64_t run, const uint8_t *graph, size_t graph_len, daedalus_client_carry_t *out);",
    "daedalus_client_status_t daedalus_client_inspect_run(daedalus_client_handle_t client, uint64_t run, uint8_t *buf, size_t cap, size_t *len);",
    "daedalus_client_status_t daedalus_client_inspect_graph(daedalus_client_handle_t client, const uint8_t *name, size_t name_len, uint8_t *buf, size_t cap, size_t *len);",
    "daedalus_client_status_t daedalus_client_inspect_cache(daedalus_client_handle_t client, uint8_t *buf, size_t cap, size_t *len);",
    "daedalus_client_status_t daedalus_client_inspect_telemetry(daedalus_client_handle_t client, uint64_t run, uint8_t *buf, size_t cap, size_t *len);",
    "daedalus_client_status_t daedalus_client_inspect_host(daedalus_client_handle_t client, uint8_t *buf, size_t cap, size_t *len);",
    "daedalus_client_status_t daedalus_client_cache_clear(daedalus_client_handle_t client);",
    "daedalus_client_status_t daedalus_client_trace(daedalus_client_handle_t client, uint32_t limit, uint8_t *buf, size_t cap, size_t *len);",
    "daedalus_client_status_t daedalus_client_feed_i64(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, int64_t value, daedalus_client_fed_t *out);",
    "daedalus_client_status_t daedalus_client_feed_f64(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, double value, daedalus_client_fed_t *out);",
    "daedalus_client_status_t daedalus_client_feed_bool(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, uint8_t value, daedalus_client_fed_t *out);",
    "daedalus_client_status_t daedalus_client_feed_bytes(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, const uint8_t *bytes, size_t len, daedalus_client_fed_t *out);",
    "daedalus_client_status_t daedalus_client_feed_str(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, const uint8_t *text, size_t len, daedalus_client_fed_t *out);",
    "daedalus_client_status_t daedalus_client_feed_wire(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, const uint8_t *bytes, size_t len, daedalus_client_fed_t *out);",
    "daedalus_client_status_t daedalus_client_take_i64(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, int64_t *out, uint8_t *present);",
    "daedalus_client_status_t daedalus_client_take_f64(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, double *out, uint8_t *present);",
    "daedalus_client_status_t daedalus_client_take_bool(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, uint8_t *out, uint8_t *present);",
    "daedalus_client_status_t daedalus_client_take_bytes(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, uint8_t *buf, size_t cap, size_t *len, uint8_t *present);",
    "daedalus_client_status_t daedalus_client_take_wire(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, uint8_t *buf, size_t cap, size_t *len, uint8_t *present);",
    "daedalus_client_status_t daedalus_client_watch_open(daedalus_client_handle_t client, uint64_t run, uint32_t kinds, daedalus_client_handle_t *out);",
    "daedalus_client_status_t daedalus_client_watch_next(daedalus_client_handle_t watch, uint32_t timeout_ms, uint8_t *buf, size_t cap, size_t *len, uint32_t *state);",
    "daedalus_client_status_t daedalus_client_watch_close(daedalus_client_handle_t watch);",
];

#[cfg(unix)]
/// The C size of each struct, as the Rust type has it.
fn sizes() -> Vec<(&'static str, usize)> {
    vec![
        ("daedalus_client_text_t", size_of::<Text>()),
        ("daedalus_client_version_t", size_of::<Version>()),
        ("daedalus_client_config_t", size_of::<Config>()),
        ("daedalus_client_plan_options_t", size_of::<PlanOptions>()),
        ("daedalus_client_step_t", size_of::<Step>()),
        ("daedalus_client_stopped_t", size_of::<Stopped>()),
        ("daedalus_client_fed_t", size_of::<Fed>()),
        ("daedalus_client_carry_t", size_of::<Carry>()),
    ]
}

fn header() -> String {
    std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("include/daedalus_client.h"))
        .expect("the header")
}

/// Every line of the header that declares a function.
fn declared(header: &str) -> Vec<&str> {
    header
        .lines()
        .filter(|line| {
            line.ends_with(");")
                && (line.contains(" daedalus_client_") || line.contains("*daedalus_client_"))
        })
        .filter(|line| !line.starts_with("typedef") && !line.starts_with(' '))
        .collect()
}

/// The C compiler, or `None` on a machine with none.
#[cfg(unix)]
fn cc() -> Option<&'static str> {
    ["cc", "gcc", "clang"].into_iter().find(|cc| {
        Command::new(cc)
            .arg("--version")
            .output()
            .is_ok_and(|out| out.status.success())
    })
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
fn the_baseline_is_still_declared_and_still_exported() {
    let header = header();
    let declared = declared(&header);
    let table = table();
    for prototype in BASELINE {
        assert!(
            declared.contains(prototype),
            "the header lost or changed `{prototype}`"
        );
        let name = table
            .iter()
            .find(|(_, have)| have == prototype)
            .map(|(name, _)| *name)
            .unwrap_or_else(|| panic!("the library has no function for `{prototype}`"));
        assert!(name.starts_with("daedalus_client_"));
    }
    #[cfg(target_os = "linux")]
    {
        let exported = exported_symbols();
        for prototype in BASELINE {
            let name = prototype
                .split('(')
                .next()
                .and_then(|head| head.split_whitespace().last())
                .expect("a name")
                .trim_start_matches('*');
            assert!(
                exported.contains(name),
                "the library no longer exports `{name}`"
            );
        }
    }
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
            line.starts_with("#define DAEDALUS_CLIENT_")
                && !line.ends_with("_H")
                && !CONSTANTS
                    .iter()
                    .any(|(name, _)| line.starts_with(&format!("#define {name} ")))
        })
        .count();
    assert_eq!(
        defined,
        Status::ALL.len(),
        "a status the library doesn't have"
    );
}

#[test]
fn the_header_gives_every_named_number_its_value() {
    let header = header();
    for (name, value) in CONSTANTS {
        let line = format!("#define {name} {value}");
        assert!(
            header.lines().any(|have| have == line),
            "the header lacks `{line}`"
        );
    }
}

#[test]
fn the_statuses_this_library_shares_with_core_capi_keep_its_numbers() {
    // The statuses both libraries have, by the names the two headers give them. Matched by
    // name, the two headers can't disagree: no other status of this library has a core number.
    const SHARED: &[(&str, &str)] = &[
        ("DAEDALUS_CLIENT_OK", "DAEDALUS_OK"),
        ("DAEDALUS_CLIENT_NULL_POINTER", "DAEDALUS_NULL_POINTER"),
        ("DAEDALUS_CLIENT_BAD_HANDLE", "DAEDALUS_BAD_HANDLE"),
        ("DAEDALUS_CLIENT_STALE_HANDLE", "DAEDALUS_STALE_HANDLE"),
        ("DAEDALUS_CLIENT_BAD_TEXT", "DAEDALUS_BAD_TEXT"),
        (
            "DAEDALUS_CLIENT_BUFFER_TOO_SMALL",
            "DAEDALUS_BUFFER_TOO_SMALL",
        ),
        ("DAEDALUS_CLIENT_PANIC", "DAEDALUS_PANIC"),
    ];
    let core = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../core/capi/include/daedalus.h"),
    )
    .expect("core/capi's header");
    for (ours, theirs) in SHARED {
        let number = |name: &str| -> i32 {
            let line = core
                .lines()
                .find(|line| line.starts_with(&format!("#define {name} ")))
                .unwrap_or_else(|| panic!("core/capi lacks {name}"));
            line.rsplit(' ')
                .next()
                .expect("a number")
                .parse()
                .expect("a number")
        };
        let ours_number = Status::ALL
            .iter()
            .find(|(_, name)| name == ours)
            .map(|(status, _)| status.0)
            .unwrap_or_else(|| panic!("this library lacks {ours}"));
        assert_eq!(ours_number, number(theirs), "{ours} and {theirs} differ");
    }
    // Every other status of this library is above the numbers core/capi uses.
    for (status, name) in Status::ALL {
        if !SHARED.iter().any(|(ours, _)| ours == name) {
            assert!(status.0 >= 18, "{name} takes a number core/capi uses");
        }
    }
}

#[cfg(unix)]
#[test]
fn the_header_compiles_as_c11_beside_core_capi_with_every_warning_an_error() {
    let cc = cc().expect("a C compiler: this check needs one");
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("client-header");
    std::fs::create_dir_all(&dir).expect("a directory");
    let source = dir.join("both.c");
    let mut text = String::from(
        "#include \"daedalus.h\"\n#include \"daedalus_client.h\"\n#include \"daedalus_client.h\"\n",
    );
    for (name, size) in sizes() {
        text.push_str(&format!(
            "_Static_assert(sizeof({name}) == {size}, \"{name}\");\n"
        ));
    }
    text.push_str("int main(void) { return 0; }\n");
    std::fs::write(&source, text).expect("the source");
    let client_include = Path::new(env!("CARGO_MANIFEST_DIR")).join("include");
    let core_include = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../core/capi/include");
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
        .arg(&client_include)
        .arg("-I")
        .arg(&core_include)
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
fn exported_symbols() -> std::collections::BTreeSet<String> {
    let library: PathBuf = support::surface_library();
    let output = Command::new("nm")
        .args(["-D", "--defined-only"])
        .arg(&library)
        .output()
        .expect("nm runs: this check needs binutils");
    assert!(output.status.success());
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| line.split_whitespace().last())
        .filter(|symbol| symbol.starts_with("daedalus_client_"))
        .map(str::to_owned)
        .collect()
}

#[cfg(target_os = "linux")]
#[test]
fn the_library_exports_exactly_the_table() {
    let exported = exported_symbols();
    let wanted: std::collections::BTreeSet<String> =
        table().iter().map(|(name, _)| (*name).to_owned()).collect();
    assert_eq!(exported, wanted);
}
