//! Shared by the surface's tests: the plugin fixture, built once; an in-process service on
//! loopback TCP with that fixture loaded; and the surface library the C caller links.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, OnceLock};

use daedalus_backend::Backend;
use daedalus_host::listen::{ListenOptions, Listener, PluginLoader, ServiceHost, WireCodecs};
use daedalus_loader::{Loader, PluginBackend};
use daedalus_registry::{RegError, Registry};
use daedalus_wire::service::PluginInfo;

/// The fixture plugin, built by cargo into the test target directory. Its node `fixture.pass`
/// forwards a `u32`, `fixture.pass_bytes` forwards bytes, and `fixture.boom` fails a step.
pub fn fixture_library() -> PathBuf {
    static BUILT: OnceLock<PathBuf> = OnceLock::new();
    BUILT
        .get_or_init(|| {
            let source = Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../testing/fixtures/dylib/Cargo.toml");
            let target = Path::new(env!("CARGO_TARGET_TMPDIR")).join("dylib-fixture");
            let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".to_owned());
            let status = Command::new(cargo)
                .args(["build", "-q", "--manifest-path"])
                .arg(source)
                .arg("--target-dir")
                .arg(&target)
                .status()
                .expect("cargo runs");
            assert!(status.success(), "the dylib fixture builds");
            target.join("debug").join(format!(
                "{}daedalus_dylib_fixture{}",
                std::env::consts::DLL_PREFIX,
                std::env::consts::DLL_SUFFIX
            ))
        })
        .clone()
}

/// The surface as a shared library, which cargo builds beside the tests.
pub fn surface_library() -> PathBuf {
    let exe = std::env::current_exe().expect("the test's own path");
    let deps = exe.parent().expect("the deps directory");
    let found = deps.join(format!(
        "{}daedalus_client_capi{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_SUFFIX
    ));
    assert!(
        found.exists(),
        "{} isn't there: the crate builds as a cdylib",
        found.display()
    );
    found
}

/// Loads plugins the way the daemon does: their nodes join the registry, and their backends run
/// them.
struct Plugins;

impl PluginLoader for Plugins {
    fn load(
        &mut self,
        path: &str,
        registry: &mut Registry,
        backends: &mut Vec<Arc<dyn Backend>>,
    ) -> Result<PluginInfo, String> {
        let plugin = Loader::new()
            .open(path)
            .map_err(|error| error.to_string())?;
        for decl in plugin.nodes() {
            match registry.register_owned(decl.clone()) {
                Ok(()) | Err(RegError::DuplicateBody { .. }) => {}
                Err(error) => return Err(format!("{path}: node `{}`: {error}", decl.id)),
            }
        }
        let plugin = Arc::new(plugin);
        let info = PluginInfo {
            name: plugin.name().to_owned(),
            generation: u32::try_from(plugin.generation().number()).unwrap_or(u32::MAX),
            nodes: plugin.nodes().iter().map(|node| node.id.clone()).collect(),
            backends: plugin
                .backends()
                .iter()
                .map(|backend| backend.id().to_owned())
                .collect(),
        };
        for index in 0..plugin.backends().len() {
            let wrapped: Arc<dyn Backend> =
                Arc::new(PluginBackend::new(Arc::clone(&plugin), index));
            backends.retain(|known| known.id() != wrapped.id());
            backends.push(wrapped);
        }
        Ok(info)
    }
}

/// A daemon in this process: a host that loads plugins, listening on loopback TCP. The listener
/// is returned too, since dropping it stops the accept loop.
pub struct Daemon {
    pub host: Arc<ServiceHost>,
    pub listener: Listener,
    pub port: u16,
}

pub fn daemon() -> Daemon {
    let host = ServiceHost::builder(Registry::new())
        .plugins(Box::new(Plugins))
        .codecs(WireCodecs::byte_form())
        .options(ListenOptions::default())
        .build();
    let (listener, addr) = host
        .listen_tcp("127.0.0.1:0".parse().expect("an address"))
        .expect("listen");
    Daemon {
        host,
        listener,
        port: addr.port(),
    }
}

/// The endpoint text for a loopback port.
pub fn tcp(port: u16) -> String {
    format!("tcp:127.0.0.1:{port}")
}

/// A graph: a host bridge with `in` and `out` as u32 ports, and `node` between them.
pub fn graph_json(node: &str) -> String {
    graph_json_as(node, "u32")
}

/// As [`graph_json`], with the host's ports of the type `key`.
pub fn graph_json_as(node: &str, key: &str) -> String {
    format!(
        r#"{{
  "format": "daedalus.graph",
  "schema_version": 1,
  "graph": {{
    "nodes": [
      {{"id": "host", "kind": "host_bridge", "port_types": {{"in": "{key}", "out": "{key}"}}}},
      {{"id": "{node}"}}
    ],
    "edges": [
      {{"from": {{"node": 0, "port": "in"}}, "to": {{"node": 1, "port": "in"}}}},
      {{"from": {{"node": 1, "port": "out"}}, "to": {{"node": 0, "port": "out"}}}}
    ]
  }}
}}"#
    )
}

// The surface, called from Rust through its own functions. Each helper makes one kind of call, so
// the unsafe calls are few and each says what it promises.

use daedalus_client_capi::*;

/// The status of a call, as the surface gives it.
pub type S = Status;

/// Connects to the daemon at `port` over loopback TCP.
pub fn connect(port: u16) -> Handle {
    let endpoint = tcp(port);
    let mut handle = 0;
    // SAFETY: the endpoint is `len` bytes of UTF-8, and `handle` is a handle to write.
    let status =
        unsafe { daedalus_client_connect(endpoint.as_ptr(), endpoint.len(), 0, &mut handle) };
    assert_eq!(status, Status::OK, "{}", last_error());
    handle
}

/// The text a call gives into a buffer: asked for its size, then read. Text that grew in between
/// asks for its size again, so the caller goes round until it fits.
pub fn text(call: impl Fn(*mut u8, usize, *mut usize) -> Status) -> String {
    for _ in 0..16 {
        let mut len = 0;
        let sized = call(std::ptr::null_mut(), 0, &mut len);
        if sized == Status::OK {
            return String::new();
        }
        assert_eq!(sized, Status::BUFFER_TOO_SMALL, "{}", last_error());
        let mut buf = vec![0; len];
        match call(buf.as_mut_ptr(), buf.len(), &mut len) {
            Status::OK => {
                // A call that answers afresh may answer shorter than the size it gave.
                buf.truncate(len);
                return String::from_utf8(buf).expect("UTF-8");
            }
            Status::BUFFER_TOO_SMALL => continue,
            other => panic!("{other:?}: {}", last_error()),
        }
    }
    panic!("the text kept growing");
}

/// The JSON a call gives into a buffer, read back.
pub fn json(call: impl Fn(*mut u8, usize, *mut usize) -> Status) -> serde_json::Value {
    serde_json::from_str(&text(call)).expect("JSON")
}

/// Why the last failed call on this thread failed.
pub fn last_error() -> String {
    text(|buf, cap, len| {
        // SAFETY: the buffer is null or `cap` bytes, and `len` is a `usize`: the caller's contract.
        unsafe { daedalus_client_last_error(buf, cap, len) }
    })
}

/// The server's error code for the last failed call on this thread.
pub fn last_error_code() -> u32 {
    let mut code = u32::MAX;
    // SAFETY: `code` is a `uint32_t` to write.
    assert_eq!(
        unsafe { daedalus_client_last_error_code(&mut code) },
        Status::OK
    );
    code
}

/// A default start: basic telemetry, the daemon's order of backends.
pub fn config() -> Config {
    Config {
        fifo_capacity: 0,
        fault_reports: 0,
        telemetry: 1,
        unmerged: 0,
        priority: std::ptr::null(),
        priority_len: 0,
    }
}

/// Loads the fixture plugin into the daemon.
pub fn load_fixture(client: Handle) {
    let path = fixture_library();
    let path = path.to_str().expect("a UTF-8 path");
    // SAFETY: the path is `len` bytes of UTF-8.
    let status = unsafe { daedalus_client_load_plugin(client, path.as_ptr(), path.len()) };
    assert_eq!(status, Status::OK, "{}", last_error());
}

/// Stores `json` under `name`.
pub fn put_graph(client: Handle, name: &str, json: &str) -> Status {
    // SAFETY: both texts are `len` bytes of UTF-8.
    unsafe {
        daedalus_client_put_graph(client, name.as_ptr(), name.len(), json.as_ptr(), json.len())
    }
}

/// Starts a run of the stored graph `graph`, which must be stored.
pub fn start(client: Handle, graph: &str) -> u64 {
    let mut run = 0;
    let cfg = config();
    // SAFETY: the name is `len` bytes of UTF-8, the config is a value that lives for the call, and
    // `run` is a `uint64_t` to write.
    let status =
        unsafe { daedalus_client_start(client, graph.as_ptr(), graph.len(), &cfg, &mut run) };
    assert_eq!(status, Status::OK, "{}", last_error());
    run
}

/// Feeds a `u32` as its wire encoding to `port`.
pub fn feed_u32(client: Handle, run: u64, port: &str, value: u32) -> Status {
    let bytes = encode(u32_value(value));
    // SAFETY: the port and the bytes are `len` bytes each, and the output is null.
    unsafe {
        daedalus_client_feed_wire(
            client,
            run,
            port.as_ptr(),
            port.len(),
            bytes.as_ptr(),
            bytes.len(),
            std::ptr::null_mut(),
        )
    }
}

/// A typed value of the `u32` type.
pub fn u32_value(value: u32) -> daedalus_wire::service::TypedValue {
    use daedalus_types::Scalar;
    use daedalus_wire::{WireType, WireValue};
    daedalus_wire::service::TypedValue::new(WireType::Scalar(Scalar::U32), WireValue::U32(value))
}

/// The wire encoding of a typed value, as a service message carries it.
pub fn encode(value: daedalus_wire::service::TypedValue) -> Vec<u8> {
    let mut writer = daedalus_wire::codec::Writer::new();
    value
        .encode(&mut writer, &daedalus_wire::Schema::new())
        .expect("encodes");
    writer.buf
}

/// Takes a value of any type from `port` as its wire encoding, the way a caller with a buffer
/// does: a null buffer first, which keeps the value for the retry, then a buffer of the size.
/// `None` when the port is empty.
pub fn take_wire(client: Handle, run: u64, port: &str) -> Option<Vec<u8>> {
    let mut len = 0;
    let mut present = 0;
    // SAFETY: the port is `len` bytes, the buffer is null, and `len` and `present` are written.
    let sized = unsafe {
        daedalus_client_take_wire(
            client,
            run,
            port.as_ptr(),
            port.len(),
            std::ptr::null_mut(),
            0,
            &mut len,
            &mut present,
        )
    };
    if present == 0 {
        assert_eq!(sized, Status::OK, "{}", last_error());
        return None;
    }
    assert_eq!(sized, Status::BUFFER_TOO_SMALL, "{}", last_error());
    let mut buf = vec![0; len];
    // SAFETY: the buffer holds `len` bytes, and the value is the one held for this port.
    let status = unsafe {
        daedalus_client_take_wire(
            client,
            run,
            port.as_ptr(),
            port.len(),
            buf.as_mut_ptr(),
            buf.len(),
            &mut len,
            &mut present,
        )
    };
    assert_eq!(status, Status::OK, "{}", last_error());
    Some(buf)
}

/// Takes the `u32` at `port`, or `None` when it is empty.
pub fn take_u32(client: Handle, run: u64, port: &str) -> Option<u32> {
    let bytes = take_wire(client, run, port)?;
    let mut reader = daedalus_wire::codec::Reader::new(&bytes);
    let value =
        daedalus_wire::service::TypedValue::decode(&mut reader, &daedalus_wire::Schema::new())
            .expect("decodes");
    match value.value {
        daedalus_wire::WireValue::U32(value) => Some(value),
        other => panic!("not a u32: {other:?}"),
    }
}

/// Runs `count` ticks and returns the step's summary, as the surface gives it.
pub fn step(client: Handle, run: u64, count: u32) -> Step {
    let mut out = Step::default();
    // SAFETY: `out` is a step to write, and the call is on the handle and run given.
    let status = unsafe { daedalus_client_step(client, run, count, &mut out) };
    assert_eq!(status, Status::OK, "{}", last_error());
    out
}
