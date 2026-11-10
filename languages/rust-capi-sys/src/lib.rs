//! The C surface, loaded from its shared library at run time.
//!
//! This is the one crate that holds `unsafe` for a Rust program that uses the surface as a
//! library: it opens the library, looks every function up once into a typed table, and offers
//! each as a safe method that passes the pointers and lengths the C contract asks for. The
//! shapes here are the header's, written again in Rust, and a test holds them to the
//! library's own.
//!
//! A status is passed through as its number, [`Code`]. Naming them is the safe crate's job.

use std::ffi::{CStr, c_char};
use std::fmt;
use std::path::Path;

use libloading::Library;

/// A status number, as the library returns it. Zero is success.
pub type Code = i32;
/// A host or a graph.
pub type Handle = u64;

pub const OK: Code = 0;
pub const BUFFER_TOO_SMALL: Code = 6;

/// `daedalus_version_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Version {
    pub major: u32,
    pub minor: u32,
    pub layout_hash: u64,
}

/// `daedalus_port_t`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RawPort {
    pub name: *const u8,
    pub name_len: usize,
    pub type_key: *const u8,
    pub type_key_len: usize,
    pub is_input: u8,
}

/// `daedalus_telemetry_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Telemetry {
    pub ticks: u64,
    pub idle_ticks: u64,
    pub step_calls: u64,
    pub faults: u64,
    pub pushes: u64,
    pub takes: u64,
}

/// `daedalus_tick_t`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Tick {
    pub idle: u8,
    pub step_calls: u32,
    pub faults: u32,
}

/// `daedalus_bytes_t`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RawBytes {
    pub data: *const u8,
    pub len: usize,
}

/// `daedalus_push_value_t`.
#[repr(C)]
#[derive(Clone, Copy)]
pub union RawValue {
    pub i64_value: i64,
    pub f64_value: f64,
    pub bool_value: u8,
    pub bytes: RawBytes,
}

/// `daedalus_push_t`.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RawPush {
    pub port: usize,
    pub kind: u32,
    pub value: RawValue,
}

pub const KIND_I64: u32 = 0;
pub const KIND_F64: u32 = 1;
pub const KIND_BOOL: u32 = 2;
pub const KIND_BYTES: u32 = 3;

/// A host port, with its strings copied out of the graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Port {
    pub name: String,
    pub type_key: String,
    pub is_input: bool,
}

/// One value of a batch.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Pushed<'a> {
    I64(i64),
    F64(f64),
    Bool(bool),
    Bytes(&'a [u8]),
}

/// Why the library couldn't be loaded.
#[derive(Debug)]
pub enum LoadError {
    /// The file couldn't be opened as a library.
    Open(String),
    /// The library lacks a function the surface has.
    Missing(&'static str),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadError::Open(why) => write!(f, "the library couldn't be opened: {why}"),
            LoadError::Missing(name) => write!(f, "the library has no `{name}`"),
        }
    }
}

impl std::error::Error for LoadError {}

type Name = *const u8;

/// Declares the function table: each field, the symbol it is, and its C type.
macro_rules! table {
    ($($field:ident = $symbol:literal: $ty:ty;)*) => {
        struct Table {
            $($field: $ty,)*
        }

        impl Table {
            /// # Safety
            /// `library` is the Daedalus C surface: each symbol has the type given here.
            unsafe fn load(library: &Library) -> Result<Table, LoadError> {
                Ok(Table {
                    $($field: {
                        // SAFETY: the caller says the symbol, when present, has this type.
                        let found = unsafe { library.get::<$ty>(concat!($symbol, "\0").as_bytes()) };
                        *found.map_err(|_| LoadError::Missing($symbol))?
                    },)*
                })
            }
        }

        /// Every symbol the surface has, for a check that none is missed.
        pub const SYMBOLS: &[&str] = &[$($symbol,)*];
    };
}

table! {
    version = "daedalus_version": unsafe extern "C" fn(*mut Version) -> Code;
    status_name = "daedalus_status_name": unsafe extern "C" fn(Code) -> *const c_char;
    last_error = "daedalus_last_error": unsafe extern "C" fn(*mut u8, usize, *mut usize) -> Code;
    selftest_panic = "daedalus_selftest_panic": unsafe extern "C" fn() -> Code;
    host_create = "daedalus_host_create": unsafe extern "C" fn(*mut Handle) -> Code;
    host_release = "daedalus_host_release": unsafe extern "C" fn(Handle) -> Code;
    host_install = "daedalus_host_install": unsafe extern "C" fn(Handle, Name, usize) -> Code;
    host_schemas = "daedalus_host_schemas": unsafe extern "C" fn(Handle, *mut u8, usize, *mut usize) -> Code;
    graph_compile = "daedalus_graph_compile": unsafe extern "C" fn(Handle, Name, usize, *mut Handle) -> Code;
    graph_release = "daedalus_graph_release": unsafe extern "C" fn(Handle) -> Code;
    graph_port_count = "daedalus_graph_port_count": unsafe extern "C" fn(Handle, *mut usize) -> Code;
    graph_port = "daedalus_graph_port": unsafe extern "C" fn(Handle, usize, *mut RawPort) -> Code;
    graph_port_index = "daedalus_graph_port_index": unsafe extern "C" fn(Handle, Name, usize, *mut usize) -> Code;
    graph_explain = "daedalus_graph_explain": unsafe extern "C" fn(Handle, *mut u8, usize, *mut usize) -> Code;
    graph_telemetry = "daedalus_graph_telemetry": unsafe extern "C" fn(Handle, *mut Telemetry) -> Code;
    tick = "daedalus_tick": unsafe extern "C" fn(Handle, *mut Tick) -> Code;
    push_i64 = "daedalus_push_i64": unsafe extern "C" fn(Handle, Name, usize, i64) -> Code;
    push_i64_at = "daedalus_push_i64_at": unsafe extern "C" fn(Handle, usize, i64) -> Code;
    push_f64 = "daedalus_push_f64": unsafe extern "C" fn(Handle, Name, usize, f64) -> Code;
    push_f64_at = "daedalus_push_f64_at": unsafe extern "C" fn(Handle, usize, f64) -> Code;
    push_bool = "daedalus_push_bool": unsafe extern "C" fn(Handle, Name, usize, u8) -> Code;
    push_bool_at = "daedalus_push_bool_at": unsafe extern "C" fn(Handle, usize, u8) -> Code;
    push_bytes = "daedalus_push_bytes": unsafe extern "C" fn(Handle, Name, usize, *const u8, usize) -> Code;
    push_bytes_at = "daedalus_push_bytes_at": unsafe extern "C" fn(Handle, usize, *const u8, usize) -> Code;
    push_batch = "daedalus_push_batch": unsafe extern "C" fn(Handle, *const RawPush, usize, *mut usize) -> Code;
    take_i64 = "daedalus_take_i64": unsafe extern "C" fn(Handle, Name, usize, *mut i64, *mut u8) -> Code;
    take_i64_at = "daedalus_take_i64_at": unsafe extern "C" fn(Handle, usize, *mut i64, *mut u8) -> Code;
    take_f64 = "daedalus_take_f64": unsafe extern "C" fn(Handle, Name, usize, *mut f64, *mut u8) -> Code;
    take_f64_at = "daedalus_take_f64_at": unsafe extern "C" fn(Handle, usize, *mut f64, *mut u8) -> Code;
    take_bool = "daedalus_take_bool": unsafe extern "C" fn(Handle, Name, usize, *mut u8, *mut u8) -> Code;
    take_bool_at = "daedalus_take_bool_at": unsafe extern "C" fn(Handle, usize, *mut u8, *mut u8) -> Code;
    take_bytes = "daedalus_take_bytes": unsafe extern "C" fn(Handle, Name, usize, *mut u8, usize, *mut usize, *mut u8) -> Code;
    take_bytes_at = "daedalus_take_bytes_at": unsafe extern "C" fn(Handle, usize, *mut u8, usize, *mut usize, *mut u8) -> Code;
}

/// A port, as a caller names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum At<'a> {
    Name(&'a str),
    Index(usize),
}

/// The loaded surface. Every method is one call of the library, made as the header says.
pub struct Api {
    table: Table,
    /// Declared after the table, so the functions are dropped before the library closes.
    _library: Library,
}

fn done(code: Code) -> Result<(), Code> {
    match code {
        OK => Ok(()),
        code => Err(code),
    }
}

impl Api {
    /// Opens the shared library at `path` and looks up every function of the surface.
    pub fn load(path: impl AsRef<Path>) -> Result<Api, LoadError> {
        // SAFETY: opening a library runs its initialisers. The caller names the Daedalus
        // surface, which has none of its own beyond Rust's runtime.
        let library = unsafe { Library::new(path.as_ref()) }
            .map_err(|error| LoadError::Open(error.to_string()))?;
        // SAFETY: the library is the surface, whose symbols have the types the table gives.
        // A library that isn't lacks one of the 33 names and is refused.
        let table = unsafe { Table::load(&library)? };
        Ok(Api {
            table,
            _library: library,
        })
    }

    /// Text from a call that fills a caller's buffer: asked for its size, then read.
    fn text(&self, call: impl Fn(*mut u8, usize, *mut usize) -> Code) -> Result<String, Code> {
        let mut len = 0;
        match call(std::ptr::null_mut(), 0, &mut len) {
            OK | BUFFER_TOO_SMALL => {}
            code => return Err(code),
        }
        let mut buf = vec![0; len];
        if len > 0 {
            done(call(buf.as_mut_ptr(), buf.len(), &mut len))?;
            buf.truncate(len);
        }
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }

    pub fn version(&self) -> Result<Version, Code> {
        let mut version = Version::default();
        // SAFETY: `version` is a version to write.
        done(unsafe { (self.table.version)(&mut version) })?;
        Ok(version)
    }

    /// The library's name for a status.
    pub fn status_name(&self, code: Code) -> String {
        // SAFETY: the call takes any number and returns a NUL-terminated string that lives as
        // long as the library, which `self` keeps open.
        let name = unsafe { CStr::from_ptr((self.table.status_name)(code)) };
        name.to_string_lossy().into_owned()
    }

    /// Why the last failed call on this thread failed.
    pub fn last_error(&self) -> String {
        // SAFETY: `text` gives a null buffer or one of the size it says, and a length to write.
        self.text(|buf, cap, len| unsafe { (self.table.last_error)(buf, cap, len) })
            .unwrap_or_default()
    }

    /// Makes the library panic inside a call, which comes back as a status.
    pub fn selftest_panic(&self) -> Code {
        // SAFETY: the call takes nothing.
        unsafe { (self.table.selftest_panic)() }
    }

    pub fn host_create(&self) -> Result<Handle, Code> {
        let mut host = 0;
        // SAFETY: `host` is a handle to write.
        done(unsafe { (self.table.host_create)(&mut host) })?;
        Ok(host)
    }

    pub fn host_release(&self, host: Handle) -> Result<(), Code> {
        // SAFETY: any number is a valid argument: one that isn't a host is a status.
        done(unsafe { (self.table.host_release)(host) })
    }

    pub fn host_install(&self, host: Handle, path: &str) -> Result<(), Code> {
        // SAFETY: the text is `len` bytes of UTF-8.
        done(unsafe { (self.table.host_install)(host, path.as_ptr(), path.len()) })
    }

    pub fn host_schemas(&self, host: Handle) -> Result<String, Code> {
        // SAFETY: `text` gives a null buffer or one of the size it says, and a length to write.
        self.text(|buf, cap, len| unsafe { (self.table.host_schemas)(host, buf, cap, len) })
    }

    pub fn graph_compile(&self, host: Handle, json: &str) -> Result<Handle, Code> {
        let mut graph = 0;
        // SAFETY: the text is `len` bytes of UTF-8, and `graph` is a handle to write.
        done(unsafe { (self.table.graph_compile)(host, json.as_ptr(), json.len(), &mut graph) })?;
        Ok(graph)
    }

    pub fn graph_release(&self, graph: Handle) -> Result<(), Code> {
        // SAFETY: any number is a valid argument: one that isn't a graph is a status.
        done(unsafe { (self.table.graph_release)(graph) })
    }

    pub fn graph_port_count(&self, graph: Handle) -> Result<usize, Code> {
        let mut count = 0;
        // SAFETY: `count` is a size to write.
        done(unsafe { (self.table.graph_port_count)(graph, &mut count) })?;
        Ok(count)
    }

    pub fn graph_port(&self, graph: Handle, index: usize) -> Result<Port, Code> {
        let mut raw = RawPort {
            name: std::ptr::null(),
            name_len: 0,
            type_key: std::ptr::null(),
            type_key_len: 0,
            is_input: 0,
        };
        // SAFETY: `raw` is a port to write.
        done(unsafe { (self.table.graph_port)(graph, index, &mut raw) })?;
        let copy = |data: *const u8, len: usize| -> String {
            if data.is_null() {
                return String::new();
            }
            // SAFETY: on success the library wrote a pointer to `len` bytes of the graph's own,
            // which lives at least until this call returns: releasing it takes another call.
            String::from_utf8_lossy(unsafe { std::slice::from_raw_parts(data, len) }).into_owned()
        };
        Ok(Port {
            name: copy(raw.name, raw.name_len),
            type_key: copy(raw.type_key, raw.type_key_len),
            is_input: raw.is_input != 0,
        })
    }

    pub fn graph_port_index(&self, graph: Handle, name: &str) -> Result<usize, Code> {
        let mut index = 0;
        // SAFETY: the name is `len` bytes of UTF-8, and `index` is a size to write.
        done(unsafe {
            (self.table.graph_port_index)(graph, name.as_ptr(), name.len(), &mut index)
        })?;
        Ok(index)
    }

    pub fn graph_explain(&self, graph: Handle) -> Result<String, Code> {
        // SAFETY: `text` gives a null buffer or one of the size it says, and a length to write.
        self.text(|buf, cap, len| unsafe { (self.table.graph_explain)(graph, buf, cap, len) })
    }

    pub fn graph_telemetry(&self, graph: Handle) -> Result<Telemetry, Code> {
        let mut telemetry = Telemetry::default();
        // SAFETY: `telemetry` is a struct to write.
        done(unsafe { (self.table.graph_telemetry)(graph, &mut telemetry) })?;
        Ok(telemetry)
    }

    pub fn tick(&self, graph: Handle) -> Result<Tick, Code> {
        let mut tick = Tick::default();
        // SAFETY: `tick` is a struct to write.
        done(unsafe { (self.table.tick)(graph, &mut tick) })?;
        Ok(tick)
    }

    pub fn push_i64(&self, graph: Handle, port: At<'_>, value: i64) -> Result<(), Code> {
        // SAFETY: a name is `len` bytes of UTF-8, and an index is any number.
        done(unsafe {
            match port {
                At::Name(name) => (self.table.push_i64)(graph, name.as_ptr(), name.len(), value),
                At::Index(index) => (self.table.push_i64_at)(graph, index, value),
            }
        })
    }

    pub fn push_f64(&self, graph: Handle, port: At<'_>, value: f64) -> Result<(), Code> {
        // SAFETY: a name is `len` bytes of UTF-8, and an index is any number.
        done(unsafe {
            match port {
                At::Name(name) => (self.table.push_f64)(graph, name.as_ptr(), name.len(), value),
                At::Index(index) => (self.table.push_f64_at)(graph, index, value),
            }
        })
    }

    pub fn push_bool(&self, graph: Handle, port: At<'_>, value: bool) -> Result<(), Code> {
        let value = u8::from(value);
        // SAFETY: a name is `len` bytes of UTF-8, and an index is any number.
        done(unsafe {
            match port {
                At::Name(name) => (self.table.push_bool)(graph, name.as_ptr(), name.len(), value),
                At::Index(index) => (self.table.push_bool_at)(graph, index, value),
            }
        })
    }

    pub fn push_bytes(&self, graph: Handle, port: At<'_>, bytes: &[u8]) -> Result<(), Code> {
        // SAFETY: a name is `len` bytes of UTF-8, and the bytes are `len` readable bytes that
        // the library copies before it returns.
        done(unsafe {
            match port {
                At::Name(name) => (self.table.push_bytes)(
                    graph,
                    name.as_ptr(),
                    name.len(),
                    bytes.as_ptr(),
                    bytes.len(),
                ),
                At::Index(index) => {
                    (self.table.push_bytes_at)(graph, index, bytes.as_ptr(), bytes.len())
                }
            }
        })
    }

    /// Pushes every value or none. On a bad entry, its position comes back with the status.
    pub fn push_batch(
        &self,
        graph: Handle,
        pushes: &[(usize, Pushed<'_>)],
    ) -> Result<(), (Code, Option<usize>)> {
        let raw: Vec<RawPush> = pushes
            .iter()
            .map(|&(port, value)| {
                let (kind, value) = match value {
                    Pushed::I64(value) => (KIND_I64, RawValue { i64_value: value }),
                    Pushed::F64(value) => (KIND_F64, RawValue { f64_value: value }),
                    Pushed::Bool(value) => (
                        KIND_BOOL,
                        RawValue {
                            bool_value: u8::from(value),
                        },
                    ),
                    Pushed::Bytes(bytes) => (
                        KIND_BYTES,
                        RawValue {
                            bytes: RawBytes {
                                data: bytes.as_ptr(),
                                len: bytes.len(),
                            },
                        },
                    ),
                };
                RawPush { port, kind, value }
            })
            .collect();
        let mut bad = usize::MAX;
        // SAFETY: `raw` is `len` entries, each with the member its kind names written, whose
        // byte pointers are the caller's slices, alive for the call. `bad` is a size to write.
        let code = unsafe { (self.table.push_batch)(graph, raw.as_ptr(), raw.len(), &mut bad) };
        done(code).map_err(|code| (code, (bad != usize::MAX).then_some(bad)))
    }

    pub fn take_i64(&self, graph: Handle, port: At<'_>) -> Result<Option<i64>, Code> {
        let (mut out, mut present) = (0, 0);
        // SAFETY: a name is `len` bytes of UTF-8, and the rest are values to write.
        done(unsafe {
            match port {
                At::Name(name) => {
                    (self.table.take_i64)(graph, name.as_ptr(), name.len(), &mut out, &mut present)
                }
                At::Index(index) => (self.table.take_i64_at)(graph, index, &mut out, &mut present),
            }
        })?;
        Ok((present != 0).then_some(out))
    }

    pub fn take_f64(&self, graph: Handle, port: At<'_>) -> Result<Option<f64>, Code> {
        let (mut out, mut present) = (0.0, 0);
        // SAFETY: a name is `len` bytes of UTF-8, and the rest are values to write.
        done(unsafe {
            match port {
                At::Name(name) => {
                    (self.table.take_f64)(graph, name.as_ptr(), name.len(), &mut out, &mut present)
                }
                At::Index(index) => (self.table.take_f64_at)(graph, index, &mut out, &mut present),
            }
        })?;
        Ok((present != 0).then_some(out))
    }

    pub fn take_bool(&self, graph: Handle, port: At<'_>) -> Result<Option<bool>, Code> {
        let (mut out, mut present) = (0, 0);
        // SAFETY: a name is `len` bytes of UTF-8, and the rest are values to write.
        done(unsafe {
            match port {
                At::Name(name) => {
                    (self.table.take_bool)(graph, name.as_ptr(), name.len(), &mut out, &mut present)
                }
                At::Index(index) => (self.table.take_bool_at)(graph, index, &mut out, &mut present),
            }
        })?;
        Ok((present != 0).then_some(out != 0))
    }

    /// Takes bytes: asked for their size, then read. The value waits in the graph between
    /// the two calls.
    pub fn take_bytes(&self, graph: Handle, port: At<'_>) -> Result<Option<Vec<u8>>, Code> {
        let call = |buf: *mut u8, cap: usize, len: &mut usize, present: &mut u8| -> Code {
            // SAFETY: a name is `len` bytes of UTF-8; the buffer is null or `cap` bytes; the
            // rest are values to write.
            unsafe {
                match port {
                    At::Name(name) => (self.table.take_bytes)(
                        graph,
                        name.as_ptr(),
                        name.len(),
                        buf,
                        cap,
                        len,
                        present,
                    ),
                    At::Index(index) => {
                        (self.table.take_bytes_at)(graph, index, buf, cap, len, present)
                    }
                }
            }
        };
        let (mut len, mut present) = (0, 0);
        match call(std::ptr::null_mut(), 0, &mut len, &mut present) {
            OK if present == 0 => return Ok(None),
            // Empty bytes fit a buffer of none.
            OK => return Ok(Some(Vec::new())),
            BUFFER_TOO_SMALL => {}
            code => return Err(code),
        }
        let mut buf = vec![0; len];
        done(call(buf.as_mut_ptr(), buf.len(), &mut len, &mut present))?;
        buf.truncate(len);
        Ok((present != 0).then_some(buf))
    }
}
