//! A client handle: its connection, the values a take could not hand over yet, and the calls
//! that open and close it.

use std::net::SocketAddr;
use std::sync::Mutex;

use daedalus_client::{Client, ClientError, Endpoint, Options};
use daedalus_wire::service::TypedValue;

use crate::guard::{fail, fail_service, non_null, read_text, write_out};
use crate::handles::{Handle, Table};
use crate::status::Status;

/// A value taken from a port that the caller's buffer, or the caller's type, could not take yet.
/// It waits here for the next take of the same run and port.
pub(crate) struct Held {
    pub(crate) run: u64,
    pub(crate) port: String,
    pub(crate) value: TypedValue,
}

/// One client handle. Calls on it are made by any thread: the client itself takes care of its
/// requests, and only the held values, the trace and the report are behind a lock.
pub(crate) struct Link {
    pub(crate) client: Client,
    pub(crate) held: Mutex<Vec<Held>>,
    /// A trace that did not fit its buffer, with the limit it was asked for. The trace changes with
    /// every request the daemon serves, so the retry is given this one.
    pub(crate) trace: Mutex<Option<(u32, Vec<u8>)>>,
    /// The JSON of the last call that kept a report: a plugin, a graph's diagnostics, or a carry
    /// report. `daedalus_client_report` gives it out, as many times as it is asked.
    pub(crate) report: Mutex<Option<Vec<u8>>>,
}

pub(crate) static LINKS: Table<Link> = Table::new(1);

/// The link a handle names, or the status that refuses it.
pub(crate) fn link(handle: Handle) -> Result<std::sync::Arc<Link>, Status> {
    LINKS.get(handle)
}

/// Notes why the client refused a call, and gives the status for it.
pub(crate) fn refuse(error: ClientError) -> Status {
    match error {
        ClientError::Service(error) => fail_service(
            u32::from(error.code.code()),
            format!("{}: {}", error.code.name(), error.message),
        ),
        ClientError::Disconnected => fail(Status::DISCONNECTED, "disconnected: nothing was sent"),
        ClientError::UnknownDelivery => fail(
            Status::UNKNOWN_DELIVERY,
            "the connection dropped after the request was sent, before its answer",
        ),
        ClientError::Refused(error) => {
            fail(Status::REFUSED, format!("the server was refused: {error}"))
        }
        ClientError::Wire(error) => fail(Status::WIRE, error),
        ClientError::TooLarge { len, cap } => fail(
            Status::TOO_LARGE,
            format!("a request of {len} bytes is over the server's cap of {cap}"),
        ),
        ClientError::Io(error) => fail(Status::IO, error),
        ClientError::NotLoopback(addr) => fail(
            Status::BAD_ENDPOINT,
            format!("{addr} is not a loopback address"),
        ),
        ClientError::BadSession(len) => fail(
            Status::BAD_SESSION,
            format!("a session name of {len} bytes is empty or too long"),
        ),
        ClientError::Unexpected { op, reply } => fail(
            Status::UNEXPECTED,
            format!("{op} was answered with {reply}"),
        ),
    }
}

/// `unix:/path` or `tcp:address`. A TCP address must be on the loopback interface.
fn endpoint(text: &str) -> Result<Endpoint, Status> {
    if let Some(addr) = text.strip_prefix("tcp:") {
        let addr: SocketAddr = addr.parse().map_err(|error| {
            fail(
                Status::BAD_ENDPOINT,
                format!("`{addr}` is not an address: {error}"),
            )
        })?;
        return Endpoint::tcp(addr).map_err(refuse);
    }
    if let Some(path) = text.strip_prefix("unix:") {
        return unix_endpoint(path).ok_or_else(|| {
            fail(
                Status::UNSUPPORTED,
                "unix sockets are not available on this platform: use tcp:127.0.0.1:PORT",
            )
        });
    }
    Err(fail(
        Status::BAD_ENDPOINT,
        "an endpoint is `unix:/path` or `tcp:address`",
    ))
}

#[cfg(unix)]
fn unix_endpoint(path: &str) -> Option<Endpoint> {
    Some(Endpoint::unix(path))
}

#[cfg(not(unix))]
fn unix_endpoint(_path: &str) -> Option<Endpoint> {
    None
}

/// Connects to the server at `endpoint` and writes the client's handle to `out`. `reconnect`
/// non-zero keeps trying after a disconnect, as the client does for a long-lived caller.
///
/// # Safety
/// `endpoint` is null only when `len` is zero, else points to `len` bytes of UTF-8. `out` points
/// to a `daedalus_client_handle_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_connect(
    endpoint_text: *const u8,
    len: usize,
    reconnect: u8,
    out: *mut Handle,
) -> Status {
    crate::guard::guard(|| {
        non_null(out, "the output handle")?;
        // SAFETY: the caller's contract is this function's.
        let text = unsafe { read_text(endpoint_text, len) }?;
        let endpoint = endpoint(text)?;
        let options = match reconnect {
            0 => Options::default(),
            _ => Options::reconnecting(),
        };
        let client = Client::connect(endpoint, options).map_err(refuse)?;
        let handle = LINKS.insert(Link {
            client,
            held: Mutex::new(Vec::new()),
            trace: Mutex::new(None),
            report: Mutex::new(None),
        });
        // SAFETY: the caller's contract is this function's, and `out` was checked above.
        unsafe { write_out(out, handle) }
    })
}

/// Closes the connection for every handle made from it, sessions included, and releases this
/// handle. Watches of the connection end. Calls on the other handles then fail with
/// `DAEDALUS_CLIENT_DISCONNECTED`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub extern "C" fn daedalus_client_close(client: Handle) -> Status {
    crate::guard::guard(|| {
        let link = LINKS.remove(client)?;
        link.client.close();
        Ok(())
    })
}

/// A handle for the same connection, whose requests are in the session `name`. Closing either
/// closes the connection for both.
///
/// # Safety
/// As [`daedalus_client_connect`] for `name`, and `out` points to a `daedalus_client_handle_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_in_session(
    client: Handle,
    name: *const u8,
    len: usize,
    out: *mut Handle,
) -> Status {
    crate::guard::guard(|| {
        non_null(out, "the output handle")?;
        let link = LINKS.get(client)?;
        // SAFETY: the caller's contract is this function's.
        let name = unsafe { read_text(name, len) }?;
        let client = link.client.in_session(name).map_err(refuse)?;
        let handle = LINKS.insert(Link {
            client,
            held: Mutex::new(Vec::new()),
            trace: Mutex::new(None),
            report: Mutex::new(None),
        });
        // SAFETY: the caller's contract is this function's, and `out` was checked above.
        unsafe { write_out(out, handle) }
    })
}

/// Writes the server's protocol version to `version`.
///
/// # Safety
/// `version` points to a `uint32_t`.
// SAFETY: the name is this library's own, with its prefix: no other symbol has it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn daedalus_client_ping(client: Handle, version: *mut u32) -> Status {
    crate::guard::guard(|| {
        non_null(version, "the output version")?;
        let link = LINKS.get(client)?;
        let found = link.client.ping().map_err(refuse)?;
        // SAFETY: the caller's contract is this function's, and `version` was checked above.
        unsafe { write_out(version, found) }
    })
}

#[cfg(test)]
mod tests {
    use super::endpoint;
    use crate::status::Status;

    #[test]
    fn an_endpoint_is_unix_or_loopback_tcp() {
        for text in ["", "tcp:8.8.8.8:1", "tcp:nonsense", "http://x"] {
            let error = endpoint(text).expect_err("refused");
            assert!(error == Status::BAD_ENDPOINT, "{text:?} gave {error:?}");
        }
        assert!(endpoint("tcp:127.0.0.1:4100").is_ok());
    }

    /// A `unix:` endpoint is well formed. Where the platform has unix sockets it is accepted (the
    /// connect then decides); where it has none it is unsupported, not a bad endpoint.
    #[cfg(unix)]
    #[test]
    fn a_unix_endpoint_is_accepted_where_the_platform_has_unix_sockets() {
        assert!(endpoint("unix:/tmp/daedalus.sock").is_ok());
    }

    #[cfg(not(unix))]
    #[test]
    fn a_unix_endpoint_is_unsupported_where_the_platform_has_no_unix_sockets() {
        let error = endpoint("unix:/tmp/daedalus.sock").expect_err("refused");
        assert_eq!(error, Status::UNSUPPORTED);
    }
}
