"""The ctypes plumbing shared by `daedalus.client` and `daedalus.host`: finding a library, its
statuses and last error, the size-query convention, and the exceptions that carry a status.

Nothing here is public API except through `client` and `host`. The library is never built or
fetched by this package: cargo builds it, and the caller gives its path.
"""

import ctypes
import os
from ctypes import POINTER, byref, c_char_p, c_int32, c_size_t, c_uint8, c_void_p

# The statuses the two libraries share keep their numbers (the client header says so).
STATUS_OK = 0
STATUS_NULL_POINTER = 1
STATUS_BAD_HANDLE = 2
STATUS_STALE_HANDLE = 3
STATUS_BUFFER_TOO_SMALL = 6

# How many times a size-query call may grow its buffer before the caller gets an error. A text
# that keeps growing between two calls is the only reason to need a second try.
_MAX_GROWS = 16
# The first buffer a text call gets: a reply that fits it is a single request to the daemon.
_FIRST_CAP = 64 * 1024


class DaedalusError(Exception):
    """A call the library refused or failed. `status` is the library's number for it and
    `status_name` the name the library gives that number. `message` is the library's own text for
    the last error on this thread. `code` is the service's error code name when the daemon
    refused the call (for example `unknown_run`), and None otherwise."""

    def __init__(self, status, status_name, message, code=None):
        text = f"{status_name}: {message}" if message else status_name
        if code is not None:
            text = f"{text} (service error {code})"
        super().__init__(text)
        self.status = status
        self.status_name = status_name
        self.message = message
        self.code = code


class ServiceError(DaedalusError):
    """The daemon refused the call. `code` is its error code name, such as `unknown_run`, and
    `code` is `other` for a code this SDK does not know."""


class LibraryError(Exception):
    """The library cannot be found, loaded or used: not a call's refusal."""


def load_library(path, env_var, build_hint):
    """Loads the shared library at `path`, or at the path in `env_var` when `path` is None."""
    if path is None:
        path = os.environ.get(env_var)
    if not path:
        raise LibraryError(
            f"no library: give its path, or set {env_var} to it. {build_hint}"
        )
    try:
        return ctypes.CDLL(os.fspath(path))
    except OSError as error:
        raise LibraryError(f"cannot load {path}: {error}") from error


def text_bytes(value, what):
    """UTF-8 bytes of a text argument. A str is encoded; bytes are taken as they are."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    raise TypeError(f"{what} must be a str, not {type(value).__name__}")


def declare(library, name, restype, *argtypes):
    """Looks a function up by name and declares its prototype. The function's arguments are
    checked by ctypes against these types, so a wrong call fails before the library sees it."""
    function = getattr(library, name)
    function.restype = restype
    function.argtypes = list(argtypes)
    return function


class Native:
    """One loaded library, with its status and last-error calls. `prefix` names its functions."""

    def __init__(self, library, prefix):
        self.library = library
        self.prefix = prefix
        self.status_name = declare(library, f"{prefix}_status_name", c_char_p, c_int32)
        self._last_error = declare(
            library, f"{prefix}_last_error", c_int32, c_void_p, c_size_t, POINTER(c_size_t)
        )

    def last_message(self):
        """The text of the last error on this thread."""
        try:
            return self.sized(self._last_error, first=1024).decode("utf-8", "replace")
        except DaedalusError:
            return ""

    def name_of(self, status):
        name = self.status_name(status)
        return name.decode("ascii") if name else f"status {status}"

    def failure(self, status, code=None):
        """The exception for a status that is not OK: the name, the message, and the service code
        when the daemon refused the call."""
        cls = ServiceError if code is not None else DaedalusError
        return cls(status, self.name_of(status), self.last_message(), code)

    def sized(self, function, *args, failure=None, first=_FIRST_CAP):
        """Calls a function that writes text into (buffer, capacity, &length) and returns its text.

        A buffer too small is answered with the size the text needs, and the call is repeated with
        a buffer that large. The first buffer is `first` bytes, so a reply that fits is one call: a
        call repeated is a second request to the daemon, which a trace would show. `args` come before
        the buffer; `failure` builds the exception for a status that is not OK."""
        cap = first
        for _ in range(_MAX_GROWS):
            buffer = (c_uint8 * cap)()
            length = c_size_t(0)
            status = function(*args, buffer, cap, byref(length))
            if status == STATUS_OK:
                return bytes(buffer[: length.value])
            if status != STATUS_BUFFER_TOO_SMALL:
                raise (failure or self.failure)(status)
            cap = length.value
        raise DaedalusError(STATUS_BUFFER_TOO_SMALL, "buffer_too_small", "the text kept growing")


def check(native, status):
    """Raises the exception for a status that is not OK, and returns nothing otherwise."""
    if status != STATUS_OK:
        raise native.failure(status)


def u32_range(value, what):
    """A whole number that fits a u32."""
    return _in_range(value, what, 0, 2**32 - 1)


def u64_range(value, what):
    """A whole number that fits a u64."""
    return _in_range(value, what, 0, 2**64 - 1)


def _in_range(value, what, low, high):
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{what} must be an int, not {type(value).__name__}")
    if not low <= value <= high:
        raise ValueError(f"{what} must be from {low} to {high}, not {value}")
    return value


__all__ = [
    "DaedalusError",
    "LibraryError",
    "ServiceError",
    "STATUS_BUFFER_TOO_SMALL",
    "STATUS_OK",
    "STATUS_STALE_HANDLE",
    "Native",
    "check",
    "declare",
    "load_library",
    "text_bytes",
    "u32_range",
    "u64_range",
]
