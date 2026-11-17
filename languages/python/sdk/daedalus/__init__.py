"""The Python SDK for Daedalus, in three parts.

- Write a node: `node`, `serve`, `Context`, `NodeError` and the type builders `T` (`daedalus.worker`
  and `daedalus.wire`). A worker runs the node bodies in a process of its own.
- Drive the daemon: `Client` (`daedalus.client`), over the client C library.
- Host in-process: `Host` and `Graph` (`daedalus.host`), over the host C library.

The standard library only. Nothing is installed or fetched: the two C libraries are built by cargo,
and their paths are given to `Client.connect` and `Host`, or set in `DAEDALUS_CLIENT_LIB` and
`DAEDALUS_HOST_LIB`.
"""

from . import client, host
from .client import (
    Client,
    ClosedError,
    DaedalusError,
    LibraryError,
    Run,
    ServiceError,
    StartConfig,
    Typed,
    Watch,
    WatchTimeout,
)
from .host import Graph, Host, Port, Tick, UnsupportedPort
from .wire import T, NodeError, WireError
from .worker import Context, node, serve

__all__ = [
    "Client",
    "ClosedError",
    "Context",
    "DaedalusError",
    "Graph",
    "Host",
    "LibraryError",
    "NodeError",
    "Port",
    "Run",
    "ServiceError",
    "StartConfig",
    "T",
    "Tick",
    "Typed",
    "UnsupportedPort",
    "Watch",
    "WatchTimeout",
    "WireError",
    "client",
    "host",
    "node",
    "serve",
]
