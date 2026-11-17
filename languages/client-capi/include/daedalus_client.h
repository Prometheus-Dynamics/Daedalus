/*
 * The client's C surface of Daedalus: a client to the daemon, over a unix socket or loopback TCP.
 *
 * A client handle is one connection. Each call takes the handle and sends one request, so the
 * daemon's graphs, runs and plugins are the daemon's: a host with no nodes of its own. Use
 * daedalus.h to run a graph in this process instead. The two headers can be included in one
 * program: they share no names, and the statuses they share keep the same numbers.
 *
 * Every call returns a status. Results come back through pointers the caller gives. Text is
 * given with a length and no NUL, and text a call returns uses a buffer and its size: call once
 * with a null buffer, which returns DAEDALUS_CLIENT_BUFFER_TOO_SMALL and writes the size the text
 * has, and again with a buffer that large. Text that changed in between (the trace, say, grows
 * with every request the daemon serves) can be larger the second time: the call says so again, so
 * ask until it fits. Asking again repeats a planning or reading call, which changes nothing. A
 * call that took a value out of the daemon keeps the value for the retry instead.
 *
 * Handles are numbers, not pointers. A handle that was closed, or was never one, is answered
 * with a status. Closing a client closes its connection for every handle made from it.
 *
 * Errors: after a call fails, daedalus_client_last_error gives why, as text, and
 * daedalus_client_last_error_code gives the daemon's error code when the daemon refused the call.
 * Both are kept per thread, as the last failure on that thread. A call that succeeds leaves them.
 *
 * Threads: a client handle and a watch handle may be used from any thread. Calls on one handle
 * may run at the same time from several threads, as the client's own requests do. A close made
 * while another thread calls on the same handle makes that call fail with
 * DAEDALUS_CLIENT_STALE_HANDLE. A watch handle is one subscription: its events come out in order
 * to whichever thread asks next.
 *
 * Watches are pulled, not pushed: daedalus_client_watch_next waits up to a timeout for the next
 * event and returns it as JSON text, so a caller never needs a callback and no struct layout
 * of an event is in the surface. The JSON is the event as daedalus-ctl prints it with --json:
 * {"event": "fault" | "telemetry" | "lagged" | "ended" | "unknown", "sub": n, "run": n, ...}.
 *
 * Growing this surface: a function is never removed, renamed or given another signature. New
 * functions and new statuses are added, with the next free number, and a number never changes
 * meaning. A struct gets no new field: a new field comes with a new function that takes the larger
 * struct. A caller built against an older header keeps working, since it calls only what it knows.
 * To find out whether the library has a function, look it up by name with dlsym (GetProcAddress on
 * other platforms) and check for null before calling it.
 */
#ifndef DAEDALUS_CLIENT_H
#define DAEDALUS_CLIENT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* A client or a watch. Zero is never one. */
typedef uint64_t daedalus_client_handle_t;

/* What a call returns. A number never changes meaning. */
typedef int32_t daedalus_client_status_t;

/* The statuses this library shares with daedalus.h keep daedalus.h's numbers. The numbers it
 * has no status for are left empty, so one number has one meaning in either header. */
#define DAEDALUS_CLIENT_OK 0
/* A pointer the call needs was null. */
#define DAEDALUS_CLIENT_NULL_POINTER 1
/* Not a handle of the kind the call takes. */
#define DAEDALUS_CLIENT_BAD_HANDLE 2
/* A handle that was closed. */
#define DAEDALUS_CLIENT_STALE_HANDLE 3
/* Text that isn't UTF-8. */
#define DAEDALUS_CLIENT_BAD_TEXT 5
/* The caller's buffer is too small. The size needed was written, and a value or text the call
 * took is kept for the next call that asks for it. */
#define DAEDALUS_CLIENT_BUFFER_TOO_SMALL 6
/* The call panicked. The handle it was given is still valid. */
#define DAEDALUS_CLIENT_PANIC 17
/* The endpoint is neither unix:/path nor tcp:address, or the address is not on the loopback
 * interface. */
#define DAEDALUS_CLIENT_BAD_ENDPOINT 18
/* The client has no connection. Nothing was sent. */
#define DAEDALUS_CLIENT_DISCONNECTED 19
/* The connection dropped after the request was sent and before its answer came: the daemon may
 * or may not have acted on it. */
#define DAEDALUS_CLIENT_UNKNOWN_DELIVERY 20
/* The daemon refused the call. The code is in daedalus_client_last_error_code. */
#define DAEDALUS_CLIENT_SERVICE_ERROR 21
/* The daemon speaks another protocol version or layout, so the connection was refused. */
#define DAEDALUS_CLIENT_REFUSED 22
/* A value or a message could not be encoded or decoded. */
#define DAEDALUS_CLIENT_WIRE 23
/* A request larger than the daemon reads. */
#define DAEDALUS_CLIENT_TOO_LARGE 24
/* A socket error. The last error says which. */
#define DAEDALUS_CLIENT_IO 25
/* A session name that is empty or longer than the daemon keeps. */
#define DAEDALUS_CLIENT_BAD_SESSION 26
/* The daemon answered with a reply that does not belong to the call. */
#define DAEDALUS_CLIENT_UNEXPECTED 27
/* A take asked for a type the port's value does not have. The value is kept. */
#define DAEDALUS_CLIENT_TYPE_MISMATCH 28
/* A number that names no option. */
#define DAEDALUS_CLIENT_BAD_ARGUMENT 29
/* The endpoint is well formed, but this platform cannot serve it: a unix:/path endpoint where the
 * platform has no unix sockets. Nothing was connected; use tcp:127.0.0.1:PORT there. */
#define DAEDALUS_CLIENT_UNSUPPORTED 30

/* Plan forms, for daedalus_client_plan_options_t. */
#define DAEDALUS_CLIENT_FORM_STEPS 0
#define DAEDALUS_CLIENT_FORM_BLOB 1
#define DAEDALUS_CLIENT_FORM_SOURCE 2

/* Telemetry levels, for daedalus_client_config_t. */
#define DAEDALUS_CLIENT_TELEMETRY_OFF 0
#define DAEDALUS_CLIENT_TELEMETRY_BASIC 1
#define DAEDALUS_CLIENT_TELEMETRY_DETAILED 2
#define DAEDALUS_CLIENT_TELEMETRY_TRACE 3

/* Stop modes: what is waiting is finished first, or dropped. */
#define DAEDALUS_CLIENT_STOP_DRAIN 0
#define DAEDALUS_CLIENT_STOP_IMMEDIATE 1

/* What daedalus_client_inspect_* looks at. Each has a function of its own. */
#define DAEDALUS_CLIENT_INSPECT_RUN 0
#define DAEDALUS_CLIENT_INSPECT_GRAPH 1
#define DAEDALUS_CLIENT_INSPECT_CACHE 2
#define DAEDALUS_CLIENT_INSPECT_TELEMETRY 3
#define DAEDALUS_CLIENT_INSPECT_HOST 4

/* The event kinds a watch asks for: bits, combined with |. */
#define DAEDALUS_CLIENT_EVENT_FAULT 1
#define DAEDALUS_CLIENT_EVENT_TELEMETRY 2
#define DAEDALUS_CLIENT_EVENT_ALL 3

/* The run a watch follows when it follows every run. Run numbers start at one. */
#define DAEDALUS_CLIENT_ALL_RUNS 0

/* What daedalus_client_watch_next found. */
#define DAEDALUS_CLIENT_WATCH_IDLE 0
#define DAEDALUS_CLIENT_WATCH_EVENT 1
#define DAEDALUS_CLIENT_WATCH_ENDED 2

/* A piece of text the caller gives: bytes and their length, with no NUL. */
typedef struct daedalus_client_text_t {
    const uint8_t *data;
    size_t len;
} daedalus_client_text_t;

/* The surface's version: the protocol's version and the hash of its messages' layout. A caller
 * built against another layout should stop. */
typedef struct daedalus_client_version_t {
    uint32_t protocol;
    uint64_t layout_hash;
} daedalus_client_version_t;

/* How a start, or a made plan, is configured. A null list with a zero count keeps the daemon's
 * order of backends. */
typedef struct daedalus_client_config_t {
    uint32_t fifo_capacity;
    uint32_t fault_reports;
    /* DAEDALUS_CLIENT_TELEMETRY_*. */
    uint32_t telemetry;
    /* Non-zero runs every node as its own step. */
    uint8_t unmerged;
    const daedalus_client_text_t *priority;
    size_t priority_len;
} daedalus_client_config_t;

/* How a plan is asked for. */
typedef struct daedalus_client_plan_options_t {
    /* DAEDALUS_CLIENT_FORM_*. */
    uint32_t form;
    /* Non-zero adds the planner's explanation to the plan's JSON. */
    uint8_t inspect;
    const daedalus_client_text_t *priority;
    size_t priority_len;
} daedalus_client_plan_options_t;

/* What a batch of ticks did. */
typedef struct daedalus_client_step_t {
    uint64_t tick;
    uint64_t step_calls;
    uint64_t faults;
    uint64_t skipped;
    /* Ticks run by the call. */
    uint32_t ticks;
    /* Non-zero when the last tick had nothing to do. */
    uint8_t idle;
} daedalus_client_step_t;

/* How a stop went. */
typedef struct daedalus_client_stopped_t {
    /* The run's last tick. */
    uint64_t tick;
    /* Values thrown away by an immediate stop. */
    uint32_t dropped;
} daedalus_client_stopped_t;

/* What a feed did. */
typedef struct daedalus_client_fed_t {
    /* How many consumer steps were woken. */
    uint32_t woke;
    /* Non-zero when the port's policy gave a value up for this one. */
    uint8_t dropped;
} daedalus_client_fed_t;

/* How many of each thing a reload carried, started fresh, added, removed, and dropped. */
typedef struct daedalus_client_carry_t {
    uint32_t carried;
    uint32_t fresh;
    uint32_t added;
    uint32_t removed;
    uint32_t removed_ports;
    uint32_t dropped_values;
} daedalus_client_carry_t;

/* Writes the surface's version. */
daedalus_client_status_t daedalus_client_version(daedalus_client_version_t *out);
/* The name of a status, NUL-terminated. Never null, never freed. */
const char *daedalus_client_status_name(daedalus_client_status_t status);
/* Why the last failed call on this thread failed, as text. */
daedalus_client_status_t daedalus_client_last_error(uint8_t *buf, size_t cap, size_t *len);
/* The daemon's error code for the last failed call on this thread: 1 to 12 as the protocol gives
 * them, 65535 for a code this library doesn't know, and 0 when the failure was not a refusal by
 * the daemon. */
daedalus_client_status_t daedalus_client_last_error_code(uint32_t *code);
/* Panics on purpose and returns DAEDALUS_CLIENT_PANIC, for a caller's own test. */
daedalus_client_status_t daedalus_client_selftest_panic(void);

/* Connects to the daemon at `endpoint`: "unix:/path", or "tcp:127.0.0.1:4100". Non-zero for
 * `reconnect` keeps the connection trying again after a disconnect. An endpoint that is neither
 * form, or a TCP address off the loopback interface, is DAEDALUS_CLIENT_BAD_ENDPOINT. A unix:/path
 * endpoint on a platform with no unix sockets is DAEDALUS_CLIENT_UNSUPPORTED. */
daedalus_client_status_t daedalus_client_connect(const uint8_t *endpoint, size_t len, uint8_t reconnect, daedalus_client_handle_t *out);
/* Closes the connection for every handle made from it, sessions included, and releases this
 * handle. Watches of the connection end. Other handles then fail with DISCONNECTED. */
daedalus_client_status_t daedalus_client_close(daedalus_client_handle_t client);
/* A handle for the same connection, whose requests are in the session `name`. */
daedalus_client_status_t daedalus_client_in_session(daedalus_client_handle_t client, const uint8_t *name, size_t len, daedalus_client_handle_t *out);
/* The daemon's protocol version. */
daedalus_client_status_t daedalus_client_ping(daedalus_client_handle_t client, uint32_t *version);
/* Loads the plugin at `path` into the daemon. Its nodes and backends are listed by
 * daedalus_client_inspect_host. */
daedalus_client_status_t daedalus_client_load_plugin(daedalus_client_handle_t client, const uint8_t *path, size_t len);

/* Stores a graph document, JSON text in the graph format, under `name`. Its lints come back from
 * daedalus_client_plan. */
daedalus_client_status_t daedalus_client_put_graph(daedalus_client_handle_t client, const uint8_t *name, size_t name_len, const uint8_t *json, size_t json_len);
/* Applies a patch document (JSON text) to a stored graph. */
daedalus_client_status_t daedalus_client_patch_graph(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const uint8_t *patch, size_t patch_len);
/* Plans a stored graph and writes the plan as JSON: its counts, its form, and the lints the
 * planner found under "diagnostics". Planning reads only, so a call with a buffer too small may
 * be repeated. */
daedalus_client_status_t daedalus_client_plan(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const daedalus_client_plan_options_t *options, uint8_t *buf, size_t cap, size_t *len);
/* Plans a stored graph as daedalus_client_plan does, but its explanation is the planner's own
 * text, as a JSON string (or null), so the order of its keys is kept. */
daedalus_client_status_t daedalus_client_plan_raw(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const daedalus_client_plan_options_t *options, uint8_t *buf, size_t cap, size_t *len);
/* Plans a stored graph into a made plan: writes its hash to `hash` and its bytes to the buffer.
 * A host loads a made plan with no planner. Reads only, so it may be repeated. */
daedalus_client_status_t daedalus_client_make_plan(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const daedalus_client_config_t *config, uint64_t *hash, uint8_t *buf, size_t cap, size_t *len);
/* Stores a made plan under `name`. A run started on that name uses it. */
daedalus_client_status_t daedalus_client_put_plan(daedalus_client_handle_t client, const uint8_t *name, size_t name_len, const uint8_t *bytes, size_t len);
/* Plans a stored graph (or a made plan stored under `name`) and starts a run; writes its number. */
daedalus_client_status_t daedalus_client_start(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const daedalus_client_config_t *config, uint64_t *run);
/* Runs `count` ticks of a run. `out` may be null. */
daedalus_client_status_t daedalus_client_step(daedalus_client_handle_t client, uint64_t run, uint32_t count, daedalus_client_step_t *out);
/* Cancels a run. The run stays until it is stopped. */
daedalus_client_status_t daedalus_client_cancel(daedalus_client_handle_t client, uint64_t run);
/* Stops a run: `mode` is DAEDALUS_CLIENT_STOP_DRAIN or DAEDALUS_CLIENT_STOP_IMMEDIATE. `out` may be null. */
daedalus_client_status_t daedalus_client_stop(daedalus_client_handle_t client, uint64_t run, uint32_t mode, daedalus_client_stopped_t *out);
/* Replaces a run's graph with a stored one. `out` may be null. */
daedalus_client_status_t daedalus_client_reload(daedalus_client_handle_t client, uint64_t run, const uint8_t *graph, size_t graph_len, daedalus_client_carry_t *out);

/* Reports. The calls below do what their plain forms do, and keep what they gave the caller on
 * this client, as JSON, for daedalus_client_report: a plugin's description, the diagnostics of a
 * graph, or what a reload carried. Keeping a report replaces the one before it. */
/* Loads the plugin at `path`. Its description is kept: name, generation, nodes and backends. */
daedalus_client_status_t daedalus_client_load_plugin_report(daedalus_client_handle_t client, const uint8_t *path, size_t len);
/* Stores a graph document under `name`. The diagnostics it gave are kept, as a JSON array. */
daedalus_client_status_t daedalus_client_put_graph_report(daedalus_client_handle_t client, const uint8_t *name, size_t name_len, const uint8_t *json, size_t json_len);
/* Applies a patch to a stored graph. The diagnostics it gave are kept, as a JSON array. */
daedalus_client_status_t daedalus_client_patch_graph_report(daedalus_client_handle_t client, const uint8_t *graph, size_t graph_len, const uint8_t *patch, size_t patch_len);
/* Reloads a run to a stored graph. What it carried is kept: the names of each kind, and the counts. */
daedalus_client_status_t daedalus_client_reload_report(daedalus_client_handle_t client, uint64_t run, const uint8_t *graph, size_t graph_len);
/* Copies the report kept by the last of the calls above on this client. Empty when none was made.
 * Reading it changes nothing, so a call with a buffer too small may be repeated. */
daedalus_client_status_t daedalus_client_report(daedalus_client_handle_t client, uint8_t *buf, size_t cap, size_t *len);

/* Writes a stored graph's document as it was stored, as text: its key order is kept. */
daedalus_client_status_t daedalus_client_graph_text(daedalus_client_handle_t client, const uint8_t *name, size_t name_len, uint8_t *buf, size_t cap, size_t *len);
/* Inspects a run as JSON: its graph, state, tick, ports and nodes. */
daedalus_client_status_t daedalus_client_inspect_run(daedalus_client_handle_t client, uint64_t run, uint8_t *buf, size_t cap, size_t *len);
/* Inspects a stored graph as JSON: its name and its document. */
daedalus_client_status_t daedalus_client_inspect_graph(daedalus_client_handle_t client, const uint8_t *name, size_t name_len, uint8_t *buf, size_t cap, size_t *len);
/* Inspects the plan cache as JSON: its entries, hits and misses. */
daedalus_client_status_t daedalus_client_inspect_cache(daedalus_client_handle_t client, uint8_t *buf, size_t cap, size_t *len);
/* Inspects a run's telemetry as JSON. */
daedalus_client_status_t daedalus_client_inspect_telemetry(daedalus_client_handle_t client, uint64_t run, uint8_t *buf, size_t cap, size_t *len);
/* Inspects the host as JSON: its sessions, graphs, runs and plugins. */
daedalus_client_status_t daedalus_client_inspect_host(daedalus_client_handle_t client, uint8_t *buf, size_t cap, size_t *len);
/* Empties the daemon's plan cache. */
daedalus_client_status_t daedalus_client_cache_clear(daedalus_client_handle_t client);
/* The newest `limit` requests the daemon served, oldest first, as JSON. Zero is all it keeps. */
daedalus_client_status_t daedalus_client_trace(daedalus_client_handle_t client, uint32_t limit, uint8_t *buf, size_t cap, size_t *len);

/* Feeds an int64 to a port that carries int64. `out` may be null. */
daedalus_client_status_t daedalus_client_feed_i64(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, int64_t value, daedalus_client_fed_t *out);
/* Feeds a float64 to a port that carries float64. */
daedalus_client_status_t daedalus_client_feed_f64(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, double value, daedalus_client_fed_t *out);
/* Feeds a boolean, non-zero for true, to a port that carries bool. */
daedalus_client_status_t daedalus_client_feed_bool(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, uint8_t value, daedalus_client_fed_t *out);
/* Feeds bytes, copied, to a port that carries bytes. */
daedalus_client_status_t daedalus_client_feed_bytes(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, const uint8_t *bytes, size_t len, daedalus_client_fed_t *out);
/* Feeds UTF-8 text, copied, to a port that carries string. */
daedalus_client_status_t daedalus_client_feed_str(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, const uint8_t *text, size_t len, daedalus_client_fed_t *out);
/* Feeds a value of any type, given as its wire encoding: the type, then the value (see below).
 * Its type must be the port's. */
daedalus_client_status_t daedalus_client_feed_wire(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, const uint8_t *bytes, size_t len, daedalus_client_fed_t *out);

/* Takes the oldest value of an output port. `present` is 1 when there was one, and `out` is
 * written then. A value a buffer or a type can't take is kept for the next take of its port. */
daedalus_client_status_t daedalus_client_take_i64(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, int64_t *out, uint8_t *present);
daedalus_client_status_t daedalus_client_take_f64(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, double *out, uint8_t *present);
daedalus_client_status_t daedalus_client_take_bool(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, uint8_t *out, uint8_t *present);
/* Takes the bytes of a bytes value into `buf`. With the buffer too small the call fails with
 * BUFFER_TOO_SMALL, writes the size, and the value waits for the next take of its port. */
daedalus_client_status_t daedalus_client_take_bytes(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, uint8_t *buf, size_t cap, size_t *len, uint8_t *present);
/* Takes a value of any type as its wire encoding into `buf`, with the same waiting as
 * daedalus_client_take_bytes. */
daedalus_client_status_t daedalus_client_take_wire(daedalus_client_handle_t client, uint64_t run, const uint8_t *port, size_t port_len, uint8_t *buf, size_t cap, size_t *len, uint8_t *present);

/* Subscribes to the events of `run` (DAEDALUS_CLIENT_ALL_RUNS for every run) in `kinds`. */
daedalus_client_status_t daedalus_client_watch_open(daedalus_client_handle_t client, uint64_t run, uint32_t kinds, daedalus_client_handle_t *out);
/* Waits up to `timeout_ms` for the next event. `state` is DAEDALUS_CLIENT_WATCH_EVENT with the
 * event as JSON in the buffer, DAEDALUS_CLIENT_WATCH_IDLE when none came in time, or
 * DAEDALUS_CLIENT_WATCH_ENDED when no more will. `len` is the JSON's length in every case.
 * A watch keeps at most 64 unread events, as the daemon does: past that the oldest are dropped,
 * and the next event is a `lagged` one with their count. */
daedalus_client_status_t daedalus_client_watch_next(daedalus_client_handle_t watch, uint32_t timeout_ms, uint8_t *buf, size_t cap, size_t *len, uint32_t *state);
/* Ends a watch and releases its handle. */
daedalus_client_status_t daedalus_client_watch_close(daedalus_client_handle_t watch);

/*
 * The wire encoding of a typed value, for daedalus_client_feed_wire and the take_wire call: the
 * type, then the value, with nothing after either.
 *
 * A type is a tag byte and then its fields. Tag 0 is a scalar, and its field is one byte, the
 * scalar's code: 0 unit, 1 bool, 2 i8, 3 i16, 4 i32, 5 i64, 6 isize, 7 u8, 8 u16, 9 u32, 10 u64,
 * 11 usize, 12 f32, 13 f64, 14 string, 15 bytes. Other tags (opaque 1, optional 2, list 3, map 4,
 * tuple 5, struct 6, enum 7) are as core/wire/src/value.rs writes them.
 *
 * A value is the encoding of its type. Integers and floats are little-endian at their width, a
 * bool is one byte (0 or 1), and a string or bytes value is a u32 length, little-endian, and then
 * its bytes. The port's type must be the value's type: a port that carries i64 takes the scalar
 * 5 and eight bytes, and no other encoding.
 */

#ifdef __cplusplus
}
#endif

#endif
