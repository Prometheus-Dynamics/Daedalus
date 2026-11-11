/*
 * The C surface of Daedalus.
 *
 * A host loads plugins. A graph is compiled on a host from JSON text, proved against the
 * host's nodes before anything runs, and then pushed to, ticked and taken from.
 *
 * Every call returns a status, and results come back through pointers. Text is given and
 * returned as UTF-8 with a length and no NUL. A call that returns text takes a buffer and its
 * size and writes the length the text has: call once with a null buffer for the size, which
 * returns DAEDALUS_BUFFER_TOO_SMALL, and again with a buffer that large.
 *
 * Handles are numbers, not pointers. A handle that was released, or was never one, is answered
 * with a status. Calls on one graph may come from any thread, one at a time; calls on
 * different graphs don't wait on each other.
 */
#ifndef DAEDALUS_H
#define DAEDALUS_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* A host or a graph. Zero is never one. */
typedef uint64_t daedalus_handle_t;

/* What a call returns. A number never changes meaning. */
typedef int32_t daedalus_status_t;

#define DAEDALUS_OK 0
/* A pointer the call needs was null. */
#define DAEDALUS_NULL_POINTER 1
/* Not a handle of the kind the call takes. */
#define DAEDALUS_BAD_HANDLE 2
/* A handle that was released. */
#define DAEDALUS_STALE_HANDLE 3
/* The host still has graphs. */
#define DAEDALUS_BUSY 4
/* Text that isn't UTF-8. */
#define DAEDALUS_BAD_TEXT 5
/* The caller's buffer is too small. The size needed was written. */
#define DAEDALUS_BUFFER_TOO_SMALL 6
/* The graph has no host port of that name, or that index. */
#define DAEDALUS_UNKNOWN_PORT 7
/* A push to an output, or a take from an input. */
#define DAEDALUS_WRONG_DIRECTION 8
/* The port's type isn't one this kind of value goes into or comes out of. */
#define DAEDALUS_TYPE_MISMATCH 9
/* The number doesn't fit the port's type. */
#define DAEDALUS_OUT_OF_RANGE 10
/* The plugin couldn't be loaded, or its nodes couldn't join the host's. */
#define DAEDALUS_PLUGIN_FAILED 11
/* The graph's text couldn't be read. */
#define DAEDALUS_GRAPH_INVALID 12
/* The graph was read and can't be planned. */
#define DAEDALUS_PLAN_FAILED 13
/* The plan couldn't be loaded. */
#define DAEDALUS_LOAD_FAILED 14
/* The graph refused the value, or the tick failed. */
#define DAEDALUS_RUN_FAILED 15
/* A batch with an entry that can't be pushed. Nothing was pushed. */
#define DAEDALUS_BAD_BATCH 16
/* The call panicked. The handle it was given is still valid. */
#define DAEDALUS_PANIC 17

/* The surface's version. Stop on another major, or another layout hash than the one built
 * against. */
typedef struct daedalus_version_t {
    uint32_t major;
    uint32_t minor;
    uint64_t layout_hash;
} daedalus_version_t;

/* One host port of a graph. The strings belong to the graph, are valid until it is released,
 * and have no NUL after them. */
typedef struct daedalus_port_t {
    const uint8_t *name;
    size_t name_len;
    const uint8_t *type_key;
    size_t type_key_len;
    /* 1 for a port the caller pushes to, 0 for one it takes from. */
    uint8_t is_input;
} daedalus_port_t;

/* What a graph has done since it was compiled. */
typedef struct daedalus_telemetry_t {
    uint64_t ticks;
    uint64_t idle_ticks;
    uint64_t step_calls;
    uint64_t faults;
    uint64_t pushes;
    uint64_t takes;
} daedalus_telemetry_t;

/* What one tick did. */
typedef struct daedalus_tick_t {
    uint8_t idle;
    uint32_t step_calls;
    /* Steps that failed. The graph keeps running, and their outputs are missing. */
    uint32_t faults;
} daedalus_tick_t;

/* The kinds of value a batch entry holds. */
#define DAEDALUS_KIND_I64 0
#define DAEDALUS_KIND_F64 1
#define DAEDALUS_KIND_BOOL 2
#define DAEDALUS_KIND_BYTES 3

typedef struct daedalus_bytes_t {
    const uint8_t *data;
    size_t len;
} daedalus_bytes_t;

typedef union daedalus_push_value_t {
    int64_t i64_value;
    double f64_value;
    uint8_t bool_value;
    daedalus_bytes_t bytes;
} daedalus_push_value_t;

/* One push of a batch: a port by its index, and the value its kind names. */
typedef struct daedalus_push_t {
    size_t port;
    uint32_t kind;
    daedalus_push_value_t value;
} daedalus_push_t;

/* Writes the surface's version. */
daedalus_status_t daedalus_version(daedalus_version_t *out);
/* The name of a status, NUL-terminated. Never null, never freed. */
const char *daedalus_status_name(daedalus_status_t status);
/* Why the last failed call on this thread failed, as text. */
daedalus_status_t daedalus_last_error(uint8_t *buf, size_t cap, size_t *len);
/* Panics on purpose and returns DAEDALUS_PANIC, for a caller's own test. */
daedalus_status_t daedalus_selftest_panic(void);

/* Makes a host with no nodes. */
daedalus_status_t daedalus_host_create(daedalus_handle_t *out);
/* Releases a host. DAEDALUS_BUSY while it has graphs. */
daedalus_status_t daedalus_host_release(daedalus_handle_t host);
/*
 * Loads the plugin at `path`: its nodes join the host's, and its backends run them.
 *
 * A plugin may hand bytes to the host without a copy: a node marks an output cell as a hand-over
 * (owner 6, reserved 1) and describes the bytes with a release. The host calls that release once,
 * when the last host value of the bytes is dropped, on whichever thread drops it. The release must
 * therefore be safe to call from any thread.
 */
daedalus_status_t daedalus_host_install(daedalus_handle_t host, const uint8_t *path, size_t len);
/* The host's nodes, as JSON text. */
daedalus_status_t daedalus_host_schemas(daedalus_handle_t host, uint8_t *buf, size_t cap, size_t *len);

/* Compiles a graph given as JSON text in the graph format. */
daedalus_status_t daedalus_graph_compile(daedalus_handle_t host, const uint8_t *json, size_t len, daedalus_handle_t *out);
/* Releases a graph. */
daedalus_status_t daedalus_graph_release(daedalus_handle_t graph);
/* How many host ports the graph has. A port's index is its place in the ports sorted by name. */
daedalus_status_t daedalus_graph_port_count(daedalus_handle_t graph, size_t *count);
/* Describes the host port at `index`. */
daedalus_status_t daedalus_graph_port(daedalus_handle_t graph, size_t index, daedalus_port_t *out);
/* The index of the host port called `name`. */
daedalus_status_t daedalus_graph_port_index(daedalus_handle_t graph, const uint8_t *name, size_t len, size_t *index);
/* How the graph was planned, as JSON text. */
daedalus_status_t daedalus_graph_explain(daedalus_handle_t graph, uint8_t *buf, size_t cap, size_t *len);
/* What the graph has done since it was compiled. */
daedalus_status_t daedalus_graph_telemetry(daedalus_handle_t graph, daedalus_telemetry_t *out);
/* Runs one tick. `out` may be null. */
daedalus_status_t daedalus_tick(daedalus_handle_t graph, daedalus_tick_t *out);

/* Pushes: an integer into a port of any integer type it fits, a float into f64 or f32, a
 * boolean into bool, bytes into bytes. Bytes are copied. */
daedalus_status_t daedalus_push_i64(daedalus_handle_t graph, const uint8_t *name, size_t len, int64_t value);
daedalus_status_t daedalus_push_i64_at(daedalus_handle_t graph, size_t index, int64_t value);
daedalus_status_t daedalus_push_f64(daedalus_handle_t graph, const uint8_t *name, size_t len, double value);
daedalus_status_t daedalus_push_f64_at(daedalus_handle_t graph, size_t index, double value);
daedalus_status_t daedalus_push_bool(daedalus_handle_t graph, const uint8_t *name, size_t len, uint8_t value);
daedalus_status_t daedalus_push_bool_at(daedalus_handle_t graph, size_t index, uint8_t value);
daedalus_status_t daedalus_push_bytes(daedalus_handle_t graph, const uint8_t *name, size_t name_len, const uint8_t *bytes, size_t len);
daedalus_status_t daedalus_push_bytes_at(daedalus_handle_t graph, size_t index, const uint8_t *bytes, size_t len);
/* Pushes `count` values, or none: every entry is checked first. On DAEDALUS_BAD_BATCH, `bad`,
 * when not null, gets the first bad entry's position. */
daedalus_status_t daedalus_push_batch(daedalus_handle_t graph, const daedalus_push_t *pushes, size_t count, size_t *bad);

/* Takes the oldest value of an output port. `present` is 1 and `out` written when there was
 * one, and `present` is 0 when there was none. */
daedalus_status_t daedalus_take_i64(daedalus_handle_t graph, const uint8_t *name, size_t len, int64_t *out, uint8_t *present);
daedalus_status_t daedalus_take_i64_at(daedalus_handle_t graph, size_t index, int64_t *out, uint8_t *present);
daedalus_status_t daedalus_take_f64(daedalus_handle_t graph, const uint8_t *name, size_t len, double *out, uint8_t *present);
daedalus_status_t daedalus_take_f64_at(daedalus_handle_t graph, size_t index, double *out, uint8_t *present);
daedalus_status_t daedalus_take_bool(daedalus_handle_t graph, const uint8_t *name, size_t len, uint8_t *out, uint8_t *present);
daedalus_status_t daedalus_take_bool_at(daedalus_handle_t graph, size_t index, uint8_t *out, uint8_t *present);
/* Takes bytes into `buf`. With a buffer too small the value waits for the next take. */
daedalus_status_t daedalus_take_bytes(daedalus_handle_t graph, const uint8_t *name, size_t name_len, uint8_t *buf, size_t cap, size_t *len, uint8_t *present);
daedalus_status_t daedalus_take_bytes_at(daedalus_handle_t graph, size_t index, uint8_t *buf, size_t cap, size_t *len, uint8_t *present);

#ifdef __cplusplus
}
#endif

#endif
