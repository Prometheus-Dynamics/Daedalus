/*
 * daedalus_surface.h: the C ABI of the Daedalus shared-library surface (libdaedalus_surface.so).
 *
 * Hand-written; crates/surface/tests/header.rs keeps it in sync with the Rust exports and with the
 * lemnos-abi layouts (the LAYOUT_HASH and the struct sizes below).
 *
 * Conventions (lemnos-abi): every call returns a daedalus_status: 0 is success, a negative value
 * in -1..-4095 is a Linux errno, and the DAEDALUS_* codes below are the surface's own. Handles are
 * generation-counted 64-bit values; 0 is null, and a released or never-issued handle is refused
 * with DAEDALUS_STALE_HANDLE, never dereferenced. Every call runs under a panic guard: a panic
 * comes back as DAEDALUS_PANIC. Calls are per frame or per job, never per node.
 *
 * Buffers: a call that writes data into a caller buffer takes (buf, cap, needed). It always writes
 * the length of the data to *needed. If cap is too small it returns DAEDALUS_BUFFER_TOO_SMALL and
 * writes nothing; the caller allocates *needed bytes and calls again.
 *
 * Strings (port names, paths, documents) are UTF-8 bytes with an explicit length; they are not
 * NUL-terminated.
 */
#ifndef DAEDALUS_SURFACE_H
#define DAEDALUS_SURFACE_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* The surface ABI: major changes break callers, minor adds. */
#define DAEDALUS_SURFACE_ABI_MAJOR 1
#define DAEDALUS_SURFACE_ABI_MINOR 0

/* lemnos-abi's LAYOUT_HASH for this build: a caller and a surface must agree on it. */
#define DAEDALUS_LAYOUT_HASH 0xA02CFB4A8DD4545BULL

/* Sizes of the lemnos-abi structs, as the layout hash covers them. */
#define DAEDALUS_ABI_VERSION_SIZE 16
#define DAEDALUS_PLANE_DESC_SIZE 32
#define DAEDALUS_BUFFER_DESC_SIZE 160
#define DAEDALUS_FENCE_DESC_SIZE 16
#define DAEDALUS_MAX_PLANES 4

/* Feature bits, from daedalus_surface_features(). */
#define DAEDALUS_FEATURE_PLUGINS (1ULL << 0)
#define DAEDALUS_FEATURE_GRAPHS (1ULL << 1)
#define DAEDALUS_FEATURE_TELEMETRY (1ULL << 2)
#define DAEDALUS_FEATURE_ALLOC_COUNT (1ULL << 3)

/* Status codes (daedalus_status). */
typedef int32_t daedalus_status;
#define DAEDALUS_OK 0
#define DAEDALUS_IO (-5)
#define DAEDALUS_NOT_FOUND (-2)
#define DAEDALUS_AGAIN (-11)
#define DAEDALUS_NO_MEMORY (-12)
#define DAEDALUS_BUSY (-16)
#define DAEDALUS_INVALID_ARGUMENT (-22)
#define DAEDALUS_NOT_SUPPORTED (-95)
#define DAEDALUS_STALE_HANDLE (-4096)
#define DAEDALUS_VERSION_MISMATCH (-4097)
#define DAEDALUS_BUFFER_TOO_SMALL (-4098)
#define DAEDALUS_PANIC (-4099)

/* A generation-counted handle; 0 is null. */
typedef uint64_t daedalus_handle;

/* The version handshake (lemnos-abi AbiVersion). */
typedef struct daedalus_abi_version {
  uint32_t struct_size;
  uint16_t major;
  uint16_t minor;
  uint64_t layout;
} daedalus_abi_version;

/* One memory plane of a buffer (lemnos-abi PlaneDesc). */
typedef struct daedalus_plane_desc {
  int32_t fd;
  uint32_t reserved;
  uint64_t offset;
  uint64_t stride;
  uint64_t size;
} daedalus_plane_desc;

/* A dma-buf frame (lemnos-abi BufferDesc). */
typedef struct daedalus_buffer_desc {
  uint32_t struct_size;
  uint32_t flags;
  uint32_t width;
  uint32_t height;
  uint32_t fourcc;
  uint32_t plane_count;
  uint64_t modifier;
  daedalus_plane_desc planes[DAEDALUS_MAX_PLANES];
} daedalus_buffer_desc;

/* A fence by file descriptor (lemnos-abi FenceDesc). */
typedef struct daedalus_fence_desc {
  uint32_t kind;
  int32_t fd;
  uint64_t point;
} daedalus_fence_desc;

/* Where a call writes a message (lemnos-abi MessageSink): the caller appends. A null write drops
 * the message. The text is valid for the callback only. */
typedef struct daedalus_message_sink {
  void *context;
  void (*write)(void *context, const uint8_t *text, size_t len);
} daedalus_message_sink;

#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(daedalus_abi_version) == DAEDALUS_ABI_VERSION_SIZE, "AbiVersion layout");
_Static_assert(sizeof(daedalus_plane_desc) == DAEDALUS_PLANE_DESC_SIZE, "PlaneDesc layout");
_Static_assert(sizeof(daedalus_buffer_desc) == DAEDALUS_BUFFER_DESC_SIZE, "BufferDesc layout");
_Static_assert(sizeof(daedalus_fence_desc) == DAEDALUS_FENCE_DESC_SIZE, "FenceDesc layout");
#endif

/* ---- Version and features ---- */

/* Writes the surface's version (its ABI version and LAYOUT_HASH) to *out. */
daedalus_status daedalus_surface_version(daedalus_abi_version *out);
/* Writes the feature bit mask to *out. */
daedalus_status daedalus_surface_features(uint64_t *out);

/* ---- Host: a plugin registry and its engine ---- */

daedalus_status daedalus_surface_host_create(daedalus_handle *out);
/* DAEDALUS_BUSY while graphs compiled from the host are alive. */
daedalus_status daedalus_surface_host_release(daedalus_handle host);
/* Installs the plugin library at path (UTF-8) through the stable handler path. A missing file is
 * DAEDALUS_NOT_FOUND; a library the stable ABI refuses is DAEDALUS_NOT_SUPPORTED. The reason goes to
 * sink. */
daedalus_status daedalus_surface_host_install(daedalus_handle host, const uint8_t *path,
                                              size_t path_len, daedalus_message_sink sink);
/* Writes the installed plugins' schemas as a JSON array. */
daedalus_status daedalus_surface_host_schemas(daedalus_handle host, uint8_t *buf, size_t cap,
                                              size_t *needed);

/* ---- Graphs ---- */

/* Compiles and prepares a daedalus.graph document (JSON text) against the host's registry. */
daedalus_status daedalus_surface_graph_compile(daedalus_handle host, const uint8_t *json,
                                               size_t json_len, daedalus_handle *out,
                                               daedalus_message_sink sink);
daedalus_status daedalus_surface_graph_release(daedalus_handle graph);
/* Writes {"inputs": [{"name", "kind"}], "outputs": [...]} as JSON. kind: int, float, bool, bytes or
 * other. */
daedalus_status daedalus_surface_graph_ports(daedalus_handle graph, uint8_t *buf, size_t cap,
                                             size_t *needed);
/* Writes the plan explanation as JSON. */
daedalus_status daedalus_surface_graph_explain(daedalus_handle graph, uint8_t *buf, size_t cap,
                                               size_t *needed);
/* Writes the last tick's telemetry as JSON, or null before the first tick. */
daedalus_status daedalus_surface_graph_telemetry(daedalus_handle graph, uint8_t *buf, size_t cap,
                                                 size_t *needed);

/* ---- Per frame: push, tick, take ---- */

/* Pushes a value into an input port. DAEDALUS_INVALID_ARGUMENT when the port's type is not the
 * one pushed; DAEDALUS_AGAIN when the input is full. */
daedalus_status daedalus_surface_push_i64(daedalus_handle graph, const uint8_t *port,
                                          size_t port_len, int64_t value);
daedalus_status daedalus_surface_push_f64(daedalus_handle graph, const uint8_t *port,
                                          size_t port_len, double value);
daedalus_status daedalus_surface_push_bool(daedalus_handle graph, const uint8_t *port,
                                           size_t port_len, uint8_t value);
/* Copies len bytes into the port (the surface keeps and reuses the vector). */
daedalus_status daedalus_surface_push_bytes(daedalus_handle graph, const uint8_t *port,
                                            size_t port_len, const uint8_t *data, size_t len);

/* Ticks the graph once. A failing node's message goes to sink, with DAEDALUS_IO. */
daedalus_status daedalus_surface_tick(daedalus_handle graph, daedalus_message_sink sink);

/* Takes the next value from an output port: DAEDALUS_OK with the value, or DAEDALUS_AGAIN when
 * nothing is queued. */
daedalus_status daedalus_surface_take_i64(daedalus_handle graph, const uint8_t *port,
                                          size_t port_len, int64_t *out);
daedalus_status daedalus_surface_take_f64(daedalus_handle graph, const uint8_t *port,
                                          size_t port_len, double *out);
daedalus_status daedalus_surface_take_bool(daedalus_handle graph, const uint8_t *port,
                                           size_t port_len, uint8_t *out);
/* Takes the next bytes into buf. DAEDALUS_BUFFER_TOO_SMALL keeps the bytes for the next take. */
daedalus_status daedalus_surface_take_bytes(daedalus_handle graph, const uint8_t *port,
                                            size_t port_len, uint8_t *buf, size_t cap,
                                            size_t *needed);

/* ---- Frames (declared; built as stubs returning DAEDALUS_NOT_SUPPORTED in this version) ---- */

/* A frame as a dma-buf descriptor, with its fence. Not wired to graph ports yet. */
daedalus_status daedalus_surface_push_frame(daedalus_handle graph, const uint8_t *port,
                                            size_t port_len, const daedalus_buffer_desc *desc,
                                            const daedalus_fence_desc *fence);
daedalus_status daedalus_surface_take_frame(daedalus_handle graph, const uint8_t *port,
                                            size_t port_len, daedalus_buffer_desc *desc,
                                            daedalus_fence_desc *fence);

#ifdef __cplusplus
}
#endif

#endif /* DAEDALUS_SURFACE_H */
