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
 * Dma-buf frames (Linux; elsewhere the frame calls return DAEDALUS_NOT_SUPPORTED): the descriptors
 * follow the BufferDesc::OWNED flag. Without it a push borrows them for the call, and a take gives
 * out duplicates the caller closes. With it the receiver takes the descriptors over and closes them
 * whether or not the call succeeds, so a failed call never leaks the caller's fds.
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
#define DAEDALUS_SURFACE_ABI_MINOR 3

/* lemnos-abi's LAYOUT_HASH for this build: a caller and a surface must agree on it. */
#define DAEDALUS_LAYOUT_HASH 0xA02CFB4A8DD4545BULL

/* Sizes of the lemnos-abi structs, as the layout hash covers them. */
#define DAEDALUS_ABI_VERSION_SIZE 16
#define DAEDALUS_PLANE_DESC_SIZE 32
#define DAEDALUS_BUFFER_DESC_SIZE 160
#define DAEDALUS_FENCE_DESC_SIZE 16
#define DAEDALUS_MAX_PLANES 4

/* Descriptor flags and fence kinds (lemnos-abi BufferDesc::OWNED and FenceDesc::{NONE, SYNC_FILE,
 * SYNCOBJ}). */
#define DAEDALUS_BUFFER_OWNED 1U
#define DAEDALUS_FENCE_NONE 0
#define DAEDALUS_FENCE_SYNC_FILE 1
#define DAEDALUS_FENCE_SYNCOBJ 2

/* Feature bits, from daedalus_surface_features(). */
#define DAEDALUS_FEATURE_PLUGINS (1ULL << 0)
#define DAEDALUS_FEATURE_GRAPHS (1ULL << 1)
#define DAEDALUS_FEATURE_TELEMETRY (1ULL << 2)
#define DAEDALUS_FEATURE_ALLOC_COUNT (1ULL << 3)
/* An image surface (export_surface!): every new host has the image's own nodes installed through
 * the Rust path. */
#define DAEDALUS_FEATURE_IMAGE_NODES (1ULL << 4)

/* daedalus_push_entry.kind: what a batch entry pushes (ABI 1.1). */
#define DAEDALUS_PUSH_I64 1U
#define DAEDALUS_PUSH_F64 2U
#define DAEDALUS_PUSH_BOOL 3U
#define DAEDALUS_PUSH_BYTES 4U

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

/* One push of daedalus_surface_push_batch (ABI 1.1): the input port's index, what is pushed
 * (DAEDALUS_PUSH_*), and the value in the field that kind reads: int_value for I64 and BOOL (0 or
 * non-zero), float_value for F64, data and len for BYTES (copied during the call). The other fields
 * are ignored. */
typedef struct daedalus_push_entry {
  uint32_t port;
  uint32_t kind;
  int64_t int_value;
  double float_value;
  const uint8_t *data;
  size_t len;
} daedalus_push_entry;

#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(daedalus_push_entry) == 24 + sizeof(void *) + sizeof(size_t),
               "PushEntry layout");
_Static_assert(sizeof(daedalus_abi_version) == DAEDALUS_ABI_VERSION_SIZE, "AbiVersion layout");
_Static_assert(sizeof(daedalus_plane_desc) == DAEDALUS_PLANE_DESC_SIZE, "PlaneDesc layout");
_Static_assert(sizeof(daedalus_buffer_desc) == DAEDALUS_BUFFER_DESC_SIZE, "BufferDesc layout");
_Static_assert(sizeof(daedalus_fence_desc) == DAEDALUS_FENCE_DESC_SIZE, "FenceDesc layout");
#endif

/* ---- Version and features ---- */

/* The stable plugin ABI versions this surface installs, oldest and newest (data, since 1.3): a
 * plugin's daedalus_plugin_stable_version must lie in [min, max]. */
extern const uint32_t daedalus_surface_stable_abi_min;
extern const uint32_t daedalus_surface_stable_abi_max;

/* Writes the surface's version (its ABI version and LAYOUT_HASH) to *out. */
daedalus_status daedalus_surface_version(daedalus_abi_version *out);
/* Writes the feature bit mask to *out. */
daedalus_status daedalus_surface_features(uint64_t *out);

/* ---- Host: a plugin registry and its engine ---- */

/* A new host's registry is empty, or, in an image surface (DAEDALUS_FEATURE_IMAGE_NODES), holds the
 * image's nodes; a failing image install is DAEDALUS_IO. */
daedalus_status daedalus_surface_host_create(daedalus_handle *out);
/* As daedalus_surface_host_create, with a failing image install's reason written to sink (ABI 1.2). */
daedalus_status daedalus_surface_host_create_with(daedalus_handle *out, daedalus_message_sink sink);
/* DAEDALUS_BUSY while graphs compiled from the host are alive. */
daedalus_status daedalus_surface_host_release(daedalus_handle host);
/* Installs the plugin library at path (UTF-8) through the stable handler path. A missing file is
 * DAEDALUS_NOT_FOUND; a library the stable ABI refuses is DAEDALUS_NOT_SUPPORTED. The reason goes to
 * sink. */
daedalus_status daedalus_surface_host_install(daedalus_handle host, const uint8_t *path,
                                              size_t path_len, daedalus_message_sink sink);
/* Writes the host's plugin schemas as a JSON array: an image surface's in-image plugins, then the
 * installed libraries. */
daedalus_status daedalus_surface_host_schemas(daedalus_handle host, uint8_t *buf, size_t cap,
                                              size_t *needed);

/* ---- Graphs ---- */

/* Compiles and prepares a daedalus.graph document (JSON text) against the host's registry. */
daedalus_status daedalus_surface_graph_compile(daedalus_handle host, const uint8_t *json,
                                               size_t json_len, daedalus_handle *out,
                                               daedalus_message_sink sink);
daedalus_status daedalus_surface_graph_release(daedalus_handle graph);
/* Writes {"inputs": [{"name", "kind"}], "outputs": [...]} as JSON. kind: int, float, bool, bytes,
 * frame or other. */
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
 * one pushed; DAEDALUS_AGAIN when the input is full. The port's kind is what daedalus_surface_
 * graph_ports reports (int, float, bool, bytes, frame or other). */
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

/* ---- Per frame by port index (ABI 1.1) ---- */

/* A port's index is its position in the inputs or outputs array of daedalus_surface_graph_ports.
 * These two look one up by name (whatever its kind); DAEDALUS_NOT_FOUND for an unknown name. A
 * caller resolves each port once and then pushes and takes by index: no name is matched per call.
 */
daedalus_status daedalus_surface_graph_input_index(daedalus_handle graph, const uint8_t *port,
                                                   size_t port_len, uint32_t *index);
daedalus_status daedalus_surface_graph_output_index(daedalus_handle graph, const uint8_t *port,
                                                    size_t port_len, uint32_t *index);

/* As the push and take calls above, with the port by index. DAEDALUS_NOT_FOUND past the last port,
 * DAEDALUS_INVALID_ARGUMENT for a port of another kind. */
daedalus_status daedalus_surface_push_i64_at(daedalus_handle graph, uint32_t port, int64_t value);
daedalus_status daedalus_surface_push_f64_at(daedalus_handle graph, uint32_t port, double value);
daedalus_status daedalus_surface_push_bool_at(daedalus_handle graph, uint32_t port, uint8_t value);
daedalus_status daedalus_surface_push_bytes_at(daedalus_handle graph, uint32_t port,
                                               const uint8_t *data, size_t len);
daedalus_status daedalus_surface_take_i64_at(daedalus_handle graph, uint32_t port, int64_t *out);
daedalus_status daedalus_surface_take_f64_at(daedalus_handle graph, uint32_t port, double *out);
daedalus_status daedalus_surface_take_bool_at(daedalus_handle graph, uint32_t port, uint8_t *out);
daedalus_status daedalus_surface_take_bytes_at(daedalus_handle graph, uint32_t port, uint8_t *buf,
                                               size_t cap, size_t *needed);

/* Pushes count entries in order, under one lock and one panic guard. Every entry is checked first
 * (port index, kind, bytes pointer): a bad one pushes nothing and returns DAEDALUS_NOT_FOUND or
 * DAEDALUS_INVALID_ARGUMENT with *done 0. A push the engine refuses (DAEDALUS_AGAIN for a full
 * input) stops the batch there, with *done the number pushed before it. done may be NULL. */
daedalus_status daedalus_surface_push_batch(daedalus_handle graph,
                                            const daedalus_push_entry *entries, size_t count,
                                            size_t *done);

/* ---- Frames: dma-buf frames into and out of daedalus:frame ports (Linux) ---- */

/* Pushes the dma-buf frame described by desc (one descriptor per plane) into an input port. The
 * frame is shared, not copied. fence is NULL or a sync-file acquire fence: it is attached to the
 * frame's dma-bufs as a write fence, so a consumer that syncs through dma-buf waits for it.
 * Ownership of the descriptors and of the fence follows desc->flags (see above). A SYNCOBJ fence is
 * DAEDALUS_NOT_SUPPORTED. DAEDALUS_INVALID_ARGUMENT for a descriptor that does not validate, or a
 * port that is not a frame port; DAEDALUS_AGAIN when the input is full. */
daedalus_status daedalus_surface_push_frame(daedalus_handle graph, const uint8_t *port,
                                            size_t port_len, const daedalus_buffer_desc *desc,
                                            const daedalus_fence_desc *fence);
/* Takes the next frame from an output port. desc receives one owned (duplicated) descriptor per
 * plane, with desc->flags set to DAEDALUS_BUFFER_OWNED: the caller closes them. fence receives
 * DAEDALUS_FENCE_NONE: pending writes on the dma-bufs are carried by their reservations. frame
 * receives a lease: the frame stays alive until daedalus_surface_frame_release. DAEDALUS_AGAIN when
 * nothing is queued; DAEDALUS_NOT_SUPPORTED for a frame with no dma-buf plane (it stays queued). */
daedalus_status daedalus_surface_take_frame(daedalus_handle graph, const uint8_t *port,
                                            size_t port_len, daedalus_buffer_desc *desc,
                                            daedalus_fence_desc *fence, daedalus_handle *frame);
/* Releases a frame lease from daedalus_surface_take_frame. A released lease is
 * DAEDALUS_STALE_HANDLE. */
daedalus_status daedalus_surface_frame_release(daedalus_handle frame);

#ifdef __cplusplus
}
#endif

#endif /* DAEDALUS_SURFACE_H */
