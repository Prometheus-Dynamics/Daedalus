/* The Daedalus worker SDK for C: the wire codec, the arena, and the worker loop. See daedalus.h.
 *
 * Progress and log events are written to the packet stream as the node reports them, so they go
 * out in order before the run's outputs. A node's values live in the run's arena, which is freed
 * when the run ends: a value a node keeps across runs must be copied out by the node.
 */

#ifndef _WIN32
#define _POSIX_C_SOURCE 200809L
#endif

#include "daedalus.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <fcntl.h>
#include <io.h>
#define DD_DUP _dup
#define DD_DUP2 _dup2
#define DD_FILENO _fileno
#define DD_FDOPEN _fdopen
#else
#include <unistd.h>
#define DD_DUP dup
#define DD_DUP2 dup2
#define DD_FILENO fileno
#define DD_FDOPEN fdopen
#endif

#define DD_PROTOCOL 1u
#define DD_REASON_CAP 512u
#define DD_CAP (16u * 1024u * 1024u)
#define DD_MAX_DEPTH 32
#define DD_LAYOUT_HASH 0xebbd19e914919865ULL
#define DD_MAX_NODES 64

enum {
    KIND_RUN = 1,
    KIND_CANCEL = 2,
    KIND_STATE_EXPORT = 3,
    KIND_STATE_IMPORT = 4,
    KIND_PING = 5,
    KIND_SHUTDOWN = 6,
    KIND_HELLO = 0x40,
    KIND_OUTPUTS = 0x41,
    KIND_PROGRESS = 0x42,
    KIND_LOG = 0x43,
    KIND_FAULT = 0x44,
    KIND_STATE_VALUE = 0x45,
    KIND_PONG = 0x46
};

/* ---- arena ---------------------------------------------------------------------------------- */

typedef struct dd_chunk {
    struct dd_chunk *next;
    size_t used;
    size_t cap;
    unsigned char data[];
} dd_chunk;

typedef struct {
    dd_chunk *head;
    int failed;
} dd_arena;

static void *arena_alloc(dd_arena *arena, size_t size) {
    size_t aligned = (size + 15u) & ~(size_t)15u;
    if (aligned == 0) aligned = 16;
    if (!arena->head || arena->head->used + aligned > arena->head->cap) {
        size_t cap = aligned > 65536u ? aligned : 65536u;
        dd_chunk *chunk = malloc(sizeof(dd_chunk) + cap);
        if (!chunk) {
            arena->failed = 1;
            return NULL;
        }
        chunk->next = arena->head;
        chunk->used = 0;
        chunk->cap = cap;
        arena->head = chunk;
    }
    void *out = arena->head->data + arena->head->used;
    arena->head->used += aligned;
    return out;
}

static void arena_reset(dd_arena *arena) {
    dd_chunk *chunk = arena->head;
    while (chunk) {
        dd_chunk *next = chunk->next;
        free(chunk);
        chunk = next;
    }
    arena->head = NULL;
}

/* ---- growable buffer ----------------------------------------------------------------------- */

typedef struct {
    unsigned char *p;
    size_t len;
    size_t cap;
    int failed;
} dd_buf;

static void buf_put(dd_buf *b, const void *data, size_t n) {
    if (b->failed) return;
    if (b->len + n > b->cap) {
        size_t want = b->cap ? b->cap : 256;
        while (want < b->len + n) want *= 2;
        unsigned char *grown = realloc(b->p, want);
        if (!grown) {
            b->failed = 1;
            return;
        }
        b->p = grown;
        b->cap = want;
    }
    if (n) memcpy(b->p + b->len, data, n);
    b->len += n;
}

static void buf_free(dd_buf *b) {
    free(b->p);
    b->p = NULL;
    b->len = 0;
    b->cap = 0;
}

static void put_u8(dd_buf *b, uint8_t v) { buf_put(b, &v, 1); }

static void put_le(dd_buf *b, uint64_t v, int bytes) {
    unsigned char raw[8];
    for (int i = 0; i < bytes; i++) raw[i] = (unsigned char)(v >> (8 * i));
    buf_put(b, raw, (size_t)bytes);
}

static void put_u32(dd_buf *b, uint32_t v) { put_le(b, v, 4); }

static void put_bytes(dd_buf *b, const void *data, size_t n) {
    put_u32(b, (uint32_t)n);
    buf_put(b, data, n);
}

static void put_str(dd_buf *b, const char *text) { put_bytes(b, text, strlen(text)); }

/* ---- reader --------------------------------------------------------------------------------- */

typedef struct {
    const unsigned char *p;
    size_t len;
    size_t pos;
} dd_in;

static int in_take(dd_in *in, size_t n, const unsigned char **out) {
    if (n > in->len - in->pos) return -1;
    *out = in->p + in->pos;
    in->pos += n;
    return 0;
}

static int in_le(dd_in *in, int bytes, uint64_t *out) {
    const unsigned char *raw;
    if (in_take(in, (size_t)bytes, &raw) != 0) return -1;
    uint64_t v = 0;
    for (int i = 0; i < bytes; i++) v |= (uint64_t)raw[i] << (8 * i);
    *out = v;
    return 0;
}

/* A count is checked against the bytes left: each item takes at least one byte. */
static int in_count(dd_in *in, uint32_t *out) {
    uint64_t v;
    if (in_le(in, 4, &v) != 0) return -1;
    if (v > in->len - in->pos) return -1;
    *out = (uint32_t)v;
    return 0;
}

static int in_bytes(dd_in *in, const unsigned char **data, size_t *len) {
    uint32_t n;
    if (in_count(in, &n) != 0) return -1;
    *len = n;
    return in_take(in, n, data);
}

/* ---- types on the wire ---------------------------------------------------------------------- */

static void encode_type(dd_buf *b, const dd_type *ty, int depth) {
    if (depth > DD_MAX_DEPTH) {
        b->failed = 1;
        return;
    }
    switch (ty->kind) {
    case DD_K_SCALAR:
        put_u8(b, 0);
        put_u8(b, (uint8_t)ty->scalar);
        break;
    case DD_K_OPAQUE:
        put_u8(b, 1);
        put_str(b, ty->key);
        break;
    case DD_K_OPTIONAL:
        put_u8(b, 2);
        encode_type(b, ty->items[0], depth + 1);
        break;
    case DD_K_LIST:
        put_u8(b, 3);
        encode_type(b, ty->items[0], depth + 1);
        break;
    case DD_K_MAP:
        put_u8(b, 4);
        encode_type(b, ty->items[0], depth + 1);
        encode_type(b, ty->items[1], depth + 1);
        break;
    case DD_K_TUPLE:
        put_u8(b, 5);
        put_u32(b, (uint32_t)ty->count);
        for (size_t i = 0; i < ty->count; i++) encode_type(b, ty->items[i], depth + 1);
        break;
    case DD_K_STRUCT:
        put_u8(b, 6);
        put_u32(b, (uint32_t)ty->count);
        for (size_t i = 0; i < ty->count; i++) {
            put_str(b, ty->names[i]);
            encode_type(b, ty->items[i], depth + 1);
        }
        break;
    case DD_K_ENUM:
        put_u8(b, 7);
        put_u32(b, (uint32_t)ty->count);
        for (size_t i = 0; i < ty->count; i++) {
            put_str(b, ty->names[i]);
            if (ty->items[i]) {
                put_u8(b, 1);
                encode_type(b, ty->items[i], depth + 1);
            } else {
                put_u8(b, 0);
            }
        }
        break;
    }
}

/* ---- values on the wire --------------------------------------------------------------------- */

/* Writes a scalar value. -1 when the value does not fit its type. */
static int encode_scalar(const dd_type *ty, const dd_value *v, dd_buf *b) {
    switch (ty->scalar) {
    case DD_UNIT: return 0;
    case DD_BOOL:
        put_u8(b, v->as.b ? 1 : 0);
        return 0;
    case DD_I8:
        if (v->as.i < -128 || v->as.i > 127) return -1;
        put_le(b, (uint64_t)v->as.i, 1);
        return 0;
    case DD_I16:
        if (v->as.i < -32768 || v->as.i > 32767) return -1;
        put_le(b, (uint64_t)v->as.i, 2);
        return 0;
    case DD_I32:
        if (v->as.i < -2147483648LL || v->as.i > 2147483647LL) return -1;
        put_le(b, (uint64_t)v->as.i, 4);
        return 0;
    case DD_I64:
    case DD_ISIZE:
        put_le(b, (uint64_t)v->as.i, 8);
        return 0;
    case DD_U8:
        if (v->as.u > 255u) return -1;
        put_le(b, v->as.u, 1);
        return 0;
    case DD_U16:
        if (v->as.u > 65535u) return -1;
        put_le(b, v->as.u, 2);
        return 0;
    case DD_U32:
        if (v->as.u > 4294967295u) return -1;
        put_le(b, v->as.u, 4);
        return 0;
    case DD_U64:
    case DD_USIZE:
        put_le(b, v->as.u, 8);
        return 0;
    case DD_F32: {
        float f = (float)v->as.f;
        uint32_t raw;
        memcpy(&raw, &f, 4);
        put_le(b, raw, 4);
        return 0;
    }
    case DD_F64: {
        uint64_t raw;
        memcpy(&raw, &v->as.f, 8);
        put_le(b, raw, 8);
        return 0;
    }
    case DD_STRING:
    case DD_BYTES:
        put_bytes(b, v->as.bytes.data, v->as.bytes.len);
        return 0;
    }
    return -1;
}

static int encode_value(const dd_type *ty, const dd_value *v, dd_buf *b, int depth) {
    if (depth > DD_MAX_DEPTH) return -1;
    switch (ty->kind) {
    case DD_K_SCALAR:
        return encode_scalar(ty, v, b);
    case DD_K_OPAQUE:
        put_bytes(b, v->as.bytes.data, v->as.bytes.len);
        return 0;
    case DD_K_OPTIONAL:
        if (!v->present) {
            put_u8(b, 0);
            return 0;
        }
        put_u8(b, 1);
        return encode_value(ty->items[0], v->as.seq.items, b, depth + 1);
    case DD_K_LIST:
        put_u32(b, (uint32_t)v->as.seq.count);
        for (size_t i = 0; i < v->as.seq.count; i++) {
            if (encode_value(ty->items[0], &v->as.seq.items[i], b, depth + 1) != 0) return -1;
        }
        return 0;
    case DD_K_MAP:
        /* A map's value is its key-value pairs, flattened: key, value, key, value. */
        put_u32(b, (uint32_t)(v->as.seq.count / 2));
        for (size_t i = 0; i < v->as.seq.count; i++) {
            const dd_type *item = (i % 2 == 0) ? ty->items[0] : ty->items[1];
            if (encode_value(item, &v->as.seq.items[i], b, depth + 1) != 0) return -1;
        }
        return 0;
    case DD_K_TUPLE:
    case DD_K_STRUCT:
        if (v->as.seq.count != ty->count) return -1;
        for (size_t i = 0; i < ty->count; i++) {
            if (encode_value(ty->items[i], &v->as.seq.items[i], b, depth + 1) != 0) return -1;
        }
        return 0;
    case DD_K_ENUM: {
        size_t variant = v->as.en.variant;
        if (variant >= ty->count) return -1;
        put_u32(b, (uint32_t)variant);
        if (ty->items[variant]) {
            if (!v->as.en.payload) return -1;
            return encode_value(ty->items[variant], v->as.en.payload, b, depth + 1);
        }
        return 0;
    }
    }
    return -1;
}

static int decode_value(const dd_type *ty, dd_in *in, dd_arena *arena, dd_value *out, int depth);

static int decode_scalar(const dd_type *ty, dd_in *in, dd_value *out) {
    uint64_t raw;
    switch (ty->scalar) {
    case DD_UNIT:
        return 0;
    case DD_BOOL:
        if (in_le(in, 1, &raw) != 0 || raw > 1) return -1;
        out->as.b = raw == 1;
        return 0;
    case DD_I8:
        if (in_le(in, 1, &raw) != 0) return -1;
        out->as.i = (int8_t)raw;
        return 0;
    case DD_I16:
        if (in_le(in, 2, &raw) != 0) return -1;
        out->as.i = (int16_t)raw;
        return 0;
    case DD_I32:
        if (in_le(in, 4, &raw) != 0) return -1;
        out->as.i = (int32_t)raw;
        return 0;
    case DD_I64:
    case DD_ISIZE:
        if (in_le(in, 8, &raw) != 0) return -1;
        out->as.i = (int64_t)raw;
        return 0;
    case DD_U8:
        if (in_le(in, 1, &raw) != 0) return -1;
        out->as.u = raw;
        return 0;
    case DD_U16:
        if (in_le(in, 2, &raw) != 0) return -1;
        out->as.u = raw;
        return 0;
    case DD_U32:
        if (in_le(in, 4, &raw) != 0) return -1;
        out->as.u = raw;
        return 0;
    case DD_U64:
    case DD_USIZE:
        if (in_le(in, 8, &raw) != 0) return -1;
        out->as.u = raw;
        return 0;
    case DD_F32: {
        uint32_t bits;
        float f;
        if (in_le(in, 4, &raw) != 0) return -1;
        bits = (uint32_t)raw;
        memcpy(&f, &bits, 4);
        out->as.f = f;
        return 0;
    }
    case DD_F64:
        if (in_le(in, 8, &raw) != 0) return -1;
        memcpy(&out->as.f, &raw, 8);
        return 0;
    case DD_STRING:
    case DD_BYTES: {
        const unsigned char *data;
        size_t len;
        if (in_bytes(in, &data, &len) != 0) return -1;
        out->as.bytes.data = data;
        out->as.bytes.len = len;
        return 0;
    }
    }
    return -1;
}

static int decode_value(const dd_type *ty, dd_in *in, dd_arena *arena, dd_value *out, int depth) {
    if (depth > DD_MAX_DEPTH) return -1;
    switch (ty->kind) {
    case DD_K_SCALAR:
        return decode_scalar(ty, in, out);
    case DD_K_OPAQUE: {
        const unsigned char *data;
        size_t len;
        if (in_bytes(in, &data, &len) != 0) return -1;
        out->as.bytes.data = data;
        out->as.bytes.len = len;
        return 0;
    }
    case DD_K_OPTIONAL: {
        uint64_t flag;
        dd_value *inner;
        if (in_le(in, 1, &flag) != 0 || flag > 1) return -1;
        out->present = flag == 1;
        if (!out->present) return 0;
        inner = arena_alloc(arena, sizeof *inner);
        if (!inner) return -1;
        memset(inner, 0, sizeof *inner);
        if (decode_value(ty->items[0], in, arena, inner, depth + 1) != 0) return -1;
        out->as.seq.items = inner;
        return 0;
    }
    case DD_K_LIST:
    case DD_K_MAP:
    case DD_K_TUPLE:
    case DD_K_STRUCT: {
        size_t count;
        dd_value *items;
        if (ty->kind == DD_K_TUPLE || ty->kind == DD_K_STRUCT) {
            count = ty->count;
        } else {
            uint32_t n;
            if (in_count(in, &n) != 0) return -1;
            count = n;
            if (ty->kind == DD_K_MAP) count *= 2;
        }
        items = count ? arena_alloc(arena, sizeof(dd_value) * count) : NULL;
        if (count && !items) return -1;
        for (size_t i = 0; i < count; i++) {
            const dd_type *item;
            if (ty->kind == DD_K_MAP) item = ty->items[i % 2];
            else if (ty->kind == DD_K_LIST) item = ty->items[0];
            else item = ty->items[i];
            memset(&items[i], 0, sizeof items[i]);
            if (decode_value(item, in, arena, &items[i], depth + 1) != 0) return -1;
        }
        out->as.seq.items = items;
        out->as.seq.count = count;
        return 0;
    }
    case DD_K_ENUM: {
        uint64_t variant;
        dd_value *payload;
        if (in_le(in, 4, &variant) != 0 || variant >= ty->count) return -1;
        out->as.en.variant = (size_t)variant;
        out->as.en.payload = NULL;
        if (!ty->items[variant]) return 0;
        payload = arena_alloc(arena, sizeof *payload);
        if (!payload) return -1;
        memset(payload, 0, sizeof *payload);
        if (decode_value(ty->items[variant], in, arena, payload, depth + 1) != 0) return -1;
        out->as.en.payload = payload;
        return 0;
    }
    }
    return -1;
}

/* ---- packets -------------------------------------------------------------------------------- */

/* Reads exactly n bytes. Returns 1 when read, 0 at a clean end (only when allow_eof and nothing was
 * read), or -1 on a short read. */
static int read_exact(FILE *in, unsigned char *dst, size_t n, int allow_eof) {
    size_t got = 0;
    while (got < n) {
        size_t r = fread(dst + got, 1, n - got, in);
        if (r == 0) return (allow_eof && got == 0) ? 0 : -1;
        got += r;
    }
    return 1;
}

/* One packet's body into `body`: 1 when read, 0 at a clean end, -1 on an error or an over-cap length. */
static int read_packet(FILE *in, dd_buf *body) {
    unsigned char header[4];
    uint64_t len;
    int r = read_exact(in, header, 4, 1);
    if (r <= 0) return r;
    len = (uint64_t)header[0] | ((uint64_t)header[1] << 8) | ((uint64_t)header[2] << 16) | ((uint64_t)header[3] << 24);
    if (len > DD_CAP) return -1;
    body->len = 0;
    buf_put(body, NULL, 0);
    if (body->failed) return -1;
    if (body->cap < len) {
        unsigned char *grown = realloc(body->p, len ? (size_t)len : 1);
        if (!grown) return -1;
        body->p = grown;
        body->cap = (size_t)len;
    }
    if (read_exact(in, body->p, (size_t)len, 0) != 1) return -1;
    body->len = (size_t)len;
    return 1;
}

static int write_packet(FILE *out, const dd_buf *body) {
    unsigned char header[4];
    uint32_t len = (uint32_t)body->len;
    header[0] = (unsigned char)len;
    header[1] = (unsigned char)(len >> 8);
    header[2] = (unsigned char)(len >> 16);
    header[3] = (unsigned char)(len >> 24);
    if (fwrite(header, 1, 4, out) != 4) return -1;
    if (body->len && fwrite(body->p, 1, body->len, out) != body->len) return -1;
    return fflush(out) == 0 ? 0 : -1;
}

/* ---- the context ---------------------------------------------------------------------------- */

struct dd_ctx {
    dd_arena *arena;
    FILE *packets;
    uint64_t run;
    int has_fault;
    const char *fault_code;
    char fault_reason[DD_REASON_CAP + 1];
    dd_value *state;      /* the node's state, NULL when it has none */
    dd_value state_store; /* backing store for a state set this run */
    int state_set;
};

void *dd_alloc(dd_ctx *cx, size_t size) { return arena_alloc(cx->arena, size); }

void dd_fault(dd_ctx *cx, const char *code, const char *reason) {
    size_t n = strlen(reason);
    if (n > DD_REASON_CAP) n = DD_REASON_CAP;
    cx->fault_code = code;
    memcpy(cx->fault_reason, reason, n);
    cx->fault_reason[n] = '\0';
    cx->has_fault = 1;
}

void dd_progress(dd_ctx *cx, float fraction, const char *message) {
    dd_buf b = {0};
    uint32_t raw;
    put_u8(&b, KIND_PROGRESS);
    put_le(&b, cx->run, 8);
    put_u32(&b, 0);
    put_str(&b, message);
    put_u8(&b, 1);
    memcpy(&raw, &fraction, 4);
    put_le(&b, raw, 4);
    write_packet(cx->packets, &b);
    buf_free(&b);
}

void dd_log(dd_ctx *cx, const char *level, const char *text) {
    static const char *const levels[] = {"error", "warn", "info", "debug", "trace"};
    uint8_t tag = 2;
    dd_buf b = {0};
    for (uint8_t i = 0; i < 5; i++) {
        if (strcmp(level, levels[i]) == 0) tag = i;
    }
    put_u8(&b, KIND_LOG);
    put_u8(&b, tag);
    put_str(&b, text);
    write_packet(cx->packets, &b);
    buf_free(&b);
}

dd_value *dd_state(dd_ctx *cx) { return cx->state; }

void dd_set_state(dd_ctx *cx, dd_value value) {
    cx->state_store = value;
    cx->state = &cx->state_store;
    cx->state_set = 1;
}

/* ---- faults and the loop -------------------------------------------------------------------- */

static uint16_t fault_number(const char *code) {
    static const struct {
        const char *name;
        uint16_t number;
    } table[] = {
        {"node_error", 1}, {"node_panic", 2}, {"bad_input", 3}, {"unknown_node", 4},
        {"unsupported", 5}, {"internal", 6}, {"malformed_packet", 7},
    };
    for (size_t i = 0; i < sizeof table / sizeof table[0]; i++) {
        if (strcmp(table[i].name, code) == 0) return table[i].number;
    }
    return 6;
}

static void send_fault(FILE *out, uint64_t run, const char *node, const char *code, const char *reason) {
    dd_buf b = {0};
    size_t n = strlen(reason);
    if (n > DD_REASON_CAP) n = DD_REASON_CAP;
    /* Cut at a character boundary. */
    while (n > 0 && n < strlen(reason) && (((unsigned char)reason[n]) & 0xC0) == 0x80) n--;
    put_u8(&b, KIND_FAULT);
    put_le(&b, run, 8);
    if (node) {
        put_u8(&b, 1);
        put_str(&b, node);
    } else {
        put_u8(&b, 0);
    }
    put_le(&b, fault_number(code), 2);
    put_bytes(&b, reason, n);
    write_packet(out, &b);
    buf_free(&b);
}

/* Ends a run that cannot continue: its fault, then outputs with status fault, so the host's wait
 * for the run's outputs always ends. */
static void fail_run(FILE *out, uint64_t run, const char *node, const char *code, const char *reason) {
    dd_buf b = {0};
    send_fault(out, run, node, code, reason);
    put_u8(&b, KIND_OUTPUTS);
    put_le(&b, run, 8);
    put_u8(&b, 2);
    put_u32(&b, 0);
    write_packet(out, &b);
    buf_free(&b);
}

/* Per-node state for the loop: the stored value, and whether there is one. */
typedef struct {
    dd_value value;
    int present;
} dd_slot;

/* The inputs a run carries, by node and port: decoded into the arena. */
typedef struct {
    int node;
    int port;
    dd_value value;
    int present;
} dd_bound;

static const dd_node *find_node(const dd_node *nodes, size_t count, const unsigned char *id, size_t len, int *index) {
    for (size_t i = 0; i < count; i++) {
        if (strlen(nodes[i].id) == len && memcmp(nodes[i].id, id, len) == 0) {
            *index = (int)i;
            return &nodes[i];
        }
    }
    return NULL;
}

static int find_port(const dd_port *ports, size_t count, const unsigned char *name, size_t len) {
    for (size_t i = 0; i < count; i++) {
        if (strlen(ports[i].name) == len && memcmp(ports[i].name, name, len) == 0) return (int)i;
    }
    return -1;
}

static void send_outputs(FILE *out, uint64_t run, int status, const dd_node *nodes, const int *node_ids,
                         size_t node_count, dd_value (*results)[DD_MAX_NODES][8]) {
    dd_buf b = {0};
    size_t total = 0;
    if (status == 0) {
        for (size_t n = 0; n < node_count; n++) total += nodes[node_ids[n]].output_count;
    }
    put_u8(&b, KIND_OUTPUTS);
    put_le(&b, run, 8);
    put_u8(&b, (uint8_t)(status == 0 ? 0 : 2));
    put_u32(&b, (uint32_t)total);
    if (status == 0) {
        for (size_t n = 0; n < node_count; n++) {
            const dd_node *node = &nodes[node_ids[n]];
            for (size_t p = 0; p < node->output_count; p++) {
                put_str(&b, node->id);
                put_str(&b, node->outputs[p].name);
                if (encode_value(node->outputs[p].type, &(*results)[n][p], &b, 0) != 0) {
                    /* A value that does not fit its declared type: report a fault in its place. */
                    b.len = 0;
                    b.failed = 0;
                    put_u8(&b, KIND_OUTPUTS);
                    put_le(&b, run, 8);
                    put_u8(&b, 2);
                    put_u32(&b, 0);
                    break;
                }
            }
        }
    }
    write_packet(out, &b);
    buf_free(&b);
}

static int build_hello(dd_buf *hello, const dd_node *nodes, size_t count, const char *worker_name) {
    put_u8(hello, KIND_HELLO);
    put_u32(hello, DD_PROTOCOL);
    put_le(hello, DD_LAYOUT_HASH, 8);
    put_str(hello, worker_name);
    put_u32(hello, DD_CAP);
    put_u32(hello, (uint32_t)count);
    for (size_t i = 0; i < count; i++) {
        const dd_node *node = &nodes[i];
        put_str(hello, node->id);
        put_u32(hello, (uint32_t)node->input_count);
        for (size_t p = 0; p < node->input_count; p++) {
            put_str(hello, node->inputs[p].name);
            encode_type(hello, node->inputs[p].type, 0);
            put_u8(hello, 0);
        }
        put_u32(hello, (uint32_t)node->output_count);
        for (size_t p = 0; p < node->output_count; p++) {
            put_str(hello, node->outputs[p].name);
            encode_type(hello, node->outputs[p].type, 0);
            put_u8(hello, 0);
        }
        put_u32(hello, 0); /* no config ports */
        if (node->state) {
            put_u8(hello, 1);
            encode_type(hello, node->state, 0);
        } else {
            put_u8(hello, 0);
        }
        put_u32(hello, 0); /* no capabilities */
    }
    return hello->failed ? -1 : 0;
}

/* Runs one run message. Returns 0 on success, -1 when the stream can no longer be answered. */
static int handle_run(const dd_node *nodes, size_t count, dd_in *in, FILE *out, dd_arena *arena,
                      dd_slot *states) {
    uint64_t run;
    uint64_t region;
    uint32_t node_count;
    uint32_t input_count;
    int node_ids[DD_MAX_NODES];
    size_t picked = 0;
    dd_bound bound[DD_MAX_NODES * 8];
    size_t bound_count = 0;
    static dd_value results[DD_MAX_NODES][8];
    int status = 0;

    if (in_le(in, 8, &run) != 0 || in_le(in, 4, &region) != 0 || in_count(in, &node_count) != 0) {
        send_fault(out, 0, NULL, "malformed_packet", "the run message does not decode");
        return 0;
    }
    if (node_count > DD_MAX_NODES) {
        fail_run(out, run, NULL, "unsupported", "a run has more nodes than the SDK serves at once");
        return 0;
    }
    for (uint32_t i = 0; i < node_count; i++) {
        const unsigned char *id;
        size_t len;
        int index = -1;
        if (in_bytes(in, &id, &len) != 0) {
            fail_run(out, run, NULL, "malformed_packet", "a node id does not decode");
            return 0;
        }
        if (!find_node(nodes, count, id, len, &index)) {
            send_fault(out, run, NULL, "unknown_node", "no such node");
            status = -2;
        }
        node_ids[picked++] = index;
    }
    if (in_count(in, &input_count) != 0) {
        fail_run(out, run, NULL, "malformed_packet", "the inputs do not decode");
        return 0;
    }
    for (uint32_t i = 0; i < input_count; i++) {
        const unsigned char *node_name, *port_name;
        size_t node_len, port_len;
        int node_index = -1;
        int port_index;
        const dd_node *node;
        dd_value value;
        if (in_bytes(in, &node_name, &node_len) != 0 || in_bytes(in, &port_name, &port_len) != 0) {
            fail_run(out, run, NULL, "malformed_packet", "an input does not decode");
            return 0;
        }
        node = find_node(nodes, count, node_name, node_len, &node_index);
        port_index = node ? find_port(node->inputs, node->input_count, port_name, port_len) : -1;
        if (!node || port_index < 0) {
            fail_run(out, run, NULL, "bad_input", "an input names no declared port");
            return 0;
        }
        memset(&value, 0, sizeof value);
        if (decode_value(node->inputs[port_index].type, in, arena, &value, 0) != 0) {
            fail_run(out, run, node->id, "malformed_packet", "an input value does not decode");
            return 0;
        }
        if (bound_count < sizeof bound / sizeof bound[0]) {
            bound[bound_count].node = node_index;
            bound[bound_count].port = port_index;
            bound[bound_count].value = value;
            bound[bound_count].present = 1;
            bound_count++;
        }
    }
    if (in->pos != in->len) {
        fail_run(out, run, NULL, "malformed_packet", "bytes after the run message");
        return 0;
    }
    if (status == -2) {
        send_outputs(out, run, 1, nodes, node_ids, picked, &results);
        return 0;
    }

    for (size_t n = 0; n < picked && status == 0; n++) {
        const dd_node *node = &nodes[node_ids[n]];
        dd_value inputs[8];
        dd_ctx cx;
        int code;
        memset(inputs, 0, sizeof inputs);
        memset(&cx, 0, sizeof cx);
        for (size_t p = 0; p < node->input_count && p < 8; p++) {
            int found = 0;
            for (size_t b = 0; b < bound_count; b++) {
                if (bound[b].node == node_ids[n] && bound[b].port == (int)p) {
                    inputs[p] = bound[b].value;
                    found = 1;
                }
            }
            if (!found) {
                send_fault(out, run, node->id, "bad_input", "a declared input has no value");
                status = 1;
                break;
            }
        }
        if (status != 0) break;
        if (node->output_count > 8) {
            send_fault(out, run, node->id, "unsupported", "a node has more than eight outputs");
            status = 1;
            break;
        }
        cx.arena = arena;
        cx.packets = out;
        cx.run = run;
        if (node->state && states[node_ids[n]].present) {
            cx.state = &states[node_ids[n]].value;
        } else {
            cx.state = NULL;
        }
        memset(results[n], 0, sizeof results[n]);
        code = node->run(&cx, inputs, results[n]);
        if (code != 0) {
            if (cx.has_fault) send_fault(out, run, node->id, cx.fault_code, cx.fault_reason);
            else send_fault(out, run, node->id, "node_error", "the node failed");
            status = 1;
            break;
        }
        if (node->state && cx.state_set) {
            states[node_ids[n]].value = cx.state_store;
            states[node_ids[n]].present = 1;
        }
    }
    send_outputs(out, run, status, nodes, node_ids, picked, &results);
    return 0;
}

int dd_serve(const dd_node *nodes, size_t count, const char *worker_name) {
    dd_buf hello = {0};
    dd_buf body = {0};
    dd_arena arena = {NULL, 0};
    dd_slot states[DD_MAX_NODES];
    FILE *packets = NULL;
    int packet_fd;
    int status = 0;

    if (count > DD_MAX_NODES) return -1;
#ifdef _WIN32
    _setmode(_fileno(stdin), _O_BINARY);
    _setmode(_fileno(stdout), _O_BINARY);
#endif
    /* Packets go to a copy of stdout. Stdout itself becomes stderr, so a printf cannot corrupt them. */
    packet_fd = DD_DUP(DD_FILENO(stdout));
    if (packet_fd < 0) return -1;
    if (DD_DUP2(DD_FILENO(stderr), DD_FILENO(stdout)) < 0) return -1;
    packets = DD_FDOPEN(packet_fd, "wb");
    if (!packets) return -1;
    memset(states, 0, sizeof states);

    if (build_hello(&hello, nodes, count, worker_name) != 0 || write_packet(packets, &hello) != 0) {
        status = -1;
    }
    buf_free(&hello);

    while (status == 0) {
        int r = read_packet(stdin, &body);
        dd_in in;
        uint8_t kind;
        if (r == 0) break;
        if (r < 0) {
            status = -1;
            break;
        }
        if (body.len == 0) continue;
        kind = body.p[0];
        in.p = body.p;
        in.len = body.len;
        in.pos = 1;
        if (kind == KIND_PING) {
            dd_buf pong = {0};
            put_u8(&pong, KIND_PONG);
            write_packet(packets, &pong);
            buf_free(&pong);
        } else if (kind == KIND_SHUTDOWN) {
            break;
        } else if (kind == KIND_RUN) {
            arena_reset(&arena);
            handle_run(nodes, count, &in, packets, &arena, states);
            arena_reset(&arena);
        } else if (kind != KIND_CANCEL && kind != KIND_STATE_EXPORT && kind != KIND_STATE_IMPORT) {
            /* Not a message this worker reads: answer with a fault, so the host never waits for nothing. */
            send_fault(packets, 0, NULL, "malformed_packet", "unknown message kind");
        }
        /* A cancel between runs has nothing to stop; state messages are not served by the C SDK. */
    }
    buf_free(&body);
    arena_reset(&arena);
    fclose(packets);
    return status;
}
