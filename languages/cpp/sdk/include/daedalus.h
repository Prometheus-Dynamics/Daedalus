/*
 * The Daedalus worker SDK for C (C11). A worker serves nodes over the wire protocol on its stdin and
 * stdout: it reads host packets, runs node functions, and writes the replies. Node output on stdout
 * is sent to stderr, so a printf cannot corrupt the stream.
 *
 * A type is a dd_type; a value is a dd_value built in an arena that the loop resets after each
 * run. Unsigned integers are exact: a u64 is a uint64_t, and a range check is explicit.
 */
#ifndef DAEDALUS_H
#define DAEDALUS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ---- types ---------------------------------------------------------------------------------- */

typedef enum {
    DD_K_SCALAR,
    DD_K_OPAQUE,
    DD_K_OPTIONAL,
    DD_K_LIST,
    DD_K_MAP,
    DD_K_TUPLE,
    DD_K_STRUCT,
    DD_K_ENUM
} dd_kind;

/* The scalars, in the wire's tag order. */
typedef enum {
    DD_UNIT,
    DD_BOOL,
    DD_I8,
    DD_I16,
    DD_I32,
    DD_I64,
    DD_ISIZE,
    DD_U8,
    DD_U16,
    DD_U32,
    DD_U64,
    DD_USIZE,
    DD_F32,
    DD_F64,
    DD_STRING,
    DD_BYTES
} dd_scalar;

typedef struct dd_type dd_type;
struct dd_type {
    dd_kind kind;
    dd_scalar scalar;               /* DD_K_SCALAR */
    const char *key;                /* DD_K_OPAQUE */
    const dd_type *const *items;    /* optional/list: [0]; map: [0] key, [1] value; tuple: all;
                                       struct: field types; enum: payload per variant, NULL if none */
    const char *const *names;       /* struct: field names; enum: variant names */
    size_t count;                   /* the number of items (or variants) */
};

/* Constructors for static type tables. */
#define DD_SCALAR(s) (&(dd_type){.kind = DD_K_SCALAR, .scalar = (s)})

/* ---- values --------------------------------------------------------------------------------- */

typedef struct dd_value dd_value;
struct dd_value {
    bool present;                   /* optional: whether there is a value */
    union {
        int64_t i;
        uint64_t u;
        double f;
        bool b;
        struct {
            const uint8_t *data;
            size_t len;
        } bytes;                    /* string, bytes, opaque */
        struct {
            dd_value *items;
            size_t count;
        } seq;                      /* list, tuple, struct (in field order), map (key, value pairs) */
        struct {
            size_t variant;
            dd_value *payload;      /* NULL when the variant has none */
        } en;
    } as;
};

/* ---- the node context ----------------------------------------------------------------------- */

/* What a node function gets: its arena, its progress and log sinks, and its state. */
typedef struct dd_ctx dd_ctx;

/* Allocates `size` bytes in the run's arena. Valid until the run ends. */
void *dd_alloc(dd_ctx *cx, size_t size);

/* Reports progress (0.0 to 1.0) and a message. Sent before the run's outputs, in order. */
void dd_progress(dd_ctx *cx, float fraction, const char *message);

/* Logs a line at a level: "error", "warn", "info", "debug" or "trace". */
void dd_log(dd_ctx *cx, const char *level, const char *text);

/* A node's state, kept in the worker between runs. Nodes with no state never touch it. */
dd_value *dd_state(dd_ctx *cx);
void dd_set_state(dd_ctx *cx, dd_value value);

/* A node function returns 0 for success. Nonzero is a node_error fault, and `reason` is sent. */
typedef int (*dd_node_fn)(dd_ctx *cx, const dd_value *inputs, dd_value *outputs);

typedef struct dd_port {
    const char *name;
    const dd_type *type;
} dd_port;

typedef struct dd_node {
    const char *id;
    const dd_port *inputs;
    size_t input_count;
    const dd_port *outputs;
    size_t output_count;
    const dd_type *state;           /* NULL for a node with no state */
    dd_node_fn run;
    /* A node's fault code and reason, set by the function through dd_fault. */
} dd_node;

/* Sets the fault a node reports when it returns nonzero. `code` is one of the closed fault codes:
 * "node_error", "bad_input", "internal". */
void dd_fault(dd_ctx *cx, const char *code, const char *reason);

/* ---- the worker loop ------------------------------------------------------------------------ */

/* Serves `nodes` until the host shuts the worker down or closes the pipe. Returns 0 on a clean end,
 * nonzero when the stream could not be read or written. */
int dd_serve(const dd_node *nodes, size_t count, const char *worker_name);

#ifdef __cplusplus
}
#endif

#endif /* DAEDALUS_H */
