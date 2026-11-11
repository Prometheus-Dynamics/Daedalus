/* Internal entry points for the SDK's own native tests. A worker does not call these: they run the
 * worker loop over given streams, and encode and decode one value of a type. */
#ifndef DAEDALUS_INTERNAL_H
#define DAEDALUS_INTERNAL_H

#include <stdio.h>

#include "daedalus.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Serves the worker protocol: reads packets from `in`, writes them to `packets`. */
int dd_serve_io(const dd_node *nodes, size_t count, FILE *in, FILE *packets, const char *worker_name);

/* Encodes one value of a type. On success `*out` holds `*len` bytes, freed by dd_internal_free_bytes. */
int dd_internal_encode(const dd_type *ty, const dd_value *v, unsigned char **out, size_t *len);

/* Decodes exactly `len` bytes as one value of a type. The value's pointers live in an arena that
 * `*keep` owns, freed by dd_internal_free_arena. Returns -1 if the bytes do not decode exactly. */
int dd_internal_decode(const dd_type *ty, const unsigned char *p, size_t len, dd_value *out, void **keep);

void dd_internal_free_bytes(unsigned char *p);
void dd_internal_free_arena(void *keep);

#ifdef __cplusplus
}
#endif

#endif /* DAEDALUS_INTERNAL_H */
