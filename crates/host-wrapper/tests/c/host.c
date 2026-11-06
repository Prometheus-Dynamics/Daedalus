/* A C program on the daedalus-surface C ABI, built and run by tests/c_host.rs: the handshake, a
 * host with the stable_abi plugin installed by path, a graph compiled from a document, ports
 * resolved to indices, and N frames of push, tick and take. It prints one line per frame: the
 * `total` output, or "none" when the frame has none. Any failing call prints its name, status and
 * message to stderr and exits 1.
 *
 * Usage: host <plugin library> <graph document (JSON file)> <frames> */

#include "daedalus_surface.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Message sink: the last message, kept for the error report. */
typedef struct {
  char text[1024];
} last_message;

static void keep_message(void *context, const uint8_t *text, size_t len) {
  last_message *message = context;
  size_t n = len < sizeof message->text - 1 ? len : sizeof message->text - 1;
  memcpy(message->text, text, n);
  message->text[n] = '\0';
}

static last_message message;

static void check(const char *call, daedalus_status status) {
  if (status != DAEDALUS_OK) {
    fprintf(stderr, "%s failed: status %" PRId32 ": %s\n", call, status, message.text);
    exit(1);
  }
}

static char *read_file(const char *path, size_t *len) {
  FILE *file = fopen(path, "rb");
  if (!file) {
    perror(path);
    exit(1);
  }
  fseek(file, 0, SEEK_END);
  long size = ftell(file);
  fseek(file, 0, SEEK_SET);
  char *text = malloc((size_t)size);
  if (!text || fread(text, 1, (size_t)size, file) != (size_t)size) {
    fprintf(stderr, "cannot read %s\n", path);
    exit(1);
  }
  fclose(file);
  *len = (size_t)size;
  return text;
}

static uint32_t port_index(daedalus_handle graph, const char *name, int input) {
  uint32_t index = 0;
  const uint8_t *bytes = (const uint8_t *)name;
  check(input ? "graph_input_index" : "graph_output_index",
        input ? daedalus_surface_graph_input_index(graph, bytes, strlen(name), &index)
              : daedalus_surface_graph_output_index(graph, bytes, strlen(name), &index));
  return index;
}

int main(int argc, char **argv) {
  if (argc != 4) {
    fprintf(stderr, "usage: %s <plugin> <graph.json> <frames>\n", argv[0]);
    return 2;
  }
  daedalus_message_sink sink = {&message, keep_message};

  daedalus_abi_version version;
  check("version", daedalus_surface_version(&version));
  if (version.major != DAEDALUS_SURFACE_ABI_MAJOR || version.minor < DAEDALUS_SURFACE_ABI_MINOR) {
    fprintf(stderr, "surface %u.%u, built against %u.%u\n", version.major, version.minor,
            DAEDALUS_SURFACE_ABI_MAJOR, DAEDALUS_SURFACE_ABI_MINOR);
    return 1;
  }

  daedalus_handle host = 0;
  check("host_create_with", daedalus_surface_host_create_with(&host, sink));
  const char *plugin = argv[1];
  check("host_install",
        daedalus_surface_host_install(host, (const uint8_t *)plugin, strlen(plugin), sink));

  size_t json_len = 0;
  char *json = read_file(argv[2], &json_len);
  daedalus_handle graph = 0;
  check("graph_compile",
        daedalus_surface_graph_compile(host, (const uint8_t *)json, json_len, &graph, sink));
  free(json);

  uint32_t step = port_index(graph, "step", 1);
  uint32_t period = port_index(graph, "period", 1);
  uint32_t total = port_index(graph, "total", 0);

  long frames = strtol(argv[3], NULL, 10);
  for (long i = 0; i < frames; i++) {
    daedalus_push_entry pushes[2] = {
        {.port = step, .kind = DAEDALUS_PUSH_I64, .int_value = (i * 7) % 11 - 5},
        {.port = period, .kind = DAEDALUS_PUSH_I64, .int_value = 3},
    };
    size_t done = 0;
    check("push_batch", daedalus_surface_push_batch(graph, pushes, 2, &done));
    check("tick", daedalus_surface_tick(graph, sink));
    int64_t value = 0;
    daedalus_status status = daedalus_surface_take_i64_at(graph, total, &value);
    if (status == DAEDALUS_AGAIN) {
      printf("none\n");
    } else {
      check("take_i64_at", status);
      printf("%" PRId64 "\n", value);
    }
  }

  check("graph_release", daedalus_surface_graph_release(graph));
  check("host_release", daedalus_surface_host_release(host));
  return 0;
}
