/* A C caller of the client surface: connects to a daemon, loads the plugin, stores a graph over the
 * plugin's bytes node, starts a run, feeds bytes, steps, takes them back with a size query, and
 * stops and closes. Prints "c client: ok <bytes>" and exits 0 when the bytes come back. The
 * behaviour checks are in the Rust tests of the surface. */
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "daedalus_client.h"

static const char GRAPH[] =
    "{\"format\": \"daedalus.graph\", \"schema_version\": 1, \"graph\": {"
    "\"nodes\": ["
    "{\"id\": \"host\", \"kind\": \"host_bridge\", \"port_types\": {\"in\": \"bytes\", \"out\": \"bytes\"}},"
    "{\"id\": \"fixture.pass_bytes\"}],"
    "\"edges\": ["
    "{\"from\": {\"node\": 0, \"port\": \"in\"}, \"to\": {\"node\": 1, \"port\": \"in\"}},"
    "{\"from\": {\"node\": 1, \"port\": \"out\"}, \"to\": {\"node\": 0, \"port\": \"out\"}}]}}";

static const uint8_t *text(const char *s) { return (const uint8_t *)s; }

static void report(const char *what, daedalus_client_status_t status) {
    char why[512];
    size_t len = 0;
    fprintf(stderr, "FAILED: %s: %s\n", what, daedalus_client_status_name(status));
    if (daedalus_client_last_error((uint8_t *)why, sizeof why, &len) == DAEDALUS_CLIENT_OK) {
        fprintf(stderr, "  %.*s\n", (int)len, why);
    }
}

#define CHECK(call)                                  \
    do {                                             \
        daedalus_client_status_t status_ = (call);   \
        if (status_ != DAEDALUS_CLIENT_OK) {         \
            report(#call, status_);                  \
            return 1;                                \
        }                                            \
    } while (0)

int main(int argc, char **argv) {
    if (argc != 4) {
        fprintf(stderr, "usage: caller <tcp:host:port> <plugin path> <bytes>\n");
        return 2;
    }
    const char *endpoint = argv[1], *plugin = argv[2], *payload = argv[3];
    size_t payload_len = strlen(payload);

    daedalus_client_handle_t client = 0;
    CHECK(daedalus_client_connect(text(endpoint), strlen(endpoint), 0, &client));
    CHECK(daedalus_client_load_plugin(client, text(plugin), strlen(plugin)));
    CHECK(daedalus_client_put_graph(client, text("g"), 1, text(GRAPH), strlen(GRAPH)));

    daedalus_client_config_t config;
    config.fifo_capacity = 0;
    config.fault_reports = 0;
    config.telemetry = DAEDALUS_CLIENT_TELEMETRY_BASIC;
    config.unmerged = 0;
    config.priority = NULL;
    config.priority_len = 0;
    uint64_t run = 0;
    CHECK(daedalus_client_start(client, text("g"), 1, &config, &run));

    CHECK(daedalus_client_feed_bytes(client, run, text("in"), 2, text(payload), payload_len, NULL));
    daedalus_client_step_t step;
    CHECK(daedalus_client_step(client, run, 1, &step));
    if (step.ticks != 1) {
        fprintf(stderr, "FAILED: one tick was asked for, %u ran\n", (unsigned)step.ticks);
        return 1;
    }

    /* The size first, then the bytes: a value the buffer can't hold waits for the second call. */
    size_t len = 0;
    uint8_t present = 0;
    daedalus_client_status_t sized =
        daedalus_client_take_bytes(client, run, text("out"), 3, NULL, 0, &len, &present);
    if (sized != DAEDALUS_CLIENT_BUFFER_TOO_SMALL || present != 1) {
        fprintf(stderr, "FAILED: the size of the taken bytes was not asked for\n");
        return 1;
    }
    char out[256];
    if (len > sizeof out) {
        fprintf(stderr, "FAILED: %u bytes is more than the caller holds\n", (unsigned)len);
        return 1;
    }
    CHECK(daedalus_client_take_bytes(client, run, text("out"), 3, (uint8_t *)out, sizeof out, &len, &present));

    CHECK(daedalus_client_stop(client, run, DAEDALUS_CLIENT_STOP_DRAIN, NULL));
    CHECK(daedalus_client_close(client));
    printf("c client: ok %.*s\n", (int)len, out);
    return 0;
}
