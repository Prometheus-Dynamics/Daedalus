/* A C host: loads a plugin, compiles a graph over its node, pushes, ticks and takes.
 * Usage: host <plugin path>. Exits 0 when everything is as it should be, and otherwise
 * prints what wasn't and exits 1. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "daedalus.h"

static int failures = 0;

static void expect(int ok, const char *what, daedalus_status_t status) {
    if (!ok) {
        uint8_t why[512];
        size_t len = 0;
        why[0] = 0;
        if (daedalus_last_error(why, sizeof why - 1, &len) == DAEDALUS_OK) {
            why[len] = 0;
        }
        fprintf(stderr, "FAILED: %s (%s) %s\n", what, daedalus_status_name(status), (const char *)why);
        failures += 1;
    }
}

#define CHECK(call, want, what)                 \
    do {                                        \
        daedalus_status_t status_ = (call);     \
        expect(status_ == (want), what, status_); \
    } while (0)

static const char GRAPH[] =
    "{\"format\": \"daedalus.graph\", \"schema_version\": 1, \"graph\": {"
    "\"nodes\": ["
    "{\"id\": \"host\", \"kind\": \"host_bridge\", \"port_types\": {\"in\": \"u32\", \"out\": \"u32\"}},"
    "{\"id\": \"fixture.pass\"}],"
    "\"edges\": ["
    "{\"from\": {\"node\": 0, \"port\": \"in\"}, \"to\": {\"node\": 1, \"port\": \"in\"}},"
    "{\"from\": {\"node\": 1, \"port\": \"out\"}, \"to\": {\"node\": 0, \"port\": \"out\"}}]}}";

static const uint8_t *text(const char *s) { return (const uint8_t *)s; }

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: host <plugin path>\n");
        return 2;
    }

    daedalus_version_t version;
    CHECK(daedalus_version(&version), DAEDALUS_OK, "version");
    expect(version.layout_hash != 0, "a layout hash", DAEDALUS_OK);

    daedalus_handle_t host = 0, graph = 0;
    CHECK(daedalus_host_create(&host), DAEDALUS_OK, "host_create");
    CHECK(daedalus_host_install(host, text(argv[1]), strlen(argv[1])), DAEDALUS_OK, "host_install");

    /* Text comes back in two calls: the size, then the text. */
    size_t len = 0;
    CHECK(daedalus_host_schemas(host, NULL, 0, &len), DAEDALUS_BUFFER_TOO_SMALL, "schemas size");
    uint8_t *schemas = malloc(len + 1);
    CHECK(daedalus_host_schemas(host, schemas, len, &len), DAEDALUS_OK, "schemas");
    schemas[len] = 0;
    expect(strstr((const char *)schemas, "fixture.pass") != NULL, "the node is in the schemas", DAEDALUS_OK);
    free(schemas);

    CHECK(daedalus_graph_compile(host, text(GRAPH), strlen(GRAPH), &graph), DAEDALUS_OK, "graph_compile");
    CHECK(daedalus_host_release(host), DAEDALUS_BUSY, "a host with a graph is busy");

    size_t ports = 0, out_index = 99;
    CHECK(daedalus_graph_port_count(graph, &ports), DAEDALUS_OK, "port_count");
    expect(ports == 2, "two host ports", DAEDALUS_OK);
    CHECK(daedalus_graph_port_index(graph, text("out"), 3, &out_index), DAEDALUS_OK, "port_index");
    daedalus_port_t port;
    CHECK(daedalus_graph_port(graph, out_index, &port), DAEDALUS_OK, "port");
    expect(port.name_len == 3 && memcmp(port.name, "out", 3) == 0 && port.is_input == 0,
           "the port at that index is `out`", DAEDALUS_OK);
    expect(port.type_key_len == 3 && memcmp(port.type_key, "u32", 3) == 0, "its type is u32", DAEDALUS_OK);

    /* Push, tick, take. */
    int64_t value = 0;
    uint8_t present = 9;
    CHECK(daedalus_take_i64(graph, text("out"), 3, &value, &present), DAEDALUS_OK, "take before any push");
    expect(present == 0, "nothing to take yet", DAEDALUS_OK);
    CHECK(daedalus_push_i64(graph, text("in"), 2, 41), DAEDALUS_OK, "push");
    daedalus_tick_t ran;
    CHECK(daedalus_tick(graph, &ran), DAEDALUS_OK, "tick");
    expect(ran.idle == 0 && ran.step_calls == 1 && ran.faults == 0, "one step ran", DAEDALUS_OK);
    CHECK(daedalus_take_i64(graph, text("out"), 3, &value, &present), DAEDALUS_OK, "take");
    expect(present == 1 && value == 41, "41 came back", DAEDALUS_OK);

    /* What the port won't take. */
    CHECK(daedalus_push_i64(graph, text("in"), 2, -1), DAEDALUS_OUT_OF_RANGE, "a negative into a u32");
    CHECK(daedalus_push_f64(graph, text("in"), 2, 1.5), DAEDALUS_TYPE_MISMATCH, "a float into a u32");
    CHECK(daedalus_push_i64(graph, text("nowhere"), 7, 1), DAEDALUS_UNKNOWN_PORT, "no such port");
    CHECK(daedalus_push_i64(graph, text("out"), 3, 1), DAEDALUS_WRONG_DIRECTION, "a push to an output");

    /* A batch with one bad entry pushes nothing. */
    daedalus_push_t batch[2];
    memset(batch, 0, sizeof batch);
    batch[0].port = 0;
    batch[0].kind = DAEDALUS_KIND_I64;
    batch[0].value.i64_value = 7;
    batch[1].port = 0;
    batch[1].kind = DAEDALUS_KIND_I64;
    batch[1].value.i64_value = -7;
    size_t bad = 99;
    CHECK(daedalus_push_batch(graph, batch, 2, &bad), DAEDALUS_BAD_BATCH, "a bad batch");
    expect(bad == 1, "the second entry is the bad one", DAEDALUS_OK);
    CHECK(daedalus_tick(graph, &ran), DAEDALUS_OK, "tick after the bad batch");
    expect(ran.idle == 1, "nothing was pushed", DAEDALUS_OK);
    CHECK(daedalus_push_batch(graph, batch, 1, NULL), DAEDALUS_OK, "a good batch");
    CHECK(daedalus_tick(graph, NULL), DAEDALUS_OK, "tick");
    CHECK(daedalus_take_i64_at(graph, out_index, &value, &present), DAEDALUS_OK, "take by index");
    expect(present == 1 && value == 7, "7 came back", DAEDALUS_OK);

    daedalus_telemetry_t telemetry;
    CHECK(daedalus_graph_telemetry(graph, &telemetry), DAEDALUS_OK, "telemetry");
    expect(telemetry.ticks == 3 && telemetry.pushes == 2 && telemetry.takes == 2, "the counts", DAEDALUS_OK);

    /* A panic is a status, and null and stale handles are too. */
    CHECK(daedalus_selftest_panic(), DAEDALUS_PANIC, "a panic");
    CHECK(daedalus_tick(0, NULL), DAEDALUS_BAD_HANDLE, "a null handle");
    CHECK(daedalus_graph_release(graph), DAEDALUS_OK, "graph_release");
    CHECK(daedalus_tick(graph, NULL), DAEDALUS_STALE_HANDLE, "a released graph");
    CHECK(daedalus_push_i64(graph, text("in"), 2, 1), DAEDALUS_STALE_HANDLE, "a push to a released graph");
    CHECK(daedalus_host_release(host), DAEDALUS_OK, "host_release");
    CHECK(daedalus_host_release(host), DAEDALUS_STALE_HANDLE, "a released host");

    if (failures == 0) {
        printf("c host: ok\n");
    }
    return failures == 0 ? 0 : 1;
}
