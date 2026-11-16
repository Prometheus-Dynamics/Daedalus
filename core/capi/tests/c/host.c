/* A C caller: loads a plugin, compiles a graph over its node, pushes 41, ticks, takes it back
 * and frees both handles. Exits 0 and prints "c host: ok" when 41 comes back. The behaviour
 * checks are in the Rust tests of the surface and of the Rust wrapper. */
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "daedalus.h"

static const char GRAPH[] =
    "{\"format\": \"daedalus.graph\", \"schema_version\": 1, \"graph\": {"
    "\"nodes\": ["
    "{\"id\": \"host\", \"kind\": \"host_bridge\", \"port_types\": {\"in\": \"u32\", \"out\": \"u32\"}},"
    "{\"id\": \"fixture.pass\"}],"
    "\"edges\": ["
    "{\"from\": {\"node\": 0, \"port\": \"in\"}, \"to\": {\"node\": 1, \"port\": \"in\"}},"
    "{\"from\": {\"node\": 1, \"port\": \"out\"}, \"to\": {\"node\": 0, \"port\": \"out\"}}]}}";

static const uint8_t *text(const char *s) { return (const uint8_t *)s; }

#define CHECK(call)                                                             \
    do {                                                                        \
        daedalus_status_t status_ = (call);                                     \
        if (status_ != DAEDALUS_OK) {                                           \
            fprintf(stderr, "FAILED: %s: %s\n", #call, daedalus_status_name(status_)); \
            return 1;                                                           \
        }                                                                       \
    } while (0)

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: host <plugin path>\n");
        return 2;
    }

    daedalus_handle_t host = 0, graph = 0;
    CHECK(daedalus_host_create(&host));
    CHECK(daedalus_host_install(host, text(argv[1]), strlen(argv[1])));
    CHECK(daedalus_graph_compile(host, text(GRAPH), strlen(GRAPH), &graph));

    CHECK(daedalus_push_i64(graph, text("in"), 2, 41));
    CHECK(daedalus_tick(graph, NULL));
    int64_t value = 0;
    uint8_t present = 0;
    CHECK(daedalus_take_i64(graph, text("out"), 3, &value, &present));
    if (present != 1 || value != 41) {
        fprintf(stderr, "FAILED: 41 did not come back (present %d, value %lld)\n", present, (long long)value);
        return 1;
    }

    CHECK(daedalus_graph_release(graph));
    CHECK(daedalus_host_release(host));
    printf("c host: ok\n");
    return 0;
}
