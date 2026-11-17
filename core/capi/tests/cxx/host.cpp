// A C++ caller of the host wrapper: loads a plugin, compiles a graph over its node, pushes 41, ticks,
// takes it back, and checks that a refused call throws a HostError carrying the status. Exits 0 and
// prints "c++ host: ok" when every check holds. The behaviour checks are in the Rust tests.
#include <cstdint>
#include <cstdio>
#include <optional>
#include <string>

#include "daedalus_host.hpp"

namespace {

const char kGraph[] =
    R"({"format": "daedalus.graph", "schema_version": 1, "graph": {
  "nodes": [
    {"id": "host", "kind": "host_bridge", "port_types": {"in": "u32", "out": "u32"}},
    {"id": "fixture.pass"}
  ],
  "edges": [
    {"from": {"node": 0, "port": "in"}, "to": {"node": 1, "port": "in"}},
    {"from": {"node": 1, "port": "out"}, "to": {"node": 0, "port": "out"}}
  ]}})";

int fail(const char* what) {
    std::fprintf(stderr, "FAILED: %s\n", what);
    return 1;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::fprintf(stderr, "usage: host <plugin path>\n");
        return 2;
    }
    try {
        dd::Host host = dd::Host::create();
        host.install(argv[1]);
        dd::Graph graph = host.compile(kGraph);

        if (graph.port_count() != 2) return fail("the graph has two host ports");
        if (graph.port_index("in") != 0 || graph.port_index("out") != 1) return fail("the ports are sorted by name");
        if (!graph.port(0).is_input || graph.port(1).is_input) return fail("in is an input, out is not");

        graph.push_i64("in", 41);
        graph.tick();
        std::optional<std::int64_t> value = graph.take_i64("out");
        if (!value || *value != 41) return fail("41 comes back");
        if (graph.take_i64("out")) return fail("the port is empty after the take");

        graph.push_i64(0, 7);
        graph.tick();
        if (graph.take_i64(1).value_or(-1) != 7) return fail("the index forms agree with the names");

        const dd::Graph::Telemetry telemetry = graph.telemetry();
        if (telemetry.pushes != 2 || telemetry.takes < 2) return fail("telemetry counts the pushes and takes");

        // A refused call throws, carrying its status. The text is the library's for that failure: none
        // here, so nothing from an earlier failure is carried over.
        bool threw = false;
        try {
            graph.push_i64("nope", 1);
        } catch (const dd::HostError& error) {
            threw = error.status() == DAEDALUS_UNKNOWN_PORT &&
                    std::string(error.status_name()) == "DAEDALUS_UNKNOWN_PORT" && error.detail().empty();
        }
        if (!threw) return fail("a push to a port the graph lacks throws UNKNOWN_PORT, with no stale text");

        // A graph that cannot be read throws GRAPH_INVALID, with the text the library kept for it.
        bool refused = false;
        try {
            host.compile("{ not a graph");
        } catch (const dd::HostError& error) {
            refused = error.status() == DAEDALUS_GRAPH_INVALID && !error.detail().empty();
        }
        if (!refused) return fail("a graph that cannot be read throws GRAPH_INVALID, with text");
    } catch (const dd::HostError& error) {
        std::fprintf(stderr, "FAILED: %s: %s\n", error.status_name(), error.detail().c_str());
        return 1;
    }
    std::printf("c++ host: ok\n");
    return 0;
}
