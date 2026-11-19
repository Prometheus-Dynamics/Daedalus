// A C++ caller of the client wrapper (daedalus_client.hpp): what the wrapper adds to the C calls.
// A result larger than the wrapper's first buffer comes back whole through its retry, a refused
// call throws a ClientError with the status and the daemon's code, a failure that is not a refusal
// carries no code, and a client's handle is closed when the client goes out of scope. Exits 0 and
// prints "c++ client: ok" when every check holds. The behaviour of the C calls themselves is checked
// in Rust: the surface's tests and the drive suite.
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "daedalus_client.hpp"

namespace {

// Larger than the wrapper's first buffer, so a result this large is read again at its size.
constexpr std::size_t kLarge = dd::client_detail::kFirstBuffer + 4096 + 3;

// A host bridge with bytes ports, and the fixture's bytes node between them, labelled `label`.
std::string graph(const std::string& label) {
    return R"({"format": "daedalus.graph", "schema_version": 1, "graph": {
  "nodes": [
    {"id": "host", "kind": "host_bridge", "port_types": {"in": "bytes", "out": "bytes"}},
    {"id": "fixture.pass_bytes", "label": ")" +
           label + R"("}
  ],
  "edges": [
    {"from": {"node": 0, "port": "in"}, "to": {"node": 1, "port": "in"}},
    {"from": {"node": 1, "port": "out"}, "to": {"node": 0, "port": "out"}}
  ]}})";
}

int fail(const char* what) {
    std::fprintf(stderr, "FAILED: %s\n", what);
    return 1;
}

int run(const std::string& endpoint, const std::string& plugin) {
    daedalus_client_handle_t raw = 0;
    {
        dd::Client client = dd::Client::connect(endpoint);
        raw = client.handle();
        client.load_plugin(plugin);

        // A stored document larger than the first buffer comes back whole: its label is in it.
        std::string label(kLarge, 'x');
        for (std::size_t i = 0; i < label.size(); i += 61) label[i] = static_cast<char>('a' + i % 26);
        client.put_graph("g", graph(label));
        std::string text = client.graph_text("g");
        if (text.size() <= kLarge || text.find(label) == std::string::npos) return fail("a large text comes back whole");

        // So do the bytes of a large value.
        std::vector<std::uint8_t> bytes(kLarge);
        for (std::size_t i = 0; i < bytes.size(); i++) bytes[i] = static_cast<std::uint8_t>(i * 7 + i / 251);
        daedalus_client_config_t config{};
        config.telemetry = DAEDALUS_CLIENT_TELEMETRY_BASIC;
        std::uint64_t run = client.start("g", config);
        client.feed_bytes(run, "in", bytes);
        if (client.step(run, 1).ticks != 1) return fail("one tick runs");
        std::optional<std::vector<std::uint8_t>> taken = client.take_bytes(run, "out");
        if (!taken || *taken != bytes) return fail("a large value comes back whole");
        if (client.take_bytes(run, "out")) return fail("the port is empty after the take");

        // A refusal by the daemon: its status, and the daemon's code for an unknown graph.
        bool refused = false;
        try {
            client.start("nope", config);
        } catch (const dd::ClientError& error) {
            refused = error.status() == DAEDALUS_CLIENT_SERVICE_ERROR &&
                      std::string(error.status_name()) == "DAEDALUS_CLIENT_SERVICE_ERROR" &&
                      error.service_code() == 2 && !error.detail().empty();
        }
        if (!refused) return fail("a start of a graph the daemon lacks throws SERVICE_ERROR with code 2");

        // A failure that is not a refusal: its own status, and no daemon code.
        bool local = false;
        try {
            dd::Client::connect("nowhere");
        } catch (const dd::ClientError& error) {
            local = error.status() == DAEDALUS_CLIENT_BAD_ENDPOINT && error.service_code() == 0 &&
                    std::string(error.status_name()) == "DAEDALUS_CLIENT_BAD_ENDPOINT";
        }
        if (!local) return fail("an endpoint that is no endpoint throws BAD_ENDPOINT with no code");

        // A moved client gives up its handle: the one it moved to closes it.
        dd::Client moved = std::move(client);
        if (client.handle() != 0 || moved.handle() != raw) return fail("a move takes the handle");
    }
    std::uint32_t version = 0;
    if (daedalus_client_ping(raw, &version) == DAEDALUS_CLIENT_OK) return fail("the handle is closed with its client");
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: client <tcp:host:port> <plugin path>\n");
        return 2;
    }
    try {
        if (run(argv[1], argv[2]) != 0) return 1;
    } catch (const dd::ClientError& error) {
        std::fprintf(stderr, "FAILED: %s: %s\n", error.status_name(), error.detail().c_str());
        return 1;
    }
    std::printf("c++ client: ok\n");
    return 0;
}
