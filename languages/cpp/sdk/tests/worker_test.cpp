// Native tests of the worker loop's state messages. A counter node keeps a running total as its state.
// The tests write host packets to a file, run the loop over it, and read the worker's packets back.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <vector>

#include "daedalus.hpp"
#include "internal.h"

namespace {

int failures = 0;

#define CHECK(cond)                                                    \
    do {                                                               \
        if (!(cond)) {                                                 \
            std::fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #cond); \
            ++failures;                                                \
        }                                                              \
    } while (0)

enum : uint8_t {
    KIND_RUN = 1,
    KIND_STATE_EXPORT = 3,
    KIND_STATE_IMPORT = 4,
    KIND_SHUTDOWN = 6,
    KIND_OUTPUTS = 0x41,
    KIND_FAULT = 0x44,
    KIND_STATE_VALUE = 0x45,
};

// A body under construction: the little-endian integers and length-prefixed strings the wire uses.
struct Body {
    std::vector<unsigned char> bytes;
    void u8(uint8_t v) { bytes.push_back(v); }
    void le(uint64_t v, int width) {
        for (int i = 0; i < width; i++) bytes.push_back(static_cast<unsigned char>(v >> (8 * i)));
    }
    void u32(uint32_t v) { le(v, 4); }
    void str(const char* text) {
        u32(static_cast<uint32_t>(std::strlen(text)));
        bytes.insert(bytes.end(), text, text + std::strlen(text));
    }
    void i64(int64_t v) { le(static_cast<uint64_t>(v), 8); }
};

// The counter: its total is its state, and its output is the total after adding the input.
int counter(dd_ctx* cx, const dd_value* in, dd_value* out) {
    dd_value* state = dd_state(cx);
    dd_value total{};
    total.as.i = (state ? state->as.i : 0) + in[0].as.i;
    dd_set_state(cx, total);
    out[0] = total;
    return 0;
}

const dd_type* i64_type() { return dd::scalar(DD_I64); }

const dd_port counter_inputs[] = {{"delta", i64_type()}};
const dd_port counter_outputs[] = {{"total", i64_type()}};
const dd_node nodes[] = {
    {"test.counter", counter_inputs, 1, counter_outputs, 1, i64_type(), counter},
    {"test.stateless", counter_inputs, 1, counter_outputs, 1, nullptr, counter},
};

// One host packet: a 4-byte little-endian length, then the body.
void write_packet(std::FILE* file, const Body& body) {
    uint32_t len = static_cast<uint32_t>(body.bytes.size());
    unsigned char header[4];
    for (int i = 0; i < 4; i++) header[i] = static_cast<unsigned char>(len >> (8 * i));
    std::fwrite(header, 1, 4, file);
    std::fwrite(body.bytes.data(), 1, body.bytes.size(), file);
}

// Reads one packet from a worker's output. Returns false at the end.
bool read_packet(std::FILE* file, std::vector<unsigned char>* body) {
    unsigned char header[4];
    if (std::fread(header, 1, 4, file) != 4) return false;
    uint32_t len = 0;
    for (int i = 0; i < 4; i++) len |= static_cast<uint32_t>(header[i]) << (8 * i);
    body->assign(len, 0);
    return len == 0 || std::fread(body->data(), 1, len, file) == len;
}

int64_t last_i64(const std::vector<unsigned char>& body) {
    uint64_t v = 0;
    for (int i = 0; i < 8; i++) v |= static_cast<uint64_t>(body[body.size() - 8 + i]) << (8 * i);
    return static_cast<int64_t>(v);
}

Body run(uint64_t number, const char* node, int64_t delta) {
    Body b;
    b.u8(KIND_RUN);
    b.le(number, 8);
    b.u32(0);
    b.u32(1);
    b.str(node);
    b.u32(1);
    b.str(node);
    b.str("delta");
    b.i64(delta);
    return b;
}

Body state_export(const char* node) {
    Body b;
    b.u8(KIND_STATE_EXPORT);
    b.str(node);
    return b;
}

Body state_import(const char* node, int64_t value) {
    Body b;
    b.u8(KIND_STATE_IMPORT);
    b.str(node);
    b.i64(value);
    return b;
}

// Runs the loop over the packets in `script`, and returns the worker's packets in order.
std::vector<std::vector<unsigned char>> serve(const std::vector<Body>& script) {
    std::FILE* in = std::tmpfile();
    std::FILE* out = std::tmpfile();
    CHECK(in != nullptr && out != nullptr);
    for (const Body& body : script) write_packet(in, body);
    write_packet(in, Body{{KIND_SHUTDOWN}});
    std::rewind(in);
    CHECK(dd_serve_io(nodes, 2, in, out, "native-test") == 0);
    std::rewind(out);
    std::vector<std::vector<unsigned char>> packets;
    std::vector<unsigned char> body;
    while (read_packet(out, &body)) packets.push_back(body);
    std::fclose(in);
    std::fclose(out);
    return packets;
}

void state_is_exported_after_a_run_and_imported_before_the_next() {
    auto packets = serve({
        run(1, "test.counter", 5),
        state_export("test.counter"),
        state_import("test.counter", 100),
        run(2, "test.counter", 5),
        state_export("test.counter"),
    });
    // hello, outputs of run 1, its state, the outputs of run 2 and its state. The import has no reply.
    CHECK(packets.size() == 5);
    if (packets.size() != 5) return;
    CHECK(packets[1][0] == KIND_OUTPUTS);
    CHECK(last_i64(packets[1]) == 5);
    CHECK(packets[2][0] == KIND_STATE_VALUE);
    CHECK(last_i64(packets[2]) == 5);
    CHECK(packets[3][0] == KIND_OUTPUTS);
    CHECK(last_i64(packets[3]) == 105);
    CHECK(packets[4][0] == KIND_STATE_VALUE);
    CHECK(last_i64(packets[4]) == 105);
}

void a_node_without_state_has_none_to_export() {
    auto packets = serve({state_export("test.stateless")});
    CHECK(packets.size() == 2);
    if (packets.size() != 2) return;
    CHECK(packets[1][0] == KIND_FAULT);
}

void an_unknown_node_has_no_state_to_export() {
    auto packets = serve({state_export("test.missing")});
    CHECK(packets.size() == 2);
    if (packets.size() != 2) return;
    CHECK(packets[1][0] == KIND_FAULT);
}

void an_import_of_the_wrong_size_leaves_the_state_alone() {
    Body bad;
    bad.u8(KIND_STATE_IMPORT);
    bad.str("test.counter");
    bad.le(7, 4);  // four bytes where the state type needs eight
    auto packets = serve({
        run(1, "test.counter", 3),
        bad,
        state_export("test.counter"),
    });
    CHECK(packets.size() == 4);
    if (packets.size() != 4) return;
    CHECK(packets[2][0] == KIND_FAULT);
    CHECK(packets[3][0] == KIND_STATE_VALUE);
    CHECK(last_i64(packets[3]) == 3);
}

void an_import_to_a_node_without_state_is_a_fault() {
    auto packets = serve({state_import("test.stateless", 9)});
    CHECK(packets.size() == 2);
    if (packets.size() != 2) return;
    CHECK(packets[1][0] == KIND_FAULT);
}

}  // namespace

int main() {
    state_is_exported_after_a_run_and_imported_before_the_next();
    a_node_without_state_has_none_to_export();
    an_unknown_node_has_no_state_to_export();
    an_import_of_the_wrong_size_leaves_the_state_alone();
    an_import_to_a_node_without_state_is_a_fault();
    if (failures != 0) {
        std::fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    std::printf("the worker loop's state messages pass\n");
    return 0;
}
