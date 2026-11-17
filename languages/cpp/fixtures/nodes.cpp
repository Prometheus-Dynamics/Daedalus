// The fixture worker: the thirteen canonical value fixtures, and the four behaviour fixtures of the
// worker behaviour suite (testing/crates/worker-conformance). Their ids and ports match the Rust
// fixtures exactly. Each body is a C++ function, served through dd::run, so an exception it throws
// is its node's fault.

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <thread>

#include "daedalus_worker.hpp"

namespace {

dd_value i64_value(int64_t v) {
    dd_value value{};
    value.as.i = v;
    return value;
}

dd_value bytes_value(const uint8_t* data, std::size_t len) {
    dd_value value{};
    value.as.bytes.data = data;
    value.as.bytes.len = len;
    return value;
}

// a + b
int scalar_add(dd::Ctx&, const dd_value* in, dd_value* out) {
    out[0] = i64_value(in[0].as.i + in[1].as.i);
    return 0;
}

// The identity on the one input: bytes, struct, enum, optional, tuple, map and opaque echoes.
int echo(dd::Ctx&, const dd_value* in, dd_value* out) {
    out[0] = in[0];
    return 0;
}

int list_reverse(dd::Ctx& cx, const dd_value* in, dd_value* out) {
    std::size_t n = in[0].as.seq.count;
    auto* items = static_cast<dd_value*>(cx.alloc(sizeof(dd_value) * (n ? n : 1)));
    for (std::size_t i = 0; i < n; i++) items[i] = in[0].as.seq.items[n - 1 - i];
    out[0].as.seq.items = items;
    out[0].as.seq.count = n;
    return 0;
}

int multi_output(dd::Ctx&, const dd_value* in, dd_value* out) {
    uint64_t x = in[0].as.u;
    out[0].as.u = x;
    out[1].as.u = (x * 2) & 0xFFFFFFFFu;
    return 0;
}

int raw_io(dd::Ctx& cx, const dd_value* in, dd_value* out) {
    std::size_t n = in[0].as.bytes.len;
    auto* data = static_cast<uint8_t*>(cx.alloc(n ? n : 1));
    for (std::size_t i = 0; i < n; i++) data[i] = in[0].as.bytes.data[n - 1 - i];
    out[0] = bytes_value(data, n);
    return 0;
}

int stateful_counter(dd::Ctx& cx, const dd_value* in, dd_value* out) {
    dd_value* state = cx.state();
    int64_t current = state ? state->as.i : 0;
    int64_t total = current + in[0].as.i;
    cx.set_state(i64_value(total));
    out[0] = i64_value(total);
    return 0;
}

int failure_diagnostic(dd::Ctx& cx, const dd_value*, dd_value*) {
    cx.detail("diagnostic detail: the value is rejected, ");
    cx.detail("and this is the rest of it");
    cx.fault_code(77);
    throw dd::Fault("diagnostic: the value is rejected");
}

// The behaviour fixtures.

// Runs until the host cancels, then gives its input.
int wait_cancel(dd::Ctx& cx, const dd_value* in, dd_value* out) {
    while (!cx.cancelled()) std::this_thread::sleep_for(std::chrono::milliseconds(2));
    out[0] = in[0];
    return 0;
}

// Progress, log and detail, in that order, then gives its input back.
int events(dd::Ctx& cx, const dd_value* in, dd_value* out) {
    cx.progress(0.5f, "half");
    cx.log("info", "after the progress");
    cx.detail("a detail line");
    cx.progress(1.0f, "done");
    out[0] = in[0];
    return 0;
}

// Prints a line to stdout, then gives its input plus one. The SDK keeps stdout for the protocol.
int chatty(dd::Ctx&, const dd_value* in, dd_value* out) {
    std::printf("a node line on stdout\n");
    std::fflush(stdout);
    out[0] = i64_value(in[0].as.i + 1);
    return 0;
}

// Appends its input to a string state, and gives the state.
int stateful_label(dd::Ctx& cx, const dd_value* in, dd_value* out) {
    const dd_value* prior = cx.state();
    std::size_t old_len = prior ? prior->as.bytes.len : 0;
    std::size_t add_len = in[0].as.bytes.len;
    auto* text = static_cast<uint8_t*>(cx.alloc(old_len + add_len ? old_len + add_len : 1));
    if (old_len) std::memcpy(text, prior->as.bytes.data, old_len);
    if (add_len) std::memcpy(text + old_len, in[0].as.bytes.data, add_len);
    out[0] = bytes_value(text, old_len + add_len);
    cx.set_state(out[0]);
    return 0;
}

}  // namespace

int main() {
    dd::Types types;

    const char* const kStructNames[] = {"name", "count"};
    const char* const kEnumNames[] = {"idle", "busy"};
    const dd_type* const kStructTypes[] = {types.scalar(DD_STRING), types.scalar(DD_I64)};
    const dd_type* const kEnumPayloads[] = {nullptr, types.scalar(DD_U32)};
    const dd_type* const kPairTypes[] = {types.scalar(DD_STRING), types.scalar(DD_BOOL)};
    const dd_type* const kMapPair[] = {types.scalar(DD_STRING), types.scalar(DD_I64)};

    const dd_type* const kStructT = types.strukt(
        {{kStructNames[0], kStructTypes[0]}, {kStructNames[1], kStructTypes[1]}});
    const dd_type* const kEnumT = types.enm(
        {{kEnumNames[0], kEnumPayloads[0]}, {kEnumNames[1], kEnumPayloads[1]}});
    const dd_type* const kOptionalT = types.optional(types.scalar(DD_I64));
    const dd_type* const kListT = types.list(types.scalar(DD_U32));
    const dd_type* const kTupleT = types.tuple({kPairTypes[0], kPairTypes[1]});
    const dd_type* const kMapT = types.map(kMapPair[0], kMapPair[1]);
    const dd_type* const kCustomT = types.opaque("fixture.custom");

    const dd_port kScalarAddIn[] = {dd::port("a", types.scalar(DD_I64)), dd::port("b", types.scalar(DD_I64))};
    const dd_port kOut[] = {dd::port("out", types.scalar(DD_I64))};
    const dd_port kBytesIn[] = {dd::port("in", types.scalar(DD_BYTES))};
    const dd_port kBytesOut[] = {dd::port("out", types.scalar(DD_BYTES))};
    const dd_port kStructPort[] = {dd::port("in", kStructT)};
    const dd_port kStructOut[] = {dd::port("out", kStructT)};
    const dd_port kEnumPort[] = {dd::port("in", kEnumT)};
    const dd_port kEnumOut[] = {dd::port("out", kEnumT)};
    const dd_port kOptPort[] = {dd::port("in", kOptionalT)};
    const dd_port kOptOut[] = {dd::port("out", kOptionalT)};
    const dd_port kListPort[] = {dd::port("in", kListT)};
    const dd_port kListOut[] = {dd::port("out", kListT)};
    const dd_port kTuplePort[] = {dd::port("in", kTupleT)};
    const dd_port kTupleOut[] = {dd::port("out", kTupleT)};
    const dd_port kMapPort[] = {dd::port("in", kMapT)};
    const dd_port kMapOut[] = {dd::port("out", kMapT)};
    const dd_port kCustomPort[] = {dd::port("in", kCustomT)};
    const dd_port kCustomOut[] = {dd::port("out", kCustomT)};
    const dd_port kXIn[] = {dd::port("x", types.scalar(DD_U32))};
    const dd_port kAbOut[] = {dd::port("a", types.scalar(DD_U32)), dd::port("b", types.scalar(DD_U32))};
    const dd_port kPayloadIn[] = {dd::port("payload", types.scalar(DD_BYTES))};
    const dd_port kDeltaIn[] = {dd::port("delta", types.scalar(DD_I64))};
    const dd_port kTotalOut[] = {dd::port("total", types.scalar(DD_I64))};
    const dd_port kValueIn[] = {dd::port("value", types.scalar(DD_I64))};
    const dd_port kIntIn[] = {dd::port("x", types.scalar(DD_I64))};
    const dd_port kIntOut[] = {dd::port("y", types.scalar(DD_I64))};
    const dd_port kLabelIn[] = {dd::port("in", types.scalar(DD_STRING))};
    const dd_port kLabelOut[] = {dd::port("out", types.scalar(DD_STRING))};

    const dd_node kNodes[] = {
        {"fixture.scalar_add", kScalarAddIn, 2, kOut, 1, nullptr, &dd::run<scalar_add>},
        {"fixture.bytes_echo", kBytesIn, 1, kBytesOut, 1, nullptr, &dd::run<echo>},
        {"fixture.struct_record", kStructPort, 1, kStructOut, 1, nullptr, &dd::run<echo>},
        {"fixture.enum_choice", kEnumPort, 1, kEnumOut, 1, nullptr, &dd::run<echo>},
        {"fixture.optional_value", kOptPort, 1, kOptOut, 1, nullptr, &dd::run<echo>},
        {"fixture.list_reverse", kListPort, 1, kListOut, 1, nullptr, &dd::run<list_reverse>},
        {"fixture.tuple_pair", kTuplePort, 1, kTupleOut, 1, nullptr, &dd::run<echo>},
        {"fixture.map_pairs", kMapPort, 1, kMapOut, 1, nullptr, &dd::run<echo>},
        {"fixture.multi_output", kXIn, 1, kAbOut, 2, nullptr, &dd::run<multi_output>},
        {"fixture.raw_io", kPayloadIn, 1, kBytesOut, 1, nullptr, &dd::run<raw_io>},
        {"fixture.stateful_counter", kDeltaIn, 1, kTotalOut, 1, types.scalar(DD_I64), &dd::run<stateful_counter>},
        {"fixture.custom_type_key", kCustomPort, 1, kCustomOut, 1, nullptr, &dd::run<echo>},
        {"fixture.failure_diagnostic", kValueIn, 1, kOut, 1, nullptr, &dd::run<failure_diagnostic>},
        {"fixture.wait_cancel", kIntIn, 1, kIntOut, 1, nullptr, &dd::run<wait_cancel>},
        {"fixture.events", kIntIn, 1, kIntOut, 1, nullptr, &dd::run<events>},
        {"fixture.chatty", kIntIn, 1, kIntOut, 1, nullptr, &dd::run<chatty>},
        {"fixture.stateful_label", kLabelIn, 1, kLabelOut, 1, types.scalar(DD_STRING), &dd::run<stateful_label>},
    };

    return dd::serve(kNodes, sizeof kNodes / sizeof kNodes[0], "daedalus-cpp-worker");
}
