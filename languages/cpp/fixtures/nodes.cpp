// The thirteen canonical fixture nodes, as the C++ worker serves them. Their ports and types match
// testing/crates/worker-conformance/src/fixtures.rs exactly.

#include <cstdint>

#include "daedalus.h"
#include "daedalus.hpp"

namespace {

dd_value i64_value(int64_t v) {
    dd_value value{};
    value.as.i = v;
    return value;
}

}  // namespace

extern "C" {

// a + b
int scalar_add(dd_ctx*, const dd_value* in, dd_value* out) {
    out[0] = i64_value(in[0].as.i + in[1].as.i);
    return 0;
}

// The identity on the one input: bytes, struct, enum, optional, tuple, map and opaque echoes.
int echo(dd_ctx*, const dd_value* in, dd_value* out) {
    out[0] = in[0];
    return 0;
}

int list_reverse(dd_ctx* cx, const dd_value* in, dd_value* out) {
    size_t n = in[0].as.seq.count;
    dd_value* items = static_cast<dd_value*>(dd_alloc(cx, sizeof(dd_value) * (n ? n : 1)));
    for (size_t i = 0; i < n; i++) items[i] = in[0].as.seq.items[n - 1 - i];
    out[0].as.seq.items = items;
    out[0].as.seq.count = n;
    return 0;
}

int multi_output(dd_ctx*, const dd_value* in, dd_value* out) {
    uint64_t x = in[0].as.u;
    out[0].as.u = x;
    out[1].as.u = (x * 2) & 0xFFFFFFFFu;
    return 0;
}

int raw_io(dd_ctx* cx, const dd_value* in, dd_value* out) {
    size_t n = in[0].as.bytes.len;
    uint8_t* data = static_cast<uint8_t*>(dd_alloc(cx, n ? n : 1));
    for (size_t i = 0; i < n; i++) data[i] = in[0].as.bytes.data[n - 1 - i];
    out[0].as.bytes.data = data;
    out[0].as.bytes.len = n;
    return 0;
}

int stateful_counter(dd_ctx* cx, const dd_value* in, dd_value* out) {
    dd_value* state = dd_state(cx);
    int64_t current = state ? state->as.i : 0;
    int64_t total = current + in[0].as.i;
    dd_set_state(cx, i64_value(total));
    out[0] = i64_value(total);
    return 0;
}

int failure_diagnostic(dd_ctx* cx, const dd_value*, dd_value*) {
    dd_fault(cx, "node_error", "diagnostic: the value is rejected");
    return 1;
}

}  // extern "C"

namespace {

const char* const kStructNames[] = {"name", "count"};
const char* const kEnumNames[] = {"idle", "busy"};
const dd_type* const kStructTypes[] = {dd::scalar(DD_STRING), dd::scalar(DD_I64)};
const dd_type* const kEnumPayloads[] = {nullptr, dd::scalar(DD_U32)};
const dd_type* const kPairTypes[] = {dd::scalar(DD_STRING), dd::scalar(DD_BOOL)};
const dd_type* const kMapPair[] = {dd::scalar(DD_STRING), dd::scalar(DD_I64)};
const dd_type* const kI64Item[] = {dd::scalar(DD_I64)};
const dd_type* const kU32Item[] = {dd::scalar(DD_U32)};

const dd_type* const kStructT = dd::strukt(kStructTypes, kStructNames, 2);
const dd_type* const kEnumT = dd::enm(kEnumPayloads, kEnumNames, 2);
const dd_type* const kOptionalT = dd::optional(kI64Item);
const dd_type* const kListT = dd::list(kU32Item);
const dd_type* const kTupleT = dd::tuple(kPairTypes, 2);
const dd_type* const kMapT = dd::map(kMapPair);
const dd_type* const kCustomT = dd::opaque("fixture.custom");

const dd_port kScalarAddIn[] = {dd::port("a", dd::scalar(DD_I64)), dd::port("b", dd::scalar(DD_I64))};
const dd_port kOut[] = {dd::port("out", dd::scalar(DD_I64))};
const dd_port kBytesIn[] = {dd::port("in", dd::scalar(DD_BYTES))};
const dd_port kBytesOut[] = {dd::port("out", dd::scalar(DD_BYTES))};
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
const dd_port kXIn[] = {dd::port("x", dd::scalar(DD_U32))};
const dd_port kAbOut[] = {dd::port("a", dd::scalar(DD_U32)), dd::port("b", dd::scalar(DD_U32))};
const dd_port kPayloadIn[] = {dd::port("payload", dd::scalar(DD_BYTES))};
const dd_port kDeltaIn[] = {dd::port("delta", dd::scalar(DD_I64))};
const dd_port kTotalOut[] = {dd::port("total", dd::scalar(DD_I64))};
const dd_port kValueIn[] = {dd::port("value", dd::scalar(DD_I64))};

const dd_node kNodes[] = {
    {"fixture.scalar_add", kScalarAddIn, 2, kOut, 1, nullptr, scalar_add},
    {"fixture.bytes_echo", kBytesIn, 1, kBytesOut, 1, nullptr, echo},
    {"fixture.struct_record", kStructPort, 1, kStructOut, 1, nullptr, echo},
    {"fixture.enum_choice", kEnumPort, 1, kEnumOut, 1, nullptr, echo},
    {"fixture.optional_value", kOptPort, 1, kOptOut, 1, nullptr, echo},
    {"fixture.list_reverse", kListPort, 1, kListOut, 1, nullptr, list_reverse},
    {"fixture.tuple_pair", kTuplePort, 1, kTupleOut, 1, nullptr, echo},
    {"fixture.map_pairs", kMapPort, 1, kMapOut, 1, nullptr, echo},
    {"fixture.multi_output", kXIn, 1, kAbOut, 2, nullptr, multi_output},
    {"fixture.raw_io", kPayloadIn, 1, kBytesOut, 1, nullptr, raw_io},
    {"fixture.stateful_counter", kDeltaIn, 1, kTotalOut, 1, dd::scalar(DD_I64), stateful_counter},
    {"fixture.custom_type_key", kCustomPort, 1, kCustomOut, 1, nullptr, echo},
    {"fixture.failure_diagnostic", kValueIn, 1, kOut, 1, nullptr, failure_diagnostic},
};

}  // namespace

int main() {
    return dd_serve(kNodes, sizeof kNodes / sizeof kNodes[0], "daedalus-cpp-worker") == 0 ? 0 : 1;
}
