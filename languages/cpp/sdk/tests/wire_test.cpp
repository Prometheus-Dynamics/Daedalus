// Native tests of the value codec: every scalar round-trips, and each value outside its type's range
// is refused on encode, and each malformed byte string is refused on decode.

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

// Encodes `value` as `type`. Returns true when the encoder accepts it.
bool encodes(const dd_type* type, const dd_value& value, std::vector<unsigned char>* bytes) {
    unsigned char* out = nullptr;
    size_t len = 0;
    if (dd_internal_encode(type, &value, &out, &len) != 0) return false;
    bytes->assign(out, out + len);
    dd_internal_free_bytes(out);
    return true;
}

// Decodes `bytes` as `type` and returns true when the bytes are exactly one value.
bool decodes(const dd_type* type, const std::vector<unsigned char>& bytes, dd_value* value) {
    void* keep = nullptr;
    if (dd_internal_decode(type, bytes.data(), bytes.size(), value, &keep) != 0) return false;
    dd_internal_free_arena(keep);
    return true;
}

dd_value signed_value(int64_t v) {
    dd_value value{};
    value.as.i = v;
    return value;
}

dd_value unsigned_value(uint64_t v) {
    dd_value value{};
    value.as.u = v;
    return value;
}

void round_trips_in_range() {
    struct Case {
        dd_scalar scalar;
        dd_value value;
        size_t width;
    };
    const Case cases[] = {
        {DD_I8, signed_value(-128), 1},
        {DD_I8, signed_value(127), 1},
        {DD_I16, signed_value(-32768), 2},
        {DD_I32, signed_value(2147483647LL), 4},
        {DD_I32, signed_value(-2147483648LL), 4},
        {DD_I64, signed_value(-5), 8},
        {DD_U8, unsigned_value(255), 1},
        {DD_U16, unsigned_value(65535), 2},
        {DD_U32, unsigned_value(4294967295u), 4},
        {DD_U64, unsigned_value(UINT64_MAX), 8},
    };
    for (const Case& c : cases) {
        const dd_type* type = dd::scalar(c.scalar);
        std::vector<unsigned char> bytes;
        CHECK(encodes(type, c.value, &bytes));
        CHECK(bytes.size() == c.width);
        dd_value back{};
        CHECK(decodes(type, bytes, &back));
        if (c.scalar == DD_U8 || c.scalar == DD_U16 || c.scalar == DD_U32 || c.scalar == DD_U64) {
            CHECK(back.as.u == c.value.as.u);
        } else {
            CHECK(back.as.i == c.value.as.i);
        }
    }
    dd_value flag{};
    flag.as.b = true;
    std::vector<unsigned char> bytes;
    CHECK(encodes(dd::scalar(DD_BOOL), flag, &bytes));
    CHECK(bytes.size() == 1 && bytes[0] == 1);
}

void out_of_range_is_refused_on_encode() {
    struct Case {
        dd_scalar scalar;
        dd_value value;
    };
    const Case cases[] = {
        {DD_I8, signed_value(128)},
        {DD_I8, signed_value(-129)},
        {DD_I16, signed_value(32768)},
        {DD_I16, signed_value(-32769)},
        {DD_I32, signed_value(2147483648LL)},
        {DD_I32, signed_value(-2147483649LL)},
        {DD_U8, unsigned_value(256)},
        {DD_U16, unsigned_value(65536)},
        {DD_U32, unsigned_value(4294967296ull)},
    };
    for (const Case& c : cases) {
        std::vector<unsigned char> bytes;
        CHECK(!encodes(dd::scalar(c.scalar), c.value, &bytes));
    }
}

void malformed_bytes_are_refused_on_decode() {
    dd_value out{};
    // A bool is 0 or 1 only.
    CHECK(!decodes(dd::scalar(DD_BOOL), {2}, &out));
    // A fixed-width value needs all its bytes.
    CHECK(!decodes(dd::scalar(DD_I32), {1, 2, 3}, &out));
    // One value and nothing after it.
    CHECK(!decodes(dd::scalar(DD_U8), {1, 2}, &out));
    // A string's length must fit in the bytes that follow it.
    CHECK(!decodes(dd::scalar(DD_STRING), {9, 0, 0, 0, 'a'}, &out));
    // A list's count must fit in the bytes that follow it.
    const dd_type* item = dd::scalar(DD_U8);
    const dd_type* const items[] = {item};
    const dd_type* list = dd::list(items);
    CHECK(!decodes(list, {0xff, 0xff, 0xff, 0xff}, &out));
    // Empty bytes for a value that needs some.
    CHECK(!decodes(dd::scalar(DD_U64), {}, &out));
    // A correct u8 still decodes.
    CHECK(decodes(dd::scalar(DD_U8), {7}, &out));
    CHECK(out.as.u == 7);
}

}  // namespace

int main() {
    round_trips_in_range();
    out_of_range_is_refused_on_encode();
    malformed_bytes_are_refused_on_decode();
    if (failures != 0) {
        std::fprintf(stderr, "%d check(s) failed\n", failures);
        return 1;
    }
    std::printf("the value codec passes\n");
    return 0;
}
