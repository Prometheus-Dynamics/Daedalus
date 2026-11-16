// The one native C++ test: the header links against the Rust library, and a value round-trips
// through the codec a worker uses. The codec and the worker loop are tested in Rust.

#include <cstddef>
#include <cstdio>

#include "daedalus_worker.hpp"

extern "C" {
int dd_internal_encode(const dd_type* ty, const dd_value* v, unsigned char** out, size_t* len);
int dd_internal_decode(const dd_type* ty, const unsigned char* p, size_t len, dd_value* out, void** keep);
void dd_internal_free_bytes(unsigned char* p);
void dd_internal_free_arena(void* keep);
}

int main() {
    const dd_type* type = dd::scalar(DD_I64);
    dd_value in{};
    in.as.i = -42;

    unsigned char* bytes = nullptr;
    size_t len = 0;
    if (dd_internal_encode(type, &in, &bytes, &len) != 0) return 1;

    dd_value out{};
    void* keep = nullptr;
    const int decoded = dd_internal_decode(type, bytes, len, &out, &keep);
    dd_internal_free_bytes(bytes);
    if (decoded != 0) return 1;

    const bool same = len == 8 && out.as.i == -42;
    dd_internal_free_arena(keep);
    if (!same) {
        std::fprintf(stderr, "the value did not round-trip\n");
        return 1;
    }
    std::printf("the value round-trips\n");
    return 0;
}
