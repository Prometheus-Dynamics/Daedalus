// The Daedalus worker SDK for C++17: header-only builders over the C API in daedalus.h.
//
// A type is built once, with static storage, and passed by pointer:
//   static const dd_type* const pair[] = {dd::scalar(DD_STRING), dd::scalar(DD_BOOL)};
//   dd::tuple(pair, 2)
// A node is a dd_node in a table, with a function written as a C++ lambda without captures.
#ifndef DAEDALUS_HPP
#define DAEDALUS_HPP

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <string>
#include <vector>

#include "daedalus.h"

namespace dd {

// Scalar types, with static storage. Each call with the same scalar returns the same object.
inline const dd_type* scalar(dd_scalar s) {
    static const dd_type table[] = {
        {DD_K_SCALAR, DD_UNIT, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_BOOL, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_I8, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_I16, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_I32, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_I64, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_ISIZE, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_U8, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_U16, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_U32, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_U64, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_USIZE, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_F32, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_F64, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_STRING, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_BYTES, nullptr, nullptr, nullptr, 0},
    };
    return &table[static_cast<int>(s)];
}

// A type with one item: optional or list.
inline const dd_type* optional(const dd_type* const* item) {
    return new dd_type{DD_K_OPTIONAL, DD_UNIT, nullptr, item, nullptr, 1};
}
inline const dd_type* list(const dd_type* const* item) {
    return new dd_type{DD_K_LIST, DD_UNIT, nullptr, item, nullptr, 1};
}
inline const dd_type* map(const dd_type* const* pair) {
    return new dd_type{DD_K_MAP, DD_UNIT, nullptr, pair, nullptr, 2};
}
inline const dd_type* tuple(const dd_type* const* items, std::size_t count) {
    return new dd_type{DD_K_TUPLE, DD_UNIT, nullptr, items, nullptr, count};
}
inline const dd_type* strukt(const dd_type* const* items, const char* const* names, std::size_t count) {
    return new dd_type{DD_K_STRUCT, DD_UNIT, nullptr, items, names, count};
}
inline const dd_type* enm(const dd_type* const* payloads, const char* const* names, std::size_t count) {
    return new dd_type{DD_K_ENUM, DD_UNIT, nullptr, payloads, names, count};
}
inline const dd_type* opaque(const char* key) {
    return new dd_type{DD_K_OPAQUE, DD_UNIT, key, nullptr, nullptr, 0};
}

// A port with its name and type.
inline dd_port port(const char* name, const dd_type* type) { return dd_port{name, type}; }

}  // namespace dd

#endif  // DAEDALUS_HPP
