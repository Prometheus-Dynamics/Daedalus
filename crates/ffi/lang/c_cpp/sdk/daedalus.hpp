// Header-only C++ SDK for Daedalus C/C++ bridge nodes.
//
// Goals:
// - "Rust-like" authoring ergonomics: declare typed functions; infer TypeExpr from C++ types.
// - Single-artifact flow: a shared library can export its own manifest JSON (no separate file).
// - No external dependencies.
//
// Supported type inference (for ports):
// - int32_t, int64_t, uint32_t
// - float, double
// - bool
// - std::string
// - std::vector<T> for supported T
// - std::optional<T> for supported T
// - std::tuple<T...> return (multi-output)
//
// Struct support:
// - `DAEDALUS_STRUCT(...)` derives a JSON TypeExpr + JSON Codec for a simple C++ struct.
// - This enables "typed config" style nodes (Rust `NodeConfig` parity for basic cases).

#pragma once

#include "daedalus_c_cpp.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

namespace daedalus {

// Marker type for "unit" payloads (e.g. enum variants without data).
struct Unit {};

inline char* dup_cstr(const std::string& s) {
  char* out = (char*)std::malloc(s.size() + 1);
  if (!out) return nullptr;
  std::memcpy(out, s.c_str(), s.size() + 1);
  return out;
}

inline DaedalusCppResult ok_json(const std::string& json) {
  DaedalusCppResult r;
  r.json = dup_cstr(json);
  r.error = nullptr;
  return r;
}

inline DaedalusCppResult err_str(const std::string& err) {
  DaedalusCppResult r;
  r.json = nullptr;
  r.error = dup_cstr(err);
  return r;
}

// Very small JSON helpers (enough for the bridge payload).
// This parser only supports:
// - numbers (int/float)
// - booleans
// - null
// - strings (no unicode escaping)
// - arrays of scalars

inline void skip_ws(const char* s, size_t& i) {
  while (s[i] == ' ' || s[i] == '\t' || s[i] == '\n' || s[i] == '\r') i++;
}

inline bool consume(const char* s, size_t& i, char c) {
  skip_ws(s, i);
  if (s[i] != c) return false;
  i++;
  return true;
}

inline bool match_literal(const char* s, size_t& i, const char* lit) {
  skip_ws(s, i);
  const size_t start = i;
  for (size_t j = 0; lit[j]; j++) {
    if (s[i] != lit[j]) {
      i = start;
      return false;
    }
    i++;
  }
  return true;
}

inline bool parse_string(const char* s, size_t& i, std::string& out) {
  skip_ws(s, i);
  if (s[i] != '"') return false;
  i++;
  std::string buf;
  while (s[i] && s[i] != '"') {
    if (s[i] == '\\') {
      i++;
      if (!s[i]) return false;
      const char esc = s[i];
      if (esc == '"' || esc == '\\' || esc == '/') buf.push_back(esc);
      else if (esc == 'b') buf.push_back('\b');
      else if (esc == 'f') buf.push_back('\f');
      else if (esc == 'n') buf.push_back('\n');
      else if (esc == 'r') buf.push_back('\r');
      else if (esc == 't') buf.push_back('\t');
      else return false;
      i++;
      continue;
    }
    buf.push_back(s[i]);
    i++;
  }
  if (s[i] != '"') return false;
  i++;
  out = std::move(buf);
  return true;
}

inline bool parse_number(const char* s, size_t& i, double& out) {
  skip_ws(s, i);
  const size_t start = i;
  if (s[i] == '-') i++;
  bool any = false;
  while (s[i] >= '0' && s[i] <= '9') {
    any = true;
    i++;
  }
  if (s[i] == '.') {
    i++;
    while (s[i] >= '0' && s[i] <= '9') {
      any = true;
      i++;
    }
  }
  if (!any) {
    i = start;
    return false;
  }
  out = std::strtod(s + start, nullptr);
  return true;
}

inline bool find_key(const char* s, const char* key, size_t& out_pos) {
  // naive scan for '"key"' (sufficient for the daemon payload).
  std::string pat = std::string("\"") + key + "\"";
  const char* p = std::strstr(s, pat.c_str());
  if (!p) return false;
  out_pos = (size_t)(p - s) + pat.size();
  return true;
}

inline bool find_key_from(const char* s, size_t start, const char* key, size_t& out_pos) {
  std::string pat = std::string("\"") + key + "\"";
  const char* p = std::strstr(s + start, pat.c_str());
  if (!p) return false;
  out_pos = (size_t)(p - s) + pat.size();
  return true;
}

inline bool extract_number_field_in_object(const char* s,
                                           const char* object_key,
                                           const char* field_key,
                                           double& out) {
  size_t pos = 0;
  if (!find_key(s, object_key, pos)) return false;
  // Find object start '{' after key.
  while (s[pos] && s[pos] != '{' && s[pos] != 'n') pos++;
  if (!s[pos]) return false;
  if (s[pos] == 'n') {
    // null
    return false;
  }
  const size_t obj_start = pos;
  size_t fpos = 0;
  if (!find_key_from(s, obj_start, field_key, fpos)) return false;
  // Parse number after field key.
  size_t i = fpos;
  while (s[i] && s[i] != ':' ) i++;
  if (!s[i]) return false;
  i++;
  return parse_number(s, i, out);
}

template <typename T>
struct TypeExpr;

inline std::string json_escape(const std::string& s) {
  std::string out;
  out.reserve(s.size() + 8);
  for (char c : s) {
    if (c == '\\' || c == '"') {
      out.push_back('\\');
      out.push_back(c);
    } else if (c == '\n') {
      out += "\\n";
    } else if (c == '\r') {
      out += "\\r";
    } else if (c == '\t') {
      out += "\\t";
    } else {
      out.push_back(c);
    }
  }
  return out;
}

inline std::string json_scalar(const char* value_type) {
  return std::string("{\"Scalar\":\"") + value_type + "\"}";
}

template <>
struct TypeExpr<int32_t> {
  static std::string json() { return json_scalar("I32"); }
};
template <>
struct TypeExpr<int64_t> {
  static std::string json() { return json_scalar("Int"); }
};
template <>
struct TypeExpr<uint32_t> {
  static std::string json() { return json_scalar("U32"); }
};
template <>
struct TypeExpr<float> {
  static std::string json() { return json_scalar("F32"); }
};
template <>
struct TypeExpr<double> {
  static std::string json() { return json_scalar("Float"); }
};
template <>
struct TypeExpr<bool> {
  static std::string json() { return json_scalar("Bool"); }
};
template <>
struct TypeExpr<std::string> {
  static std::string json() { return json_scalar("String"); }
};

template <typename T>
struct TypeExpr<std::vector<T>> {
  static std::string json() { return std::string("{\"List\":") + TypeExpr<T>::json() + "}"; }
};

template <typename T>
struct TypeExpr<std::optional<T>> {
  static std::string json() { return std::string("{\"Optional\":") + TypeExpr<T>::json() + "}"; }
};

// Value encoding/decoding
template <typename T>
struct Codec;

template <>
struct Codec<int32_t> {
  static bool decode(const char* s, size_t& i, int32_t& out) {
    double v = 0;
    if (!parse_number(s, i, v)) return false;
    out = (int32_t)v;
    return true;
  }
  static std::string encode(int32_t v) { return std::to_string((long long)v); }
};
template <>
struct Codec<int64_t> {
  static bool decode(const char* s, size_t& i, int64_t& out) {
    double v = 0;
    if (!parse_number(s, i, v)) return false;
    out = (int64_t)v;
    return true;
  }
  static std::string encode(int64_t v) { return std::to_string((long long)v); }
};
template <>
struct Codec<uint32_t> {
  static bool decode(const char* s, size_t& i, uint32_t& out) {
    double v = 0;
    if (!parse_number(s, i, v)) return false;
    out = (uint32_t)v;
    return true;
  }
  static std::string encode(uint32_t v) { return std::to_string((unsigned long long)v); }
};
template <>
struct Codec<float> {
  static bool decode(const char* s, size_t& i, float& out) {
    double v = 0;
    if (!parse_number(s, i, v)) return false;
    out = (float)v;
    return true;
  }
  static std::string encode(float v) { return std::to_string((double)v); }
};
template <>
struct Codec<double> {
  static bool decode(const char* s, size_t& i, double& out) { return parse_number(s, i, out); }
  static std::string encode(double v) { return std::to_string(v); }
};
template <>
struct Codec<bool> {
  static bool decode(const char* s, size_t& i, bool& out) {
    if (match_literal(s, i, "true")) { out = true; return true; }
    if (match_literal(s, i, "false")) { out = false; return true; }
    return false;
  }
  static std::string encode(bool v) { return v ? "true" : "false"; }
};
template <>
struct Codec<std::string> {
  static bool decode(const char* s, size_t& i, std::string& out) { return parse_string(s, i, out); }
  static std::string encode(const std::string& v) { return std::string("\"") + json_escape(v) + "\""; }
};

template <typename T>
struct Codec<std::optional<T>> {
  static bool decode(const char* s, size_t& i, std::optional<T>& out) {
    if (match_literal(s, i, "null")) { out.reset(); return true; }
    T inner{};
    if (!Codec<T>::decode(s, i, inner)) return false;
    out = inner;
    return true;
  }
  static std::string encode(const std::optional<T>& v) {
    if (!v.has_value()) return "null";
    return Codec<T>::encode(*v);
  }
};

template <typename T>
struct Codec<std::vector<T>> {
  static bool decode(const char* s, size_t& i, std::vector<T>& out) {
    if (!consume(s, i, '[')) return false;
    std::vector<T> items;
    skip_ws(s, i);
    if (consume(s, i, ']')) { out = std::move(items); return true; }
    while (true) {
      T v{};
      if (!Codec<T>::decode(s, i, v)) return false;
      items.push_back(std::move(v));
      skip_ws(s, i);
      if (consume(s, i, ']')) break;
      if (!consume(s, i, ',')) return false;
    }
    out = std::move(items);
    return true;
  }
  static std::string encode(const std::vector<T>& v) {
    std::string out = "[";
    for (size_t idx = 0; idx < v.size(); idx++) {
      if (idx) out += ",";
      out += Codec<T>::encode(v[idx]);
    }
    out += "]";
    return out;
  }
};

inline bool skip_value(const char* s, size_t& i, int depth = 0) {
  if (depth > 16) return false;
  skip_ws(s, i);
  if (!s[i]) return false;
  if (s[i] == '"') {
    std::string tmp;
    return parse_string(s, i, tmp);
  }
  if ((s[i] >= '0' && s[i] <= '9') || s[i] == '-') {
    double tmp = 0;
    return parse_number(s, i, tmp);
  }
  if (match_literal(s, i, "true")) return true;
  if (match_literal(s, i, "false")) return true;
  if (match_literal(s, i, "null")) return true;
  if (s[i] == '[') {
    i++;
    skip_ws(s, i);
    if (s[i] == ']') { i++; return true; }
    while (true) {
      if (!skip_value(s, i, depth + 1)) return false;
      skip_ws(s, i);
      if (s[i] == ']') { i++; return true; }
      if (s[i] != ',') return false;
      i++;
    }
  }
  if (s[i] == '{') {
    i++;
    skip_ws(s, i);
    if (s[i] == '}') { i++; return true; }
    while (true) {
      std::string key;
      if (!parse_string(s, i, key)) return false;
      if (!consume(s, i, ':')) return false;
      if (!skip_value(s, i, depth + 1)) return false;
      skip_ws(s, i);
      if (s[i] == '}') { i++; return true; }
      if (s[i] != ',') return false;
      i++;
    }
  }
  return false;
}

inline bool capture_value_json(const char* s, size_t& i, std::string& out) {
  skip_ws(s, i);
  const size_t start = i;
  size_t j = i;
  if (!skip_value(s, j)) return false;
  out.assign(s + start, j - start);
  i = j;
  return true;
}

// Define a simple struct type with TypeExpr + Codec derived from field list.
//
// Example:
//   DAEDALUS_STRUCT(ScaleCfg,
//     (factor, int32_t, 2)
//   );
#define DAEDALUS_STRUCT(StructName, ...)                                                                    \
  struct StructName {                                                                                        \
    DAEDALUS_STRUCT_FIELDS(__VA_ARGS__)                                                                      \
  };                                                                                                         \
  template <>                                                                                                \
  struct daedalus::TypeExpr<StructName> {                                                                    \
    static std::string json() {                                                                              \
      std::string out = "{\"Struct\":[";                                                                     \
      bool first = true;                                                                                     \
      DAEDALUS_STRUCT_TYPEEXPR(out, first, __VA_ARGS__)                                                      \
      out += "]}";                                                                                           \
      return out;                                                                                            \
    }                                                                                                        \
  };                                                                                                         \
  template <>                                                                                                \
  struct daedalus::Codec<StructName> {                                                                       \
    static bool decode(const char* s, size_t& i, StructName& out) {                                          \
      if (!daedalus::consume(s, i, '{')) return false;                                                       \
      daedalus::skip_ws(s, i);                                                                               \
      if (daedalus::consume(s, i, '}')) return true;                                                         \
      while (true) {                                                                                         \
        std::string key;                                                                                     \
        if (!daedalus::parse_string(s, i, key)) return false;                                                \
        if (!daedalus::consume(s, i, ':')) return false;                                                     \
        bool handled = false;                                                                                \
        DAEDALUS_STRUCT_DECODE_FIELD(s, i, out, key, handled, __VA_ARGS__)                                   \
        if (!handled) {                                                                                      \
          if (!daedalus::skip_value(s, i)) return false;                                                     \
        }                                                                                                    \
        daedalus::skip_ws(s, i);                                                                             \
        if (daedalus::consume(s, i, '}')) return true;                                                       \
        if (!daedalus::consume(s, i, ',')) return false;                                                     \
      }                                                                                                      \
    }                                                                                                        \
    static std::string encode(const StructName& v) {                                                         \
      std::string out = "{";                                                                                 \
      bool first = true;                                                                                     \
      DAEDALUS_STRUCT_ENCODE(out, first, v, __VA_ARGS__)                                                     \
      out += "}";                                                                                            \
      return out;                                                                                            \
    }                                                                                                        \
  };

#define DAEDALUS_STRUCT_FIELD_ONE(field, ty, def) ty field = def;

// These macros assume the following variable names exist in scope:
// - TypeExpr: `std::string out`, `bool first`
// - Decode: `const char* s`, `size_t& i`, `StructName& out`, `std::string key`, `bool handled`
// - Encode: `std::string out`, `bool first`, `const StructName& v`
#define DAEDALUS_STRUCT_TYPEEXPR_ONE(field, ty, def)                                                        \
  do {                                                                                                      \
    if (!first) out += ",";                                                                                 \
    first = false;                                                                                          \
    out += "{\"name\":\"" + daedalus::json_escape(#field) + "\",\"ty\":" + daedalus::TypeExpr<ty>::json() + "}"; \
  } while (0);

#define DAEDALUS_STRUCT_DECODE_ONE(field, ty, def)                                                          \
  do {                                                                                                      \
    if (!handled && key == #field) {                                                                        \
      ty tmp = out.field;                                                                                   \
      if (!daedalus::Codec<ty>::decode(s, i, tmp)) return false;                                            \
      out.field = tmp;                                                                                      \
      handled = true;                                                                                       \
    }                                                                                                       \
  } while (0);

#define DAEDALUS_STRUCT_ENCODE_ONE(field, ty, def)                                                          \
  do {                                                                                                      \
    if (!first) out += ",";                                                                                 \
    first = false;                                                                                          \
    out += "\"" + daedalus::json_escape(#field) + "\":" + daedalus::Codec<ty>::encode(v.field);            \
  } while (0);

// Preprocessor helpers (supports up to 8 fields).
#define DAEDALUS_PP_CAT(a, b) DAEDALUS_PP_CAT_I(a, b)
#define DAEDALUS_PP_CAT_I(a, b) a##b
#define DAEDALUS_PP_NARG(...) DAEDALUS_PP_NARG_I(__VA_ARGS__, 8, 7, 6, 5, 4, 3, 2, 1)
#define DAEDALUS_PP_NARG_I(_1, _2, _3, _4, _5, _6, _7, _8, N, ...) N

#define DAEDALUS_FOR_EACH(m, ...) DAEDALUS_PP_CAT(DAEDALUS_FOR_EACH_, DAEDALUS_PP_NARG(__VA_ARGS__))(m, __VA_ARGS__)
#define DAEDALUS_FOR_EACH_1(m, a1) m a1
#define DAEDALUS_FOR_EACH_2(m, a1, a2) m a1 m a2
#define DAEDALUS_FOR_EACH_3(m, a1, a2, a3) m a1 m a2 m a3
#define DAEDALUS_FOR_EACH_4(m, a1, a2, a3, a4) m a1 m a2 m a3 m a4
#define DAEDALUS_FOR_EACH_5(m, a1, a2, a3, a4, a5) m a1 m a2 m a3 m a4 m a5
#define DAEDALUS_FOR_EACH_6(m, a1, a2, a3, a4, a5, a6) m a1 m a2 m a3 m a4 m a5 m a6
#define DAEDALUS_FOR_EACH_7(m, a1, a2, a3, a4, a5, a6, a7) m a1 m a2 m a3 m a4 m a5 m a6 m a7
#define DAEDALUS_FOR_EACH_8(m, a1, a2, a3, a4, a5, a6, a7, a8) m a1 m a2 m a3 m a4 m a5 m a6 m a7 m a8

#define DAEDALUS_STRUCT_FIELDS(...) DAEDALUS_FOR_EACH(DAEDALUS_STRUCT_FIELD_ONE, __VA_ARGS__)
#define DAEDALUS_STRUCT_TYPEEXPR(out, first, ...) DAEDALUS_FOR_EACH(DAEDALUS_STRUCT_TYPEEXPR_ONE, __VA_ARGS__)
#define DAEDALUS_STRUCT_DECODE_FIELD(s, i, out, key, handled, ...) DAEDALUS_FOR_EACH(DAEDALUS_STRUCT_DECODE_ONE, __VA_ARGS__)
#define DAEDALUS_STRUCT_ENCODE(out, first, v, ...) DAEDALUS_FOR_EACH(DAEDALUS_STRUCT_ENCODE_ONE, __VA_ARGS__)

// Enum helper macro (typed sum types + TypeExpr + Codec).
//
// Example:
//   DAEDALUS_STRUCT(Point, (x, int32_t, 0), (y, int32_t, 0));
//   DAEDALUS_ENUM(Mode,
//     (A, int32_t),
//     (B, Point),
//     (None, daedalus::Unit)
//   );
//
// JSON shape matches other language bridges: {"name":"A","value":...} or {"name":"None"}.
#define DAEDALUS_ENUM_VNAME_I(name, ty) name
#define DAEDALUS_ENUM_VNAME(spec) DAEDALUS_ENUM_VNAME_I spec
#define DAEDALUS_ENUM_VTY_I(name, ty) ty
#define DAEDALUS_ENUM_VTY(spec) DAEDALUS_ENUM_VTY_I spec

#define DAEDALUS_STR_I(x) #x
#define DAEDALUS_STR(x) DAEDALUS_STR_I(x)

#define DAEDALUS_FOR_EACH_CTX(m, ctx, ...)                                                                    \
  DAEDALUS_PP_CAT(DAEDALUS_FOR_EACH_CTX_, DAEDALUS_PP_NARG(__VA_ARGS__))(m, ctx, __VA_ARGS__)
#define DAEDALUS_FOR_EACH_CTX_1(m, ctx, a1) m(ctx, a1)
#define DAEDALUS_FOR_EACH_CTX_2(m, ctx, a1, a2) m(ctx, a1) m(ctx, a2)
#define DAEDALUS_FOR_EACH_CTX_3(m, ctx, a1, a2, a3) m(ctx, a1) m(ctx, a2) m(ctx, a3)
#define DAEDALUS_FOR_EACH_CTX_4(m, ctx, a1, a2, a3, a4) m(ctx, a1) m(ctx, a2) m(ctx, a3) m(ctx, a4)
#define DAEDALUS_FOR_EACH_CTX_5(m, ctx, a1, a2, a3, a4, a5) m(ctx, a1) m(ctx, a2) m(ctx, a3) m(ctx, a4) m(ctx, a5)
#define DAEDALUS_FOR_EACH_CTX_6(m, ctx, a1, a2, a3, a4, a5, a6) m(ctx, a1) m(ctx, a2) m(ctx, a3) m(ctx, a4) m(ctx, a5) m(ctx, a6)
#define DAEDALUS_FOR_EACH_CTX_7(m, ctx, a1, a2, a3, a4, a5, a6, a7) m(ctx, a1) m(ctx, a2) m(ctx, a3) m(ctx, a4) m(ctx, a5) m(ctx, a6) m(ctx, a7)
#define DAEDALUS_FOR_EACH_CTX_8(m, ctx, a1, a2, a3, a4, a5, a6, a7, a8) m(ctx, a1) m(ctx, a2) m(ctx, a3) m(ctx, a4) m(ctx, a5) m(ctx, a6) m(ctx, a7) m(ctx, a8)

#define DAEDALUS_COMMA_LIST_CTX(m, ctx, ...)                                                                  \
  DAEDALUS_PP_CAT(DAEDALUS_COMMA_LIST_CTX_, DAEDALUS_PP_NARG(__VA_ARGS__))(m, ctx, __VA_ARGS__)
#define DAEDALUS_COMMA_LIST_CTX_1(m, ctx, a1) m(ctx, a1)
#define DAEDALUS_COMMA_LIST_CTX_2(m, ctx, a1, a2) m(ctx, a1), m(ctx, a2)
#define DAEDALUS_COMMA_LIST_CTX_3(m, ctx, a1, a2, a3) m(ctx, a1), m(ctx, a2), m(ctx, a3)
#define DAEDALUS_COMMA_LIST_CTX_4(m, ctx, a1, a2, a3, a4) m(ctx, a1), m(ctx, a2), m(ctx, a3), m(ctx, a4)
#define DAEDALUS_COMMA_LIST_CTX_5(m, ctx, a1, a2, a3, a4, a5) m(ctx, a1), m(ctx, a2), m(ctx, a3), m(ctx, a4), m(ctx, a5)
#define DAEDALUS_COMMA_LIST_CTX_6(m, ctx, a1, a2, a3, a4, a5, a6) m(ctx, a1), m(ctx, a2), m(ctx, a3), m(ctx, a4), m(ctx, a5), m(ctx, a6)
#define DAEDALUS_COMMA_LIST_CTX_7(m, ctx, a1, a2, a3, a4, a5, a6, a7) m(ctx, a1), m(ctx, a2), m(ctx, a3), m(ctx, a4), m(ctx, a5), m(ctx, a6), m(ctx, a7)
#define DAEDALUS_COMMA_LIST_CTX_8(m, ctx, a1, a2, a3, a4, a5, a6, a7, a8) m(ctx, a1), m(ctx, a2), m(ctx, a3), m(ctx, a4), m(ctx, a5), m(ctx, a6), m(ctx, a7), m(ctx, a8)

#define DAEDALUS_ENUM_TAG(EnumName, V) DAEDALUS_PP_CAT(EnumName, DAEDALUS_PP_CAT(_, DAEDALUS_PP_CAT(V, _Tag)))
#define DAEDALUS_ENUM_VTYPE(EnumName, spec) DAEDALUS_PP_CAT(V_, DAEDALUS_ENUM_VNAME(spec))

#define DAEDALUS_ENUM_DECLARE_VARIANT(EnumName, spec)                                                        \
  struct DAEDALUS_ENUM_TAG(EnumName, DAEDALUS_ENUM_VNAME(spec)) {};                                          \
  using DAEDALUS_ENUM_VTYPE(EnumName, spec) =                                                                \
      daedalus::EnumVariant<DAEDALUS_ENUM_TAG(EnumName, DAEDALUS_ENUM_VNAME(spec)), DAEDALUS_ENUM_VTY(spec)>;

#define DAEDALUS_ENUM_VARIANT_TYPE(EnumName, spec) DAEDALUS_ENUM_VTYPE(EnumName, spec)

#define DAEDALUS_ENUM_CTOR(EnumName, spec)                                                                   \
  template <typename P = typename DAEDALUS_ENUM_VTYPE(EnumName, spec)::payload_t,                            \
            std::enable_if_t<std::is_same_v<P, daedalus::Unit>, int> = 0>                                     \
  static EnumName DAEDALUS_ENUM_VNAME(spec)() {                                                              \
    return make<DAEDALUS_ENUM_VTYPE(EnumName, spec)>();                                                      \
  }                                                                                                          \
  template <typename P = typename DAEDALUS_ENUM_VTYPE(EnumName, spec)::payload_t,                            \
            std::enable_if_t<!std::is_same_v<P, daedalus::Unit>, int> = 0>                                    \
  static EnumName DAEDALUS_ENUM_VNAME(spec)(P v) {                                                           \
    return make<DAEDALUS_ENUM_VTYPE(EnumName, spec)>(std::move(v));                                          \
  }

#define DAEDALUS_ENUM_TYPEEXPR_ONE(_EnumName, spec)                                                          \
  do {                                                                                                       \
    if (!first) out += ",";                                                                                  \
    first = false;                                                                                           \
    out += "{\"name\":\"" + daedalus::json_escape(DAEDALUS_STR(DAEDALUS_ENUM_VNAME(spec))) + "\",\"ty\":" +  \
           daedalus::enum_payload_ty_json<DAEDALUS_ENUM_VTY(spec)>() + "}";                                  \
  } while (0);

#define DAEDALUS_ENUM_DECODE_ONE(EnumName, spec)                                                             \
  do {                                                                                                       \
    if (tag == DAEDALUS_STR(DAEDALUS_ENUM_VNAME(spec))) {                                                    \
      using VarT = typename EnumName::DAEDALUS_ENUM_VTYPE(EnumName, spec);                                   \
      VarT v;                                                                                                \
      if (!daedalus::enum_decode_value_json<VarT>(value_json, v)) return false;                              \
      out.value = std::move(v);                                                                              \
      return true;                                                                                           \
    }                                                                                                        \
  } while (0);

#define DAEDALUS_ENUM_ENCODE_CASE(EnumName, spec)                                                            \
  if constexpr (std::is_same_v<V, typename EnumName::DAEDALUS_ENUM_VTYPE(EnumName, spec)>) {                 \
    out += "\"name\":\"" + daedalus::json_escape(DAEDALUS_STR(DAEDALUS_ENUM_VNAME(spec))) + "\"";            \
    if constexpr (daedalus::EnumVariantTraits<V>::has_value) {                                               \
      out += ",\"value\":" + daedalus::enum_variant_value_json(v);                                           \
    }                                                                                                        \
  }

#define DAEDALUS_ENUM(EnumName, ...)                                                                         \
  struct EnumName {                                                                                          \
    DAEDALUS_FOR_EACH_CTX(DAEDALUS_ENUM_DECLARE_VARIANT, EnumName, __VA_ARGS__)                              \
    using Variant = std::variant<DAEDALUS_COMMA_LIST_CTX(DAEDALUS_ENUM_VARIANT_TYPE, EnumName, __VA_ARGS__)>; \
    Variant value;                                                                                           \
                                                                                                             \
    template <typename V, typename... Args>                                                                  \
    static EnumName make(Args&&... args) {                                                                   \
      EnumName e;                                                                                            \
      e.value = V(std::forward<Args>(args)...);                                                              \
      return e;                                                                                              \
    }                                                                                                        \
                                                                                                             \
    DAEDALUS_FOR_EACH_CTX(DAEDALUS_ENUM_CTOR, EnumName, __VA_ARGS__)                                         \
  };                                                                                                         \
  template <>                                                                                                \
  struct daedalus::TypeExpr<EnumName> {                                                                      \
    static std::string json() {                                                                              \
      std::string out = "{\"Enum\":[";                                                                       \
      bool first = true;                                                                                     \
      DAEDALUS_FOR_EACH_CTX(DAEDALUS_ENUM_TYPEEXPR_ONE, EnumName, __VA_ARGS__)                               \
      out += "]}";                                                                                           \
      return out;                                                                                            \
    }                                                                                                        \
  };                                                                                                         \
  template <>                                                                                                \
  struct daedalus::Codec<EnumName> {                                                                         \
    static bool decode(const char* s, size_t& i, EnumName& out) {                                             \
      if (!daedalus::consume(s, i, '{')) return false;                                                       \
      std::string tag;                                                                                       \
      std::string value_json;                                                                                \
      daedalus::skip_ws(s, i);                                                                               \
      if (daedalus::consume(s, i, '}')) return false;                                                        \
      while (true) {                                                                                         \
        std::string key;                                                                                     \
        if (!daedalus::parse_string(s, i, key)) return false;                                                \
        if (!daedalus::consume(s, i, ':')) return false;                                                     \
        if (key == "name") {                                                                                 \
          if (!daedalus::parse_string(s, i, tag)) return false;                                               \
        } else if (key == "value") {                                                                         \
          if (!daedalus::capture_value_json(s, i, value_json)) return false;                                 \
        } else {                                                                                             \
          if (!daedalus::skip_value(s, i)) return false;                                                     \
        }                                                                                                    \
        daedalus::skip_ws(s, i);                                                                             \
        if (daedalus::consume(s, i, '}')) break;                                                             \
        if (!daedalus::consume(s, i, ',')) return false;                                                     \
      }                                                                                                      \
      if (tag.empty()) return false;                                                                         \
      DAEDALUS_FOR_EACH_CTX(DAEDALUS_ENUM_DECODE_ONE, EnumName, __VA_ARGS__)                                 \
      return false;                                                                                          \
    }                                                                                                        \
    static std::string encode(const EnumName& e) {                                                           \
      std::string out = "{";                                                                                 \
      std::visit([&](const auto& v) {                                                                        \
        using V = std::decay_t<decltype(v)>;                                                                 \
        DAEDALUS_FOR_EACH_CTX(DAEDALUS_ENUM_ENCODE_CASE, EnumName, __VA_ARGS__)                              \
      },                                                                                                     \
                 e.value);                                                                                   \
      out += "}";                                                                                            \
      return out;                                                                                            \
    }                                                                                                        \
  };

template <typename Tag, typename Payload>
struct EnumVariant {
  using payload_t = Payload;
  Payload value{};
  EnumVariant() = default;
  explicit EnumVariant(Payload v) : value(std::move(v)) {}
};

template <typename Tag>
struct EnumVariant<Tag, Unit> {
  using payload_t = Unit;
  EnumVariant() = default;
};

template <typename V>
struct EnumVariantTraits;

template <typename Tag, typename Payload>
struct EnumVariantTraits<EnumVariant<Tag, Payload>> {
  using payload_t = Payload;
  static constexpr bool has_value = !std::is_same_v<Payload, Unit>;
};

template <typename Payload>
inline std::string enum_payload_ty_json() {
  if constexpr (std::is_same_v<Payload, Unit>) {
    return "null";
  } else {
    return daedalus::TypeExpr<Payload>::json();
  }
}

template <typename Variant>
inline std::string enum_variant_value_json(const Variant& v) {
  using Payload = typename EnumVariantTraits<Variant>::payload_t;
  if constexpr (EnumVariantTraits<Variant>::has_value) {
    return daedalus::Codec<Payload>::encode(v.value);
  } else {
    return "";
  }
}

template <typename Variant>
inline bool enum_decode_value_json(const std::string& json, Variant& out) {
  using Payload = typename EnumVariantTraits<Variant>::payload_t;
  if constexpr (!EnumVariantTraits<Variant>::has_value) {
    (void)json;
    out = Variant{};
    return true;
  } else {
    size_t k = 0;
    Payload p{};
    if (!daedalus::Codec<Payload>::decode(json.c_str(), k, p)) return false;
    daedalus::skip_ws(json.c_str(), k);
    if (json.c_str()[k] != '\0') return false;
    out = Variant(std::move(p));
    return true;
  }
}

// Function traits
template <typename>
struct FnTraits;

template <typename R, typename... Args>
struct FnTraits<R (*)(Args...)> {
  using Return = R;
  using ArgsTuple = std::tuple<Args...>;
  static constexpr size_t arity = sizeof...(Args);
};

template <typename R, typename... Args>
FnTraits<R (*)(Args...)> traits_of(R (*)(Args...));

template <typename T>
struct IsTuple : std::false_type {};
template <typename... Ts>
struct IsTuple<std::tuple<Ts...>> : std::true_type {};

template <typename Tuple, size_t... I>
inline void push_types_for_tuple(std::vector<std::string>& out, std::index_sequence<I...>) {
  (out.push_back(daedalus::TypeExpr<std::tuple_element_t<I, Tuple>>::json()), ...);
}

template <typename R>
inline void push_types_for_return(std::vector<std::string>& out) {
  if constexpr (daedalus::IsTuple<R>::value) {
    daedalus::push_types_for_tuple<R>(out, std::make_index_sequence<std::tuple_size<R>::value>{});
  } else {
    out.push_back(daedalus::TypeExpr<R>::json());
  }
}

// Decode positional args from payload JSON:
// - Find `"args"` then parse the array elements using Codec<T>.
template <typename... Args>
bool decode_args(const char* payload, std::tuple<Args...>& out) {
  size_t pos = 0;
  if (!find_key(payload, "args", pos)) return false;
  // find '[' after key
  while (payload[pos] && payload[pos] != '[') pos++;
  if (!payload[pos]) return false;
  size_t i = pos;
  if (!consume(payload, i, '[')) return false;

  bool ok = true;
  size_t arg_index = 0;
  auto decode_one = [&](auto& slot) {
    if (!ok) return;
    skip_ws(payload, i);
    if (arg_index > 0) {
      if (!consume(payload, i, ',')) { ok = false; return; }
    }
    using SlotT = std::decay_t<decltype(slot)>;
    if (!Codec<SlotT>::decode(payload, i, slot)) { ok = false; return; }
    arg_index++;
  };

  std::apply([&](auto&... items) { (decode_one(items), ...); }, out);
  if (!ok) return false;
  skip_ws(payload, i);
  if (!consume(payload, i, ']')) return false;
  return true;
}

template <typename R>
std::string encode_return(const R& r) {
  if constexpr (IsTuple<R>::value) {
    std::string out = "[";
    bool first = true;
    std::apply([&](auto const&... items) {
      ((out += (first ? (first = false, "") : ","), out += Codec<std::decay_t<decltype(items)>>::encode(items)), ...);
    }, r);
    out += "]";
    return out;
  } else {
    return Codec<R>::encode(r);
  }
}

struct NodeDef {
  const char* id = nullptr;
  const char* cc_function = nullptr;
  struct PortDef {
    const char* name = nullptr;
    std::string ty_json;          // JSON TypeExpr
    std::string source;           // optional
    std::string const_value_json; // optional raw JSON
  };
  std::vector<PortDef> inputs;
  std::vector<PortDef> outputs;
  bool stateful = false;
  const char* state_json = nullptr; // raw JSON literal, optional
  std::string shader_json;          // raw JSON object, optional
  std::string label;                // optional
  std::string metadata_json;        // optional raw JSON object
  std::string default_compute = "CpuOnly";
  std::vector<std::string> feature_flags;
  std::vector<std::vector<std::string>> sync_groups_ports;
  std::string sync_groups_json; // optional raw JSON array (overrides sync_groups_ports)

  NodeDef& set_input_const_json(const char* port, const char* json) {
    for (auto& p : inputs) {
      if (p.name && port && std::string(p.name) == port) {
        p.const_value_json = json ? json : "";
        break;
      }
    }
    return *this;
  }

  NodeDef& set_input_source(const char* port, const char* source_name) {
    for (auto& p : inputs) {
      if (p.name && port && std::string(p.name) == port) {
        p.source = source_name ? source_name : "";
        break;
      }
    }
    return *this;
  }

  NodeDef& set_label(const char* s) {
    label = s ? s : "";
    return *this;
  }

  NodeDef& set_metadata_json(const char* s) {
    metadata_json = s ? s : "";
    return *this;
  }

  NodeDef& set_default_compute(const char* s) {
    default_compute = (s && *s) ? s : "CpuOnly";
    return *this;
  }

  NodeDef& add_feature_flag(const char* s) {
    if (s && *s) feature_flags.emplace_back(s);
    return *this;
  }

  NodeDef& add_sync_group_ports(const std::vector<const char*>& ports) {
    std::vector<std::string> g;
    g.reserve(ports.size());
    for (auto* p : ports) {
      g.emplace_back(p ? p : "");
    }
    sync_groups_ports.push_back(std::move(g));
    return *this;
  }

  NodeDef& set_sync_groups_json(const char* s) {
    sync_groups_json = s ? s : "";
    return *this;
  }
};

struct StatefulContext {
  const char* payload = nullptr;

  explicit StatefulContext(const char* p) : payload(p) {}

  template <typename T>
  std::optional<T> decode_payload_field(const char* key) const {
    if (!payload || !key) return std::nullopt;
    size_t pos = 0;
    if (!find_key(payload, key, pos)) return std::nullopt;
    size_t i = pos;
    while (payload[i] && payload[i] != ':') i++;
    if (!payload[i]) return std::nullopt;
    i++;
    skip_ws(payload, i);
    if (match_literal(payload, i, "null")) return std::nullopt;
    T out{};
    if (!Codec<T>::decode(payload, i, out)) return std::nullopt;
    return out;
  }

  template <typename T>
  std::optional<T> state() const {
    return decode_payload_field<T>("state");
  }

  template <typename T>
  std::optional<T> state_spec() const {
    return decode_payload_field<T>("state_spec");
  }

  std::optional<int64_t> state_i64(const char* field) const {
    if (!payload) return std::nullopt;
    double v = 0;
    if (!extract_number_field_in_object(payload, "state", field, v)) return std::nullopt;
    return (int64_t)v;
  }

  int64_t state_spec_i64(const char* field, int64_t def) const {
    if (!payload) return def;
    double v = 0;
    if (!extract_number_field_in_object(payload, "state_spec", field, v)) return def;
    return (int64_t)v;
  }
};

template <typename R>
struct StatefulResult {
  std::string state_json; // raw JSON object, e.g. {"value": 123}
  R outputs;
};

template <typename>
struct StatefulFnTraits;

template <typename R, typename... Args>
struct StatefulFnTraits<StatefulResult<R> (*)(const StatefulContext&, Args...)> {
  using Return = R;
  using ArgsTuple = std::tuple<Args...>;
  static constexpr size_t arity = sizeof...(Args);
};

template <typename R>
inline std::string encode_stateful_result(const StatefulResult<R>& r) {
  std::string out = "{\"state\":";
  out += r.state_json.empty() ? "null" : r.state_json;
  out += ",\"outputs\":";
  out += encode_return(r.outputs);
  out += "}";
  return out;
}

inline std::vector<NodeDef>& registry() {
  static std::vector<NodeDef> g;
  return g;
}

inline NodeDef& register_node(NodeDef def) {
  registry().push_back(std::move(def));
  return registry().back();
}

inline NodeDef* find_node(const char* id) {
  if (!id) return nullptr;
  for (auto& n : registry()) {
    if (n.id && std::string(n.id) == id) return &n;
  }
  return nullptr;
}

inline std::vector<NodeDef::PortDef> make_ports(const std::vector<const char*>& names,
                                                const std::vector<std::string>& tys) {
  std::vector<NodeDef::PortDef> out;
  out.reserve(names.size());
  for (size_t i = 0; i < names.size(); i++) {
    NodeDef::PortDef p;
    p.name = names[i];
    p.ty_json = i < tys.size() ? tys[i] : daedalus::json_scalar("Unit");
    out.push_back(std::move(p));
  }
  return out;
}

inline std::string emit_manifest_json(const char* plugin_name,
                                      const char* plugin_version,
                                      const char* plugin_description) {
  std::string out;
  out += "{";
  out += "\"manifest_version\":\"1\",";
  out += "\"language\":\"c_cpp\",";
  out += "\"plugin\":{";
  out += "\"name\":\"" + json_escape(plugin_name ? plugin_name : "plugin") + "\",";
  out += "\"version\":" + (plugin_version ? (std::string("\"") + json_escape(plugin_version) + "\"") : "null") + ",";
  out += "\"description\":" + (plugin_description ? (std::string("\"") + json_escape(plugin_description) + "\"") : "null") + ",";
  out += "\"metadata\":{}";
  out += "},";
  out += "\"nodes\":[";
  const auto& nodes = registry();
  for (size_t ni = 0; ni < nodes.size(); ni++) {
    const auto& n = nodes[ni];
    if (ni) out += ",";
    out += "{";
    out += "\"id\":\"" + json_escape(n.id) + "\",";
    if (!n.label.empty()) {
      out += "\"label\":\"" + json_escape(n.label) + "\",";
    }
    if (n.cc_function) {
      out += "\"cc_function\":\"" + json_escape(n.cc_function) + "\",";
    }
    out += "\"stateful\":" + std::string(n.stateful ? "true" : "false") + ",";
    if (n.state_json) {
      out += "\"state\":" + std::string(n.state_json) + ",";
    }
    if (!n.shader_json.empty()) {
      out += "\"shader\":" + n.shader_json + ",";
    }
    out += "\"default_compute\":\"" + json_escape(n.default_compute.empty() ? "CpuOnly" : n.default_compute) + "\",";
    out += "\"feature_flags\":[";
    for (size_t fi = 0; fi < n.feature_flags.size(); fi++) {
      if (fi) out += ",";
      out += "\"" + json_escape(n.feature_flags[fi]) + "\"";
    }
    out += "],";
    out += "\"sync_groups\":";
    if (!n.sync_groups_json.empty()) {
      out += n.sync_groups_json;
    } else {
      out += "[";
      for (size_t gi = 0; gi < n.sync_groups_ports.size(); gi++) {
        if (gi) out += ",";
        out += "[";
        for (size_t pi = 0; pi < n.sync_groups_ports[gi].size(); pi++) {
          if (pi) out += ",";
          out += "\"" + json_escape(n.sync_groups_ports[gi][pi]) + "\"";
        }
        out += "]";
      }
      out += "]";
    }
    out += ",";
    if (!n.metadata_json.empty()) {
      out += "\"metadata\":" + n.metadata_json + ",";
    } else {
      out += "\"metadata\":{},";
    }
    out += "\"inputs\":[";
    for (size_t pi = 0; pi < n.inputs.size(); pi++) {
      if (pi) out += ",";
      const auto& p = n.inputs[pi];
      out += "{\"name\":\"" + json_escape(p.name ? p.name : "") + "\",\"ty\":" + p.ty_json;
      if (!p.source.empty()) out += ",\"source\":\"" + json_escape(p.source) + "\"";
      if (!p.const_value_json.empty()) out += ",\"const_value\":" + p.const_value_json;
      out += "}";
    }
    out += "],";
    out += "\"outputs\":[";
    for (size_t pi = 0; pi < n.outputs.size(); pi++) {
      if (pi) out += ",";
      const auto& p = n.outputs[pi];
      out += "{\"name\":\"" + json_escape(p.name ? p.name : "") + "\",\"ty\":" + p.ty_json;
      if (!p.source.empty()) out += ",\"source\":\"" + json_escape(p.source) + "\"";
      out += "}";
    }
    out += "]";
    out += "}";
  }
  out += "]";
  out += "}";
  return out;
}

// Default manifest symbol (manifest-from-dylib flow).
// Rust loads the library, calls this function, parses manifest JSON, and assumes node symbols live in this library.
inline const char*& plugin_name_ref() { static const char* v = "plugin"; return v; }
inline const char*& plugin_version_ref() { static const char* v = nullptr; return v; }
inline const char*& plugin_desc_ref() { static const char* v = nullptr; return v; }

} // namespace daedalus

// Macro helpers
#define DAEDALUS_NAMES(...) std::vector<const char*>{__VA_ARGS__}

// Rust-like port naming convenience:
// - DAEDALUS_PORTS(a, b, cfg) => {"a","b","cfg"}
// Useful to avoid repeating quoted strings (closer to Rust `#[node(inputs(a,b))]` ergonomics).
#define DAEDALUS_PP_STR(x) #x
#define DAEDALUS_PORTS_1(a1) std::vector<const char*>{DAEDALUS_PP_STR(a1)}
#define DAEDALUS_PORTS_2(a1, a2) std::vector<const char*>{DAEDALUS_PP_STR(a1), DAEDALUS_PP_STR(a2)}
#define DAEDALUS_PORTS_3(a1, a2, a3) std::vector<const char*>{DAEDALUS_PP_STR(a1), DAEDALUS_PP_STR(a2), DAEDALUS_PP_STR(a3)}
#define DAEDALUS_PORTS_4(a1, a2, a3, a4) std::vector<const char*>{DAEDALUS_PP_STR(a1), DAEDALUS_PP_STR(a2), DAEDALUS_PP_STR(a3), DAEDALUS_PP_STR(a4)}
#define DAEDALUS_PORTS_5(a1, a2, a3, a4, a5) std::vector<const char*>{DAEDALUS_PP_STR(a1), DAEDALUS_PP_STR(a2), DAEDALUS_PP_STR(a3), DAEDALUS_PP_STR(a4), DAEDALUS_PP_STR(a5)}
#define DAEDALUS_PORTS_6(a1, a2, a3, a4, a5, a6) std::vector<const char*>{DAEDALUS_PP_STR(a1), DAEDALUS_PP_STR(a2), DAEDALUS_PP_STR(a3), DAEDALUS_PP_STR(a4), DAEDALUS_PP_STR(a5), DAEDALUS_PP_STR(a6)}
#define DAEDALUS_PORTS_7(a1, a2, a3, a4, a5, a6, a7) std::vector<const char*>{DAEDALUS_PP_STR(a1), DAEDALUS_PP_STR(a2), DAEDALUS_PP_STR(a3), DAEDALUS_PP_STR(a4), DAEDALUS_PP_STR(a5), DAEDALUS_PP_STR(a6), DAEDALUS_PP_STR(a7)}
#define DAEDALUS_PORTS_8(a1, a2, a3, a4, a5, a6, a7, a8) std::vector<const char*>{DAEDALUS_PP_STR(a1), DAEDALUS_PP_STR(a2), DAEDALUS_PP_STR(a3), DAEDALUS_PP_STR(a4), DAEDALUS_PP_STR(a5), DAEDALUS_PP_STR(a6), DAEDALUS_PP_STR(a7), DAEDALUS_PP_STR(a8)}
#define DAEDALUS_PORTS_DISPATCH(_1, _2, _3, _4, _5, _6, _7, _8, NAME, ...) NAME
#define DAEDALUS_PORTS(...) DAEDALUS_PORTS_DISPATCH(__VA_ARGS__, DAEDALUS_PORTS_8, DAEDALUS_PORTS_7, DAEDALUS_PORTS_6, DAEDALUS_PORTS_5, DAEDALUS_PORTS_4, DAEDALUS_PORTS_3, DAEDALUS_PORTS_2, DAEDALUS_PORTS_1)(__VA_ARGS__)

// Node-level ergonomics helpers (set manifest label/metadata without hand-writing boilerplate).
#define DAEDALUS_NODE_LABEL(node_id, label_str)                                                              \
  static bool daedalus__label_##__LINE__ = []() {                                                            \
    if (auto* n = daedalus::find_node(node_id)) n->set_label(label_str);                                     \
    return true;                                                                                             \
  }();

#define DAEDALUS_NODE_METADATA_JSON(node_id, json_object_literal)                                            \
  static bool daedalus__meta_##__LINE__ = []() {                                                             \
    if (auto* n = daedalus::find_node(node_id)) n->set_metadata_json(json_object_literal);                   \
    return true;                                                                                             \
  }();

// Compute affinity (matches Rust/manifest values, e.g. "CpuOnly", "GpuPreferred", "GpuRequired").
#define DAEDALUS_NODE_DEFAULT_COMPUTE(node_id, affinity_str)                                                 \
  static bool daedalus__compute_##__LINE__ = []() {                                                          \
    if (auto* n = daedalus::find_node(node_id)) n->set_default_compute(affinity_str);                        \
    return true;                                                                                             \
  }();

// Sync groups: shorthand ports array (equivalent to manifest `sync_groups: [["a","b"]]`).
#define DAEDALUS_NODE_SYNC_GROUP_PORTS(node_id, ports_vec)                                                   \
  static bool daedalus__sync_##__LINE__ = []() {                                                             \
    if (auto* n = daedalus::find_node(node_id)) n->add_sync_group_ports(ports_vec);                          \
    return true;                                                                                             \
  }();

// Register a stateless typed node:
// - `symbol`: exported C ABI function name (stable)
// - `id`: Daedalus node id (string literal)
// - `fn`: pointer to typed function
// - `inputs`: DAEDALUS_NAMES("a","b",...)
// - `outputs`: DAEDALUS_NAMES("out",...) (for tuple returns, provide out0/out1... or your own names)
#define DAEDALUS_REGISTER_NODE(symbol, node_id, fn, input_names, output_names)                              \
  extern "C" DAEDALUS_EXPORT DaedalusCppResult symbol(const char* payload_json) {                           \
    using FnPtr = decltype(+fn);                                                                            \
    using Traits = daedalus::FnTraits<FnPtr>;                                                               \
    typename Traits::ArgsTuple args;                                                                        \
    if (!payload_json) return daedalus::err_str("missing payload");                                         \
    if (!daedalus::decode_args(payload_json, args)) return daedalus::err_str("failed to decode args");      \
    auto out = std::apply(fn, args);                                                                        \
    return daedalus::ok_json(daedalus::encode_return(out));                                                 \
  }                                                                                                         \
  static bool symbol##_registered = []() {                                                                   \
    using FnPtr = decltype(+fn);                                                                            \
    using Traits = daedalus::FnTraits<FnPtr>;                                                               \
    daedalus::NodeDef def;                                                                                  \
    def.id = node_id;                                                                                       \
    def.cc_function = #symbol;                                                                              \
    const std::vector<const char*> in_names = input_names;                                                  \
    const std::vector<const char*> out_names = output_names;                                                \
    std::vector<std::string> in_types;                                                                       \
    std::vector<std::string> out_types;                                                                      \
    in_types.reserve(in_names.size());                                                                       \
    out_types.reserve(out_names.size());                                                                     \
    std::apply([&](auto... t) {                                                                             \
      (in_types.push_back(daedalus::TypeExpr<std::decay_t<decltype(t)>>::json()), ...);                     \
    }, typename Traits::ArgsTuple{});                                                                        \
    using R = typename Traits::Return;                                                                      \
    daedalus::push_types_for_return<R>(out_types);                                                          \
    def.inputs = daedalus::make_ports(in_names, in_types);                                                   \
    def.outputs = daedalus::make_ports(out_names, out_types);                                                \
    daedalus::register_node(std::move(def));                                                                \
    return true;                                                                                            \
  }();

// Same as `DAEDALUS_REGISTER_NODE`, but allows inline node definition tweaks (closer to Rust `#[node(...)]`).
//
// Example:
//   DAEDALUS_REGISTER_NODE_WITH(sym, "pkg:scale", scale,
//     DAEDALUS_PORTS(value,cfg), DAEDALUS_PORTS(out),
//     { def.set_label("Scale"); def.set_metadata_json("{\"category\":\"config\"}"); }
//   )
#define DAEDALUS_REGISTER_NODE_WITH(symbol, node_id, fn, input_names, output_names, customize_block)         \
  extern "C" DAEDALUS_EXPORT DaedalusCppResult symbol(const char* payload_json) {                            \
    using FnPtr = decltype(+fn);                                                                             \
    using Traits = daedalus::FnTraits<FnPtr>;                                                                \
    typename Traits::ArgsTuple args;                                                                         \
    if (!payload_json) return daedalus::err_str("missing payload");                                          \
    if (!daedalus::decode_args(payload_json, args)) return daedalus::err_str("failed to decode args");       \
    auto out = std::apply(fn, args);                                                                         \
    return daedalus::ok_json(daedalus::encode_return(out));                                                  \
  }                                                                                                          \
  static bool symbol##_registered = []() {                                                                   \
    using FnPtr = decltype(+fn);                                                                             \
    using Traits = daedalus::FnTraits<FnPtr>;                                                                \
    daedalus::NodeDef def;                                                                                   \
    def.id = node_id;                                                                                        \
    def.cc_function = #symbol;                                                                               \
    const std::vector<const char*> in_names = input_names;                                                   \
    const std::vector<const char*> out_names = output_names;                                                 \
    std::vector<std::string> in_types;                                                                       \
    std::vector<std::string> out_types;                                                                      \
    in_types.reserve(in_names.size());                                                                       \
    out_types.reserve(out_names.size());                                                                     \
    std::apply([&](auto... t) {                                                                              \
      (in_types.push_back(daedalus::TypeExpr<std::decay_t<decltype(t)>>::json()), ...);                      \
    }, typename Traits::ArgsTuple{});                                                                        \
    using R = typename Traits::Return;                                                                       \
    daedalus::push_types_for_return<R>(out_types);                                                           \
    def.inputs = daedalus::make_ports(in_names, in_types);                                                   \
    def.outputs = daedalus::make_ports(out_names, out_types);                                                \
    do customize_block while (0);                                                                            \
    daedalus::register_node(std::move(def));                                                                 \
    return true;                                                                                             \
  }();

// "Rust-like" node macros (auto-generate exported symbol names).
// These are ideal for the manifest-from-dylib flow where the manifest is generated in-process,
// so the symbol name doesn't need to be human-chosen or referenced in a separate manifest file.
#define DAEDALUS_NODE_WITH(node_id, fn, input_names, output_names, customize_block)                          \
  DAEDALUS_NODE_WITH_I(node_id, fn, input_names, output_names, customize_block, __COUNTER__)
#define DAEDALUS_NODE_WITH_I(node_id, fn, input_names, output_names, customize_block, ctr)                   \
  DAEDALUS_REGISTER_NODE_WITH(DAEDALUS_PP_CAT(daedalus_node_, ctr), node_id, fn, input_names, output_names, customize_block)
#define DAEDALUS_NODE(node_id, fn, input_names, output_names)                                                \
  DAEDALUS_NODE_WITH(node_id, fn, input_names, output_names, {})

// Declare plugin metadata for the manifest-from-dylib flow.
// The library exports `daedalus_cpp_manifest()` returning manifest JSON.
#define DAEDALUS_PLUGIN(name, version, description)                                                         \
  extern "C" DAEDALUS_EXPORT void daedalus_free(char* p) { if (p) std::free(p); }                            \
  static bool daedalus__plugin_meta = []() {                                                                 \
    daedalus::plugin_name_ref() = name;                                                                      \
    daedalus::plugin_version_ref() = version;                                                                \
    daedalus::plugin_desc_ref() = description;                                                               \
    return true;                                                                                             \
  }();                                                                                                       \
  extern "C" DAEDALUS_EXPORT DaedalusCppResult daedalus_cpp_manifest() {                                     \
    const std::string json = daedalus::emit_manifest_json(daedalus::plugin_name_ref(),                       \
                                                          daedalus::plugin_version_ref(),                    \
                                                          daedalus::plugin_desc_ref());                      \
    return daedalus::ok_json(json);                                                                          \
  }

// Register a stateful node:
// - The typed function signature is:
//     daedalus::StatefulResult<R> fn(const daedalus::StatefulContext& ctx, Args... args)
// - The wrapper returns {"state":..., "outputs":...} so Rust can persist state across runs.
#define DAEDALUS_REGISTER_STATEFUL_NODE(symbol, node_id, fn, input_names, output_names, state_json_literal)  \
  extern "C" DAEDALUS_EXPORT DaedalusCppResult symbol(const char* payload_json) {                           \
    using FnPtr = decltype(+fn);                                                                            \
    using Traits = daedalus::StatefulFnTraits<FnPtr>;                                                       \
    typename Traits::ArgsTuple args;                                                                        \
    if (!payload_json) return daedalus::err_str("missing payload");                                         \
    if (!daedalus::decode_args(payload_json, args)) return daedalus::err_str("failed to decode args");      \
    daedalus::StatefulContext ctx(payload_json);                                                            \
    auto r = std::apply([&](auto... a) { return fn(ctx, a...); }, args);                                    \
    return daedalus::ok_json(daedalus::encode_stateful_result(r));                                          \
  }                                                                                                         \
  static bool symbol##_registered = []() {                                                                   \
    using FnPtr = decltype(+fn);                                                                            \
    using Traits = daedalus::StatefulFnTraits<FnPtr>;                                                       \
    daedalus::NodeDef def;                                                                                  \
    def.id = node_id;                                                                                       \
    def.cc_function = #symbol;                                                                              \
    const std::vector<const char*> in_names = input_names;                                                  \
    const std::vector<const char*> out_names = output_names;                                                \
    def.stateful = true;                                                                                    \
    def.state_json = state_json_literal;                                                                     \
    std::vector<std::string> in_types;                                                                       \
    std::vector<std::string> out_types;                                                                      \
    in_types.reserve(in_names.size());                                                                       \
    out_types.reserve(out_names.size());                                                                     \
    std::apply([&](auto... t) {                                                                             \
      (in_types.push_back(daedalus::TypeExpr<std::decay_t<decltype(t)>>::json()), ...);                     \
    }, typename Traits::ArgsTuple{});                                                                        \
    using R = typename Traits::Return;                                                                      \
    daedalus::push_types_for_return<R>(out_types);                                                          \
    def.inputs = daedalus::make_ports(in_names, in_types);                                                   \
    def.outputs = daedalus::make_ports(out_names, out_types);                                                \
    daedalus::register_node(std::move(def));                                                                \
    return true;                                                                                            \
  }();

// Same as `DAEDALUS_REGISTER_STATEFUL_NODE`, but allows inline node definition tweaks.
#define DAEDALUS_REGISTER_STATEFUL_NODE_WITH(symbol, node_id, fn, input_names, output_names, state_json_literal, customize_block) \
  extern "C" DAEDALUS_EXPORT DaedalusCppResult symbol(const char* payload_json) {                            \
    using FnPtr = decltype(+fn);                                                                             \
    using Traits = daedalus::StatefulFnTraits<FnPtr>;                                                        \
    typename Traits::ArgsTuple args;                                                                         \
    if (!payload_json) return daedalus::err_str("missing payload");                                          \
    if (!daedalus::decode_args(payload_json, args)) return daedalus::err_str("failed to decode args");       \
    daedalus::StatefulContext ctx(payload_json);                                                             \
    auto r = std::apply([&](auto... a) { return fn(ctx, a...); }, args);                                     \
    return daedalus::ok_json(daedalus::encode_stateful_result(r));                                           \
  }                                                                                                          \
  static bool symbol##_registered = []() {                                                                   \
    using FnPtr = decltype(+fn);                                                                             \
    using Traits = daedalus::StatefulFnTraits<FnPtr>;                                                        \
    daedalus::NodeDef def;                                                                                   \
    def.id = node_id;                                                                                        \
    def.cc_function = #symbol;                                                                               \
    const std::vector<const char*> in_names = input_names;                                                   \
    const std::vector<const char*> out_names = output_names;                                                 \
    def.stateful = true;                                                                                     \
    def.state_json = state_json_literal;                                                                     \
    std::vector<std::string> in_types;                                                                       \
    std::vector<std::string> out_types;                                                                      \
    in_types.reserve(in_names.size());                                                                       \
    out_types.reserve(out_names.size());                                                                     \
    std::apply([&](auto... t) {                                                                              \
      (in_types.push_back(daedalus::TypeExpr<std::decay_t<decltype(t)>>::json()), ...);                      \
    }, typename Traits::ArgsTuple{});                                                                        \
    using R = typename Traits::Return;                                                                       \
    daedalus::push_types_for_return<R>(out_types);                                                           \
    def.inputs = daedalus::make_ports(in_names, in_types);                                                   \
    def.outputs = daedalus::make_ports(out_names, out_types);                                                \
    do customize_block while (0);                                                                            \
    daedalus::register_node(std::move(def));                                                                 \
    return true;                                                                                             \
  }();

// Auto-symbol stateful node macros (manifest-from-dylib friendly).
#define DAEDALUS_STATEFUL_NODE_WITH(node_id, fn, input_names, output_names, state_json_literal, customize_block) \
  DAEDALUS_STATEFUL_NODE_WITH_I(node_id, fn, input_names, output_names, state_json_literal, customize_block, __COUNTER__)
#define DAEDALUS_STATEFUL_NODE_WITH_I(node_id, fn, input_names, output_names, state_json_literal, customize_block, ctr) \
  DAEDALUS_REGISTER_STATEFUL_NODE_WITH(DAEDALUS_PP_CAT(daedalus_stateful_node_, ctr), node_id, fn, input_names, output_names, state_json_literal, customize_block)
#define DAEDALUS_STATEFUL_NODE(node_id, fn, input_names, output_names, state_json_literal)                   \
  DAEDALUS_STATEFUL_NODE_WITH(node_id, fn, input_names, output_names, state_json_literal, {})

namespace daedalus {

// Shader node builder (manifest-only; executed by Rust GPU shader runner).
struct ShaderBinding {
  int binding = 0;
  std::string kind;       // e.g. "storage_buffer"
  std::string access;     // "read_only" | "read_write" | "write_only"
  bool readback = false;
  std::string to_port;
  std::string from_state;
  std::string to_state;
  std::string state_backend; // "cpu" | "gpu"
  int size_bytes = 0;
};

struct ShaderSpecBuilder {
  std::string src_path;
  std::string entry = "main";
  std::string name;
  int inv_x = 1, inv_y = 1, inv_z = 1;
  std::vector<ShaderBinding> bindings;

  ShaderSpecBuilder& file(const std::string& p) {
    src_path = p;
    return *this;
  }
  ShaderSpecBuilder& entrypoint(const std::string& e) {
    entry = e;
    return *this;
  }
  ShaderSpecBuilder& shader_name(const std::string& n) {
    name = n;
    return *this;
  }
  ShaderSpecBuilder& invocations(int x, int y, int z) {
    inv_x = x;
    inv_y = y;
    inv_z = z;
    return *this;
  }
  ShaderSpecBuilder& storage_u32_rw(int binding, const char* to_port, int size_bytes, bool readback = true) {
    ShaderBinding b;
    b.binding = binding;
    b.kind = "storage_buffer";
    b.access = "read_write";
    b.readback = readback;
    b.to_port = to_port ? to_port : "";
    b.size_bytes = size_bytes;
    bindings.push_back(std::move(b));
    return *this;
  }
  ShaderSpecBuilder& storage_u32_state(int binding,
                                      const char* from_state,
                                      const char* to_state,
                                      const char* to_port,
                                      int size_bytes,
                                      const char* backend = nullptr,
                                      bool readback = true) {
    ShaderBinding b;
    b.binding = binding;
    b.kind = "storage_buffer";
    b.access = "read_write";
    b.readback = readback;
    b.from_state = from_state ? from_state : "";
    b.to_state = to_state ? to_state : "";
    b.to_port = to_port ? to_port : "";
    b.size_bytes = size_bytes;
    if (backend) b.state_backend = backend;
    bindings.push_back(std::move(b));
    return *this;
  }

  std::string json() const {
    std::string out = "{";
    out += "\"src_path\":\"" + json_escape(src_path) + "\",";
    out += "\"entry\":\"" + json_escape(entry) + "\",";
    if (!name.empty()) out += "\"name\":\"" + json_escape(name) + "\",";
    out += "\"invocations\":[" + std::to_string(inv_x) + "," + std::to_string(inv_y) + "," + std::to_string(inv_z) + "],";
    out += "\"bindings\":[";
    for (size_t i = 0; i < bindings.size(); i++) {
      if (i) out += ",";
      const auto& b = bindings[i];
      out += "{";
      out += "\"binding\":" + std::to_string(b.binding) + ",";
      out += "\"kind\":\"" + json_escape(b.kind) + "\",";
      out += "\"access\":\"" + json_escape(b.access) + "\",";
      if (b.readback) out += "\"readback\":true,";
      if (!b.to_port.empty()) out += "\"to_port\":\"" + json_escape(b.to_port) + "\",";
      if (!b.from_state.empty()) out += "\"from_state\":\"" + json_escape(b.from_state) + "\",";
      if (!b.to_state.empty()) out += "\"to_state\":\"" + json_escape(b.to_state) + "\",";
      if (!b.state_backend.empty()) out += "\"state_backend\":\"" + json_escape(b.state_backend) + "\",";
      if (b.size_bytes > 0) out += "\"size_bytes\":" + std::to_string(b.size_bytes) + ",";
      if (!out.empty() && out.back() == ',') out.pop_back();
      out += "}";
    }
    out += "]";
    if (!out.empty() && out.back() == ',') out.pop_back();
    out += "}";
    return out;
  }
};

inline ShaderSpecBuilder shader() { return ShaderSpecBuilder{}; }

template <typename Tuple>
inline std::vector<std::string> infer_types_from_tuple(const Tuple& tup) {
  std::vector<std::string> out;
  std::apply([&](auto... items) {
    (out.push_back(daedalus::TypeExpr<std::decay_t<decltype(items)>>::json()), ...);
  }, tup);
  return out;
}

} // namespace daedalus

// Register a shader-only node into the plugin manifest (no C ABI symbol needed).
// The node will execute on the Rust GPU shader runner when built with `gpu-wgpu`.
//
// `in_types` / `out_types` are tuples used for type inference, e.g. `std::tuple<uint32_t>{}`.
#define DAEDALUS_REGISTER_SHADER_NODE_T(regsym, node_id, input_names, in_types, output_names, out_types, shader_builder) \
  static bool regsym##_registered_shader = []() {                                                           \
    daedalus::NodeDef def;                                                                                  \
    def.id = node_id;                                                                                       \
    const std::vector<const char*> in_names = input_names;                                                  \
    const std::vector<const char*> out_names = output_names;                                                \
    def.inputs = daedalus::make_ports(in_names, daedalus::infer_types_from_tuple(in_types));                \
    def.outputs = daedalus::make_ports(out_names, daedalus::infer_types_from_tuple(out_types));             \
    def.shader_json = (shader_builder).json();                                                              \
    daedalus::register_node(std::move(def));                                                                \
    return true;                                                                                            \
  }();
