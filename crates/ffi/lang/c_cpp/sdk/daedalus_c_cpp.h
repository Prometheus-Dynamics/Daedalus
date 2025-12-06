#pragma once

// Minimal C ABI for Daedalus C/C++ manifest nodes.
//
// Each node exports a function:
//   DaedalusCppResult <symbol>(const char* payload_json);
//
// And a free function:
//   void daedalus_free(char* p);
//
// The node function returns malloc-allocated UTF-8 JSON on success (json != NULL),
// or malloc-allocated UTF-8 error (error != NULL). Rust always calls `daedalus_free`
// on any non-null pointer returned.

#include <stddef.h>

#ifdef _WIN32
#  define DAEDALUS_EXPORT __declspec(dllexport)
#else
#  define DAEDALUS_EXPORT __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct DaedalusCppResult {
  const char* json;
  const char* error;
} DaedalusCppResult;

DAEDALUS_EXPORT void daedalus_free(char* p);

#ifdef __cplusplus
}
#endif

