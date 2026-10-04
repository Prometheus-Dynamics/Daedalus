// Wire round trips; prints encoder output for the Rust test to decode with serde.
#include <cassert>
#include <cstdint>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <string>

#include <daedalus.hpp>

// What the Rust host serializes for `WireValue::UInt(u64::MAX)`.
constexpr const char* UINT_MAX_JSON = R"({"kind":"uint","value":18446744073709551615})";

template <typename T, typename Error>
bool throws(const char* json) {
  try {
    (void)daedalus::from_wire<T>(json);
  } catch (const Error&) {
    return true;
  }
  return false;
}

int main() {
  constexpr auto u64_max = std::numeric_limits<uint64_t>::max();
  assert(daedalus::to_wire(u64_max) == UINT_MAX_JSON);
  assert(daedalus::from_wire<uint64_t>(UINT_MAX_JSON) == u64_max);
  assert(daedalus::to_wire(uint32_t{7}) == R"({"kind":"int","value":7})");
  assert(daedalus::from_wire<int64_t>(daedalus::to_wire(int64_t{-3})) == -3);
  assert(daedalus::from_wire<uint64_t>(R"({"kind": "int", "value": 5})") == 5);
  assert(daedalus::from_wire<double>(daedalus::to_wire(0.1)) == 0.1);
  assert(daedalus::from_wire<float>(daedalus::to_wire(1.5F)) == 1.5F);
  assert(daedalus::from_wire<bool>(daedalus::to_wire(true)));
  const std::string text = "a\"b\\c\n\t";
  assert(daedalus::from_wire<std::string>(daedalus::to_wire(text)) == text);
  assert((throws<int64_t, std::out_of_range>(UINT_MAX_JSON)));
  assert((throws<uint64_t, std::out_of_range>(R"({"kind":"int","value":-1})")));
  assert((throws<uint8_t, std::out_of_range>(R"({"kind":"int","value":256})")));
  assert((throws<int64_t, std::invalid_argument>(R"({"kind":"string","value":"1"})")));

  std::cout << "[" << daedalus::to_wire(u64_max) << "," << daedalus::to_wire(uint64_t{5}) << ","
            << daedalus::to_wire(int64_t{-3}) << "," << daedalus::to_wire(text) << "]\n";
}
