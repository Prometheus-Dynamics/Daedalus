#include <cassert>
#include <cstdint>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include <daedalus.hpp>

struct State {
  int64_t sum = 0;
};

DAEDALUS_TYPE_KEY(Point, "test.Point")
struct Point {
  double x;
  double y;
};

DAEDALUS_ADAPTER(point_to_i64, Point, int64_t, reinterpret)
int64_t point_to_i64(const Point& point) {
  return static_cast<int64_t>(point.x);
}

DAEDALUS_NODE(add, inputs(a, b), outputs(out))
int64_t add(int64_t a, int64_t b) {
  return a + b;
}

DAEDALUS_STATEFUL_NODE(accum, State, inputs(value), outputs(sum))
int64_t accum(int64_t value, State& state) {
  state.sum += value;
  return state.sum;
}

DAEDALUS_NODE(payload_len, inputs(frame), outputs(len), access(view))
uint64_t payload_len(daedalus::BytesView frame) {
  return frame.size();
}

struct Tagged {
  static constexpr const char* daedalus_type_key = "test.Tagged";
};

std::tuple<float, double> widths(
    int8_t i8, int16_t i16, int32_t i32, int64_t i64, uint8_t u8, uint16_t u16, uint32_t u32,
    uint64_t u64, bool flag, const std::string& text, std::optional<int32_t> maybe,
    std::vector<uint16_t> list, std::map<std::string, float> table, std::tuple<int8_t, bool> pair,
    std::vector<uint8_t> raw, const daedalus::BytesView& view, Tagged tagged, State& state) {
  return {0.0F, 0.0};
}
DAEDALUS_NODE(
    widths,
    inputs(i8, i16, i32, i64, u8, u16, u32, u64, flag, text, maybe, list, table, pair, raw, view, tagged),
    outputs(f32, f64),
    daedalus::signature<decltype(widths)>())

DAEDALUS_NODE(scale_u32, inputs(value), outputs(out), daedalus::signature<uint32_t(uint32_t) noexcept>())

DAEDALUS_BOUNDARY_CONTRACT("test.Point", host_read, worker_write)
DAEDALUS_PACKAGE_ARTIFACT("_bundle/native/any/libsdk_test.so")
DAEDALUS_PLUGIN(sdk_test, add, accum, payload_len)

int main() {
  auto descriptor = daedalus::PackageBuilder::from_plugin("sdk_test")
      .shared_library("build/libsdk_test.so")
      .source_file("tests/sdk_descriptor.cpp")
      .descriptor();
  assert(descriptor.find("\"nodes\": [") != std::string::npos);
  assert(descriptor.find("\"id\":\"add\"") != std::string::npos);
  assert(descriptor.find("\"id\":\"accum\"") != std::string::npos);
  assert(descriptor.find("\"stateful\":true") != std::string::npos);
  assert(descriptor.find("\"access\":\"view\"") != std::string::npos);
  assert(descriptor.find("\"backends\": {\"add\"") != std::string::npos);
  assert(descriptor.find("\"boundary_contracts\": [") != std::string::npos);
  assert(descriptor.find("\"test.Point\"") != std::string::npos);
  assert(descriptor.find("\"point_to_i64\"") != std::string::npos);
  const auto port = [](const std::string& name, const std::string& ty) {
    return "{\"name\":\"" + name + "\",\"ty\":" + ty + ",";
  };
  const auto has_scalar = [&](const std::string& name, const std::string& scalar) {
    return descriptor.find(port(name, "{\"Scalar\":\"" + scalar + "\"}")) != std::string::npos;
  };
  assert(has_scalar("i8", "I8") && has_scalar("i16", "I16") && has_scalar("i32", "I32"));
  assert(has_scalar("i64", "Int") && has_scalar("u8", "U8") && has_scalar("u16", "U16"));
  assert(has_scalar("u32", "U32") && has_scalar("u64", "U64") && has_scalar("flag", "Bool"));
  assert(has_scalar("text", "String") && has_scalar("f32", "F32") && has_scalar("f64", "Float"));
  assert(has_scalar("raw", "Bytes") && has_scalar("view", "Bytes"));
  assert(descriptor.find(port("maybe", "{\"Optional\":{\"Scalar\":\"I32\"}}")) != std::string::npos);
  assert(descriptor.find(port("list", "{\"List\":{\"Scalar\":\"U16\"}}")) != std::string::npos);
  assert(descriptor.find(port("table", "{\"Map\":[{\"Scalar\":\"String\"},{\"Scalar\":\"F32\"}]}")) != std::string::npos);
  assert(descriptor.find(port("pair", "{\"Tuple\":[{\"Scalar\":\"I8\"},{\"Scalar\":\"Bool\"}]}")) != std::string::npos);
  assert(descriptor.find(port("tagged", "{\"Opaque\":\"test.Tagged\"}")) != std::string::npos);
  assert(has_scalar("value", "U32") && has_scalar("out", "U32"));
  // Untyped nodes keep name-based ports: `frame` is Bytes, `a` is Int.
  assert(has_scalar("frame", "Bytes") && has_scalar("a", "Int"));

  auto saved = daedalus::registry();
  daedalus::registry().nodes.push_back(daedalus::NodeSpec::make("add", inputs(a), outputs(out)));
  try {
    (void)daedalus::PackageBuilder::from_plugin("sdk_test").descriptor();
    assert(false && "duplicate node id should fail");
  } catch (const std::invalid_argument& error) {
    assert(std::string(error.what()).find("duplicate node id") != std::string::npos);
  }

  daedalus::registry() = saved;
  daedalus::registry().nodes.push_back(
      daedalus::NodeSpec::make("bad_access", inputs(value), outputs(out), access(project)));
  try {
    (void)daedalus::PackageBuilder::from_plugin("sdk_test").descriptor();
    assert(false && "unsupported access should fail");
  } catch (const std::invalid_argument& error) {
    assert(std::string(error.what()).find("unsupported access") != std::string::npos);
  }

  daedalus::registry() = saved;
  daedalus::registry().nodes.push_back(
      daedalus::NodeSpec::make("bad_boundary", inputs(frame), outputs(frame), layout(rgba8_hwc)));
  try {
    (void)daedalus::PackageBuilder::from_plugin("sdk_test").descriptor();
    assert(false && "layout without residency should fail");
  } catch (const std::invalid_argument& error) {
    assert(std::string(error.what()).find("layout requires residency") != std::string::npos);
  }

  daedalus::registry() = saved;
  daedalus::registry().boundaries.push_back({"", {"host_read"}});
  try {
    (void)daedalus::PackageBuilder::from_plugin("sdk_test").descriptor();
    assert(false && "empty boundary type key should fail");
  } catch (const std::invalid_argument& error) {
    assert(std::string(error.what()).find("type_key") != std::string::npos);
  }

  daedalus::registry() = saved;
  daedalus::registry().boundaries.push_back({"test.Bad", {"teleport"}});
  try {
    (void)daedalus::PackageBuilder::from_plugin("sdk_test").descriptor();
    assert(false && "unsupported boundary capability should fail");
  } catch (const std::invalid_argument& error) {
    assert(std::string(error.what()).find("unsupported boundary") != std::string::npos);
  }

  struct NoMapping {};
  const auto expect_signature_error = [&](daedalus::NodeSpec spec, const std::string& message) {
    daedalus::registry() = saved;
    daedalus::registry().nodes.push_back(std::move(spec));
    try {
      (void)daedalus::PackageBuilder::from_plugin("sdk_test").descriptor();
      assert(false && "invalid signature should fail");
    } catch (const std::invalid_argument& error) {
      assert(std::string(error.what()).find(message) != std::string::npos);
    }
  };
  expect_signature_error(
      daedalus::NodeSpec::make("unmapped", inputs(value), outputs(out), daedalus::signature<int32_t(std::optional<NoMapping>)>()),
      "no Daedalus mapping");
  expect_signature_error(
      daedalus::NodeSpec::make("arity", inputs(a, b), outputs(out), daedalus::signature<int32_t(int32_t)>()),
      "more inputs than its signature");
  expect_signature_error(
      daedalus::NodeSpec::make("outputs", inputs(a), outputs(out), daedalus::signature<std::tuple<int32_t, int32_t>(int32_t)>()),
      "output count");

  daedalus::registry() = saved;
}
