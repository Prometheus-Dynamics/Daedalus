#include <cassert>
#include <cstdint>
#include <stdexcept>
#include <string>

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

  daedalus::registry() = saved;
}
