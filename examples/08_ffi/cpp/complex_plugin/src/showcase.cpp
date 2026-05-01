#include <cstdint>
#include <map>
#include <optional>
#include <span>
#include <string>
#include <tuple>
#include <vector>

#include <daedalus.hpp>

struct ScaleConfig {
  int64_t factor = 2;
};

struct AccumState {
  int64_t sum = 0;
};

struct InternalCount {
  int64_t value;
};

struct ExternalLegacyCount {
  std::string raw;
};

DAEDALUS_TYPE_KEY(Point, "ffi.showcase.Point")
struct Point {
  double x;
  double y;
};

DAEDALUS_TYPE_KEY(InternalCount, "ffi.showcase.InternalCount")
DAEDALUS_TYPE_KEY(ExternalLegacyCount, "ffi.external.LegacyCount")

DAEDALUS_ADAPTER_KEY(internal_count_to_i64, "ffi.showcase.internal_count_to_i64", InternalCount, int64_t, reinterpret)
int64_t internal_count_to_i64(const InternalCount& value) {
  return value.value;
}

DAEDALUS_ADAPTER_KEY(external_legacy_count_to_i64, "ffi.showcase.external_legacy_count_to_i64", ExternalLegacyCount, int64_t, materialize)
int64_t external_legacy_count_to_i64(const ExternalLegacyCount& value) {
  return std::stoll(value.raw);
}

enum class Mode {
  Fast,
  Precise,
};

DAEDALUS_NODE(scalar_add, inputs(a, b), outputs(out))
int64_t scalar_add_i64(int64_t a, int64_t b) {
  return a + b;
}

DAEDALUS_NODE(split_sign, inputs(value), outputs(positive, negative))
daedalus::Outputs split_sign_i64(int64_t value) {
  return (daedalus::outputs)("positive", value, "negative", -value);
}

DAEDALUS_NODE(scale, inputs(value, config), outputs(out))
int64_t scale_i64(int64_t value, const ScaleConfig& config) {
  return value * config.factor;
}

DAEDALUS_STATEFUL_NODE(accumulate, AccumState, inputs(value), outputs(sum))
int64_t accumulate_i64(int64_t value, AccumState& state) {
  state.sum += value;
  return state.sum;
}

DAEDALUS_NODE(bytes_len, inputs(payload), outputs(len))
uint64_t bytes_len(daedalus::BytesView payload) {
  return payload.size();
}

DAEDALUS_NODE(image_boost, inputs(rgba8), outputs(rgba8))
daedalus::Rgba8Image image_boost(daedalus::Rgba8Image rgba8) {
  rgba8.map_pixels([](auto pixel) {
    pixel.r = std::min(pixel.r + 8, 255);
    return pixel;
  });
  return rgba8;
}

DAEDALUS_NODE(shape_summary, inputs(point, mode, maybe, items, labels, pair, unit), outputs(summary))
std::string shape_summary(
    Point point,
    Mode mode,
    std::optional<int64_t> maybe,
    std::vector<int64_t> items,
    std::map<std::string, int64_t> labels,
    std::tuple<int64_t, bool> pair,
    daedalus::Unit unit) {
  return daedalus::format_summary(mode, point, maybe.value_or(0), items.size(), labels.size(), std::get<0>(pair));
}

DAEDALUS_NODE(emit_event, inputs(message), outputs(ok))
bool emit_event(std::string message, daedalus::EventContext& context) {
  context.info("showcase.event", message);
  return true;
}

DAEDALUS_CAPABILITY_NODE(capability_add, Add, inputs(a, b), outputs(out))
int64_t capability_add_i64(int64_t a, int64_t b) {
  return a + b;
}

DAEDALUS_NODE(checked_divide, inputs(a, b), outputs(out))
int64_t checked_divide_i64(int64_t a, int64_t b) {
  if (b == 0) {
    throw daedalus::typed_error("division_by_zero", "b must not be zero");
  }
  return a / b;
}

DAEDALUS_NODE(array_dynamic_sum, inputs(values), outputs(sum))
int64_t array_dynamic_sum(std::span<const int64_t> values) {
  int64_t sum = 0;
  for (auto value : values) {
    sum += value;
  }
  return sum;
}

DAEDALUS_NODE(node_io_complex, inputs(point, weights, metadata), outputs(score, label, point))
daedalus::Outputs node_io_complex(
    Point point,
    std::span<const double> weights,
    std::map<std::string, std::string> metadata) {
  double score = point.x + point.y;
  for (auto value : weights) {
    score += value;
  }
  auto label = metadata.contains("label") ? metadata["label"] : "unlabeled";
  return (daedalus::outputs)("score", score, "label", label, "point", point);
}

DAEDALUS_GPU_NODE(gpu_tint, inputs(rgba8), outputs(rgba8), residency(gpu), layout(rgba8_hwc))
daedalus::GpuRgba8Image gpu_tint(daedalus::GpuRgba8Image rgba8) {
  return rgba8.dispatch("ffi_showcase_tint");
}

DAEDALUS_NODE(internal_adapter_consume, inputs(count), outputs(out))
int64_t internal_adapter_consume(int64_t count) {
  return count + 1;
}

DAEDALUS_NODE(external_adapter_consume, inputs(count), outputs(out))
int64_t external_adapter_consume(int64_t count) {
  return count * 2;
}

DAEDALUS_NODE(zero_copy_len, inputs(frame), outputs(len), access(view))
uint64_t zero_copy_len(daedalus::BytesView frame) {
  return frame.size();
}

DAEDALUS_NODE(shared_ref_len, inputs(frame), outputs(len), access(read))
uint64_t shared_ref_len(daedalus::SharedBytes frame) {
  return frame.size();
}

DAEDALUS_NODE(cow_append_marker, inputs(frame), outputs(frame), access(modify))
daedalus::CowBytes cow_append_marker(daedalus::CowBytes frame) {
  frame.push_back(255);
  return frame;
}

DAEDALUS_NODE(mutable_brighten, inputs(rgba8), outputs(rgba8), access(modify))
daedalus::MutableRgba8Image mutable_brighten(daedalus::MutableRgba8Image rgba8) {
  rgba8.map_pixels([](auto pixel) {
    pixel.r = std::min(pixel.r + 1, 255);
    return pixel;
  });
  return rgba8;
}

DAEDALUS_NODE(owned_bytes_len, inputs(blob), outputs(len), access(move))
uint64_t owned_bytes_len(daedalus::OwnedBytes blob) {
  return blob.size();
}

DAEDALUS_BOUNDARY_CONTRACT("ffi.showcase.Point", host_read, worker_write)
DAEDALUS_PACKAGE_ARTIFACT("_bundle/native/any/libffi_showcase.so")
DAEDALUS_PLUGIN(
    ffi_showcase,
    scalar_add_i64,
    split_sign_i64,
    scale_i64,
    accumulate_i64,
    bytes_len,
    image_boost,
    shape_summary,
    emit_event,
    capability_add_i64,
    checked_divide_i64,
    array_dynamic_sum,
    node_io_complex,
    gpu_tint,
    internal_adapter_consume,
    external_adapter_consume,
    zero_copy_len,
    shared_ref_len,
    cow_append_marker,
    mutable_brighten,
    owned_bytes_len)
