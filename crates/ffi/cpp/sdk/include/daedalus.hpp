#pragma once

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <map>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

namespace daedalus {

struct Unit {};

struct Outputs {
  std::map<std::string, std::string> values;
};

template <typename... Args>
Outputs outputs(Args&&...) {
  return {};
}

struct EventContext {
  void info(const std::string&, const std::string&) {}
};

inline std::runtime_error typed_error(const std::string& code, const std::string& message) {
  return std::runtime_error(code + ": " + message);
}

class BytesView {
 public:
  BytesView() = default;
  explicit BytesView(std::vector<std::uint8_t> bytes) : bytes_(std::move(bytes)) {}

  std::size_t size() const {
    return bytes_.size();
  }

 protected:
  std::vector<std::uint8_t> bytes_;
};

class SharedBytes : public BytesView {
 public:
  using BytesView::BytesView;
};

class CowBytes : public BytesView {
 public:
  using BytesView::BytesView;

  void push_back(std::uint8_t value) {
    bytes_.push_back(value);
  }
};

class OwnedBytes : public BytesView {
 public:
  using BytesView::BytesView;
};

struct Pixel {
  int r = 0;
  int g = 0;
  int b = 0;
  int a = 255;
};

class Rgba8Image {
 public:
  template <typename Fn>
  void map_pixels(Fn&& fn) {
    pixel_ = fn(pixel_);
  }

 protected:
  Pixel pixel_;
};

class MutableRgba8Image : public Rgba8Image {};

class GpuRgba8Image : public Rgba8Image {
 public:
  GpuRgba8Image dispatch(const std::string&) {
    return *this;
  }
};

template <typename Mode, typename Point>
std::string format_summary(
    Mode,
    const Point& point,
    std::int64_t maybe,
    std::size_t item_count,
    std::size_t label_count,
    std::int64_t first) {
  return std::to_string(point.x) + "," + std::to_string(point.y) + ":" + std::to_string(maybe)
      + ":" + std::to_string(item_count) + ":" + std::to_string(label_count) + ":"
      + std::to_string(first);
}

struct AccessSpec {
  std::string value;
};

struct ResidencySpec {
  std::string value;
};

struct LayoutSpec {
  std::string value;
};

inline std::string trim(std::string value) {
  auto begin = value.find_first_not_of(" \t\n\r");
  auto end = value.find_last_not_of(" \t\n\r");
  if (begin == std::string::npos || end == std::string::npos) return "";
  return value.substr(begin, end - begin + 1);
}

inline std::string json_escape(const std::string& value) {
  std::string out;
  for (char ch : value) {
    if (ch == '\\') out += "\\\\";
    else if (ch == '"') out += "\\\"";
    else if (ch == '\n') out += "\\n";
    else out += ch;
  }
  return out;
}

inline std::vector<std::string> split_csv(const std::string& csv) {
  std::vector<std::string> values;
  std::stringstream stream(csv);
  std::string item;
  while (std::getline(stream, item, ',')) {
    auto value = trim(item);
    if (!value.empty()) values.push_back(value);
  }
  return values;
}

inline std::vector<std::string> ports(const char* csv) {
  return split_csv(csv);
}

inline std::string normalize_layout(std::string value) {
  std::replace(value.begin(), value.end(), '_', '-');
  return value;
}

struct NodeSpec {
  std::string id;
  std::vector<std::string> inputs;
  std::vector<std::string> outputs;
  std::string access = "read";
  std::string residency;
  std::string layout;
  std::string capability;
  std::string state_type;
  bool stateful = false;

  template <typename... Options>
  static NodeSpec make(
      std::string id,
      std::vector<std::string> inputs,
      std::vector<std::string> outputs,
      Options... options) {
    NodeSpec spec{std::move(id), std::move(inputs), std::move(outputs)};
    (apply_option(spec, options), ...);
    return spec;
  }

  template <typename... Options>
  static NodeSpec make_stateful(
      std::string id,
      std::string state_type,
      std::vector<std::string> inputs,
      std::vector<std::string> outputs,
      Options... options) {
    auto spec = make(std::move(id), std::move(inputs), std::move(outputs), options...);
    spec.stateful = true;
    spec.state_type = std::move(state_type);
    return spec;
  }

  template <typename... Options>
  static NodeSpec make_capability(
      std::string id,
      std::string capability,
      std::vector<std::string> inputs,
      std::vector<std::string> outputs,
      Options... options) {
    auto spec = make(std::move(id), std::move(inputs), std::move(outputs), options...);
    spec.capability = std::move(capability);
    return spec;
  }

  template <typename... Options>
  static NodeSpec make_gpu(
      std::string id,
      std::vector<std::string> inputs,
      std::vector<std::string> outputs,
      Options... options) {
    auto spec = make(std::move(id), std::move(inputs), std::move(outputs), options...);
    if (spec.residency.empty()) spec.residency = "gpu";
    return spec;
  }
};

inline void apply_option(NodeSpec& spec, AccessSpec option) {
  spec.access = std::move(option.value);
}

inline void apply_option(NodeSpec& spec, ResidencySpec option) {
  spec.residency = std::move(option.value);
}

inline void apply_option(NodeSpec& spec, LayoutSpec option) {
  spec.layout = normalize_layout(std::move(option.value));
}

struct TypeKeySpec {
  std::string type_name;
  std::string key;
};

struct AdapterSpec {
  std::string id;
  std::string source;
  std::string target;
  std::string mode;
};

struct BoundarySpec {
  std::string type_key;
  std::vector<std::string> capabilities;
};

struct Registry {
  std::string plugin_id;
  std::vector<NodeSpec> nodes;
  std::vector<TypeKeySpec> type_keys;
  std::vector<AdapterSpec> adapters;
  std::vector<BoundarySpec> boundaries;
  std::vector<std::string> artifacts;
};

inline Registry& registry() {
  static Registry registry;
  return registry;
}

struct NodeRegistration {
  explicit NodeRegistration(NodeSpec spec) {
    registry().nodes.push_back(std::move(spec));
  }
};

struct TypeKeyRegistration {
  TypeKeyRegistration(std::string type_name, std::string key) {
    registry().type_keys.push_back({std::move(type_name), std::move(key)});
  }
};

struct AdapterRegistration {
  AdapterRegistration(std::string id, std::string source, std::string target, std::string mode) {
    registry().adapters.push_back({std::move(id), std::move(source), std::move(target), std::move(mode)});
  }
};

struct BoundaryRegistration {
  BoundaryRegistration(std::string type_key, std::string capabilities) {
    registry().boundaries.push_back({std::move(type_key), split_csv(capabilities)});
  }
};

struct PackageArtifactRegistration {
  explicit PackageArtifactRegistration(std::string path) {
    registry().artifacts.push_back(std::move(path));
  }
};

struct PluginRegistration {
  PluginRegistration(std::string plugin_id, std::string) {
    registry().plugin_id = std::move(plugin_id);
  }
};

inline std::string string_array(const std::vector<std::string>& values) {
  std::string out = "[";
  for (std::size_t i = 0; i < values.size(); ++i) {
    if (i > 0) out += ",";
    out += "\"" + json_escape(values[i]) + "\"";
  }
  out += "]";
  return out;
}

class PackageBuilder {
 public:
  static PackageBuilder from_plugin(std::string plugin_id) {
    return PackageBuilder(std::move(plugin_id));
  }

  PackageBuilder& shared_library(std::string path) {
    shared_library_ = std::move(path);
    return *this;
  }

  PackageBuilder& source_file(std::string path) {
    source_file_ = std::move(path);
    return *this;
  }

  void write(const std::string& path) const {
    std::ofstream out(path);
    out << descriptor();
  }

  std::string descriptor() const {
    const auto& reg = registry();
    std::string plugin_id = reg.plugin_id.empty() ? plugin_id_ : reg.plugin_id;
    validate_registry(plugin_id, reg);
    std::string out = "{\n";
    out += "  \"schema_version\": 1,\n";
    out += "  \"schema\": {\n";
    out += "    \"schema_version\": 1,\n";
    out += "    \"plugin\": {\"name\": \"" + json_escape(plugin_id) + "\", \"version\": \"1.0.0\", \"description\": null, \"metadata\": {}},\n";
    out += "    \"dependencies\": [],\n";
    out += "    \"required_host_capabilities\": [],\n";
    out += "    \"feature_flags\": [],\n";
    out += "    \"boundary_contracts\": [";
    for (std::size_t i = 0; i < reg.boundaries.size(); ++i) {
      if (i > 0) out += ",";
      const auto& boundary = reg.boundaries[i];
      const bool host_read = std::find(boundary.capabilities.begin(), boundary.capabilities.end(), "host_read") != boundary.capabilities.end();
      const bool worker_write = std::find(boundary.capabilities.begin(), boundary.capabilities.end(), "worker_write") != boundary.capabilities.end();
      out += "{\"type_key\":\"" + json_escape(boundary.type_key) + "\",\"rust_type_name\":null,\"abi_version\":1,\"layout_hash\":\"" + json_escape(boundary.type_key) + "\",\"capabilities\":{";
      out += "\"owned_move\":true,\"shared_clone\":" + std::string(host_read ? "true" : "false");
      out += ",\"borrow_ref\":" + std::string(host_read ? "true" : "false");
      out += ",\"borrow_mut\":" + std::string(worker_write ? "true" : "false");
      out += ",\"metadata_read\":" + std::string(host_read ? "true" : "false");
      out += ",\"metadata_write\":" + std::string(worker_write ? "true" : "false");
      out += ",\"backing_read\":" + std::string(host_read ? "true" : "false");
      out += ",\"backing_write\":" + std::string(worker_write ? "true" : "false") + "}}";
    }
    out += "],\n";
    out += "    \"nodes\": [";
    for (std::size_t i = 0; i < reg.nodes.size(); ++i) {
      if (i > 0) out += ",";
      out += node_json(reg.nodes[i]);
    }
    out += "]\n";
    out += "  },\n";
    out += "  \"backends\": {";
    for (std::size_t i = 0; i < reg.nodes.size(); ++i) {
      if (i > 0) out += ",";
      const auto& node = reg.nodes[i];
      out += "\"" + json_escape(node.id) + "\":{\"backend\":\"c_cpp\",\"runtime_model\":\"in_process_abi\",\"entry_module\":\"" + json_escape(shared_library_) + "\",\"entry_symbol\":\"" + json_escape(node.id) + "\",\"args\":[],\"classpath\":[],\"native_library_paths\":[],\"env\":{},\"options\":{\"pointer_length_abi\":{\"pointer_type\":\"const uint8_t*\",\"length_type\":\"size_t\",\"mutable\":false}}}";
    }
    out += "},\n";
    out += "  \"artifacts\": [";
    std::vector<std::string> artifacts = reg.artifacts;
    if (!shared_library_.empty()) artifacts.push_back(shared_library_);
    if (!source_file_.empty()) artifacts.push_back(source_file_);
    for (std::size_t i = 0; i < artifacts.size(); ++i) {
      if (i > 0) out += ",";
      const bool source = artifacts[i].find(".cpp") != std::string::npos;
      out += "{\"path\":\"" + json_escape(artifacts[i]) + "\",\"kind\":\"" + std::string(source ? "source_file" : "shared_library") + "\",\"backend\":\"c_cpp\",\"platform\":null,\"sha256\":null,\"metadata\":{}}";
    }
    out += "],\n";
    out += "  \"lockfile\": \"plugin.lock.json\",\n";
    out += "  \"manifest_hash\": null,\n";
    out += "  \"signature\": null,\n";
    out += "  \"metadata\": {\"language\": \"c_cpp\", \"package_builder\": \"daedalus-ffi-cpp\", \"adapters\": " + adapter_array(reg.adapters) + ", \"type_keys\": " + type_key_array(reg.type_keys) + "}\n";
    out += "}\n";
    return out;
  }

 private:
  explicit PackageBuilder(std::string plugin_id) : plugin_id_(std::move(plugin_id)) {}

  static void validate_registry(const std::string& plugin_id, const Registry& reg) {
    if (trim(plugin_id).empty()) {
      throw std::invalid_argument("plugin id must not be empty");
    }
    std::map<std::string, bool> node_ids;
    for (const auto& node : reg.nodes) {
      if (trim(node.id).empty()) {
        throw std::invalid_argument("node id must not be empty");
      }
      if (node_ids.contains(node.id)) {
        throw std::invalid_argument("duplicate node id `" + node.id + "`");
      }
      node_ids[node.id] = true;
      validate_ports("input", node.id, node.inputs);
      validate_ports("output", node.id, node.outputs);
      validate_access(node);
      if (!node.layout.empty() && node.residency.empty()) {
        throw std::invalid_argument("node `" + node.id + "` layout requires residency");
      }
    }
    for (const auto& boundary : reg.boundaries) {
      if (trim(boundary.type_key).empty()) {
        throw std::invalid_argument("boundary contract type_key must not be empty");
      }
      for (const auto& capability : boundary.capabilities) {
        if (capability != "host_read" && capability != "worker_write" && capability != "borrow_ref"
            && capability != "borrow_mut" && capability != "shared_clone") {
          throw std::invalid_argument("unsupported boundary capability `" + capability + "`");
        }
      }
    }
  }

  static void validate_ports(
      const std::string& direction,
      const std::string& node_id,
      const std::vector<std::string>& ports) {
    std::map<std::string, bool> names;
    for (const auto& port : ports) {
      if (trim(port).empty()) {
        throw std::invalid_argument("node `" + node_id + "` has empty " + direction + " port");
      }
      if (names.contains(port)) {
        throw std::invalid_argument(
            "duplicate " + direction + " port `" + port + "` on node `" + node_id + "`");
      }
      names[port] = true;
    }
  }

  static void validate_access(const NodeSpec& node) {
    if (node.access != "read" && node.access != "view" && node.access != "modify"
        && node.access != "move") {
      throw std::invalid_argument("node `" + node.id + "` has unsupported access `" + node.access + "`");
    }
    if (!node.residency.empty() && node.residency != "cpu" && node.residency != "gpu") {
      throw std::invalid_argument(
          "node `" + node.id + "` has unsupported residency `" + node.residency + "`");
    }
  }

  static std::string node_json(const NodeSpec& node) {
    std::string out = "{\"id\":\"" + json_escape(node.id) + "\",\"backend\":\"c_cpp\",\"entrypoint\":\"" + json_escape(node.id) + "\",\"stateful\":" + (node.stateful ? "true" : "false") + ",\"feature_flags\":[],\"inputs\":";
    out += ports_json(node.inputs, node.access, node.residency, node.layout);
    out += ",\"outputs\":" + ports_json(node.outputs, "read", node.residency, node.layout);
    out += ",\"metadata\":{";
    bool wrote = false;
    if (!node.capability.empty()) {
      out += "\"capability\":\"" + json_escape(node.capability) + "\"";
      wrote = true;
    }
    if (!node.state_type.empty()) {
      if (wrote) out += ",";
      out += "\"state_type\":\"" + json_escape(node.state_type) + "\"";
    }
    out += "}}";
    return out;
  }

  static std::string ports_json(
      const std::vector<std::string>& ports,
      const std::string& access,
      const std::string& residency,
      const std::string& layout) {
    std::string out = "[";
    for (std::size_t i = 0; i < ports.size(); ++i) {
      if (i > 0) out += ",";
      const bool bytes = ports[i] == "payload" || ports[i] == "frame" || ports[i] == "blob" || ports[i] == "rgba8";
      out += "{\"name\":\"" + json_escape(ports[i]) + "\",\"ty\":{\"Scalar\":\"" + std::string(bytes ? "Bytes" : "Int") + "\"},\"optional\":false,\"access\":\"" + json_escape(access) + "\"";
      if (!residency.empty()) out += ",\"residency\":\"" + json_escape(residency) + "\"";
      if (!layout.empty()) out += ",\"layout\":\"" + json_escape(layout) + "\"";
      out += "}";
    }
    out += "]";
    return out;
  }

  static std::string adapter_array(const std::vector<AdapterSpec>& adapters) {
    std::vector<std::string> ids;
    for (const auto& adapter : adapters) ids.push_back(adapter.id);
    return string_array(ids);
  }

  static std::string type_key_array(const std::vector<TypeKeySpec>& type_keys) {
    std::vector<std::string> keys;
    for (const auto& key : type_keys) keys.push_back(key.key);
    return string_array(keys);
  }

  std::string plugin_id_;
  std::string shared_library_;
  std::string source_file_;
};

}  // namespace daedalus

#define DAEDALUS_CONCAT_INNER(a, b) a##b
#define DAEDALUS_CONCAT(a, b) DAEDALUS_CONCAT_INNER(a, b)

#define inputs(...) ::daedalus::ports(#__VA_ARGS__)
#define outputs(...) ::daedalus::ports(#__VA_ARGS__)
#define access(value) ::daedalus::AccessSpec{#value}
#define residency(value) ::daedalus::ResidencySpec{#value}
#define layout(value) ::daedalus::LayoutSpec{#value}

#define DAEDALUS_TYPE_KEY(type_name, key) \
  static const ::daedalus::TypeKeyRegistration DAEDALUS_CONCAT(_daedalus_type_key_, __COUNTER__)(#type_name, key);

#define DAEDALUS_ADAPTER(id, source, target, mode) \
  static const ::daedalus::AdapterRegistration DAEDALUS_CONCAT(_daedalus_adapter_, __COUNTER__)(#id, #source, #target, #mode);

#define DAEDALUS_ADAPTER_KEY(id, key, source, target, mode) \
  static const ::daedalus::AdapterRegistration DAEDALUS_CONCAT(_daedalus_adapter_, __COUNTER__)(key, #source, #target, #mode);

#define DAEDALUS_NODE(id, input_spec, output_spec, ...) \
  static const ::daedalus::NodeRegistration DAEDALUS_CONCAT(_daedalus_node_, __COUNTER__)( \
      ::daedalus::NodeSpec::make(#id, input_spec, output_spec __VA_OPT__(,) __VA_ARGS__));

#define DAEDALUS_STATEFUL_NODE(id, state_type, input_spec, output_spec, ...) \
  static const ::daedalus::NodeRegistration DAEDALUS_CONCAT(_daedalus_node_, __COUNTER__)( \
      ::daedalus::NodeSpec::make_stateful(#id, #state_type, input_spec, output_spec __VA_OPT__(,) __VA_ARGS__));

#define DAEDALUS_CAPABILITY_NODE(id, capability_name, input_spec, output_spec, ...) \
  static const ::daedalus::NodeRegistration DAEDALUS_CONCAT(_daedalus_node_, __COUNTER__)( \
      ::daedalus::NodeSpec::make_capability(#id, #capability_name, input_spec, output_spec __VA_OPT__(,) __VA_ARGS__));

#define DAEDALUS_GPU_NODE(id, input_spec, output_spec, ...) \
  static const ::daedalus::NodeRegistration DAEDALUS_CONCAT(_daedalus_node_, __COUNTER__)( \
      ::daedalus::NodeSpec::make_gpu(#id, input_spec, output_spec __VA_OPT__(,) __VA_ARGS__));

#define DAEDALUS_BOUNDARY_CONTRACT(type_key, ...) \
  static const ::daedalus::BoundaryRegistration DAEDALUS_CONCAT(_daedalus_boundary_, __COUNTER__)(type_key, #__VA_ARGS__);

#define DAEDALUS_PACKAGE_ARTIFACT(path) \
  static const ::daedalus::PackageArtifactRegistration DAEDALUS_CONCAT(_daedalus_artifact_, __COUNTER__)(path);

#define DAEDALUS_PLUGIN(id, ...) \
  static const ::daedalus::PluginRegistration DAEDALUS_CONCAT(_daedalus_plugin_, __COUNTER__)(#id, #__VA_ARGS__);
