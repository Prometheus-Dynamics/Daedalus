#pragma once

#include <algorithm>
#include <bit>
#include <charconv>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <initializer_list>
#include <map>
#include <optional>
#include <span>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

namespace daedalus {

struct Unit {};

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
    else if (static_cast<unsigned char>(ch) < 0x20) {
      constexpr const char* hex = "0123456789abcdef";
      out += std::string("\\u00") + hex[ch >> 4] + hex[ch & 15];
    } else out += ch;
  }
  return out;
}

template <typename>
inline constexpr bool unwireable_type = false;

// Daedalus `WireValue` JSON for a scalar port value. 64-bit unsigned integers are `uint`, so
// values above INT64_MAX cross the wire exactly; other integers are `int`.
template <typename T>
std::string to_wire(const T& value) {
  const auto wire = [](const char* kind, const std::string& json) {
    return std::string("{\"kind\":\"") + kind + "\",\"value\":" + json + "}";
  };
  if constexpr (std::is_same_v<T, bool>) {
    return wire("bool", value ? "true" : "false");
  } else if constexpr (std::is_integral_v<T>) {
    static_assert(sizeof(T) <= 8, "Daedalus integers are at most 64 bits wide");
    return wire(std::is_unsigned_v<T> && sizeof(T) == 8 ? "uint" : "int", std::to_string(value));
  } else if constexpr (std::is_floating_point_v<T>) {
    if (!std::isfinite(value)) throw std::domain_error("JSON has no non-finite numbers");
    char buffer[32];
    const auto end = std::to_chars(buffer, buffer + sizeof buffer, static_cast<double>(value)).ptr;
    return wire("float", std::string(buffer, end));
  } else if constexpr (std::is_convertible_v<const T&, std::string_view>) {
    return wire("string", "\"" + json_escape(std::string(std::string_view(value))) + "\"");
  } else {
    static_assert(unwireable_type<T>, "daedalus::to_wire encodes bool, integers, floats and strings");
  }
}

// The scalar port value of a `WireValue` JSON object. Integers (`int` or `uint`) are range-checked
// against `T`; throws `std::invalid_argument` for other kinds and `std::out_of_range` on overflow.
template <typename T>
T from_wire(std::string_view json) {
  const auto field = [&](std::string_view key) {
    auto at = json.find("\"" + std::string(key) + "\"");
    at = at == std::string_view::npos ? at : json.find(':', at);
    if (at == std::string_view::npos) throw std::invalid_argument("wire value has no `" + std::string(key) + "`");
    at = json.find_first_not_of(" \t\n\r", at + 1);
    auto end = at;
    if (at != std::string_view::npos && json[at] == '"') {
      for (++end; end < json.size() && json[end] != '"'; end += json[end] == '\\' ? 2 : 1) {}
      ++end;
    } else {
      end = json.find_first_of(",} \t\n\r", at);
    }
    return json.substr(at, end - at);
  };
  const auto kind = field("kind");
  const auto value = field("value");
  const auto expect = [&](bool ok) {
    if (!ok) throw std::invalid_argument("wire value kind " + std::string(kind) + " does not fit the port type");
  };
  if constexpr (std::is_same_v<T, bool>) {
    expect(kind == "\"bool\"");
    return value == "true";
  } else if constexpr (std::is_integral_v<T>) {
    expect(kind == "\"int\"" || kind == "\"uint\"");
    std::int64_t signed_value = 0;
    std::uint64_t unsigned_value = 0;
    const bool negative = value.starts_with('-');
    const auto parsed = negative
        ? std::from_chars(value.data(), value.data() + value.size(), signed_value)
        : std::from_chars(value.data(), value.data() + value.size(), unsigned_value);
    expect(parsed.ec == std::errc() && parsed.ptr == value.data() + value.size());
    if (negative ? !std::in_range<T>(signed_value) : !std::in_range<T>(unsigned_value)) {
      throw std::out_of_range(std::string(value) + " is out of range for the port type");
    }
    return negative ? static_cast<T>(signed_value) : static_cast<T>(unsigned_value);
  } else if constexpr (std::is_floating_point_v<T>) {
    expect(kind == "\"float\"" || kind == "\"int\"" || kind == "\"uint\"");
    double number = 0;
    expect(std::from_chars(value.data(), value.data() + value.size(), number).ec == std::errc());
    return static_cast<T>(number);
  } else if constexpr (std::is_same_v<T, std::string>) {
    expect(kind == "\"string\"");
    std::string out;
    for (std::size_t i = 1; i + 1 < value.size(); ++i) {
      char ch = value[i];
      if (ch == '\\') {
        ch = value[++i];
        if (ch == 'u') {
          out += static_cast<char>(std::stoi(std::string(value.substr(i + 1, 4)), nullptr, 16));
          i += 4;
          continue;
        }
        ch = ch == 'n' ? '\n' : ch == 't' ? '\t' : ch == 'r' ? '\r' : ch == 'b' ? '\b' : ch == 'f' ? '\f' : ch;
      }
      out += ch;
    }
    return out;
  } else {
    static_assert(unwireable_type<T>, "daedalus::from_wire decodes bool, integers, floats and std::string");
  }
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

// The port names of an `inputs(...)` / `outputs(...)` list, countable at compile time.
struct PortList {
  const char* csv;

  constexpr std::size_t count() const {
    std::size_t count = 0;
    bool item = false;
    for (const char* ch = csv; *ch; ++ch) {
      if (*ch == ',') {
        count += item;
        item = false;
      } else if (*ch != ' ' && *ch != '\t' && *ch != '\n') {
        item = true;
      }
    }
    return count + item;
  }

  std::vector<std::string> names() const { return split_csv(csv); }
};

inline std::string normalize_layout(std::string value) {
  std::replace(value.begin(), value.end(), '_', '-');
  return value;
}

inline std::string scalar_json(const char* name) {
  return std::string("{\"Scalar\":\"") + name + "\"}";
}

template <typename T>
std::string type_expr();

template <typename>
inline constexpr bool unmapped_port_type = false;

// Width-exact Daedalus `TypeExpr` JSON for a C++ port type. Specialize it for your own types, give
// them a `static constexpr const char* daedalus_type_key`, or register them with DAEDALUS_TYPE_KEY.
template <typename T>
struct TypeExprOf {
  static std::string json() {
    if constexpr (std::is_same_v<T, bool>) {
      return scalar_json("Bool");
    } else if constexpr (std::is_integral_v<T>) {
      static_assert(sizeof(T) <= 8, "Daedalus integers are at most 64 bits wide");
      constexpr const char* names[2][4] = {{"U8", "U16", "U32", "U64"}, {"I8", "I16", "I32", "Int"}};
      return scalar_json(names[std::is_signed_v<T>][std::countr_zero(sizeof(T))]);
    } else if constexpr (std::is_same_v<T, float>) {
      return scalar_json("F32");
    } else if constexpr (std::is_same_v<T, double>) {
      return scalar_json("Float");
    } else if constexpr (std::is_same_v<T, std::string> || std::is_same_v<T, std::string_view>) {
      return scalar_json("String");
    } else if constexpr (std::is_void_v<T> || std::is_same_v<T, Unit>) {
      return scalar_json("Unit");
    } else if constexpr (std::is_base_of_v<BytesView, T> || std::is_base_of_v<Rgba8Image, T>) {
      return scalar_json("Bytes");
    } else if constexpr (requires { T::daedalus_type_key; }) {
      return "{\"Opaque\":\"" + json_escape(T::daedalus_type_key) + "\"}";
    } else if constexpr (requires { daedalus_type_key(static_cast<const T*>(nullptr)); }) {
      return "{\"Opaque\":\"" + json_escape(daedalus_type_key(static_cast<const T*>(nullptr))) + "\"}";
    } else {
      static_assert(unmapped_port_type<T>,
          "daedalus: this port type has no Daedalus mapping; register it with DAEDALUS_TYPE_KEY(T, key), "
          "give it a `static constexpr const char* daedalus_type_key`, or specialize daedalus::TypeExprOf<T>");
      return "";
    }
  }
};

// `prefix item,item suffix`.
inline std::string compose(const char* prefix, std::initializer_list<std::string> items, const char* suffix) {
  std::string out = prefix;
  const char* separator = "";
  for (const auto& item : items) {
    out += separator + item;
    separator = ",";
  }
  return out + suffix;
}

template <typename T>
struct TypeExprOf<std::optional<T>> {
  static std::string json() { return compose("{\"Optional\":", {type_expr<T>()}, "}"); }
};

template <typename T, std::size_t N>
struct TypeExprOf<std::span<T, N>> {
  static std::string json() { return compose("{\"List\":", {type_expr<T>()}, "}"); }
};

template <typename T, typename A>
struct TypeExprOf<std::vector<T, A>> {
  static std::string json() {
    if constexpr (std::is_same_v<T, std::uint8_t>) return scalar_json("Bytes");
    else return compose("{\"List\":", {type_expr<T>()}, "}");
  }
};

template <typename K, typename V, typename C, typename A>
struct TypeExprOf<std::map<K, V, C, A>> {
  static std::string json() { return compose("{\"Map\":[", {type_expr<K>(), type_expr<V>()}, "]}"); }
};

template <typename... Ts>
struct TypeExprOf<std::tuple<Ts...>> {
  static std::string json() { return compose("{\"Tuple\":[", {type_expr<Ts>()...}, "]}"); }
};

template <typename T>
std::string type_expr() {
  return TypeExprOf<std::remove_cvref_t<T>>::json();
}

using TypeExprFn = std::string (*)();

// Port types deduced from a node function type: the first `Inputs` parameters type the inputs in
// order (trailing state/context parameters are not ports), a `std::tuple` return types each
// output, `void` none, and any other return type the single output.
struct SignatureSpec {
  std::vector<TypeExprFn> inputs;
  std::vector<TypeExprFn> outputs;
};

template <typename R>
struct OutputTypes {
  static constexpr std::size_t count = 1;
  static std::vector<TypeExprFn> get() { return {&type_expr<R>}; }
};

template <>
struct OutputTypes<void> {
  static constexpr std::size_t count = 0;
  static std::vector<TypeExprFn> get() { return {}; }
};

template <typename... Ts>
struct OutputTypes<std::tuple<Ts...>> {
  static constexpr std::size_t count = sizeof...(Ts);
  static std::vector<TypeExprFn> get() { return {&type_expr<Ts>...}; }
};

template <typename F>
struct FunctionSignature;

template <typename R, typename... Args, bool NoExcept>
struct FunctionSignature<R(Args...) noexcept(NoExcept)> {
  template <std::size_t Inputs, std::size_t Outputs>
  static SignatureSpec spec() {
    static_assert(Inputs <= sizeof...(Args), "daedalus: node declares more inputs than its function has parameters");
    using Result = std::remove_cvref_t<R>;
    static_assert(Outputs == OutputTypes<Result>::count,
        "daedalus: node output count does not match its function's return type; "
        "return std::tuple<...> for several outputs and void for none");
    if constexpr (Inputs <= sizeof...(Args)) {
      return {input_types(std::make_index_sequence<Inputs>{}), OutputTypes<Result>::get()};
    } else {
      return {};
    }
  }

 private:
  template <std::size_t... I>
  static std::vector<TypeExprFn> input_types(std::index_sequence<I...>) {
    return {&type_expr<std::tuple_element_t<I, std::tuple<Args...>>>...};
  }
};

// The signature of `F` (a function or function pointer type) with `Inputs` input ports.
template <typename F, std::size_t Inputs, std::size_t Outputs>
SignatureSpec signature() {
  return FunctionSignature<std::remove_pointer_t<F>>::template spec<Inputs, Outputs>();
}

struct NodeSpec {
  std::string id;
  std::vector<std::string> inputs;
  std::vector<std::string> outputs;
  SignatureSpec signature;
  std::string access = "read";
  std::string residency;
  std::string layout;
  std::string capability;
  std::string state_type;
  bool stateful = false;

  // A node typed from its function type `F`; `Inputs`/`Outputs` are the port counts, which the
  // DAEDALUS_*NODE macros take from the `inputs(...)`/`outputs(...)` lists.
  template <typename F, std::size_t Inputs, std::size_t Outputs, typename... Options>
  static NodeSpec make(std::string id, PortList inputs, PortList outputs, Options... options) {
    NodeSpec spec;
    spec.id = std::move(id);
    spec.inputs = inputs.names();
    spec.outputs = outputs.names();
    spec.signature = ::daedalus::signature<F, Inputs, Outputs>();
    (apply_option(spec, options), ...);
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

struct StateSpec {
  std::string type;
};

inline void apply_option(NodeSpec& spec, StateSpec option) {
  spec.stateful = true;
  spec.state_type = std::move(option.type);
}

struct CapabilitySpec {
  std::string name;
};

inline void apply_option(NodeSpec& spec, CapabilitySpec option) {
  spec.capability = std::move(option.name);
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
      if (node.inputs.size() != node.signature.inputs.size()
          || node.outputs.size() != node.signature.outputs.size()) {
        throw std::invalid_argument("node `" + node.id + "` port names do not match its signature");
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
    out += ports_json(node.inputs, node.signature.inputs, node.access, node.residency, node.layout);
    out += ",\"outputs\":" + ports_json(node.outputs, node.signature.outputs, "read", node.residency, node.layout);
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

  // Each port takes the type of its signature slot.
  static std::string ports_json(
      const std::vector<std::string>& ports,
      const std::vector<TypeExprFn>& types,
      const std::string& access,
      const std::string& residency,
      const std::string& layout) {
    std::string out = "[";
    for (std::size_t i = 0; i < ports.size(); ++i) {
      if (i > 0) out += ",";
      out += "{\"name\":\"" + json_escape(ports[i]) + "\",\"ty\":" + types[i]() + ",\"optional\":false,\"access\":\"" + json_escape(access) + "\"";
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

#define inputs(...) ::daedalus::PortList{#__VA_ARGS__}
#define outputs(...) ::daedalus::PortList{#__VA_ARGS__}
#define access(value) ::daedalus::AccessSpec{#value}
#define residency(value) ::daedalus::ResidencySpec{#value}
#define layout(value) ::daedalus::LayoutSpec{#value}

// Names a port type's Daedalus type key (found by argument-dependent lookup, so use it in the
// type's namespace, after its declaration) and records the key in the package metadata.
#define DAEDALUS_TYPE_KEY(type_name, key) \
  [[maybe_unused]] constexpr const char* daedalus_type_key(const type_name*) { return key; } \
  static const ::daedalus::TypeKeyRegistration DAEDALUS_CONCAT(_daedalus_type_key_, __COUNTER__)(#type_name, key);

#define DAEDALUS_ADAPTER(id, source, target, mode) \
  static const ::daedalus::AdapterRegistration DAEDALUS_CONCAT(_daedalus_adapter_, __COUNTER__)(#id, #source, #target, #mode);

#define DAEDALUS_ADAPTER_KEY(id, key, source, target, mode) \
  static const ::daedalus::AdapterRegistration DAEDALUS_CONCAT(_daedalus_adapter_, __COUNTER__)(key, #source, #target, #mode);

// Registers function `fn` as node `fn`, typing its ports from `decltype(&fn)`; place it after the
// function. Unmapped port types and port counts that do not match the signature fail to compile.
#define DAEDALUS_NODE(fn, input_spec, output_spec, ...) \
  static const ::daedalus::NodeRegistration DAEDALUS_CONCAT(_daedalus_node_, __COUNTER__)( \
      ::daedalus::NodeSpec::make<decltype(&fn), (input_spec).count(), (output_spec).count()>( \
          #fn, input_spec, output_spec __VA_OPT__(,) __VA_ARGS__));

#define DAEDALUS_STATEFUL_NODE(fn, state_type, input_spec, output_spec, ...) \
  DAEDALUS_NODE(fn, input_spec, output_spec, ::daedalus::StateSpec{#state_type} __VA_OPT__(,) __VA_ARGS__)

#define DAEDALUS_CAPABILITY_NODE(fn, capability_name, input_spec, output_spec, ...) \
  DAEDALUS_NODE(fn, input_spec, output_spec, ::daedalus::CapabilitySpec{#capability_name} __VA_OPT__(,) __VA_ARGS__)

#define DAEDALUS_GPU_NODE(fn, input_spec, output_spec, ...) \
  DAEDALUS_NODE(fn, input_spec, output_spec, ::daedalus::ResidencySpec{"gpu"} __VA_OPT__(,) __VA_ARGS__)

#define DAEDALUS_BOUNDARY_CONTRACT(type_key, ...) \
  static const ::daedalus::BoundaryRegistration DAEDALUS_CONCAT(_daedalus_boundary_, __COUNTER__)(type_key, #__VA_ARGS__);

#define DAEDALUS_PACKAGE_ARTIFACT(path) \
  static const ::daedalus::PackageArtifactRegistration DAEDALUS_CONCAT(_daedalus_artifact_, __COUNTER__)(path);

#define DAEDALUS_PLUGIN(id, ...) \
  static const ::daedalus::PluginRegistration DAEDALUS_CONCAT(_daedalus_plugin_, __COUNTER__)(#id, #__VA_ARGS__);
