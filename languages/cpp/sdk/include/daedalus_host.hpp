// The Daedalus host for C++17, in process: a header over the C API of core/capi (daedalus.h).
// A Host loads plugins and compiles graphs; a Graph is pushed to, ticked and taken from. Every
// handle is RAII: it releases its graph or host when it goes out of scope. A failed call throws a
// HostError that carries the status, its name, and the text the library kept for the failure.
//
// Link the host library (libdaedalus_capi, the core/capi cdylib or staticlib) and include this
// header with daedalus.h on the include path. Each method makes one or two calls to the C API; text
// results are read with the C API's size query.
//
//   dd::Host host = dd::Host::create();
//   host.install("libplugin.so");
//   dd::Graph graph = host.compile(json);
//   graph.push_i64("x", 41);
//   graph.tick();
//   std::optional<std::int64_t> y = graph.take_i64("out");
#ifndef DAEDALUS_HOST_HPP
#define DAEDALUS_HOST_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "daedalus.h"

namespace dd {

// A call that did not return DAEDALUS_OK.
class HostError : public std::runtime_error {
public:
    HostError(daedalus_status_t status, std::string detail)
        : std::runtime_error(std::string(daedalus_status_name(status)) + ": " + detail),
          status_(status),
          detail_(std::move(detail)) {}
    // The status the call returned, and its name.
    daedalus_status_t status() const noexcept { return status_; }
    const char* status_name() const noexcept { return daedalus_status_name(status_); }
    // The text the library kept for the failure on this thread.
    const std::string& detail() const noexcept { return detail_; }

private:
    daedalus_status_t status_;
    std::string detail_;
};

namespace host_detail {

// The last error on this thread, as text, read with the size query.
inline std::string last_error() {
    size_t len = 0;
    if (daedalus_last_error(nullptr, 0, &len) != DAEDALUS_BUFFER_TOO_SMALL) return std::string();
    std::string text(len, '\0');
    auto* out = reinterpret_cast<uint8_t*>(text.data());
    if (daedalus_last_error(out, text.size(), &len) != DAEDALUS_OK) return std::string();
    text.resize(len);
    return text;
}

inline void check(daedalus_status_t status) {
    if (status != DAEDALUS_OK) throw HostError(status, last_error());
}

inline const uint8_t* bytes_of(std::string_view text) {
    return reinterpret_cast<const uint8_t*>(text.data());
}

// What the library writes into a buffer the caller gives: `call` takes a buffer, its capacity and
// the length slot, as the C API's calls do. The size is asked for first with an empty buffer, then
// the result is read into a buffer that size. The library is in this process, so asking is cheap.
template <typename Buffer, typename Call>
Buffer sized(Call call) {
    Buffer out;
    size_t len = 0;
    daedalus_status_t status = call(nullptr, 0, &len);
    while (status == DAEDALUS_BUFFER_TOO_SMALL) {
        out.assign(len, typename Buffer::value_type{});
        status = call(reinterpret_cast<uint8_t*>(out.data()), out.size(), &len);
    }
    check(status);
    out.resize(len);
    return out;
}

// A text the library writes.
template <typename Call>
std::string text_of(Call call) {
    return sized<std::string>(call);
}

// A value a take gives: the call's `present` flag, and its value when it is set. Both are read
// here, after the call that wrote them has returned: the arguments are evaluated before this runs.
template <typename T>
std::optional<T> taken(daedalus_status_t status, const T& value, const uint8_t& present) {
    check(status);
    if (!present) return std::nullopt;
    return value;
}

}  // namespace host_detail

class Graph;

// A host: the plugins it loaded and the graphs compiled on it.
class Host {
public:
    // A host with no nodes. Throws on failure.
    static Host create() {
        daedalus_handle_t handle = 0;
        host_detail::check(daedalus_host_create(&handle));
        return Host(handle);
    }

    Host(Host&& other) noexcept : handle_(std::exchange(other.handle_, 0)) {}
    Host& operator=(Host&& other) noexcept {
        if (this != &other) {
            release();
            handle_ = std::exchange(other.handle_, 0);
        }
        return *this;
    }
    Host(const Host&) = delete;
    Host& operator=(const Host&) = delete;
    // Releases the host. A host with graphs still open cannot be released; its graphs keep it.
    ~Host() { release(); }

    // Loads the plugin at `path`: its nodes join this host's.
    void install(std::string_view path) {
        host_detail::check(daedalus_host_install(handle_, host_detail::bytes_of(path), path.size()));
    }

    // The host's nodes, as JSON text.
    std::string schemas() const {
        return host_detail::text_of([this](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_host_schemas(handle_, buf, cap, len);
        });
    }

    // Compiles a graph given as JSON text. Throws if it cannot be read or planned.
    Graph compile(std::string_view json) const;

    daedalus_handle_t handle() const noexcept { return handle_; }

private:
    explicit Host(daedalus_handle_t handle) : handle_(handle) {}
    void release() noexcept {
        if (handle_ != 0) daedalus_host_release(std::exchange(handle_, 0));
    }

    daedalus_handle_t handle_ = 0;
    friend class Graph;
};

// A compiled graph: pushes to its host ports, ticks, and takes from them.
class Graph {
public:
    struct Port {
        std::string name;
        std::string type_key;
        bool is_input;
    };
    struct Tick {
        bool idle;
        uint32_t step_calls;
        uint32_t faults;
    };
    struct Telemetry {
        uint64_t ticks;
        uint64_t idle_ticks;
        uint64_t step_calls;
        uint64_t faults;
        uint64_t pushes;
        uint64_t takes;
    };

    Graph(Graph&& other) noexcept : handle_(std::exchange(other.handle_, 0)) {}
    Graph& operator=(Graph&& other) noexcept {
        if (this != &other) {
            release();
            handle_ = std::exchange(other.handle_, 0);
        }
        return *this;
    }
    Graph(const Graph&) = delete;
    Graph& operator=(const Graph&) = delete;
    ~Graph() { release(); }

    // The host ports: each has an index, its place in the ports sorted by name.
    std::size_t port_count() const {
        std::size_t count = 0;
        host_detail::check(daedalus_graph_port_count(handle_, &count));
        return count;
    }
    Port port(std::size_t index) const {
        daedalus_port_t raw{};
        host_detail::check(daedalus_graph_port(handle_, index, &raw));
        return Port{std::string(reinterpret_cast<const char*>(raw.name), raw.name_len),
                    std::string(reinterpret_cast<const char*>(raw.type_key), raw.type_key_len),
                    raw.is_input != 0};
    }
    std::size_t port_index(std::string_view name) const {
        std::size_t index = 0;
        host_detail::check(daedalus_graph_port_index(handle_, host_detail::bytes_of(name), name.size(), &index));
        return index;
    }

    // How the graph was planned, as JSON text.
    std::string explain() const {
        return host_detail::text_of([this](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_graph_explain(handle_, buf, cap, len);
        });
    }
    Telemetry telemetry() const {
        daedalus_telemetry_t raw{};
        host_detail::check(daedalus_graph_telemetry(handle_, &raw));
        return Telemetry{raw.ticks, raw.idle_ticks, raw.step_calls, raw.faults, raw.pushes, raw.takes};
    }

    // Runs one tick.
    Tick tick() {
        daedalus_tick_t raw{};
        host_detail::check(daedalus_tick(handle_, &raw));
        return Tick{raw.idle != 0, raw.step_calls, raw.faults};
    }

    // Pushes, by port name or by index.
    void push_i64(std::string_view port, int64_t value) {
        host_detail::check(daedalus_push_i64(handle_, host_detail::bytes_of(port), port.size(), value));
    }
    void push_i64(std::size_t index, int64_t value) {
        host_detail::check(daedalus_push_i64_at(handle_, index, value));
    }
    void push_f64(std::string_view port, double value) {
        host_detail::check(daedalus_push_f64(handle_, host_detail::bytes_of(port), port.size(), value));
    }
    void push_f64(std::size_t index, double value) {
        host_detail::check(daedalus_push_f64_at(handle_, index, value));
    }
    void push_bool(std::string_view port, bool value) {
        host_detail::check(daedalus_push_bool(handle_, host_detail::bytes_of(port), port.size(), value ? 1 : 0));
    }
    void push_bool(std::size_t index, bool value) {
        host_detail::check(daedalus_push_bool_at(handle_, index, value ? 1 : 0));
    }
    void push_bytes(std::string_view port, const uint8_t* data, std::size_t len) {
        host_detail::check(daedalus_push_bytes(handle_, host_detail::bytes_of(port), port.size(), data, len));
    }
    void push_bytes(std::size_t index, const uint8_t* data, std::size_t len) {
        host_detail::check(daedalus_push_bytes_at(handle_, index, data, len));
    }
    // Pushes every entry or none. On failure `bad`, when not null, gets the first bad entry.
    void push_batch(const std::vector<daedalus_push_t>& pushes, std::size_t* bad = nullptr) {
        host_detail::check(daedalus_push_batch(handle_, pushes.data(), pushes.size(), bad));
    }

    // Takes the oldest value of a port, or nothing when the port has none.
    std::optional<int64_t> take_i64(std::string_view port) {
        int64_t out = 0;
        uint8_t present = 0;
        return host_detail::taken(
            daedalus_take_i64(handle_, host_detail::bytes_of(port), port.size(), &out, &present), out, present);
    }
    std::optional<int64_t> take_i64(std::size_t index) {
        int64_t out = 0;
        uint8_t present = 0;
        return host_detail::taken(daedalus_take_i64_at(handle_, index, &out, &present), out, present);
    }
    // Takes an integer that lies in low..high. One outside it throws, and stays in the port.
    std::optional<int64_t> take_i64_within(std::string_view port, int64_t low, int64_t high) {
        int64_t out = 0;
        uint8_t present = 0;
        return host_detail::taken(
            daedalus_take_i64_within(handle_, host_detail::bytes_of(port), port.size(), low, high, &out, &present),
            out, present);
    }
    std::optional<double> take_f64(std::string_view port) {
        double out = 0;
        uint8_t present = 0;
        return host_detail::taken(
            daedalus_take_f64(handle_, host_detail::bytes_of(port), port.size(), &out, &present), out, present);
    }
    std::optional<bool> take_bool(std::string_view port) {
        uint8_t out = 0;
        uint8_t present = 0;
        auto value = host_detail::taken(
            daedalus_take_bool(handle_, host_detail::bytes_of(port), port.size(), &out, &present), out, present);
        if (!value) return std::nullopt;
        return *value != 0;
    }
    // Takes a bytes value. The size is asked for first with an empty buffer: a value that does not
    // fit waits in the port for the second call, which reads it.
    std::optional<std::vector<uint8_t>> take_bytes(std::string_view port) {
        uint8_t present = 0;
        std::vector<uint8_t> out = host_detail::sized<std::vector<uint8_t>>([&](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_take_bytes(handle_, host_detail::bytes_of(port), port.size(), buf, cap, len, &present);
        });
        if (!present) return std::nullopt;
        return out;
    }

private:
    explicit Graph(daedalus_handle_t handle) : handle_(handle) {}
    void release() noexcept {
        if (handle_ != 0) daedalus_graph_release(std::exchange(handle_, 0));
    }

    daedalus_handle_t handle_ = 0;
    friend class Host;
};

inline Graph Host::compile(std::string_view json) const {
    daedalus_handle_t graph = 0;
    host_detail::check(daedalus_graph_compile(handle_, host_detail::bytes_of(json), json.size(), &graph));
    return Graph(graph);
}

}  // namespace dd

#endif  // DAEDALUS_HOST_HPP
