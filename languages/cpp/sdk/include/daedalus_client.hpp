// The Daedalus client for C++17, over the client C ABI of languages/client-capi
// (daedalus_client.h): a connection to the daemon, over a unix socket or loopback TCP.
//
// A Client is one connection, and every handle made from it (in_session) shares it. Closing a
// Client, or destroying it, closes the connection for every handle made from it, as the C call
// does: keep the root Client alive while its sessions are in use. A Watch is one subscription.
//
// A failed call throws ClientError. It carries the status, its name, the text the library kept
// for the failure, and the daemon's error code when the daemon refused the call (0 when it did
// not). Text results are read with the C API's size query. Every method is one or two C calls.
//
//   dd::Client client = dd::Client::connect("unix:/run/daedalus.sock");
//   std::uint64_t run = client.start(name, config);
//   client.feed_i64(run, "in", 41, nullptr);
#ifndef DAEDALUS_CLIENT_HPP
#define DAEDALUS_CLIENT_HPP

#include <cstddef>
#include <cstdint>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "daedalus_client.h"

namespace dd {

// A client call that did not return DAEDALUS_CLIENT_OK.
class ClientError : public std::runtime_error {
public:
    ClientError(daedalus_client_status_t status, std::string detail, std::uint32_t service_code)
        : std::runtime_error(std::string(daedalus_client_status_name(status)) + ": " + detail),
          status_(status),
          detail_(std::move(detail)),
          service_code_(service_code) {}
    daedalus_client_status_t status() const noexcept { return status_; }
    const char* status_name() const noexcept { return daedalus_client_status_name(status_); }
    // The text the library kept for the failure on this thread.
    const std::string& detail() const noexcept { return detail_; }
    // The daemon's code when it refused the call: 1 to 12 as the protocol gives them, 65535 for a
    // code this library does not know, and 0 when the failure was not a refusal.
    std::uint32_t service_code() const noexcept { return service_code_; }

private:
    daedalus_client_status_t status_;
    std::string detail_;
    std::uint32_t service_code_;
};

namespace client_detail {

// The last failure on this thread, read with the size query: its text, and the daemon's code.
inline std::string last_error_text() {
    size_t len = 0;
    if (daedalus_client_last_error(nullptr, 0, &len) != DAEDALUS_CLIENT_BUFFER_TOO_SMALL) return std::string();
    std::string text(len, '\0');
    if (daedalus_client_last_error(reinterpret_cast<uint8_t*>(text.data()), text.size(), &len) != DAEDALUS_CLIENT_OK) {
        return std::string();
    }
    text.resize(len);
    return text;
}

inline std::uint32_t last_service_code() {
    std::uint32_t code = 0;
    if (daedalus_client_last_error_code(&code) != DAEDALUS_CLIENT_OK) return 0;
    return code;
}

inline void check(daedalus_client_status_t status) {
    if (status != DAEDALUS_CLIENT_OK) throw ClientError(status, last_error_text(), last_service_code());
}

inline const uint8_t* bytes_of(std::string_view text) { return reinterpret_cast<const uint8_t*>(text.data()); }

// The first buffer a call is given. A call the daemon serves is made once: only a text larger than this
// makes a second call, and that call gives the text. A second call repeats the request on the daemon
// (a trace records it), so the size is not asked for first.
constexpr std::size_t kFirstBuffer = 256 * 1024;

// A text the daemon returns. The text is read into a buffer of kFirstBuffer; a text that does not fit
// is read again into a buffer of its size, and the size is asked again if the text has grown.
template <typename Call>
std::string text_of(Call call) {
    std::string text(kFirstBuffer, '\0');
    size_t len = 0;
    daedalus_client_status_t status = call(reinterpret_cast<uint8_t*>(text.data()), text.size(), &len);
    while (status == DAEDALUS_CLIENT_BUFFER_TOO_SMALL) {
        text.assign(len, '\0');
        status = call(reinterpret_cast<uint8_t*>(text.data()), text.size(), &len);
    }
    check(status);
    text.resize(len);
    return text;
}

// A value a take gives: the call's `present` flag, and its value when it is set. Both are read here,
// after the call that wrote them has returned: the arguments are evaluated before this runs.
template <typename T>
std::optional<T> taken(daedalus_client_status_t status, const T& value, const uint8_t& present) {
    check(status);
    if (!present) return std::nullopt;
    return value;
}

}  // namespace client_detail

// What a feed did, as the C struct gives it.
using Fed = daedalus_client_fed_t;

// What a tick batch did.
using Step = daedalus_client_step_t;
// How a stop went.
using Stopped = daedalus_client_stopped_t;
// What a reload carried.
using Carry = daedalus_client_carry_t;

// A made plan: its hash, and its bytes.
struct MadePlan {
    std::uint64_t hash;
    std::vector<std::uint8_t> bytes;
};

// One poll of a watch: a state, and the event's JSON when the state is an event.
struct WatchPoll {
    std::uint32_t state;  // DAEDALUS_CLIENT_WATCH_IDLE, _EVENT or _ENDED
    std::string json;
};

class Watch;

class Client {
public:
    // Connects to the daemon at `endpoint`: "unix:/path" or "tcp:127.0.0.1:4100". `reconnect` keeps
    // the connection trying again after a disconnect.
    static Client connect(std::string_view endpoint, bool reconnect = false) {
        daedalus_client_handle_t handle = 0;
        client_detail::check(daedalus_client_connect(client_detail::bytes_of(endpoint), endpoint.size(),
                                                     reconnect ? 1 : 0, &handle));
        return Client(handle);
    }

    Client(Client&& other) noexcept : handle_(std::exchange(other.handle_, 0)) {}
    Client& operator=(Client&& other) noexcept {
        if (this != &other) {
            release();
            handle_ = std::exchange(other.handle_, 0);
        }
        return *this;
    }
    Client(const Client&) = delete;
    Client& operator=(const Client&) = delete;
    ~Client() { release(); }

    // A handle for the same connection, whose requests are in the session `name`.
    Client in_session(std::string_view name) const {
        daedalus_client_handle_t handle = 0;
        client_detail::check(daedalus_client_in_session(handle_, client_detail::bytes_of(name), name.size(), &handle));
        return Client(handle);
    }

    // The daemon's protocol version.
    std::uint32_t ping() const {
        std::uint32_t version = 0;
        client_detail::check(daedalus_client_ping(handle_, &version));
        return version;
    }

    void load_plugin(std::string_view path) {
        client_detail::check(daedalus_client_load_plugin(handle_, client_detail::bytes_of(path), path.size()));
    }

    void put_graph(std::string_view name, std::string_view json) {
        client_detail::check(daedalus_client_put_graph(handle_, client_detail::bytes_of(name), name.size(),
                                                       client_detail::bytes_of(json), json.size()));
    }

    void patch_graph(std::string_view graph, std::string_view patch) {
        client_detail::check(daedalus_client_patch_graph(handle_, client_detail::bytes_of(graph), graph.size(),
                                                         client_detail::bytes_of(patch), patch.size()));
    }

    // The plan of a stored graph, as JSON text. Planning only reads, so it may be asked again.
    std::string plan(std::string_view graph, const daedalus_client_plan_options_t& options) const {
        return client_detail::text_of([&](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_client_plan(handle_, client_detail::bytes_of(graph), graph.size(), &options, buf, cap, len);
        });
    }

    // The plan as plan() gives it, with the explanation as the planner wrote it, as a JSON string.
    std::string plan_raw(std::string_view graph, const daedalus_client_plan_options_t& options) const {
        return client_detail::text_of([&](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_client_plan_raw(handle_, client_detail::bytes_of(graph), graph.size(), &options, buf, cap, len);
        });
    }

    // A made plan of a stored graph: its hash and its bytes. Reads only, so a plan too big for the
    // first buffer is planned again, into a buffer of its size.
    MadePlan make_plan(std::string_view graph, const daedalus_client_config_t& config) const {
        MadePlan made{0, std::vector<std::uint8_t>(client_detail::kFirstBuffer)};
        size_t len = 0;
        daedalus_client_status_t status = daedalus_client_make_plan(
            handle_, client_detail::bytes_of(graph), graph.size(), &config, &made.hash, made.bytes.data(), made.bytes.size(), &len);
        while (status == DAEDALUS_CLIENT_BUFFER_TOO_SMALL) {
            made.bytes.resize(len);
            status = daedalus_client_make_plan(handle_, client_detail::bytes_of(graph), graph.size(), &config,
                                               &made.hash, made.bytes.data(), made.bytes.size(), &len);
        }
        client_detail::check(status);
        made.bytes.resize(len);
        return made;
    }

    void put_plan(std::string_view name, const std::vector<std::uint8_t>& bytes) {
        client_detail::check(daedalus_client_put_plan(handle_, client_detail::bytes_of(name), name.size(),
                                                      bytes.data(), bytes.size()));
    }

    // Plans a stored graph (or a made plan stored under the name) and starts a run.
    std::uint64_t start(std::string_view graph, const daedalus_client_config_t& config) {
        std::uint64_t run = 0;
        client_detail::check(daedalus_client_start(handle_, client_detail::bytes_of(graph), graph.size(), &config, &run));
        return run;
    }

    Step step(std::uint64_t run, std::uint32_t count) {
        Step out{};
        client_detail::check(daedalus_client_step(handle_, run, count, &out));
        return out;
    }

    void cancel(std::uint64_t run) { client_detail::check(daedalus_client_cancel(handle_, run)); }

    Stopped stop(std::uint64_t run, std::uint32_t mode) {
        Stopped out{};
        client_detail::check(daedalus_client_stop(handle_, run, mode, &out));
        return out;
    }

    Carry reload(std::uint64_t run, std::string_view graph) {
        Carry out{};
        client_detail::check(daedalus_client_reload(handle_, run, client_detail::bytes_of(graph), graph.size(), &out));
        return out;
    }

    // The document of a stored graph, as it was stored.
    std::string graph_text(std::string_view name) const {
        return client_detail::text_of([&](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_client_graph_text(handle_, client_detail::bytes_of(name), name.size(), buf, cap, len);
        });
    }

    std::string inspect_run(std::uint64_t run) const {
        return client_detail::text_of([&](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_client_inspect_run(handle_, run, buf, cap, len);
        });
    }
    std::string inspect_graph(std::string_view name) const {
        return client_detail::text_of([&](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_client_inspect_graph(handle_, client_detail::bytes_of(name), name.size(), buf, cap, len);
        });
    }
    std::string inspect_cache() const {
        return client_detail::text_of([&](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_client_inspect_cache(handle_, buf, cap, len);
        });
    }
    std::string inspect_telemetry(std::uint64_t run) const {
        return client_detail::text_of([&](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_client_inspect_telemetry(handle_, run, buf, cap, len);
        });
    }
    std::string inspect_host() const {
        return client_detail::text_of([&](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_client_inspect_host(handle_, buf, cap, len);
        });
    }
    void cache_clear() { client_detail::check(daedalus_client_cache_clear(handle_)); }
    std::string trace(std::uint32_t limit) const {
        return client_detail::text_of([&](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_client_trace(handle_, limit, buf, cap, len);
        });
    }

    // Feeds a value to a port. Each gives what the feed did.
    Fed feed_i64(std::uint64_t run, std::string_view port, std::int64_t value) {
        Fed out{};
        client_detail::check(daedalus_client_feed_i64(handle_, run, client_detail::bytes_of(port), port.size(), value, &out));
        return out;
    }
    Fed feed_f64(std::uint64_t run, std::string_view port, double value) {
        Fed out{};
        client_detail::check(daedalus_client_feed_f64(handle_, run, client_detail::bytes_of(port), port.size(), value, &out));
        return out;
    }
    Fed feed_bool(std::uint64_t run, std::string_view port, bool value) {
        Fed out{};
        client_detail::check(daedalus_client_feed_bool(handle_, run, client_detail::bytes_of(port), port.size(), value ? 1 : 0, &out));
        return out;
    }
    Fed feed_bytes(std::uint64_t run, std::string_view port, const std::vector<std::uint8_t>& bytes) {
        Fed out{};
        client_detail::check(daedalus_client_feed_bytes(handle_, run, client_detail::bytes_of(port), port.size(),
                                                        bytes.data(), bytes.size(), &out));
        return out;
    }
    Fed feed_str(std::uint64_t run, std::string_view port, std::string_view text) {
        Fed out{};
        client_detail::check(daedalus_client_feed_str(handle_, run, client_detail::bytes_of(port), port.size(),
                                                      client_detail::bytes_of(text), text.size(), &out));
        return out;
    }
    // A value of any type, as its wire encoding (see daedalus_client.h).
    Fed feed_wire(std::uint64_t run, std::string_view port, const std::vector<std::uint8_t>& wire) {
        Fed out{};
        client_detail::check(daedalus_client_feed_wire(handle_, run, client_detail::bytes_of(port), port.size(),
                                                       wire.data(), wire.size(), &out));
        return out;
    }

    // Takes: the oldest value of a port, or nothing when there is none.
    std::optional<std::int64_t> take_i64(std::uint64_t run, std::string_view port) {
        std::int64_t out = 0;
        uint8_t present = 0;
        return client_detail::taken(daedalus_client_take_i64(handle_, run, client_detail::bytes_of(port), port.size(), &out, &present), out, present);
    }
    std::optional<double> take_f64(std::uint64_t run, std::string_view port) {
        double out = 0;
        uint8_t present = 0;
        return client_detail::taken(daedalus_client_take_f64(handle_, run, client_detail::bytes_of(port), port.size(), &out, &present), out, present);
    }
    std::optional<bool> take_bool(std::uint64_t run, std::string_view port) {
        uint8_t out = 0;
        uint8_t present = 0;
        auto value = client_detail::taken(daedalus_client_take_bool(handle_, run, client_detail::bytes_of(port), port.size(), &out, &present), out, present);
        if (!value) return std::nullopt;
        return *value != 0;
    }
    // Takes the bytes of a bytes value. A value that does not fit the size asked for waits in the port.
    std::optional<std::vector<std::uint8_t>> take_bytes(std::uint64_t run, std::string_view port) {
        return take_sized([&](uint8_t* buf, size_t cap, size_t* len, uint8_t* present) {
            return daedalus_client_take_bytes(handle_, run, client_detail::bytes_of(port), port.size(), buf, cap, len, present);
        });
    }
    // Takes a value of any type as its wire encoding.
    std::optional<std::vector<std::uint8_t>> take_wire(std::uint64_t run, std::string_view port) {
        return take_sized([&](uint8_t* buf, size_t cap, size_t* len, uint8_t* present) {
            return daedalus_client_take_wire(handle_, run, client_detail::bytes_of(port), port.size(), buf, cap, len, present);
        });
    }

    // The report forms: each does its call and keeps what it gave for report() (see daedalus_client.h).
    void load_plugin_report(std::string_view path) {
        client_detail::check(daedalus_client_load_plugin_report(handle_, client_detail::bytes_of(path), path.size()));
    }
    void put_graph_report(std::string_view name, std::string_view json) {
        client_detail::check(daedalus_client_put_graph_report(handle_, client_detail::bytes_of(name), name.size(),
                                                              client_detail::bytes_of(json), json.size()));
    }
    void patch_graph_report(std::string_view graph, std::string_view patch) {
        client_detail::check(daedalus_client_patch_graph_report(handle_, client_detail::bytes_of(graph), graph.size(),
                                                                client_detail::bytes_of(patch), patch.size()));
    }
    void reload_report(std::uint64_t run, std::string_view graph) {
        client_detail::check(daedalus_client_reload_report(handle_, run, client_detail::bytes_of(graph), graph.size()));
    }
    // The JSON the last report form kept on this connection's handle.
    std::string report() const {
        return client_detail::text_of([&](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_client_report(handle_, buf, cap, len);
        });
    }

    // Subscribes to the events of `run` (DAEDALUS_CLIENT_ALL_RUNS for every run) in `kinds`.
    Watch watch(std::uint64_t run, std::uint32_t kinds);

    daedalus_client_handle_t handle() const noexcept { return handle_; }

private:
    explicit Client(daedalus_client_handle_t handle) : handle_(handle) {}
    void release() noexcept {
        if (handle_ != 0) daedalus_client_close(std::exchange(handle_, 0));
    }

    template <typename Call>
    static std::optional<std::vector<std::uint8_t>> take_sized(Call call) {
        std::vector<std::uint8_t> out(client_detail::kFirstBuffer);
        size_t len = 0;
        uint8_t present = 0;
        daedalus_client_status_t status = call(out.data(), out.size(), &len, &present);
        while (status == DAEDALUS_CLIENT_BUFFER_TOO_SMALL) {
            out.resize(len);
            status = call(out.data(), out.size(), &len, &present);
        }
        client_detail::check(status);
        if (!present) return std::nullopt;
        out.resize(len);
        return out;
    }

    daedalus_client_handle_t handle_ = 0;
    friend class Watch;
};

// A subscription to events, pulled one at a time with a timeout.
class Watch {
public:
    Watch(Watch&& other) noexcept : handle_(std::exchange(other.handle_, 0)) {}
    Watch& operator=(Watch&& other) noexcept {
        if (this != &other) {
            release();
            handle_ = std::exchange(other.handle_, 0);
        }
        return *this;
    }
    Watch(const Watch&) = delete;
    Watch& operator=(const Watch&) = delete;
    ~Watch() { release(); }

    // Waits up to `timeout_ms` for the next event. The state says whether one came, none came in
    // time, or none will.
    WatchPoll next(std::uint32_t timeout_ms) {
        std::uint32_t state = 0;
        std::string json = client_detail::text_of([&](uint8_t* buf, size_t cap, size_t* len) {
            return daedalus_client_watch_next(handle_, timeout_ms, buf, cap, len, &state);
        });
        return WatchPoll{state, std::move(json)};
    }

private:
    explicit Watch(daedalus_client_handle_t handle) : handle_(handle) {}
    void release() noexcept {
        if (handle_ != 0) daedalus_client_watch_close(std::exchange(handle_, 0));
    }

    daedalus_client_handle_t handle_ = 0;
    friend class Client;
};

inline Watch Client::watch(std::uint64_t run, std::uint32_t kinds) {
    daedalus_client_handle_t handle = 0;
    client_detail::check(daedalus_client_watch_open(handle_, run, kinds, &handle));
    return Watch(handle);
}

}  // namespace dd

#endif  // DAEDALUS_CLIENT_HPP
