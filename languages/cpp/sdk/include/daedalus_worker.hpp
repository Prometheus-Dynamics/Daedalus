// The Daedalus worker SDK for C++17: a header over the C API in daedalus_worker.h. It adds three
// things to the C API, and no logic of its own:
//   - dd::Types owns every type a worker's table names, so nothing is allocated with new and
//     nothing leaks; the table is valid for as long as the dd::Types is.
//   - dd::Ctx is a non-owning view of a node's context: one C call per method.
//   - dd::run<fn> turns a C++ node body into the C node function. An exception the body throws
//     becomes the node's fault, and it never crosses the C boundary.
//
// A worker is built in main from a dd::Types and a table of dd_node, then served:
//   dd::Types types;
//   static const dd_port in[] = {dd::port("x", types.scalar(DD_I64))};
//   static const dd_port out[] = {dd::port("y", types.scalar(DD_I64))};
//   static const dd_node table[] = {{"example.inc", in, 1, out, 1, nullptr, &dd::run<inc>}};
//   return dd::serve(table, 1, "example-worker");
// with a node body `int inc(dd::Ctx& cx, const dd_value* in, dd_value* out)`.
#ifndef DAEDALUS_WORKER_HPP
#define DAEDALUS_WORKER_HPP

#include <cstddef>
#include <cstdint>
#include <exception>
#include <initializer_list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "daedalus_worker.h"

namespace dd {

// Scalar types, with static storage. Each call with the same scalar returns the same object.
inline const dd_type* scalar(dd_scalar s) {
    static const dd_type table[] = {
        {DD_K_SCALAR, DD_UNIT, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_BOOL, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_I8, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_I16, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_I32, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_I64, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_ISIZE, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_U8, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_U16, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_U32, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_U64, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_USIZE, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_F32, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_F64, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_STRING, nullptr, nullptr, nullptr, 0},
        {DD_K_SCALAR, DD_BYTES, nullptr, nullptr, nullptr, 0},
    };
    return &table[static_cast<int>(s)];
}

// A port: its name and type.
inline dd_port port(const char* name, const dd_type* type) { return dd_port{name, type}; }

// A fault a node body throws. Its code is one of the closed fault codes; its reason is sent with
// it. The default code is node_error.
class Fault : public std::exception {
public:
    explicit Fault(std::string reason, std::string code = "node_error")
        : code_(std::move(code)), reason_(std::move(reason)) {}
    const char* what() const noexcept override { return reason_.c_str(); }
    const std::string& code() const noexcept { return code_; }
    const std::string& reason() const noexcept { return reason_; }

private:
    std::string code_;
    std::string reason_;
};

// A node's context: a non-owning view of the C dd_ctx a node function is passed.
class Ctx {
public:
    explicit Ctx(dd_ctx* raw) : raw_(raw) {}
    dd_ctx* raw() const { return raw_; }
    void progress(float fraction, const char* message) { dd_progress(raw_, fraction, message); }
    void log(const char* level, const char* text) { dd_log(raw_, level, text); }
    void detail(const char* text) { dd_detail(raw_, text); }
    void fault_code(std::uint32_t code) { dd_fault_code(raw_, code); }
    void fault(const char* code, const char* reason) { dd_fault(raw_, code, reason); }
    bool cancelled() const { return dd_cancelled(raw_); }
    void* alloc(std::size_t size) { return dd_alloc(raw_, size); }
    dd_value* state() { return dd_state(raw_); }
    void set_state(dd_value value) { dd_set_state(raw_, value); }

private:
    dd_ctx* raw_;
};

// A node body: a function with no captures. It returns 0 for success, as the C function does.
using NodeBody = int (*)(Ctx& cx, const dd_value* inputs, dd_value* outputs);

// The C node function for `Body`. A Fault thrown by the body becomes its fault, with its code and
// reason, and any other std::exception becomes a node_error with its message; the node then returns
// 1. Nothing is thrown past this function.
template <NodeBody Body>
int run(dd_ctx* raw, const dd_value* inputs, dd_value* outputs) noexcept {
    Ctx cx(raw);
    try {
        return Body(cx, inputs, outputs);
    } catch (const Fault& fault) {
        cx.fault(fault.code().c_str(), fault.reason().c_str());
    } catch (const std::exception& error) {
        cx.fault("node_error", error.what());
    } catch (...) {
        cx.fault("node_error", "the node threw an exception");
    }
    return 1;
}

// Owns every type a worker's table names. Each builder returns a pointer that stays valid until the
// Types is destroyed, and a table built from it must not outlive it. The builders copy every name
// and key, so the caller's strings need not outlive the call.
class Types {
public:
    Types() = default;
    Types(const Types&) = delete;
    Types& operator=(const Types&) = delete;

    const dd_type* scalar(dd_scalar s) const { return dd::scalar(s); }

    const dd_type* optional(const dd_type* item) { return one(DD_K_OPTIONAL, item); }
    const dd_type* list(const dd_type* item) { return one(DD_K_LIST, item); }

    const dd_type* map(const dd_type* key, const dd_type* value) {
        Owned& owned = fresh();
        owned.items = {key, value};
        owned.type = dd_type{DD_K_MAP, DD_UNIT, nullptr, nullptr, nullptr, 2};
        return finish(owned);
    }

    // The items of a tuple, in order.
    const dd_type* tuple(std::initializer_list<const dd_type*> items) {
        Owned& owned = fresh();
        owned.items.assign(items.begin(), items.end());
        owned.type = dd_type{DD_K_TUPLE, DD_UNIT, nullptr, nullptr, nullptr, owned.items.size()};
        return finish(owned);
    }

    // A struct's fields, by name, in order.
    const dd_type* strukt(std::initializer_list<std::pair<const char*, const dd_type*>> fields) {
        return named(DD_K_STRUCT, fields);
    }

    // An enum's variants, by name, in order. A unit variant has a null payload type.
    const dd_type* enm(std::initializer_list<std::pair<const char*, const dd_type*>> variants) {
        return named(DD_K_ENUM, variants);
    }

    // An opaque type, by its key.
    const dd_type* opaque(const char* key) {
        Owned& owned = fresh();
        owned.key = key;
        owned.type = dd_type{DD_K_OPAQUE, DD_UNIT, owned.key.c_str(), nullptr, nullptr, 0};
        return finish(owned);
    }

private:
    struct Owned {
        dd_type type{};
        std::vector<const dd_type*> items;
        std::vector<std::string> name_text;
        std::vector<const char*> names;
        std::string key;
    };

    Owned& fresh() {
        owned_.push_back(std::make_unique<Owned>());
        return *owned_.back();
    }

    // The type's item and name pointers are set once every item is in, and the owned vectors are
    // not changed after that, so the pointers stay valid.
    const dd_type* finish(Owned& owned) {
        owned.type.items = owned.items.empty() ? nullptr : owned.items.data();
        owned.type.names = owned.names.empty() ? nullptr : owned.names.data();
        return &owned.type;
    }

    const dd_type* one(dd_kind kind, const dd_type* item) {
        Owned& owned = fresh();
        owned.items = {item};
        owned.type = dd_type{kind, DD_UNIT, nullptr, nullptr, nullptr, 1};
        return finish(owned);
    }

    const dd_type* named(dd_kind kind,
                         std::initializer_list<std::pair<const char*, const dd_type*>> entries) {
        Owned& owned = fresh();
        for (const auto& entry : entries) {
            owned.name_text.emplace_back(entry.first);
            owned.items.push_back(entry.second);
        }
        for (const auto& text : owned.name_text) owned.names.push_back(text.c_str());
        owned.type = dd_type{kind, DD_UNIT, nullptr, nullptr, nullptr, owned.items.size()};
        return finish(owned);
    }

    std::vector<std::unique_ptr<Owned>> owned_;
};

// Serves the table on stdin and stdout until the host shuts the worker down or closes the pipe. The
// table and its types are read once, when this starts. Returns 0 on a clean end, and 1 otherwise.
inline int serve(const dd_node* nodes, std::size_t count, const char* worker_name) {
    return dd_serve(nodes, count, worker_name) == 0 ? 0 : 1;
}

}  // namespace dd

#endif  // DAEDALUS_WORKER_HPP
