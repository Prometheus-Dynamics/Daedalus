// The drive-conformance driver in C++, over the client C ABI through daedalus_client.hpp.
//
// It follows the driver contract in testing/crates/drive-conformance/src/lib.rs: one command per
// input line, one answer line per command, and the answers of the Rust reference driver. The
// client here is the Rust client behind the C ABI, so the codec mode does not apply: this driver
// does not read or write the codec corpus. Its text is the caller's: the scenario parsing, the JSON
// the C ABI returns, and the answer lines.
//
// Usage: driver ENDPOINT, with the scenario on standard input.

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "daedalus_client.hpp"

namespace {

// ---- JSON: the values the C ABI returns. Object members keep their order; a number keeps its text.

struct Json {
    enum Kind { Null, Bool, Number, String, Array, Object } kind = Null;
    bool flag = false;
    std::string text;               // a string's text, or a number's token
    std::vector<Json> items;        // an array's items, or an object's values
    std::vector<std::string> keys;  // an object's keys, in order

    const Json* member(const char* key) const {
        for (std::size_t i = 0; i < keys.size(); i++) {
            if (keys[i] == key) return &items[i];
        }
        return nullptr;
    }
    const Json& at(const char* key) const {
        const Json* found = member(key);
        if (!found) throw std::runtime_error(std::string("no field ") + key);
        return *found;
    }
    std::int64_t integer() const { return std::stoll(text); }
    std::uint64_t unsigned_integer() const { return std::stoull(text); }
};

class JsonReader {
public:
    explicit JsonReader(const std::string& text) : text_(text) {}
    Json read() {
        Json value = parse();
        skip();
        if (at_ != text_.size()) throw std::runtime_error("trailing JSON");
        return value;
    }

private:
    void skip() {
        while (at_ < text_.size() && (text_[at_] == ' ' || text_[at_] == '\n' || text_[at_] == '\r' || text_[at_] == '\t')) at_++;
    }
    bool take(char c) {
        skip();
        if (at_ < text_.size() && text_[at_] == c) {
            at_++;
            return true;
        }
        return false;
    }
    Json parse() {
        skip();
        if (at_ >= text_.size()) throw std::runtime_error("JSON ends early");
        Json value;
        char c = text_[at_];
        if (c == '{') {
            at_++;
            value.kind = Json::Object;
            if (take('}')) return value;
            do {
                skip();
                value.keys.push_back(parse().text);
                if (!take(':')) throw std::runtime_error("expected ':'");
                value.items.push_back(parse());
            } while (take(','));
            if (!take('}')) throw std::runtime_error("expected '}'");
        } else if (c == '[') {
            at_++;
            value.kind = Json::Array;
            if (take(']')) return value;
            do {
                value.items.push_back(parse());
            } while (take(','));
            if (!take(']')) throw std::runtime_error("expected ']'");
        } else if (c == '"') {
            value.kind = Json::String;
            value.text = string();
        } else if (text_.compare(at_, 4, "true") == 0) {
            value.kind = Json::Bool;
            value.flag = true;
            at_ += 4;
        } else if (text_.compare(at_, 5, "false") == 0) {
            value.kind = Json::Bool;
            at_ += 5;
        } else if (text_.compare(at_, 4, "null") == 0) {
            at_ += 4;
        } else {
            value.kind = Json::Number;
            std::size_t start = at_;
            while (at_ < text_.size() && std::strchr("+-.eE0123456789", text_[at_])) at_++;
            value.text = text_.substr(start, at_ - start);
        }
        return value;
    }
    static void append_utf8(std::string& out, std::uint32_t cp) {
        if (cp < 0x80) {
            out += static_cast<char>(cp);
        } else if (cp < 0x800) {
            out += static_cast<char>(0xC0 | (cp >> 6));
            out += static_cast<char>(0x80 | (cp & 0x3F));
        } else if (cp < 0x10000) {
            out += static_cast<char>(0xE0 | (cp >> 12));
            out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
            out += static_cast<char>(0x80 | (cp & 0x3F));
        } else {
            out += static_cast<char>(0xF0 | (cp >> 18));
            out += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
            out += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
            out += static_cast<char>(0x80 | (cp & 0x3F));
        }
    }
    std::uint32_t hex4() {
        std::uint32_t cp = std::stoul(text_.substr(at_, 4), nullptr, 16);
        at_ += 4;
        return cp;
    }
    std::string string() {
        std::string out;
        at_++;  // the opening quote
        while (at_ < text_.size() && text_[at_] != '"') {
            char c = text_[at_++];
            if (c != '\\') {
                out += c;
                continue;
            }
            char e = text_[at_++];
            switch (e) {
                case 'n': out += '\n'; break;
                case 'r': out += '\r'; break;
                case 't': out += '\t'; break;
                case 'b': out += '\b'; break;
                case 'f': out += '\f'; break;
                case 'u': {
                    std::uint32_t cp = hex4();
                    if (cp >= 0xD800 && cp < 0xDC00 && text_.compare(at_, 2, "\\u") == 0) {
                        at_ += 2;
                        cp = 0x10000 + ((cp - 0xD800) << 10) + (hex4() - 0xDC00);
                    }
                    append_utf8(out, cp);
                    break;
                }
                default: out += e;  // \" \\ \/
            }
        }
        at_++;  // the closing quote
        return out;
    }

    const std::string& text_;
    std::size_t at_ = 0;
};

Json parse_json(const std::string& text) { return JsonReader(text).read(); }

// ---- The canonical text of an answer (the reference's show.rs, in this language).

// A code point in lowercase hex, without leading zeros.
std::string hex_text(std::uint32_t value) {
    std::ostringstream out;
    out << std::hex << value;
    return out.str();
}

// A string in quotes: the escapes the contract names, and `\u{H}` for anything outside printable ASCII.
std::string quote(const std::string& text) {
    std::string out = "\"";
    for (std::size_t i = 0; i < text.size();) {
        unsigned char c = static_cast<unsigned char>(text[i]);
        std::uint32_t cp = c;
        std::size_t width = 1;
        if (c >= 0xF0) {
            cp = c & 0x07;
            width = 4;
        } else if (c >= 0xE0) {
            cp = c & 0x0F;
            width = 3;
        } else if (c >= 0xC0) {
            cp = c & 0x1F;
            width = 2;
        }
        for (std::size_t k = 1; k < width && i + k < text.size(); k++) {
            cp = (cp << 6) | (static_cast<unsigned char>(text[i + k]) & 0x3F);
        }
        i += width;
        switch (cp) {
            case '"': out += "\\\""; break;
            case '\\': out += "\\\\"; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default:
                if (cp >= 0x20 && cp <= 0x7E) {
                    out += static_cast<char>(cp);
                } else {
                    out += "\\u{" + hex_text(cp) + "}";
                }
        }
    }
    return out + "\"";
}

std::string list(const std::vector<std::string>& items) {
    std::string out = "[";
    for (std::size_t i = 0; i < items.size(); i++) {
        if (i) out += ',';
        out += quote(items[i]);
    }
    return out + "]";
}

std::string bool_text(bool flag) { return flag ? "true" : "false"; }

// A string list in the answer's JSON, each as text.
std::vector<std::string> strings_of(const Json& array) {
    std::vector<std::string> out;
    for (const Json& item : array.items) out.push_back(item.text);
    return out;
}

// `severity|code` for each diagnostic of a JSON array.
std::string diagnostics_text(const Json& array) {
    std::vector<std::string> items;
    for (const Json& item : array.items) items.push_back(item.at("severity").text + "|" + item.at("code").text);
    return list(items);
}

std::string error_code_name(std::uint32_t code) {
    static const char* names[] = {"", "invalid_request", "unknown_graph", "unknown_run", "unknown_port", "value_type",
                                  "plugin_error", "planning_failed", "busy", "cancelled", "stopped",
                                  "frame_too_large", "unsupported"};
    if (code >= 1 && code <= 12) return names[code];
    return "other";
}

// ---- Values: TYPE:VALUE, as the wire encodes them (a scalar's tag and code, then its value bytes).

struct ScalarType {
    const char* name;
    std::uint8_t code;
    std::size_t width;  // 0 for unit; strings and bytes are 0 here, and take a u32 length
};

const ScalarType kScalars[] = {
    {"unit", 0, 0},  {"bool", 1, 1},   {"i8", 2, 1},    {"i16", 3, 2},   {"i32", 4, 4},   {"i64", 5, 8},
    {"isize", 6, 8}, {"u8", 7, 1},     {"u16", 8, 2},   {"u32", 9, 4},   {"u64", 10, 8},  {"usize", 11, 8},
    {"string", 14, 0}, {"bytes", 15, 0},
};

const ScalarType* scalar_named(const std::string& name) {
    for (const ScalarType& scalar : kScalars) {
        if (name == scalar.name) return &scalar;
    }
    return nullptr;
}

void put_le(std::vector<std::uint8_t>& out, std::uint64_t value, std::size_t width) {
    for (std::size_t i = 0; i < width; i++) out.push_back(static_cast<std::uint8_t>(value >> (8 * i)));
}

struct Usage {};   // the line is not a command, or names a watch or blob the scenario never made
struct Local {};   // a file the command reads cannot be read

std::vector<std::uint8_t> unhex(const std::string& text) {
    if (text.size() % 2) throw Usage{};
    std::vector<std::uint8_t> out;
    for (std::size_t i = 0; i < text.size(); i += 2) out.push_back(static_cast<std::uint8_t>(std::stoul(text.substr(i, 2), nullptr, 16)));
    return out;
}

// The wire encoding of `TYPE:VALUE`. Nothing for a type or a value the contract does not have.
std::vector<std::uint8_t> encode_value(const std::string& typed) {
    std::size_t colon = typed.find(':');
    if (colon == std::string::npos) throw std::invalid_argument("no type");
    const ScalarType* scalar = scalar_named(typed.substr(0, colon));
    if (!scalar) throw std::invalid_argument("no such type");
    std::string text = typed.substr(colon + 1);
    std::vector<std::uint8_t> out = {0, scalar->code};
    if (scalar->code == 14) {
        put_le(out, text.size(), 4);
        out.insert(out.end(), text.begin(), text.end());
        return out;
    }
    if (scalar->code == 15) {
        // bytes are lowercase hex in the contract: two digits a byte, and no digits for none.
        std::vector<std::uint8_t> bytes = unhex(text);
        put_le(out, bytes.size(), 4);
        out.insert(out.end(), bytes.begin(), bytes.end());
        return out;
    }
    if (scalar->code == 1) {
        if (text != "true" && text != "false") throw std::invalid_argument("not a bool");
        out.push_back(text == "true" ? 1 : 0);
        return out;
    }
    if (scalar->width == 0) return out;  // unit
    if (scalar->code == 0) return out;
    bool negative = !text.empty() && text[0] == '-';
    std::uint64_t magnitude = std::stoull(negative ? text.substr(1) : text);
    put_le(out, negative ? ~magnitude + 1 : magnitude, scalar->width);
    return out;
}

const char* scalar_name_of(std::uint8_t code) {
    for (const ScalarType& scalar : kScalars) {
        if (scalar.code == code) return scalar.name;
    }
    return nullptr;
}

// The `TYPE:VALUE` of a taken value's wire encoding, as the answer prints it.
std::string value_text(const std::vector<std::uint8_t>& wire) {
    if (wire.size() < 2 || wire[0] != 0) return "other";
    const char* name = scalar_name_of(wire[1]);
    if (!name) return "other";
    const ScalarType* scalar = scalar_named(name);
    std::string prefix = std::string(name) + ":";
    std::size_t at = 2;
    auto read = [&](std::size_t width) {
        std::uint64_t value = 0;
        for (std::size_t i = 0; i < width && at + i < wire.size(); i++) value |= std::uint64_t(wire[at + i]) << (8 * i);
        at += width;
        return value;
    };
    switch (scalar->code) {
        case 0: return "unit:";
        case 1: return prefix + (wire.size() > 2 && wire[2] ? "true" : "false");
        case 14: {
            std::uint64_t len = read(4);
            return prefix + std::string(wire.begin() + at, wire.begin() + at + len);
        }
        case 15: {
            std::uint64_t len = read(4);
            std::string hex;
            static const char* digits = "0123456789abcdef";
            for (std::uint64_t i = 0; i < len; i++) {
                hex += digits[wire[at + i] >> 4];
                hex += digits[wire[at + i] & 0xF];
            }
            return prefix + hex;
        }
        case 2: case 3: case 4: case 5: case 6: {
            std::uint64_t raw = read(scalar->width);
            std::int64_t shift = 64 - 8 * static_cast<std::int64_t>(scalar->width);
            std::int64_t value = static_cast<std::int64_t>(raw << shift) >> shift;
            return prefix + std::to_string(value);
        }
        case 7: case 8: case 9: case 10: case 11:
            return prefix + std::to_string(read(scalar->width));
        default: return "other";
    }
}

// ---- Scenario lines: tokens, options and the commands.

std::vector<std::string> tokens_of(const std::string& line) {
    std::vector<std::string> out;
    std::istringstream in(line);
    std::string token;
    while (std::getline(in, token, ' ')) out.push_back(token);
    return out;
}

std::uint64_t number(const std::string& text) {
    if (text.empty() || text.find_first_not_of("0123456789") != std::string::npos) throw Usage{};
    return std::stoull(text);
}


std::string read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw Local{};
    std::ostringstream text;
    text << in.rdbuf();
    return text.str();
}

// The start and make_plan options, as the config struct. `priority` holds the names the list gives.
struct Options {
    daedalus_client_config_t config{};
    std::vector<std::string> priority_names;
    std::vector<daedalus_client_text_t> priority;

    void parse(const std::vector<std::string>& words, std::size_t from) {
        config.telemetry = DAEDALUS_CLIENT_TELEMETRY_BASIC;
        for (std::size_t i = from; i < words.size(); i++) {
            const std::string& word = words[i];
            if (word.rfind("fifo=", 0) == 0) {
                config.fifo_capacity = static_cast<std::uint32_t>(number(word.substr(5)));
            } else if (word.rfind("faults=", 0) == 0) {
                config.fault_reports = static_cast<std::uint32_t>(number(word.substr(7)));
            } else if (word.rfind("telemetry=", 0) == 0) {
                std::string level = word.substr(10);
                if (level == "off") config.telemetry = DAEDALUS_CLIENT_TELEMETRY_OFF;
                else if (level == "basic") config.telemetry = DAEDALUS_CLIENT_TELEMETRY_BASIC;
                else if (level == "detailed") config.telemetry = DAEDALUS_CLIENT_TELEMETRY_DETAILED;
                else if (level == "trace") config.telemetry = DAEDALUS_CLIENT_TELEMETRY_TRACE;
                else throw Usage{};
            } else if (word == "unmerged") {
                config.unmerged = 1;
            } else if (word.rfind("priority=", 0) == 0) {
                priority_names = split_commas(word.substr(9));
            } else {
                throw Usage{};
            }
        }
        priority.clear();
        for (const std::string& name : priority_names) priority.push_back(daedalus_client_text_t{reinterpret_cast<const std::uint8_t*>(name.data()), name.size()});
        config.priority = priority.empty() ? nullptr : priority.data();
        config.priority_len = priority.size();
    }

    static std::vector<std::string> split_commas(const std::string& text) {
        std::vector<std::string> out;
        std::string item;
        std::istringstream in(text);
        while (std::getline(in, item, ',')) out.push_back(item);
        return out;
    }
};

std::uint32_t form_named(const std::string& name) {
    if (name == "steps") return DAEDALUS_CLIENT_FORM_STEPS;
    if (name == "blob") return DAEDALUS_CLIENT_FORM_BLOB;
    if (name == "source") return DAEDALUS_CLIENT_FORM_SOURCE;
    throw Usage{};
}

class Driver {
public:
    explicit Driver(const std::string& endpoint) : root_(dd::Client::connect(endpoint, false)) {
        current_ = &root_;
    }

    // The one answer line for one command line.
    std::string answer(const std::string& line) {
        try {
            return run(tokens_of(line));
        } catch (const Usage&) {
            return "error usage";
        } catch (const Local&) {
            return "error local";
        } catch (const dd::ClientError& error) {
            if (error.status() == DAEDALUS_CLIENT_SERVICE_ERROR) return "error " + error_code_name(error.service_code());
            return "error local";
        } catch (const std::invalid_argument&) {
            return "error usage";
        } catch (const std::out_of_range&) {
            return "error usage";
        } catch (const std::exception&) {
            return "error local";
        }
    }

private:
    std::string run(const std::vector<std::string>& t) {
        if (t.empty()) throw Usage{};
        const std::string& cmd = t[0];
        if (cmd == "ping") {
            return "pong version=" + std::to_string(current_->ping());
        }
        if (cmd == "session") {
            std::string name = t.size() > 1 ? t[1] : "default";
            sessions_.push_back(std::make_unique<dd::Client>(root_.in_session(name)));
            current_ = sessions_.back().get();
            return "session name=" + quote(name);
        }
        if (cmd == "load_plugin" && t.size() == 2) {
            current_->load_plugin_report(t[1]);
            Json info = parse_json(current_->report());
            return "plugin name=" + quote(info.at("name").text) + " generation=" + std::to_string(info.at("generation").unsigned_integer()) +
                   " nodes=" + list(strings_of(info.at("nodes"))) + " backends=" + list(strings_of(info.at("backends")));
        }
        if ((cmd == "put_graph" || cmd == "patch_graph") && t.size() == 3) {
            std::string text = read_file(t[2]);
            if (cmd == "put_graph") current_->put_graph_report(t[1], text);
            else current_->patch_graph_report(t[1], text);
            return "graph name=" + quote(t[1]) + " diagnostics=" + diagnostics_text(parse_json(current_->report()));
        }
        if (cmd == "plan" && t.size() >= 3) return plan(t);
        if (cmd == "blob" && t.size() == 3) {
            blobs_[t[1]] = unhex(t[2]);
            return "blob name=" + quote(t[1]) + " bytes=" + std::to_string(blobs_[t[1]].size());
        }
        if (cmd == "make_plan" && t.size() >= 3) {
            Options options;
            options.parse(t, 3);
            dd::MadePlan made = current_->make_plan(t[1], options.config);
            blobs_[t[2]] = made.bytes;
            return "made hash=" + std::to_string(made.hash) + " bytes=" + std::to_string(made.bytes.size());
        }
        if (cmd == "put_plan" && t.size() == 3) {
            current_->put_plan(t[1], blob(t[2]));
            return "ack";
        }
        if (cmd == "start" && t.size() >= 2) {
            Options options;
            options.parse(t, 2);
            return "started run=" + std::to_string(current_->start(t[1], options.config));
        }
        if (cmd == "feed" && t.size() >= 4) {
            // The value is the rest of the line: a string may hold spaces.
            std::string value = line_rest(t, 3);
            dd::Fed fed = current_->feed_wire(number(t[1]), t[2], encode_value(value));
            return "fed woke=" + std::to_string(fed.woke) + " dropped=" + bool_text(fed.dropped);
        }
        if (cmd == "step" && t.size() == 3) {
            daedalus_client_step_t step = current_->step(number(t[1]), static_cast<std::uint32_t>(number(t[2])));
            return "stepped ticks=" + std::to_string(step.ticks) + " tick=" + std::to_string(step.tick) +
                   " step_calls=" + std::to_string(step.step_calls) + " faults=" + std::to_string(step.faults) +
                   " skipped=" + std::to_string(step.skipped) + " idle=" + bool_text(step.idle != 0);
        }
        if (cmd == "take" && t.size() == 3) {
            std::optional<std::vector<std::uint8_t>> wire = current_->take_wire(number(t[1]), t[2]);
            return wire ? "value " + value_text(*wire) : "value none";
        }
        if (cmd == "watch" && (t.size() == 3 || t.size() == 4)) {
            std::uint64_t target = t[2] == "all" ? DAEDALUS_CLIENT_ALL_RUNS : number(t[2]);
            std::uint32_t kinds = DAEDALUS_CLIENT_EVENT_ALL;
            if (t.size() == 4) kinds = kinds_named(t[3]);
            watches_[number(t[1])] = current_->watch(target, kinds);
            return "watching " + t[1];
        }
        if (cmd == "next" && t.size() == 2) return next(number(t[1]));
        if (cmd == "unwatch" && t.size() == 2) {
            auto found = watches_.find(number(t[1]));
            if (found == watches_.end()) throw Usage{};
            found->second.reset();  // closes the subscription
            return "unwatched " + t[1];
        }
        if (cmd == "reload" && t.size() == 3) {
            current_->reload_report(number(t[1]), t[2]);
            return reloaded(parse_json(current_->report()));
        }
        if (cmd == "cancel" && t.size() == 2) {
            current_->cancel(number(t[1]));
            return "ack";
        }
        if (cmd == "stop" && t.size() == 3) {
            std::uint32_t mode = t[2] == "drain" ? DAEDALUS_CLIENT_STOP_DRAIN : t[2] == "immediate" ? DAEDALUS_CLIENT_STOP_IMMEDIATE : 99;
            if (mode == 99) throw Usage{};
            daedalus_client_stopped_t stopped = current_->stop(number(t[1]), mode);
            return "stopped tick=" + std::to_string(stopped.tick) + " dropped=" + std::to_string(stopped.dropped);
        }
        if (cmd == "inspect" && t.size() >= 2) return inspect(t);
        if (cmd == "cache" && t.size() == 2 && t[1] == "clear") {
            current_->cache_clear();
            return "ack";
        }
        if (cmd == "trace" && t.size() == 2) return trace(current_->trace(static_cast<std::uint32_t>(number(t[1]))));
        throw Usage{};
    }

    static std::string line_rest(const std::vector<std::string>& t, std::size_t from) {
        std::string out;
        for (std::size_t i = from; i < t.size(); i++) {
            if (i > from) out += ' ';
            out += t[i];
        }
        return out;
    }

    std::vector<std::uint8_t> blob(const std::string& name) {
        auto found = blobs_.find(name);
        if (found == blobs_.end()) throw Usage{};
        return found->second;
    }

    static std::uint32_t kinds_named(const std::string& word) {
        if (word == "fault") return DAEDALUS_CLIENT_EVENT_FAULT;
        if (word == "telemetry") return DAEDALUS_CLIENT_EVENT_TELEMETRY;
        if (word == "all") return DAEDALUS_CLIENT_EVENT_ALL;
        throw Usage{};
    }

    std::string plan(const std::vector<std::string>& t) {
        daedalus_client_plan_options_t options{};
        options.form = form_named(t[2]);
        std::vector<std::string> priority_names;
        for (std::size_t i = 3; i < t.size(); i++) {
            if (t[i] == "inspect") {
                options.inspect = 1;
            } else if (t[i].rfind("priority=", 0) == 0) {
                priority_names = Options::split_commas(t[i].substr(9));
            } else {
                throw Usage{};
            }
        }
        std::vector<daedalus_client_text_t> priority;
        for (const std::string& name : priority_names) priority.push_back(daedalus_client_text_t{reinterpret_cast<const std::uint8_t*>(name.data()), name.size()});
        options.priority = priority.empty() ? nullptr : priority.data();
        options.priority_len = priority.size();
        Json summary = parse_json(current_->plan_raw(t[1], options));
        const Json& explain = summary.at("explain");
        std::string explain_text = explain.kind == Json::Null ? "none" : quote(explain.text);
        std::string hash = summary.at("hash").text;  // "0x" and sixteen digits
        return "plan graph=" + quote(summary.at("graph").text) + " hash=" + std::to_string(std::stoull(hash, nullptr, 16)) +
               " form=" + summary.at("form").text + " nodes=" + std::to_string(summary.at("nodes").unsigned_integer()) +
               " edges=" + std::to_string(summary.at("edges").unsigned_integer()) +
               " regions=" + std::to_string(summary.at("regions").unsigned_integer()) +
               " steps=" + std::to_string(summary.at("steps").unsigned_integer()) +
               " cached=" + bool_text(summary.at("cached").flag) +
               " diagnostics=" + diagnostics_text(summary.at("diagnostics")) + " explain=" + explain_text;
    }

    std::string next(std::uint64_t id) {
        auto found = watches_.find(id);
        if (found == watches_.end()) throw Usage{};
        if (!found->second) return "stream " + std::to_string(id) + " ended";
        for (;;) {
            dd::WatchPoll poll = found->second->next(200);
            if (poll.state == DAEDALUS_CLIENT_WATCH_IDLE) continue;
            if (poll.state == DAEDALUS_CLIENT_WATCH_ENDED) return "stream " + std::to_string(id) + " ended";
            return event(id, parse_json(poll.json));
        }
    }

    static std::string event(std::uint64_t id, const Json& e) {
        const std::string& kind = e.at("event").text;
        std::string head = "event " + std::to_string(id) + " ";
        if (kind == "fault") {
            const Json& node = e.at("node");
            return head + "fault run=" + std::to_string(e.at("run").unsigned_integer()) + " tick=" + std::to_string(e.at("tick").unsigned_integer()) +
                   " step=" + std::to_string(e.at("step").unsigned_integer()) + " node=" + (node.kind == Json::Null ? "none" : quote(node.text)) +
                   " covers=" + list(strings_of(e.at("covers"))) + " consecutive=" + std::to_string(e.at("consecutive").unsigned_integer()) +
                   " reason=" + quote(e.at("reason").text) + " reason_cut=" + bool_text(e.at("reason_cut").flag) +
                   " detail=" + quote(e.at("detail").text) + " detail_cut=" + bool_text(e.at("detail_cut").flag) +
                   " code=" + std::to_string(e.at("code").unsigned_integer()) + " own_code=" + bool_text(e.at("own_code").flag);
        }
        if (kind == "telemetry") {
            return head + "telemetry run=" + std::to_string(e.at("run").unsigned_integer()) + " tick=" + std::to_string(e.at("tick").unsigned_integer()) +
                   " step_calls=" + std::to_string(e.at("step_calls").unsigned_integer()) + " faults=" + std::to_string(e.at("faults").unsigned_integer()) +
                   " skipped=" + std::to_string(e.at("skipped").unsigned_integer());
        }
        if (kind == "lagged") return head + "lagged dropped=" + std::to_string(e.at("dropped").unsigned_integer());
        if (kind == "ended") {
            return head + "ended run=" + std::to_string(e.at("run").unsigned_integer()) + " reason=" + e.at("reason").text;
        }
        return head + "unknown kind=" + std::to_string(e.at("kind").unsigned_integer());
    }

    static std::string reloaded(const Json& report) {
        auto names = [&](const char* field) {
            const Json& part = report.at(field);
            return list(strings_of(part.at("names"))) + "/" + std::to_string(part.at("total").unsigned_integer());
        };
        return "reloaded carried=" + names("carried") + " fresh=" + names("fresh") + " added=" + names("added") +
               " removed=" + names("removed") + " removed_ports=" + names("removed_ports") +
               " dropped_values=" + std::to_string(report.at("dropped_values").unsigned_integer());
    }

    std::string inspect(const std::vector<std::string>& t) {
        if (t[1] == "run" && t.size() == 3) {
            Json info = parse_json(current_->inspect_run(number(t[2])));
            std::vector<std::string> ports;
            for (const Json& port : info.at("ports").items) {
                ports.push_back(port.at("name").text + "|" + (port.at("input").flag ? "input" : "output") + "|" + port.at("type").text);
            }
            return "inspect run run=" + std::to_string(info.at("run").unsigned_integer()) + " session=" + quote(info.at("session").text) +
                   " graph=" + quote(info.at("graph").text) + " state=" + info.at("state").text +
                   " tick=" + std::to_string(info.at("tick").unsigned_integer()) + " faults=" + std::to_string(info.at("faults").unsigned_integer()) +
                   " ports=" + list(ports) + " nodes=" + list(strings_of(info.at("nodes")));
        }
        if (t[1] == "telemetry" && t.size() == 3) {
            Json info = parse_json(current_->inspect_telemetry(number(t[2])));
            std::vector<std::string> rows;
            for (const Json& row : info.at("rows").items) {
                std::string nodes;
                for (std::size_t i = 0; i < row.at("nodes").items.size(); i++) {
                    if (i) nodes += '+';
                    nodes += row.at("nodes").items[i].text;
                }
                rows.push_back(std::to_string(row.at("step").unsigned_integer()) + "|" + nodes + "|" +
                               std::to_string(row.at("calls").unsigned_integer()) + "|" + std::to_string(row.at("failures").unsigned_integer()) + "|" +
                               bool_text(row.at("backing_off").flag) + "|" + bool_text(row.at("disabled").flag) + "|" +
                               bool_text(row.at("auto_off").flag));
            }
            return "inspect telemetry run=" + std::to_string(info.at("run").unsigned_integer()) + " level=" + info.at("level").text +
                   " tick=" + std::to_string(info.at("tick").unsigned_integer()) + " step_calls=" + std::to_string(info.at("step_calls").unsigned_integer()) +
                   " faults=" + std::to_string(info.at("faults").unsigned_integer()) + " skipped=" + std::to_string(info.at("skipped").unsigned_integer()) +
                   " rows=" + list(rows);
        }
        if (t[1] == "graph" && t.size() == 3) return "inspect graph name=" + quote(t[2]) + " json=" + quote(current_->graph_text(t[2]));
        if (t[1] == "cache" && t.size() == 2) {
            Json info = parse_json(current_->inspect_cache());
            return "inspect cache entries=" + std::to_string(info.at("entries").unsigned_integer()) +
                   " hits=" + std::to_string(info.at("hits").unsigned_integer()) + " misses=" + std::to_string(info.at("misses").unsigned_integer());
        }
        if (t[1] == "host" && t.size() == 2) {
            Json info = parse_json(current_->inspect_host());
            std::vector<std::string> runs;
            for (const Json& run : info.at("runs").items) {
                runs.push_back(std::to_string(run.at("run").unsigned_integer()) + "|" + run.at("session").text + "|" + run.at("graph").text + "|" +
                               run.at("state").text);
            }
            return "inspect host sessions=" + list(strings_of(info.at("sessions"))) + " graphs=" + list(strings_of(info.at("graphs"))) +
                   " runs=" + list(runs) + " plugins=" + list(strings_of(info.at("plugins")));
        }
        throw Usage{};
    }

    static std::string trace(const std::string& text) {
        Json entries = parse_json(text);
        std::vector<std::string> items;
        for (const Json& entry : entries.at("entries").items) {
            const Json& error = entry.at("error");
            items.push_back(std::to_string(entry.at("seq").unsigned_integer()) + "|" + entry.at("op").text + "|" + entry.at("target").text + "|" +
                            (error.kind == Json::Null ? "none" : error.text));
        }
        return "trace entries=" + list(items);
    }

    dd::Client root_;
    dd::Client* current_ = nullptr;
    std::deque<std::unique_ptr<dd::Client>> sessions_;
    std::map<std::uint64_t, std::optional<dd::Watch>> watches_;
    std::map<std::string, std::vector<std::uint8_t>> blobs_;
};

}  // namespace

int main(int argc, char** argv) {
    if (argc != 2) {
        std::fprintf(stderr, "usage: driver ENDPOINT\n");
        return 2;
    }
    try {
        Driver driver(argv[1]);
        std::string line;
        while (std::getline(std::cin, line)) {
            std::string out = driver.answer(line);
            std::fwrite(out.data(), 1, out.size(), stdout);
            std::fputc('\n', stdout);
        }
        std::fflush(stdout);
        return 0;
    } catch (const dd::ClientError& error) {
        std::fprintf(stderr, "connect failed: %s\n", error.what());
        return 1;
    }
}
