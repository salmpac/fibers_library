#pragma once

// metrics::Exposition: a writer of the Prometheus text exposition format,
// version 0.0.4, into a caller-provided buffer that is flushed into a Sink
// whenever it fills up (so a scrape of any size needs a bounded amount of
// memory: e.g. a few KiB on a fiber stack, flushed as HTTP chunks).
//
//   char buffer[4096];
//   metrics::Exposition out(buffer, metrics::Sink::ref(write_to_socket));
//   registry.write(out);   // or, in a collector:
//   out.family("app_queue_depth", "Items waiting.", metrics::MetricType::kGauge);
//   out.sample("app_queue_depth", {{"queue", "sms"}}, depth);
//   out.flush();
//
// Format (https://prometheus.io/docs/instrumenting/exposition_formats/):
//   # HELP <name> <help, '\' and newline escaped>
//   # TYPE <name> counter|gauge|histogram|untyped
//   <name>{<label>="<value, '\', '"' and newline escaped>",...} <value>
// Values: integers as integers, doubles with the fewest digits that read
// back exactly (fixed notation, scientific below 1e-4 and from 1e16 on),
// "+Inf", "-Inf", "NaN".
//
// The writer checks nothing about names (the Registry validates what it
// registers; collectors are trusted to pass valid names and to write each
// family's samples right after its family() line, with the families'
// names distinct).
//
// Errors: the first error is sticky and later output is dropped: the
// sink's errno, or EMSGSIZE when the buffer fills up without a sink (or is
// empty).

#include <cerrno>
#include <charconv>
#include <cmath>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <span>
#include <string_view>
#include <type_traits>

namespace fibers::metrics {

enum class MetricType : std::uint8_t { kCounter, kGauge, kHistogram, kUntyped };

inline constexpr std::string_view type_name(MetricType type) noexcept {
    switch (type) {
        case MetricType::kCounter:
            return "counter";
        case MetricType::kGauge:
            return "gauge";
        case MetricType::kHistogram:
            return "histogram";
        case MetricType::kUntyped:
            break;
    }
    return "untyped";
}

namespace detail {

// Prometheus name rules: metric names [a-zA-Z_:][a-zA-Z0-9_:]*, label names
// [a-zA-Z_][a-zA-Z0-9_]* not starting with "__" (reserved).
inline bool valid_metric_name(std::string_view name) noexcept {
    if (name.empty()) {
        return false;
    }
    for (std::size_t i = 0; i < name.size(); ++i) {
        const char c = name[i];
        const bool alpha = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' || c == ':';
        if (!alpha && !(i > 0 && c >= '0' && c <= '9')) {
            return false;
        }
    }
    return true;
}

inline bool valid_label_name(std::string_view name) noexcept {
    if (name.empty() || name.starts_with("__")) {
        return false;
    }
    for (std::size_t i = 0; i < name.size(); ++i) {
        const char c = name[i];
        const bool alpha = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
        if (!alpha && !(i > 0 && c >= '0' && c <= '9')) {
            return false;
        }
    }
    return true;
}

}  // namespace detail

// One label of a sample written by a collector.
struct Label {
    std::string_view name;
    std::string_view value;
};

// A sample value: unsigned and signed integers are printed exactly, floating
// point values in their shortest round-trip form.
class Value {
public:
    template <class T>
        requires std::is_arithmetic_v<T> && (!std::same_as<T, bool>)
    constexpr Value(T v) noexcept {  // NOLINT: implicit on purpose
        if constexpr (std::is_floating_point_v<T>) {
            kind_ = Kind::kDouble;
            d_ = static_cast<double>(v);
        } else if constexpr (std::is_signed_v<T>) {
            kind_ = Kind::kSigned;
            i_ = static_cast<std::int64_t>(v);
        } else {
            kind_ = Kind::kUnsigned;
            u_ = static_cast<std::uint64_t>(v);
        }
    }

    // Writes the value into out[0..32); returns the end.
    char* format(char* out) const noexcept {
        switch (kind_) {
            case Kind::kUnsigned:
                return std::to_chars(out, out + 32, u_).ptr;
            case Kind::kSigned:
                return std::to_chars(out, out + 32, i_).ptr;
            case Kind::kDouble:
                break;
        }
        return format_double(out, d_);
    }

    static char* format_double(char* out, double d) noexcept {
        if (std::isnan(d)) {
            std::memcpy(out, "NaN", 3);
            return out + 3;
        }
        if (std::isinf(d)) {
            std::memcpy(out, d > 0 ? "+Inf" : "-Inf", 4);
            return out + 4;
        }
        // Shortest round-trip digits; fixed notation unless the value is
        // tiny or huge (0.0001 rather than 1e-04).
        const double magnitude = std::fabs(d);
        if (magnitude != 0 && (magnitude < 1e-4 || magnitude >= 1e16)) {
            return std::to_chars(out, out + 32, d, std::chars_format::scientific).ptr;
        }
        return std::to_chars(out, out + 32, d, std::chars_format::fixed).ptr;
    }

private:
    enum class Kind : std::uint8_t { kUnsigned, kSigned, kDouble };
    Kind kind_ = Kind::kUnsigned;
    union {
        std::uint64_t u_ = 0;
        std::int64_t i_;
        double d_;
    };
};

// Where an Exposition's buffer goes when it fills up (and on flush()):
// { void* context, int (*fn)(void*, std::string_view) noexcept } returning 0
// or an errno. An empty Sink: output must fit into the buffer.
class Sink {
public:
    using Fn = int (*)(void* context, std::string_view data) noexcept;

    constexpr Sink() noexcept = default;
    constexpr Sink(void* context, Fn fn) noexcept : context_(context), fn_(fn) {}

    // Refers to `f` (not copied): `f` must outlive the Exposition.
    template <class F>
        requires std::is_nothrow_invocable_r_v<int, F&, std::string_view>
    static Sink ref(F& f) noexcept {
        return Sink(static_cast<void*>(std::addressof(f)),
                    [](void* c, std::string_view data) noexcept -> int { return (*static_cast<F*>(c))(data); });
    }

    int operator()(std::string_view data) const noexcept { return fn_(context_, data); }
    explicit operator bool() const noexcept { return fn_ != nullptr; }

private:
    void* context_ = nullptr;
    Fn fn_ = nullptr;
};

class Exposition {
public:
    // `buffer` must outlive the writer (an empty one: every write fails
    // with EMSGSIZE).
    Exposition(std::span<char> buffer, Sink sink = {}) noexcept
        : buffer_(buffer.data()), capacity_(buffer.size()), sink_(sink) {}

    Exposition(const Exposition&) = delete;
    Exposition& operator=(const Exposition&) = delete;

    // "# HELP" and "# TYPE" lines of a metric family.
    void family(std::string_view name, std::string_view help, MetricType type) noexcept {
        put("# HELP ");
        put(name);
        put(' ');
        put_escaped(help, false);
        put("\n# TYPE ");
        put(name);
        put(' ');
        put(type_name(type));
        put('\n');
    }

    // One sample line.
    void sample(std::string_view name, Value value) noexcept { sample(name, std::span<const Label>{}, value); }
    void sample(std::string_view name, std::initializer_list<Label> labels, Value value) noexcept {
        sample(name, std::span<const Label>(labels.begin(), labels.size()), value);
    }
    void sample(std::string_view name, std::span<const Label> labels, Value value) noexcept {
        put(name);
        bool first = true;
        for (const Label& l : labels) {
            label(l.name, l.value, first);
            first = false;
        }
        end_sample(first, value);
    }

    // ---- building blocks (the Registry writes its families with these) ----

    // `name` + `suffix`, then labels one by one, then end_sample().
    void begin_sample(std::string_view name, std::string_view suffix = {}) noexcept {
        put(name);
        put(suffix);
    }
    // `first`: whether this is the sample's first label.
    void label(std::string_view name, std::string_view value, bool first) noexcept {
        put(first ? '{' : ',');
        put(name);
        put("=\"");
        put_escaped(value, true);
        put('"');
    }
    // `no_labels`: label() was not called for this sample.
    void end_sample(bool no_labels, Value value) noexcept {
        if (!no_labels) {
            put('}');
        }
        put(' ');
        char text[32];
        put(std::string_view(text, static_cast<std::size_t>(value.format(text) - text)));
        put('\n');
    }

    // ---- output -------------------------------------------------------------

    // Hands what is buffered to the sink (if there is one). Returns error().
    int flush() noexcept {
        if (error_ == 0 && size_ != 0 && sink_) {
            error_ = sink_(std::string_view(buffer_, size_));
            flushed_ = true;
            size_ = 0;
        }
        return error_;
    }

    // What is buffered and not yet flushed.
    std::string_view buffered() const noexcept { return {buffer_, size_}; }

    // Whether the sink was called at least once.
    bool flushed() const noexcept { return flushed_; }

    // 0, the sink's first error, or EMSGSIZE (no sink, buffer full).
    int error() const noexcept { return error_; }

    // Raw text (no escaping).
    void put(std::string_view text) noexcept {
        while (!text.empty() && error_ == 0) {
            if (size_ == capacity_) {
                make_room();
                continue;
            }
            const std::size_t n = std::min(text.size(), capacity_ - size_);
            std::memcpy(buffer_ + size_, text.data(), n);
            size_ += n;
            text.remove_prefix(n);
        }
    }

    void put(char c) noexcept { put(std::string_view(&c, 1)); }

private:
    // HELP text: '\' -> "\\", newline -> "\n"; label values also '"' -> "\"".
    void put_escaped(std::string_view text, bool quote) noexcept {
        std::size_t start = 0;
        for (std::size_t i = 0; i < text.size(); ++i) {
            const char c = text[i];
            const char* escape = c == '\\' ? "\\\\" : c == '\n' ? "\\n" : (quote && c == '"') ? "\\\"" : nullptr;
            if (escape != nullptr) {
                put(text.substr(start, i - start));
                put(std::string_view(escape, 2));
                start = i + 1;
            }
        }
        put(text.substr(start));
    }

    void make_room() noexcept {
        if (!sink_ || capacity_ == 0) {
            error_ = EMSGSIZE;
            return;
        }
        static_cast<void>(flush());
    }

    char* buffer_;
    std::size_t capacity_;
    std::size_t size_ = 0;
    Sink sink_;
    int error_ = 0;
    bool flushed_ = false;
};

}  // namespace fibers::metrics
