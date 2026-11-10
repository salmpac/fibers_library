#pragma once

// metrics::Registry: metric families (name, help, type, label names), their
// instruments (one per combination of label values), collector callbacks,
// and the Prometheus text exposition of all of it.
//
//   metrics::Registry registry;                                // + RegistryOptions
//   auto requests = registry.counter_family("app_requests_total", "Requests.", {"channel", "status"});
//   metrics::Counter sms_ok = requests.with({"sms", "ok"});    // once, at startup
//   metrics::Histogram latency = registry.histogram(
//       "app_latency_seconds", "Latency.", metrics::kLatencyBucketsUs, 1e6);  // observe() in µs
//   ...
//   sms_ok.inc();                                              // hot path: no lookup
//   latency.observe(elapsed_us);
//   ...
//   registry.write(exposition);                                // a scrape (any thread / fiber)
//
// Registration (families, with(), collectors) allocates and takes the
// registry's mutex: do it at startup, or at least not per update. with()
// returns the same instrument for the same label values (a linear search:
// label sets are meant to be small and fixed). Registering a family name
// again returns the existing family if type, label names and buckets match;
// anything else that is a programming error fails fast: invalid metric or
// label names (Prometheus rules: [a-zA-Z_:][a-zA-Z0-9_:]* for metrics,
// [a-zA-Z_][a-zA-Z0-9_]* not starting with "__" for labels, no "le" label on
// a histogram), a name registered with another type or labels, a wrong
// number of label values, unsorted bucket bounds.
//
// Scrapes (write()) take no lock and may run concurrently with each other,
// with updates and with registration: families, instruments and collectors
// are append-only lists published with release stores and walked with
// acquire loads. A scrape writes the families in registration order (each
// with its instruments in creation order; families without instruments are
// skipped), then calls the collectors in registration order. A scrape may
// park (its Exposition's sink writes to a socket) without holding anything.
//
// Collectors: Collector::Fn(context, exposition) at every scrape, for
// values that already live elsewhere (see collectors.hpp for the library's
// own). A collector writes whole families (Exposition::family + samples)
// whose names no other family uses. Its context must stay valid as long as
// scrapes can run; with a drop function the registry calls drop(context)
// when it is destroyed.
//
// Lifetime: instruments and families are handles into the registry; they
// stay valid until it is destroyed (no scrape or update may run then).

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <mutex>
#include <new>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <type_traits>
#include <vector>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/metrics/exposition.hpp"
#include "fibers/metrics/instruments.hpp"
#include "fibers/sync/sync.hpp"

namespace fibers::metrics {

struct RegistryOptions {
    // Rows of every counter / histogram (see instruments.hpp): rounded up to
    // a power of two, at most kMaxShards. 0: the number of hardware threads.
    std::size_t shards = 0;
};

// A read-only list argument: a braced list ({"a", "b"}; lives until the end
// of the full expression), or any contiguous range of T (std::vector,
// std::array, std::span).
template <class T>
class ListView {
public:
    constexpr ListView() noexcept = default;
    // A view of the caller's braced list, valid for the call it is passed to
    // (GCC warns that the view does not extend the list's lifetime: it is
    // not meant to).
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Winit-list-lifetime"
#endif
    constexpr ListView(std::initializer_list<T> list) noexcept : data_(list.begin()), size_(list.size()) {}
#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
    template <std::ranges::contiguous_range R>
        requires std::same_as<std::remove_cv_t<std::ranges::range_value_t<R>>, T> &&
                 (!std::same_as<std::remove_cvref_t<R>, ListView>)
    constexpr ListView(const R& range) noexcept  // NOLINT: implicit on purpose
        : data_(std::ranges::data(range)), size_(std::ranges::size(range)) {}

    constexpr const T* begin() const noexcept { return data_; }
    constexpr const T* end() const noexcept { return data_ + size_; }
    constexpr std::size_t size() const noexcept { return size_; }
    constexpr const T& operator[](std::size_t i) const noexcept { return data_[i]; }

private:
    const T* data_ = nullptr;
    std::size_t size_ = 0;
};

using Names = ListView<std::string_view>;
using Bounds = ListView<std::int64_t>;

// Latency buckets in microseconds, for histograms with divisor 1e6
// (seconds): 100 µs .. 10 s, roughly 1-2.5-5 per decade.
inline constexpr std::int64_t kLatencyBucketsUsArray[] = {
    100, 250, 500, 1'000, 2'500, 5'000, 10'000, 25'000, 50'000, 100'000, 250'000, 500'000, 1'000'000,
    2'500'000, 5'000'000, 10'000'000};
inline constexpr std::span<const std::int64_t> kLatencyBucketsUs{kLatencyBucketsUsArray};

// `count` bounds start, start * factor, ... (rounded down to integers;
// duplicates removed, so fewer than `count` if factor is small).
inline std::vector<std::int64_t> exponential_buckets(std::int64_t start, double factor, std::size_t count) {
    std::vector<std::int64_t> bounds;
    double bound = static_cast<double>(start);
    for (std::size_t i = 0; i < count; ++i, bound *= factor) {
        const auto b = static_cast<std::int64_t>(bound);
        if (bounds.empty() || b > bounds.back()) {
            bounds.push_back(b);
        }
    }
    return bounds;
}

// `count` bounds start, start + width, ...
inline std::vector<std::int64_t> linear_buckets(std::int64_t start, std::int64_t width, std::size_t count) {
    std::vector<std::int64_t> bounds;
    for (std::size_t i = 0; i < count; ++i) {
        bounds.push_back(start + static_cast<std::int64_t>(i) * width);
    }
    return bounds;
}

struct Collector {
    using Fn = void (*)(void* context, Exposition& out) noexcept;
    using Drop = void (*)(void* context) noexcept;
};

namespace detail {

template <class Sync>
struct Child {
    typename Sync::template Atomic<Child*> next{nullptr};
    std::vector<std::string> values;
    CellBlock<Sync> cells;
};

template <class Sync>
struct Family {
    typename Sync::template Atomic<Family*> next{nullptr};
    std::string name;
    std::string help;
    MetricType type = MetricType::kUntyped;
    std::vector<std::string> label_names;
    // Histograms: bucket bounds, their "le" texts (bound / divisor, then
    // "+Inf") and the divisor.
    std::vector<std::int64_t> bounds;
    std::vector<std::string> le;
    double divisor = 1;
    typename Sync::template Atomic<Child<Sync>*> children{nullptr};
    Child<Sync>* last = nullptr;  // under the registry mutex
};

template <class Sync>
struct CollectorNode {
    typename Sync::template Atomic<CollectorNode*> next{nullptr};
    Collector::Fn fn = nullptr;
    void* context = nullptr;
    Collector::Drop drop = nullptr;
};

}  // namespace detail

template <sync::SyncPolicy Sync>
class BasicRegistry;

// Family handles: with(label values) -> the instrument for that combination.
template <sync::SyncPolicy Sync = sync::StdSync>
class BasicCounterFamily {
public:
    BasicCounter<Sync> with(Names values) const noexcept {
        return BasicCounter<Sync>(registry_->child(*family_, values)->cells);
    }

private:
    friend class BasicRegistry<Sync>;
    BasicCounterFamily(BasicRegistry<Sync>* registry, detail::Family<Sync>* family) noexcept
        : registry_(registry), family_(family) {}

    BasicRegistry<Sync>* registry_;
    detail::Family<Sync>* family_;
};

template <sync::SyncPolicy Sync = sync::StdSync>
class BasicGaugeFamily {
public:
    BasicGauge<Sync> with(Names values) const noexcept {
        return BasicGauge<Sync>(registry_->child(*family_, values)->cells);
    }

private:
    friend class BasicRegistry<Sync>;
    BasicGaugeFamily(BasicRegistry<Sync>* registry, detail::Family<Sync>* family) noexcept
        : registry_(registry), family_(family) {}

    BasicRegistry<Sync>* registry_;
    detail::Family<Sync>* family_;
};

template <sync::SyncPolicy Sync = sync::StdSync>
class BasicHistogramFamily {
public:
    BasicHistogram<Sync> with(Names values) const noexcept {
        return BasicHistogram<Sync>(registry_->child(*family_, values)->cells, family_->bounds);
    }

private:
    friend class BasicRegistry<Sync>;
    BasicHistogramFamily(BasicRegistry<Sync>* registry, detail::Family<Sync>* family) noexcept
        : registry_(registry), family_(family) {}

    BasicRegistry<Sync>* registry_;
    detail::Family<Sync>* family_;
};

template <sync::SyncPolicy Sync = sync::StdSync>
class BasicRegistry {
public:
    explicit BasicRegistry(RegistryOptions options = {}) noexcept : arena_(normalize_shards(options.shards)) {}

    BasicRegistry(const BasicRegistry&) = delete;
    BasicRegistry& operator=(const BasicRegistry&) = delete;

    ~BasicRegistry() {
        for (detail::Family<Sync>* f = families_.load(std::memory_order_relaxed); f != nullptr;) {
            for (detail::Child<Sync>* c = f->children.load(std::memory_order_relaxed); c != nullptr;) {
                detail::Child<Sync>* next = c->next.load(std::memory_order_relaxed);
                delete c;
                c = next;
            }
            detail::Family<Sync>* next = f->next.load(std::memory_order_relaxed);
            delete f;
            f = next;
        }
        for (detail::CollectorNode<Sync>* n = collectors_.load(std::memory_order_relaxed); n != nullptr;) {
            detail::CollectorNode<Sync>* next = n->next.load(std::memory_order_relaxed);
            if (n->drop != nullptr) {
                n->drop(n->context);
            }
            delete n;
            n = next;
        }
    }

    // ---- registration (allocates; startup) ----------------------------------

    BasicCounterFamily<Sync> counter_family(std::string_view name, std::string_view help,
                                            Names label_names = {}) noexcept {
        return {this, family(name, help, MetricType::kCounter, label_names, {}, 1)};
    }
    // An unlabelled counter.
    BasicCounter<Sync> counter(std::string_view name, std::string_view help) noexcept {
        return counter_family(name, help).with({});
    }

    BasicGaugeFamily<Sync> gauge_family(std::string_view name, std::string_view help,
                                        Names label_names = {}) noexcept {
        return {this, family(name, help, MetricType::kGauge, label_names, {}, 1)};
    }
    BasicGauge<Sync> gauge(std::string_view name, std::string_view help) noexcept {
        return gauge_family(name, help).with({});
    }

    // `bounds`: bucket upper bounds (inclusive), strictly ascending, in the
    // unit of observe(); the exposition shows them and _sum divided by
    // `divisor` (> 0, e.g. 1e6 for microseconds -> seconds).
    BasicHistogramFamily<Sync> histogram_family(std::string_view name, std::string_view help, Names label_names,
                                                Bounds bounds, double divisor = 1) noexcept {
        return {this, family(name, help, MetricType::kHistogram, label_names, bounds, divisor)};
    }
    BasicHistogram<Sync> histogram(std::string_view name, std::string_view help, Bounds bounds,
                                   double divisor = 1) noexcept {
        return histogram_family(name, help, {}, bounds, divisor).with({});
    }

    // Calls fn(context, out) at every scrape, after the families (see the top
    // comment); drop(context), if given, when the registry is destroyed.
    void add_collector(Collector::Fn fn, void* context, Collector::Drop drop = nullptr) noexcept {
        auto* node = new (std::nothrow) detail::CollectorNode<Sync>;
        if (node == nullptr) {
            fail::fail_fast("metrics: out of memory");
        }
        node->fn = fn;
        node->context = context;
        node->drop = drop;
        std::lock_guard guard(mutex_);
        publish(collectors_, last_collector_, node);
    }

    // ---- scraping (any thread or fiber, concurrently) -----------------------

    // Writes every family, then the collectors' output, into `out` (does not
    // flush at the end). Errors: out.error().
    void write(Exposition& out) const noexcept {
        for (const detail::Family<Sync>* f = families_.load(std::memory_order_acquire); f != nullptr;
             f = f->next.load(std::memory_order_acquire)) {
            const detail::Child<Sync>* c = f->children.load(std::memory_order_acquire);
            if (c == nullptr) {
                continue;
            }
            out.family(f->name, f->help, f->type);
            for (; c != nullptr; c = c->next.load(std::memory_order_acquire)) {
                write_child(out, *f, *c);
            }
        }
        for (const detail::CollectorNode<Sync>* n = collectors_.load(std::memory_order_acquire); n != nullptr;
             n = n->next.load(std::memory_order_acquire)) {
            n->fn(n->context, out);
        }
    }

    // The whole exposition as a string (allocates; tests, debugging).
    std::string text() const {
        std::string result;
        auto append = [&result](std::string_view data) noexcept -> int {
            result.append(data);
            return 0;
        };
        char buffer[4096];
        Exposition out(buffer, Sink::ref(append));
        write(out);
        out.flush();
        return result;
    }

    std::size_t shards() const noexcept { return arena_.shards(); }

private:
    friend class BasicCounterFamily<Sync>;
    friend class BasicGaugeFamily<Sync>;
    friend class BasicHistogramFamily<Sync>;

    static std::size_t normalize_shards(std::size_t shards) noexcept {
        if (shards == 0) {
            shards = std::max(1u, std::thread::hardware_concurrency());
        }
        return std::bit_ceil(std::min(shards, kMaxShards));
    }

    // Appends `node` to an append-only list (under mutex_).
    template <class Node>
    static void publish(typename Sync::template Atomic<Node*>& head, Node*& last, Node* node) noexcept {
        if (last == nullptr) {
            head.store(node, std::memory_order_release);
        } else {
            last->next.store(node, std::memory_order_release);
        }
        last = node;
    }

    detail::Family<Sync>* family(std::string_view name, std::string_view help, MetricType type, Names label_names,
                                 Bounds bounds, double divisor) noexcept {
        if (!detail::valid_metric_name(name)) {
            fail::fail_fast("metrics: invalid metric name");
        }
        for (std::size_t i = 0; i < label_names.size(); ++i) {
            if (!detail::valid_label_name(label_names[i]) ||
                (type == MetricType::kHistogram && label_names[i] == "le")) {
                fail::fail_fast("metrics: invalid label name");
            }
            for (std::size_t j = 0; j < i; ++j) {
                if (label_names[i] == label_names[j]) {
                    fail::fail_fast("metrics: duplicate label name");
                }
            }
        }
        for (std::size_t i = 1; i < bounds.size(); ++i) {
            if (bounds[i - 1] >= bounds[i]) {
                fail::fail_fast("metrics: histogram bounds must be strictly ascending");
            }
        }
        if (!(divisor > 0) || !std::isfinite(divisor)) {
            fail::fail_fast("metrics: histogram divisor must be positive and finite");
        }

        std::lock_guard guard(mutex_);
        for (detail::Family<Sync>* f = families_.load(std::memory_order_relaxed); f != nullptr;
             f = f->next.load(std::memory_order_relaxed)) {
            if (f->name != name) {
                continue;
            }
            const bool same = f->type == type &&
                              std::ranges::equal(f->label_names, label_names) &&
                              std::ranges::equal(f->bounds, bounds) && f->divisor == divisor;
            if (!same) {
                fail::fail_fast("metrics: metric registered again with another type, labels or buckets");
            }
            return f;
        }

        auto* f = new (std::nothrow) detail::Family<Sync>;
        if (f == nullptr) {
            fail::fail_fast("metrics: out of memory");
        }
        f->name.assign(name);
        f->help.assign(help);
        f->type = type;
        f->label_names.assign(label_names.begin(), label_names.end());
        if (type == MetricType::kHistogram) {
            f->bounds.assign(bounds.begin(), bounds.end());
            f->divisor = divisor;
            char text[32];
            for (const std::int64_t b : f->bounds) {
                const double scaled = divisor == 1 ? static_cast<double>(b) : static_cast<double>(b) / divisor;
                f->le.emplace_back(text, Value::format_double(text, scaled));
            }
            f->le.emplace_back("+Inf");
        }
        publish(families_, last_family_, f);
        return f;
    }

    detail::Child<Sync>* child(detail::Family<Sync>& f, Names values) noexcept {
        if (values.size() != f.label_names.size()) {
            fail::fail_fast("metrics: wrong number of label values");
        }
        std::lock_guard guard(mutex_);
        for (detail::Child<Sync>* c = f.children.load(std::memory_order_relaxed); c != nullptr;
             c = c->next.load(std::memory_order_relaxed)) {
            if (std::ranges::equal(c->values, values)) {
                return c;
            }
        }
        auto* c = new (std::nothrow) detail::Child<Sync>;
        if (c == nullptr) {
            fail::fail_fast("metrics: out of memory");
        }
        c->values.assign(values.begin(), values.end());
        switch (f.type) {
            case MetricType::kCounter:
                c->cells = arena_.sharded(1);
                break;
            case MetricType::kHistogram:
                c->cells = arena_.sharded(f.bounds.size() + 2);  // buckets, +Inf, sum
                break;
            case MetricType::kGauge:
            case MetricType::kUntyped:
                c->cells = arena_.single(1);
                break;
        }
        publish(f.children, f.last, c);
        return c;
    }

    static void write_labels(Exposition& out, const detail::Family<Sync>& f, const detail::Child<Sync>& c) noexcept {
        for (std::size_t i = 0; i < c.values.size(); ++i) {
            out.label(f.label_names[i], c.values[i], i == 0);
        }
    }

    static void write_child(Exposition& out, const detail::Family<Sync>& f, const detail::Child<Sync>& c) noexcept {
        const bool no_labels = c.values.empty();
        switch (f.type) {
            case MetricType::kCounter:
                out.begin_sample(f.name);
                write_labels(out, f, c);
                out.end_sample(no_labels, c.cells.sum(0));
                return;
            case MetricType::kGauge:
            case MetricType::kUntyped:
                out.begin_sample(f.name);
                write_labels(out, f, c);
                out.end_sample(no_labels,
                               static_cast<std::int64_t>(c.cells.base->load(std::memory_order_relaxed)));
                return;
            case MetricType::kHistogram:
                break;
        }
        std::uint64_t cumulative = 0;
        for (std::size_t i = 0; i < f.le.size(); ++i) {
            cumulative += c.cells.sum(i);
            out.begin_sample(f.name, "_bucket");
            write_labels(out, f, c);
            out.label("le", f.le[i], no_labels);
            out.end_sample(false, cumulative);
        }
        const auto sum = static_cast<std::int64_t>(c.cells.sum(f.bounds.size() + 1));
        out.begin_sample(f.name, "_sum");
        write_labels(out, f, c);
        if (f.divisor == 1) {
            out.end_sample(no_labels, sum);
        } else {
            out.end_sample(no_labels, static_cast<double>(sum) / f.divisor);
        }
        out.begin_sample(f.name, "_count");
        write_labels(out, f, c);
        out.end_sample(no_labels, cumulative);
    }

    typename Sync::Mutex mutex_;  // registration
    detail::CellArena<Sync> arena_;
    typename Sync::template Atomic<detail::Family<Sync>*> families_{nullptr};
    detail::Family<Sync>* last_family_ = nullptr;
    typename Sync::template Atomic<detail::CollectorNode<Sync>*> collectors_{nullptr};
    detail::CollectorNode<Sync>* last_collector_ = nullptr;
};

using Registry = BasicRegistry<>;
using CounterFamily = BasicCounterFamily<>;
using GaugeFamily = BasicGaugeFamily<>;
using HistogramFamily = BasicHistogramFamily<>;

}  // namespace fibers::metrics
