#pragma once

// Built-in collectors: the library's own counters, read at scrape time from
// where they already live (no cost on the hot paths beyond what those parts
// count anyway). Opt-in, one call each:
//
//   metrics::register_scheduler(registry, scheduler);         // everything below
//   metrics::register_thread_pool(registry, scheduler.pool()); // or one part
//
// Every name starts with `prefix` (default "fibers"; a valid metric name of
// at most 64 characters); register several schedulers with different
// prefixes. The objects (taken by non-const reference: the scheduler hands
// out its parts that way) must outlive every scrape of the registry (e.g.
// stop the HTTP server before the scheduler is destroyed). All values are
// racy snapshots of relaxed counters.
//
// ThreadPool
//   <p>_workers                       gauge    worker threads
//   <p>_workers_parked                gauge    workers asleep now
//   <p>_worker_parks_total            counter  times a worker went to sleep
//   <p>_worker_queued_tasks{worker}   gauge    tasks in a worker's main and
//                                              overflow queues (not its mail_box)
// PollerPool (per reactor thread, label poller="i")
//   <p>_pollers                                gauge
//   <p>_poller_sleeps_total                    counter  blocking waits entered
//   <p>_poller_timer_wakeups_total             counter  wakes for an earlier deadline
//   <p>_poller_timers_fired_total              counter
//   <p>_poller_timers_cancelled_total          counter
//   <p>_poller_timer_heap_compactions_total    counter
// BlockingPool (its MPSC op queue has no cheap length, so none is reported)
//   <p>_blocking_threads              gauge
//   <p>_blocking_threads_sleeping     gauge
//   <p>_blocking_sleeps_total         counter
// StackPool
//   <p>_stacks_mapped                 gauge    stacks mapped (in use or free)
//   <p>_stacks_max                    gauge    StackPoolOptions::max_stacks
//   <p>_stack_size_bytes              gauge
// Fibers (sched::BasicScheduler::fiber_counts())
//   <p>_spawned_total                 counter  fiber::go calls
//   <p>_finished_total                counter  fibers completed (or dropped at stop)
//   <p>_alive                         gauge    spawned - finished (never < 0)

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <string>
#include <string_view>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/metrics/exposition.hpp"
#include "fibers/poller/poller.hpp"

namespace fibers::metrics {

inline constexpr std::size_t kMaxCollectorPrefix = 64;

namespace detail {

// prefix + suffix in a stack buffer.
class PrefixedName {
public:
    PrefixedName(std::string_view prefix, std::string_view suffix) noexcept {
        const std::size_t p = std::min(prefix.size(), kMaxCollectorPrefix);
        const std::size_t s = std::min(suffix.size(), sizeof text_ - p);
        std::memcpy(text_, prefix.data(), p);
        std::memcpy(text_ + p, suffix.data(), s);
        size_ = p + s;
    }
    operator std::string_view() const noexcept { return {text_, size_}; }  // NOLINT: implicit on purpose

private:
    char text_[kMaxCollectorPrefix + 64];
    std::size_t size_;
};

// A small decimal label value.
class IndexText {
public:
    explicit IndexText(std::size_t i) noexcept
        : size_(static_cast<std::size_t>(std::to_chars(text_, text_ + sizeof text_, i).ptr - text_)) {}
    operator std::string_view() const noexcept { return {text_, size_}; }  // NOLINT: implicit on purpose

private:
    char text_[24];
    std::size_t size_;
};

inline void gauge(Exposition& out, std::string_view prefix, std::string_view suffix, std::string_view help,
                  Value value) noexcept {
    const PrefixedName name(prefix, suffix);
    out.family(name, help, MetricType::kGauge);
    out.sample(name, value);
}

inline void counter(Exposition& out, std::string_view prefix, std::string_view suffix, std::string_view help,
                    Value value) noexcept {
    const PrefixedName name(prefix, suffix);
    out.family(name, help, MetricType::kCounter);
    out.sample(name, value);
}

template <class T, void (*Write)(T&, std::string_view, Exposition&) noexcept>
struct Source {
    T* object;
    std::string prefix;

    static void collect(void* context, Exposition& out) noexcept {
        const auto* self = static_cast<const Source*>(context);
        Write(*self->object, self->prefix, out);
    }
    static void drop(void* context) noexcept { delete static_cast<Source*>(context); }
};

template <class T, void (*Write)(T&, std::string_view, Exposition&) noexcept, class Registry>
void add_source(Registry& registry, T& object, std::string_view prefix) noexcept {
    if (prefix.size() > kMaxCollectorPrefix || !valid_metric_name(prefix)) {
        fail::fail_fast("metrics: invalid collector prefix");
    }
    using S = Source<T, Write>;
    auto* source = new (std::nothrow) S{&object, std::string(prefix)};
    if (source == nullptr) {
        fail::fail_fast("metrics: out of memory");
    }
    registry.add_collector(&S::collect, source, &S::drop);
}

}  // namespace detail

// ---- writers (usable from a collector of your own) --------------------------

template <class Pool>
void write_thread_pool(Pool& pool, std::string_view prefix, Exposition& out) noexcept {
    detail::gauge(out, prefix, "_workers", "Worker threads of the thread pool.", pool.num_workers());
    detail::gauge(out, prefix, "_workers_parked", "Workers asleep (parked) now.", pool.parked_workers());
    detail::counter(out, prefix, "_worker_parks_total", "Times a worker went to sleep.", pool.park_count());
    const detail::PrefixedName queued(prefix, "_worker_queued_tasks");
    out.family(queued, "Tasks queued for a worker (main and overflow queues; approximate).", MetricType::kGauge);
    for (std::size_t i = 0; i < pool.num_workers(); ++i) {
        const detail::IndexText worker(i);
        out.sample(queued, {{"worker", worker}}, pool.queued_tasks_approx(i));
    }
}

template <class PollerPool>
void write_poller_pool(PollerPool& pollers, std::string_view prefix, Exposition& out) noexcept {
    detail::gauge(out, prefix, "_pollers", "Reactor (poller) threads.", pollers.size());
    struct Field {
        std::string_view suffix;
        std::string_view help;
        std::uint64_t poller::PollerStats::*member;
    };
    static constexpr Field kFields[] = {
        {"_poller_sleeps_total", "Blocking waits entered by a poller.", &poller::PollerStats::sleeps},
        {"_poller_timer_wakeups_total", "Poller wake-ups for an earlier timer deadline.",
         &poller::PollerStats::timer_wakeups},
        {"_poller_timers_fired_total", "Timers fired.", &poller::PollerStats::timers_fired},
        {"_poller_timers_cancelled_total", "Timers cancelled.", &poller::PollerStats::timers_cancelled},
        {"_poller_timer_heap_compactions_total", "Timer heap compactions (cancelled timers purged).",
         &poller::PollerStats::compactions},
    };
    // One stats() snapshot per poller and family.
    for (const Field& field : kFields) {
        const detail::PrefixedName name(prefix, field.suffix);
        out.family(name, field.help, MetricType::kCounter);
        for (std::size_t i = 0; i < pollers.size(); ++i) {
            const detail::IndexText index(i);
            out.sample(name, {{"poller", index}}, pollers.at(i).stats().*field.member);
        }
    }
}

template <class BlockingPool>
void write_blocking_pool(BlockingPool& pool, std::string_view prefix, Exposition& out) noexcept {
    detail::gauge(out, prefix, "_blocking_threads", "Threads of the blocking-call pool.", pool.num_threads());
    detail::gauge(out, prefix, "_blocking_threads_sleeping", "Blocking-pool threads asleep now.",
                  pool.sleeping_threads());
    detail::counter(out, prefix, "_blocking_sleeps_total", "Times a blocking-pool thread went to sleep.",
                    pool.sleeps());
}

template <class StackPool>
void write_stack_pool(StackPool& stacks, std::string_view prefix, Exposition& out) noexcept {
    detail::gauge(out, prefix, "_stacks_mapped", "Fiber stacks mapped (in use or free).", stacks.mapped_stacks());
    detail::gauge(out, prefix, "_stacks_max", "Fiber stacks mapped at most.", stacks.max_stacks());
    detail::gauge(out, prefix, "_stack_size_bytes", "Usable bytes per fiber stack.", stacks.stack_size());
}

template <class Scheduler>
void write_fiber_counts(Scheduler& scheduler, std::string_view prefix, Exposition& out) noexcept {
    const auto counts = scheduler.fiber_counts();
    detail::counter(out, prefix, "_spawned_total", "Fibers spawned.", counts.spawned);
    detail::counter(out, prefix, "_finished_total", "Fibers that completed or were dropped at stop.",
                    counts.finished);
    detail::gauge(out, prefix, "_alive", "Fibers spawned and not finished (approximate).",
                  counts.spawned > counts.finished ? counts.spawned - counts.finished : 0);
}

template <class Scheduler>
void write_scheduler(Scheduler& scheduler, std::string_view prefix, Exposition& out) noexcept {
    write_fiber_counts(scheduler, prefix, out);
    write_thread_pool(scheduler.pool(), prefix, out);
    write_poller_pool(scheduler.pollers(), prefix, out);
    write_blocking_pool(scheduler.blocking(), prefix, out);
    write_stack_pool(scheduler.stacks(), prefix, out);
}

// ---- registration -----------------------------------------------------------

template <class Registry, class Pool>
void register_thread_pool(Registry& registry, Pool& pool, std::string_view prefix = "fibers") noexcept {
    detail::add_source<Pool, &write_thread_pool<Pool>>(registry, pool, prefix);
}

template <class Registry, class PollerPool>
void register_poller_pool(Registry& registry, PollerPool& pollers,
                          std::string_view prefix = "fibers") noexcept {
    detail::add_source<PollerPool, &write_poller_pool<PollerPool>>(registry, pollers, prefix);
}

template <class Registry, class BlockingPool>
void register_blocking_pool(Registry& registry, BlockingPool& pool,
                            std::string_view prefix = "fibers") noexcept {
    detail::add_source<BlockingPool, &write_blocking_pool<BlockingPool>>(registry, pool, prefix);
}

template <class Registry, class StackPool>
void register_stack_pool(Registry& registry, StackPool& stacks, std::string_view prefix = "fibers") noexcept {
    detail::add_source<StackPool, &write_stack_pool<StackPool>>(registry, stacks, prefix);
}

// Fiber counts, thread pool, pollers, blocking pool and stacks of a
// sched::BasicScheduler, as one collector.
template <class Registry, class Scheduler>
void register_scheduler(Registry& registry, Scheduler& scheduler, std::string_view prefix = "fibers") noexcept {
    detail::add_source<Scheduler, &write_scheduler<Scheduler>>(registry, scheduler, prefix);
}

}  // namespace fibers::metrics
