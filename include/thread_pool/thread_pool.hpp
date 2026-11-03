#pragma once

#include <algorithm>
#include <cassert>
#include <concepts>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "thread_pool/locked_spsc_queue.hpp"
#include "thread_pool/sharded_queue.hpp"
#include "thread_pool/slow_queue.hpp"
#include "thread_pool/sync.hpp"
#include "thread_pool/task.hpp"

namespace thread_pool {

struct ThreadPoolOptions {
    std::size_t num_workers = 0;                // 0 => hardware_concurrency
    std::size_t main_box_capacity = 256;
    std::size_t mail_box_shards = 4;
    std::size_t mail_box_shard_capacity = 256;
    std::size_t batch_size = 32;                // max tasks taken from a queue at once
    std::size_t mail_box_check_interval = 61;   // executed tasks between mail_box checks
    // Upper bound (in Sync::spin_hint calls) of the adaptive pause an idle
    // worker takes between steal attempts. It doubles after a failed or
    // meagre steal and halves after a full batch. 0 disables the backoff.
    std::size_t max_steal_backoff = 256;
};

// Interface of a worker's main_box. The owning worker is its only producer;
// try_pop_batch is called by the owner, try_steal_batch by any thread.
template <class Q>
concept MainQueue = std::constructible_from<Q, std::size_t> &&
    requires(Q q, Task task, Task* buf, std::size_t n) {
        { q.try_push(std::move(task)) } noexcept -> std::same_as<bool>;
        { q.try_push_batch(buf, n) } noexcept -> std::same_as<std::size_t>;
        { q.free_space() } noexcept -> std::same_as<std::size_t>;
        { q.try_pop_batch(buf, n) } noexcept -> std::same_as<std::size_t>;
        { q.try_steal_batch(buf, n) } noexcept -> std::same_as<std::size_t>;
    };

namespace detail {

// All per-thread state of the pool, obtained via Sync::thread_local_instance.
struct ThreadState {
    const void* pool = nullptr;   // pool whose worker this thread is
    std::size_t worker = 0;       // worker index within `pool`
    std::uint64_t rng = 0;        // xorshift64 state; 0 = not seeded yet
    std::size_t next_worker = 0;  // round-robin cursor for external submits
    std::size_t shard_hint = 0;   // fixed home shard in every mail_box
};

inline std::uint64_t xorshift(std::uint64_t& x) noexcept {
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    return x;
}

// The only way the library touches its thread-local state.
//
// Fibers may migrate between workers across a context switch, while the
// compiler is allowed to compute the address of a thread_local once per
// function (the standard says the thread cannot change mid-function) and keep
// it across the opaque switch call. So this accessor is never inlined, and the
// empty asm with a memory clobber keeps it from being inferred pure/const
// (which would let calls be merged again). Cost: one call per submit.
template <class Sync>
[[gnu::noinline]] ThreadState& thread_state() noexcept {
    asm volatile("" ::: "memory");
    ThreadState& state = Sync::template thread_local_instance<ThreadState>();
    if (state.rng == 0) [[unlikely]] {
        // splitmix64 of the policy's entropy (under Twist: its controlled
        // randomness, so explored runs differ in round-robin starts and home
        // shards, not only in interleavings).
        std::uint64_t z = static_cast<std::uint64_t>(Sync::random_seed()) + 0x9E3779B97F4A7C15ull;
        z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ull;
        z = (z ^ (z >> 27)) * 0x94D049BB133111EBull;
        z ^= z >> 31;
        state.rng = z != 0 ? z : 1;
        state.next_worker = static_cast<std::size_t>(xorshift(state.rng));
        state.shard_hint = static_cast<std::size_t>(xorshift(state.rng));
    }
    return state;
}

}  // namespace detail

template <SyncPolicy Sync = StdSync, MainQueue Q = LockedSPSCQueue<Task, Sync>>
class ThreadPool {
public:
    explicit ThreadPool(ThreadPoolOptions options = {}) : options_(normalize(options)) {
        workers_.reserve(options_.num_workers);
        for (std::size_t i = 0; i < options_.num_workers; ++i) {
            workers_.push_back(std::make_unique<Worker>(options_));
        }
        threads_.reserve(options_.num_workers);
        try {
            for (std::size_t i = 0; i < options_.num_workers; ++i) {
                threads_.emplace_back([this, i] { worker_loop(i); });
            }
        } catch (...) {
            stop();
            throw;
        }
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // Stops workers; tasks still queued are dropped (delete_ptr).
    ~ThreadPool() { stop(); }

    // From a worker of this pool: into its own main_box.
    // From any other thread: round-robin into some worker's mail_box.
    // Falls back to the target worker's overflow_box when queues are full.
    void submit(Task task) {
        if (!task) {
            return;
        }
        detail::ThreadState& state = detail::thread_state<Sync>();
        // try_push moves from `task` only on success, so it is still valid
        // for the overflow fallback.
        if (state.pool == this) {
            Worker& self = *workers_[state.worker];
            if (!self.main_box.try_push(std::move(task))) {
                self.overflow_box.push(std::move(task));
            }
            return;
        }
        Worker& target = *workers_[state.next_worker++ % workers_.size()];
        if (!target.mail_box.try_push(std::move(task), state.shard_hint)) {
            target.overflow_box.push(std::move(task));
        }
    }

    template <class F>
        requires(!std::same_as<std::decay_t<F>, Task>) && std::invocable<std::decay_t<F>&>
    void submit(F&& f) {
        submit(Task::make(std::forward<F>(f)));
    }

    // Submits tasks[0..count), moving from them. Precondition: none is empty.
    // From a worker of this pool: one batched push into its own main_box.
    // From any other thread: chunks of batch_size go round-robin to the
    // workers' mail_boxes, one shard lock and one publication per chunk.
    // Whatever does not fit goes to the target's overflow_box under one lock.
    void submit_batch(Task* tasks, std::size_t count) {
        assert(std::all_of(tasks, tasks + count, [](const Task& t) { return static_cast<bool>(t); }));
        if (count == 0) {
            return;
        }
        detail::ThreadState& state = detail::thread_state<Sync>();
        if (state.pool == this) {
            Worker& self = *workers_[state.worker];
            const std::size_t pushed = self.main_box.try_push_batch(tasks, count);
            self.overflow_box.push_batch(tasks + pushed, count - pushed);
            return;
        }
        for (std::size_t offset = 0; offset < count; offset += options_.batch_size) {
            Task* chunk = tasks + offset;
            const std::size_t size = std::min(options_.batch_size, count - offset);
            Worker& target = *workers_[state.next_worker++ % workers_.size()];
            const std::size_t pushed = target.mail_box.try_push_batch(chunk, size, state.shard_hint);
            target.overflow_box.push_batch(chunk + pushed, size - pushed);
        }
    }

    void submit_batch(std::span<Task> tasks) { submit_batch(tasks.data(), tasks.size()); }

    // Requests stop and joins workers. Each worker exits instead of running its
    // next task. Pending tasks stay queued and are dropped on destruction.
    // Idempotent. Must not be called from a worker of this pool.
    void stop() {
        assert(!in_worker_thread() && "ThreadPool::stop() called from its own worker");
        stop_requested_.store(true, std::memory_order_release);
        for (auto& thread : threads_) {
            thread.join();
        }
        threads_.clear();
    }

    std::size_t num_workers() const noexcept { return workers_.size(); }

    bool in_worker_thread() const noexcept { return detail::thread_state<Sync>().pool == this; }

    const ThreadPoolOptions& options() const noexcept { return options_; }

private:
    struct alignas(kCacheLineSize) Worker {
        explicit Worker(const ThreadPoolOptions& options)
            : main_box(options.main_box_capacity),
              mail_box(options.mail_box_shards, options.mail_box_shard_capacity) {}

        Q main_box;
        ShardedQueue<Task, Sync> mail_box;
        SlowQueue<Task, Sync> overflow_box;
    };

    static ThreadPoolOptions normalize(ThreadPoolOptions options) {
        if (options.num_workers == 0) {
            options.num_workers = std::max(1u, std::thread::hardware_concurrency());
        }
        options.main_box_capacity = std::max<std::size_t>(options.main_box_capacity, 1);
        options.mail_box_shards = std::max<std::size_t>(options.mail_box_shards, 1);
        options.mail_box_shard_capacity = std::max<std::size_t>(options.mail_box_shard_capacity, 1);
        options.batch_size = std::max<std::size_t>(options.batch_size, 1);
        options.mail_box_check_interval = std::max<std::size_t>(options.mail_box_check_interval, 1);
        return options;
    }

    bool stopping() const noexcept { return stop_requested_.load(std::memory_order_acquire); }

    void worker_loop(std::size_t index) {
        detail::ThreadState& state = detail::thread_state<Sync>();
        state.pool = this;
        state.worker = index;

        Worker& self = *workers_[index];
        const std::size_t batch_size = options_.batch_size;
        const auto buffer = std::make_unique<Task[]>(batch_size);
        Task* const batch = buffer.get();
        std::size_t mail_cursor = 0;
        std::size_t since_mail_check = 0;
        std::size_t steal_backoff = 0;

        while (!stopping()) {
            if (since_mail_check >= options_.mail_box_check_interval) {
                since_mail_check = 0;
                pull_external(self, batch, mail_cursor);
            }

            std::size_t n = self.main_box.try_pop_batch(batch, batch_size);
            if (n == 0) {
                since_mail_check = 0;
                pull_external(self, batch, mail_cursor);
                n = self.main_box.try_pop_batch(batch, batch_size);
                if (n == 0) {
                    // Pause before hitting the neighbours' queues again, so
                    // idle thieves do not keep bouncing a busy producer's
                    // cache lines.
                    for (std::size_t i = 0; i < steal_backoff; ++i) {
                        Sync::spin_hint();
                    }
                    const StealResult result = steal(index, state, batch);
                    steal_backoff = next_steal_backoff(steal_backoff, result);
                    if (result.stolen == 0) {
                        Sync::spin_hint();
                    }
                    continue;
                }
            }

            for (std::size_t i = 0; i < n; ++i) {
                if (stopping()) {
                    break;  // the rest of the batch is dropped with `buffer`
                }
                std::move(batch[i]).run();
            }
            since_mail_check += n;
        }

        state.pool = nullptr;
    }

    // Moves up to batch_size tasks from own overflow_box / mail_box into
    // own main_box. `scratch` must hold batch_size tasks and be empty.
    void pull_external(Worker& self, Task* scratch, std::size_t& mail_cursor) {
        const std::size_t room = std::min(options_.batch_size, self.main_box.free_space());
        if (room == 0) {
            return;
        }
        std::size_t n = 0;
        const bool overflow = self.overflow_box.maybe_nonempty();
        if (overflow) {
            // Give overflow a guaranteed share so it cannot starve behind mail.
            n += self.overflow_box.try_pop_batch(scratch, std::max<std::size_t>(room / 2, 1));
        }
        n += self.mail_box.try_pop_batch(scratch + n, room - n, mail_cursor++);
        if (overflow && n < room) {
            n += self.overflow_box.try_pop_batch(scratch + n, room - n);
        }
        commit(self, scratch, n);
    }

    struct StealResult {
        std::size_t stolen = 0;
        std::size_t room = 0;  // how many we were ready to take
    };

    // AIMD-ish: nothing or crumbs -> wait longer, full batch -> wait less.
    std::size_t next_steal_backoff(std::size_t backoff, StealResult r) const noexcept {
        const std::size_t max = options_.max_steal_backoff;
        if (max == 0) {
            return 0;
        }
        if (r.room != 0 && r.stolen >= r.room) {
            return backoff / 2;
        }
        if (r.stolen * 2 >= r.room && r.stolen != 0) {
            return backoff;
        }
        return std::min(max, std::max<std::size_t>(backoff * 2, 1));
    }

    // Steals a batch of up to batch_size tasks into own main_box: neighbours'
    // mail_boxes first, then their main_boxes, then their overflow_boxes.
    StealResult steal(std::size_t self_index, detail::ThreadState& state, Task* scratch) {
        const std::size_t count = workers_.size();
        Worker& self = *workers_[self_index];
        const std::size_t room = std::min(options_.batch_size, self.main_box.free_space());
        if (count == 1 || room == 0) {
            return {0, room};
        }
        const std::size_t start = static_cast<std::size_t>(detail::xorshift(state.rng));

        auto try_each = [&](auto&& take) -> std::size_t {
            for (std::size_t i = 0; i < count; ++i) {
                const std::size_t victim = (start + i) % count;
                if (victim == self_index) {
                    continue;
                }
                if (const std::size_t n = take(*workers_[victim]); n != 0) {
                    commit(self, scratch, n);
                    return n;
                }
            }
            return 0;
        };

        std::size_t stolen =
            try_each([&](Worker& v) { return v.mail_box.try_steal_batch(scratch, room, start); });
        if (stolen == 0) {
            stolen = try_each([&](Worker& v) { return v.main_box.try_steal_batch(scratch, room); });
        }
        if (stolen == 0) {
            stolen = try_each([&](Worker& v) { return v.overflow_box.try_steal_batch(scratch, room); });
        }
        return {stolen, room};
    }

    // Pushes scratch[0..n) into own main_box. Fits by construction: the owner
    // is the only producer and `n` never exceeds the free_space() it measured.
    static void commit(Worker& self, Task* scratch, std::size_t n) noexcept {
        if (n == 0) {
            return;
        }
        [[maybe_unused]] const std::size_t pushed = self.main_box.try_push_batch(scratch, n);
        assert(pushed == n);
    }

    ThreadPoolOptions options_;
    std::vector<std::unique_ptr<Worker>> workers_;
    std::vector<typename Sync::Thread> threads_;
    typename Sync::template Atomic<bool> stop_requested_{false};
};

}  // namespace thread_pool
