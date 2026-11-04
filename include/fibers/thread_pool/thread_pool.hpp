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

#include "fibers/queue/lock_free_spmc_queue.hpp"
#include "fibers/queue/locked_spsc_queue.hpp"
#include "fibers/queue/sharded_queue.hpp"
#include "fibers/queue/slow_queue.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/thread_pool/task.hpp"

namespace fibers::thread_pool {

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
    // An idle worker (searching or polling) parks after about this many
    // Sync::spin_hint calls without finding work, the pauses of its backoff
    // included and at least one per idle round: then it sleeps until a
    // producer or another worker wakes it. A budget in pauses rather than in
    // rounds, so that max_steal_backoff = 0 does not mean parking at once.
    // The default is about 40 rounds at max_steal_backoff = 256. Minimum 1.
    std::size_t idle_spins_before_park = 8192;
    // Overload wake: a worker that submits into its own main_box wakes a
    // parked worker (to come and steal) when, after the push, its main_box
    // holds at least this many tasks or the task went to its overflow_box,
    // some worker is parked and none is searching. Minimum 1.
    std::size_t overload_wake_threshold = 2;
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
        { q.size_approx() } noexcept -> std::same_as<std::size_t>;  // any thread
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

// Worker parking
// ==============
//
// An idle worker first *searches* (steals with the adaptive backoff above); a
// worker that finds nothing for idle_spins_before_park pauses *parks*: it
// sleeps in Sync::Atomic<uint32_t>::wait until someone wakes it.
//
// Per-worker state word (Worker::state): RUNNING -> PARKING -> PARKED.
// Only the owner moves RUNNING -> PARKING -> PARKED; anybody (producer, waker,
// stop()) moves PARKING / PARKED -> RUNNING with an exchange, and the one whose
// exchange returns PARKED calls notify_one. The owner's CAS PARKING -> PARKED
// fails if somebody got there first, so a worker that is still PARKING is never
// waited for: it simply does not go to sleep (this also covers a 1-worker pool).
//
// Global counters, each on its own cache line:
//   parked_     workers between "about to park" and "back to RUNNING" (an
//               over-estimate while they wake up, never an under-estimate of
//               the PARKING / PARKED ones). Written only on park / wake, so
//               the busy hot path can read it for free.
//   searchers_  [ grant : 1 | count : 63 ]: workers stealing with nothing of
//               their own, plus one unit for an outstanding grant. The grant
//               is a wake "come and search" in flight (see wake_searcher):
//               the waker adds a searcher unit on behalf of the worker it
//               wakes; the first worker that leaves PARKING / PARKED takes it.
//
// Searchers are limited, parkers are not: a worker that runs out of work may
// steal only while searchers < max(1, (workers - parked) / 2). Otherwise it
// does not steal; it only polls its own boxes (no cross-worker traffic) and
// asks again, for at most idle_spins_before_park pauses, then parks.
// When a searcher finds work and was the last one, it wakes one parked worker
// to search in its place (Go's rule: no thundering herd, but a newly found
// burst spreads, one worker at a time). The last searcher to give up, and any
// worker about to park while nobody searches, first looks at the other
// workers' queues and stays awake if it sees work (Go's "delicate dance").
//
// Memory orders: the lost-wakeup argument (Dekker)
// -------------------------------------------------
//   producer (P)                        parking worker (W)
//   push task into W's mail / overflow  state.store(PARKING)
//   fence(seq_cst)              (F1)    parked_ += 1      (RMW)
//   if state.load() != RUNNING          fence(seq_cst)                  (F2)
//     && state.exchange(RUNNING)        re-check own mail_box, overflow_box
//            == PARKED                  and the stop flag: work -> RUNNING
//       notify_one                      else CAS PARKING -> PARKED, wait
//
// F1 and F2 are both seq_cst fences, so one precedes the other in the single
// total order S. If F1 precedes F2, W's re-check (after F2) sees the push
// (before F1). If F2 precedes F1, P's state load (after F1) sees PARKING (or
// later). So at least one side sees the other: either W does not park, or P
// sees PARKING / PARKED and its exchange (an RMW, totally ordered with W's CAS
// on the same word) either makes W's CAS fail or returns PARKED and notifies.
// The exchange must be a single RMW: a load-then-store could read PARKING,
// let W's CAS succeed, then overwrite PARKED without notifying.
// (A seq_cst store of PARKING alone would not do: formally a later relaxed /
// acquire load may still be satisfied before it; hence the fence F2.)
// stop() is the same pattern with the stop flag in place of the push: a
// seq_cst store followed by an exchange on every worker's state.
//
// The same Dekker pairs the last searcher giving up (RMW searchers_ -= 1,
// fence(seq_cst), re-check the other queues: work -> keep searching), or a
// worker parking while nobody searches (parked_ += 1, F2, same re-check), with
// an external producer's "anyone parked / searching?" loads after F1.
// A local submit makes the same loads without a fence (the hot path stays
// fence-free), so on real hardware that pairing is best effort; see Liveness.
//
// The busy hot path pays nothing for this: a worker that keeps finding work
// in its own main_box runs exactly the old loop (idle bookkeeping happens
// only where an idle period ends); a local submit adds one relaxed load of
// parked_ (a shared line that stays in cache while nobody parks), and all
// wake-side code is out of line so that submit()'s fast path keeps its size.
// Only while some worker is parked it also reads searchers_ and, if that is
// 0, the size of its own main_box.
//
// Liveness
// --------
// * External submit: the task lands in a target's mail_box / overflow_box,
//   and by the Dekker argument above the target either sees it before parking
//   or is woken. If the target is RUNNING but busy, the producer additionally
//   wakes a parked worker when nobody is searching, and the last searcher
//   re-checks every queue before parking. So the task runs as long as some
//   worker eventually becomes free.
// * Local submit (from a worker): the submitting worker is alive and runs its
//   own main_box, so the task runs as soon as its current task returns.
//   The overload wake only adds parallelism.
// * A task that blocks its worker B forever (e.g. waits for a task it
//   submitted locally): B's queued tasks are taken by any worker that is
//   running, searching, or about to park (they all end up in the re-check
//   above). Workers that were already asleep before B's last submit are woken
//   by the overload wake only if B's main_box then held at least
//   overload_wake_threshold tasks or the task went to the overflow_box.
//   NOT guaranteed: (a) fewer tasks than that, submitted while every other
//   worker already sleeps, stay with B; (b) the overload check is a relaxed
//   load without a fence, so on real hardware a worker that parks or stops
//   searching at that very moment may miss B's last submit (a store-load race
//   of nanoseconds, which no test targets). Fibers never block a worker, so
//   this is a non-goal.

// main_box defaults to the lock-free SPMC ring (owner push without RMW, one
// CAS per popped/stolen batch); LockedSPSCQueue remains available as Q.
template <sync::ParkingSyncPolicy Sync = sync::StdSync, MainQueue Q = queue::LockFreeSPMCQueue<Task, Sync>>
class ThreadPool {
public:
    // Fail fast: if a worker thread or memory cannot be had, the process
    // terminates (std::system_error / std::bad_alloc hit noexcept).
    explicit ThreadPool(ThreadPoolOptions options = {}) noexcept : options_(normalize(options)) {
        workers_.reserve(options_.num_workers);
        for (std::size_t i = 0; i < options_.num_workers; ++i) {
            workers_.push_back(std::make_unique<Worker>(options_));
        }
        threads_.reserve(options_.num_workers);
        for (std::size_t i = 0; i < options_.num_workers; ++i) {
            threads_.emplace_back([this, i] { worker_loop(i); });
        }
    }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    // Stops workers; tasks still queued are dropped (delete_ptr).
    ~ThreadPool() { stop(); }

    // From a worker of this pool: into its own main_box.
    // From any other thread: round-robin into some worker's mail_box,
    // preferring a worker that is not parked, then wakes it if needed.
    // Falls back to the target worker's overflow_box when queues are full.
    void submit(Task task) noexcept {
        if (!task) {
            return;
        }
        detail::ThreadState& state = detail::thread_state<Sync>();
        // Only the local fast path is inline: everything else is out of line
        // so that it does not cost this path registers or a heavier prologue.
        if (state.pool == this) [[likely]] {
            Worker& self = *workers_[state.worker];
            // try_push moves from `task` only on success, so it is still
            // valid for the overflow fallback.
            if (!self.main_box.try_push(std::move(task))) [[unlikely]] {
                submit_local_overflow(self, state, std::move(task));
                return;
            }
            // Overload wake fast path: one relaxed load (see wake_if_overloaded).
            if (parked_.load(std::memory_order_relaxed) != 0) [[unlikely]] {
                wake_if_overloaded(self, state, false);
            }
            return;
        }
        submit_external(state, std::move(task));
    }

    // Submits tasks[0..count), moving from them. Precondition: none is empty.
    // From a worker of this pool: one batched push into its own main_box.
    // From any other thread: chunks of batch_size go round-robin to the
    // workers' mail_boxes, one shard lock, one publication and one wake check
    // (one fence) per chunk.
    // Whatever does not fit goes to the target's overflow_box under one lock.
    void submit_batch(Task* tasks, std::size_t count) noexcept {
        assert(std::all_of(tasks, tasks + count, [](const Task& t) { return static_cast<bool>(t); }));
        if (count == 0) {
            return;
        }
        detail::ThreadState& state = detail::thread_state<Sync>();
        if (state.pool == this) {
            Worker& self = *workers_[state.worker];
            const std::size_t pushed = self.main_box.try_push_batch(tasks, count);
            self.overflow_box.push_batch(tasks + pushed, count - pushed);
            wake_if_overloaded(self, state, pushed != count);
            return;
        }
        for (std::size_t offset = 0; offset < count; offset += options_.batch_size) {
            Task* chunk = tasks + offset;
            const std::size_t size = std::min(options_.batch_size, count - offset);
            Worker& target = pick_target(state);
            const std::size_t pushed = target.mail_box.try_push_batch(chunk, size, state.shard_hint);
            target.overflow_box.push_batch(chunk + pushed, size - pushed);
            wake_after_external_push(target, state);
        }
    }

    void submit_batch(std::span<Task> tasks) noexcept { submit_batch(tasks.data(), tasks.size()); }

    // Requests stop, wakes parked workers and joins them. Each worker exits
    // instead of running its next task. Pending tasks stay queued and are
    // dropped on destruction. Idempotent. Must not be called from a worker of
    // this pool.
    void stop() noexcept {
        assert(!in_worker_thread() && "ThreadPool::stop() called from its own worker");
        // seq_cst store, then an RMW on every state word: a worker either sees
        // the flag in its re-check after PARKING, or its CAS to PARKED fails,
        // or we see PARKED and notify (see "Memory orders" above).
        stop_requested_.store(true, std::memory_order_seq_cst);
        for (auto& worker : workers_) {
            if (worker->state.exchange(kRunning, std::memory_order_seq_cst) == kParked) {
                worker->state.notify_one();
            }
        }
        for (auto& thread : threads_) {
            thread.join();
        }
        threads_.clear();
    }

    std::size_t num_workers() const noexcept { return workers_.size(); }

    bool in_worker_thread() const noexcept { return detail::thread_state<Sync>().pool == this; }

    const ThreadPoolOptions& options() const noexcept { return options_; }

    // Diagnostics (racy snapshots, for tests and monitoring).
    // Workers currently asleep (state PARKED).
    std::size_t parked_workers() const noexcept {
        std::size_t n = 0;
        for (const auto& worker : workers_) {
            n += worker->state.load(std::memory_order_relaxed) == kParked ? 1 : 0;
        }
        return n;
    }

    // Times any worker went to sleep since construction.
    std::uint64_t park_count() const noexcept {
        std::uint64_t n = 0;
        for (const auto& worker : workers_) {
            n += worker->parks.load(std::memory_order_relaxed);
        }
        return n;
    }

private:
    // Worker::state values.
    static constexpr std::uint32_t kRunning = 0;
    static constexpr std::uint32_t kParking = 1;
    static constexpr std::uint32_t kParked = 2;

    // searchers_ = [ grant : 1 | count : 63 ]
    static constexpr std::uint64_t kGrant = std::uint64_t{1} << 63;

    static constexpr std::size_t searchers_of(std::uint64_t word) noexcept {
        return static_cast<std::size_t>(word & ~kGrant);
    }

    // How many round-robin candidates an external submit looks at to skip
    // workers that are not RUNNING.
    static constexpr std::size_t kTargetScan = 4;

    // Pause cap (in Sync::spin_hint calls) of a worker that may not steal and
    // only polls its own boxes, when max_steal_backoff is smaller.
    static constexpr std::size_t kMinPollPause = 64;

    struct alignas(sync::kCacheLineSize) Worker {
        explicit Worker(const ThreadPoolOptions& options)
            : main_box(options.main_box_capacity),
              mail_box(options.mail_box_shards, options.mail_box_shard_capacity) {}

        Q main_box;
        queue::ShardedQueue<Task, Sync> mail_box;
        queue::SlowQueue<Task, Sync> overflow_box;
        // Read by every producer that targets this worker, written by the
        // owner only when it parks: its own cache line.
        alignas(sync::kCacheLineSize) typename Sync::template Atomic<std::uint32_t> state{kRunning};
        typename Sync::template Atomic<std::uint64_t> parks{0};  // owner-written counter
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
        options.idle_spins_before_park = std::max<std::size_t>(options.idle_spins_before_park, 1);
        options.overload_wake_threshold = std::max<std::size_t>(options.overload_wake_threshold, 1);
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
        bool searching = false;        // holds a searcher unit in searchers_
        std::size_t idle_spins = 0;    // pauses since work was last found

        while (!stopping()) {
            if (since_mail_check >= options_.mail_box_check_interval) {
                since_mail_check = 0;
                pull_external(self, batch, mail_cursor);
                wake_if_overloaded(self, state, false);
            }

            std::size_t n = self.main_box.try_pop_batch(batch, batch_size);
            if (n == 0) {
                since_mail_check = 0;
                pull_external(self, batch, mail_cursor);
                n = self.main_box.try_pop_batch(batch, batch_size);
                if (n == 0) {
                    // Pause before looking again, so idle workers do not keep
                    // bouncing a busy producer's cache lines.
                    const std::size_t pause = steal_backoff;
                    for (std::size_t i = 0; i < pause; ++i) {
                        Sync::spin_hint();
                    }
                    if (!searching) {
                        if (try_start_searching()) {
                            searching = true;
                            idle_spins = 0;
                        } else {
                            // Too many searchers: do not steal, only poll our
                            // own boxes (at the top of the loop) for a while,
                            // asking again each round, then park. Parking at
                            // once would turn every short gap in a busy pool
                            // into a futex wait + wake. This polling pauses
                            // even with max_steal_backoff = 0 (up to
                            // kMinPollPause): a tight loop on our own
                            // mail_box would steal its cache lines from the
                            // producer on every push.
                            steal_backoff = std::min(
                                std::max(options_.max_steal_backoff, kMinPollPause),
                                std::max<std::size_t>(steal_backoff * 2, 1));
                            Sync::spin_hint();
                            idle_spins += pause + 1;
                            if (idle_spins >= options_.idle_spins_before_park) {
                                idle_spins = 0;
                                searching = park(index);
                                steal_backoff = 0;
                            }
                            continue;
                        }
                    }
                    const StealResult result = steal(index, state, batch);
                    steal_backoff = next_steal_backoff(steal_backoff, result);
                    if (result.stolen != 0) {
                        // Into our main_box: popped on the next iteration.
                        idle_spins = 0;
                        searching = false;
                        stop_searching_found_work(state);
                    } else {
                        Sync::spin_hint();
                        idle_spins += pause + 1;
                        if (idle_spins >= options_.idle_spins_before_park) {
                            idle_spins = 0;
                            if (!keep_searching_after_giving_up(index)) {
                                searching = park(index);
                                steal_backoff = 0;
                            }
                        }
                    }
                    continue;
                }
                // Idle bookkeeping is done here and after a steal, never on
                // the busy path (first pop succeeded): a searcher only gets
                // work through these two places.
                idle_spins = 0;
                if (searching) [[unlikely]] {
                    searching = false;
                    stop_searching_found_work(state);
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

    [[gnu::noinline]] void submit_local_overflow(Worker& self, detail::ThreadState& state,
                                                 Task&& task) noexcept {
        self.overflow_box.push(std::move(task));
        wake_if_overloaded(self, state, true);
    }

    [[gnu::noinline]] void submit_external(detail::ThreadState& state, Task&& task) noexcept {
        Worker& target = pick_target(state);
        if (!target.mail_box.try_push(std::move(task), state.shard_hint)) {
            target.overflow_box.push(std::move(task));
        }
        wake_after_external_push(target, state);
    }

    // ------------------------------------------------------------ parking

    // Searcher limit: max(1, active / 2) with active = workers not parked.
    bool try_start_searching() noexcept {
        const std::size_t parked = std::min(workers_.size(), parked_.load(std::memory_order_relaxed));
        const std::size_t limit = std::max<std::size_t>(1, (workers_.size() - parked) / 2);
        std::uint64_t word = searchers_.load(std::memory_order_relaxed);
        for (;;) {
            if (searchers_of(word) >= limit) {
                return false;
            }
            if (searchers_.compare_exchange_weak(word, word + 1, std::memory_order_acq_rel,
                                                 std::memory_order_relaxed)) {
                return true;
            }
        }
    }

    // A searcher found work. Go's rule: if it was the last searcher and some
    // worker is parked, wake one to search in its place.
    void stop_searching_found_work(detail::ThreadState& state) noexcept {
        const std::uint64_t prev = searchers_.fetch_sub(1, std::memory_order_acq_rel);
        if (searchers_of(prev) == 1 && parked_.load(std::memory_order_relaxed) != 0) {
            wake_searcher(state);
        }
    }

    // A searcher found nothing for long enough. Returns true if it must keep
    // searching instead of parking: it was the last searcher and some other
    // worker's queue is not empty (its own are re-checked by park()). The
    // RMW + fence pair with a producer's push + load of searchers_ (Dekker,
    // see the top comment): either the producer sees a searcher, or we see
    // its task here.
    bool keep_searching_after_giving_up(std::size_t self_index) noexcept {
        const std::uint64_t prev = searchers_.fetch_sub(1, std::memory_order_seq_cst);
        if (searchers_of(prev) != 1) {
            return false;
        }
        Sync::atomic_thread_fence(std::memory_order_seq_cst);
        if (!work_visible_elsewhere(self_index)) {
            return false;
        }
        searchers_.fetch_add(1, std::memory_order_relaxed);  // over the limit, like Go
        return true;
    }

    // Some worker other than self_index has a task a thief could take.
    bool work_visible_elsewhere(std::size_t self_index) const noexcept {
        for (std::size_t i = 0; i < workers_.size(); ++i) {
            const Worker& w = *workers_[i];
            if (i != self_index && (!w.mail_box.empty_approx() || w.overflow_box.maybe_nonempty() ||
                                    w.main_box.size_approx() != 0)) {
                return true;
            }
        }
        return false;
    }

    // Sleeps until woken unless work shows up meanwhile. Returns true if the
    // worker comes back as a searcher (it took a grant).
    bool park(std::size_t index) noexcept {
        Worker& self = *workers_[index];
        // PARKING before the count: a waker that sees us counted (its load of
        // parked_ reads our RMW) also sees PARKING.
        self.state.store(kParking, std::memory_order_relaxed);
        parked_.fetch_add(1, std::memory_order_seq_cst);
        Sync::atomic_thread_fence(std::memory_order_seq_cst);  // F2
        // (1) Re-check everything a producer may have targeted at us (our own
        //     main_box is only filled by us): the target-wake Dekker.
        // (2) If nobody is searching, we are in the position of the last
        //     searcher: a producer whose wake check ran before our parked_
        //     increment saw nobody to wake, so look at the other queues too.
        if (self.mail_box.empty_approx() && !self.overflow_box.maybe_nonempty() && !stopping() &&
            (searchers_of(searchers_.load(std::memory_order_relaxed)) != 0 ||
             !work_visible_elsewhere(index))) {
            std::uint32_t expected = kParking;
            if (self.state.compare_exchange_strong(expected, kParked, std::memory_order_seq_cst,
                                                   std::memory_order_acquire)) {
                self.parks.store(self.parks.load(std::memory_order_relaxed) + 1,
                                 std::memory_order_relaxed);
                self.state.wait(kParked, std::memory_order_acquire);  // until != PARKED
            }
            // else: a waker moved us to RUNNING while PARKING.
        }
        // Re-check found work, or a waker already stored RUNNING (harmless
        // race: both store RUNNING). seq_cst together with the seq_cst load in
        // leave_parked: a waker whose exchange read PARKING claimed its grant
        // (seq_cst RMW) before that exchange, so S orders the claim before
        // our load and we see the grant.
        self.state.store(kRunning, std::memory_order_seq_cst);
        return leave_parked();
    }

    // Back to RUNNING: uncount ourselves; take a pending grant if any.
    bool leave_parked() noexcept {
        parked_.fetch_sub(1, std::memory_order_acq_rel);
        std::uint64_t word = searchers_.load(std::memory_order_seq_cst);
        while ((word & kGrant) != 0) {
            // The grant's searcher unit becomes ours.
            if (searchers_.compare_exchange_weak(word, word - kGrant, std::memory_order_acq_rel,
                                                 std::memory_order_relaxed)) {
                return true;
            }
        }
        return false;
    }

    // Moves `worker` out of PARKING / PARKED (if it is there), notifying it if
    // it sleeps. Returns true if it was not RUNNING. The caller must have
    // published its work and executed a seq_cst fence (F1) before.
    // Out of line, like everything on the wake side: inlined, notify_one's
    // code would give submit() a heavier prologue (stack protector, more
    // saved registers) that the local fast path would pay too.
    [[gnu::noinline]] static bool unpark(Worker& worker) noexcept {
        if (worker.state.load(std::memory_order_relaxed) == kRunning) {
            return false;
        }
        const std::uint32_t prev = worker.state.exchange(kRunning, std::memory_order_seq_cst);
        if (prev == kParked) {
            worker.state.notify_one();
        }
        return prev != kRunning;
    }

    // Go's wakep: unless somebody is searching (or nobody is parked), wake one
    // parked worker to search. At most one such wake is in flight: the claim
    // adds a searcher unit (with the grant bit) on behalf of the woken worker.
    [[gnu::noinline]] void wake_searcher(detail::ThreadState& state) noexcept {
        if (parked_.load(std::memory_order_relaxed) == 0) {
            return;
        }
        std::uint64_t word = searchers_.load(std::memory_order_relaxed);
        do {
            if (searchers_of(word) != 0) {  // includes an outstanding grant
                return;
            }
        } while (!searchers_.compare_exchange_weak(word, word + 1 + kGrant, std::memory_order_seq_cst,
                                                   std::memory_order_relaxed));
        const std::size_t count = workers_.size();
        const std::size_t start = static_cast<std::size_t>(detail::xorshift(state.rng));
        for (std::size_t i = 0; i < count; ++i) {
            if (unpark(*workers_[(start + i) % count])) {
                return;  // that worker (or another leaving one) takes the grant
            }
        }
        // Everybody counted as parked was already leaving: revoke the grant,
        // unless one of them has taken it (then its searcher unit is theirs).
        word = searchers_.load(std::memory_order_relaxed);
        while ((word & kGrant) != 0 &&
               !searchers_.compare_exchange_weak(word, word - kGrant - 1, std::memory_order_acq_rel,
                                                 std::memory_order_relaxed)) {
        }
    }

    // Overload wake (local submit and the worker's periodic tick). The fast
    // path is one relaxed load of parked_ (a line written only when workers
    // park or wake); the rest runs only while some worker is parked.
    [[gnu::noinline]] void wake_if_overloaded(Worker& self, detail::ThreadState& state, bool overflowed) noexcept {
        if (parked_.load(std::memory_order_relaxed) == 0) [[likely]] {
            return;
        }
        if (searchers_of(searchers_.load(std::memory_order_relaxed)) != 0) {
            return;
        }
        if (overflowed || self.main_box.size_approx() >= options_.overload_wake_threshold) {
            wake_searcher(state);
        }
    }

    // After an external push into target's mail / overflow box.
    void wake_after_external_push(Worker& target, detail::ThreadState& state) noexcept {
        Sync::atomic_thread_fence(std::memory_order_seq_cst);  // F1
        if (target.state.load(std::memory_order_relaxed) != kRunning && unpark(target)) {
            return;  // the target itself will take the task
        }
        // The target is running, maybe busy for long: make sure somebody
        // searches if workers are parked (they steal from mail_boxes first).
        if (parked_.load(std::memory_order_relaxed) != 0 &&
            searchers_of(searchers_.load(std::memory_order_relaxed)) == 0) {
            wake_searcher(state);
        }
    }

    // Round-robin, but skip up to kTargetScan - 1 workers that are not
    // RUNNING; if all candidates are parked, the first one is used (and woken).
    Worker& pick_target(detail::ThreadState& state) noexcept {
        const std::size_t count = workers_.size();
        Worker& first = *workers_[state.next_worker++ % count];
        if (first.state.load(std::memory_order_relaxed) == kRunning) [[likely]] {
            return first;
        }
        const std::size_t scan = std::min(count, kTargetScan);
        for (std::size_t i = 1; i < scan; ++i) {
            Worker& candidate = *workers_[state.next_worker++ % count];
            if (candidate.state.load(std::memory_order_relaxed) == kRunning) {
                return candidate;
            }
        }
        return first;
    }

    // ------------------------------------------------------------ queues

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
    // Parking bookkeeping (see the top comment), each on its own cache line.
    // parked_ is read by every local submit and written only when a worker
    // parks or wakes; searchers_ changes on every search start / stop.
    alignas(sync::kCacheLineSize) typename Sync::template Atomic<std::size_t> parked_{0};
    alignas(sync::kCacheLineSize) typename Sync::template Atomic<std::uint64_t> searchers_{0};
};

}  // namespace fibers::thread_pool
