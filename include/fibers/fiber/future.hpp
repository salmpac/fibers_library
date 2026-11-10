#pragma once

// Promise<T> / Future<T>: a one-shot value hand-off whose state is a pooled
// block, freed by whichever side finishes last. The stackful front-end of
// wait::OneshotState (see wait/oneshot_core.hpp for the protocol, the
// memory orders and who frees).
//
//   auto [promise, future] = fiber::make_promise_future<Reply>(scheduler);
//   send(Request{..., std::move(promise)});
//   wait::Result<Reply> r = future.get();            // parks until set / broken
//
// T = void (make_promise_future<void>): a completion without a value;
// promise.set() takes no arguments, get() returns a Result<void>.
//
// Unlike fiber::Oneshot (state in the consumer's frame, which must outlive
// the producer), either side may walk away at any time:
//   * the producer: an unset Promise that is destroyed (or abandon()ed)
//     breaks the result: get() returns Error::kBroken;
//   * the consumer: a Future destroyed (or detach()ed) before the result is
//     consumed detaches: the producer's set() still succeeds, the value is
//     destroyed and the block freed by the producer (no leak, no
//     use-after-free); if the producer had already completed, the consumer
//     destroys the value and frees the block itself.
// That is what later timeouts / cancellation build on (a consumer that
// stops waiting withdraws its node and detaches; see "Later" in
// oneshot_core.hpp).
//
// Memory: one block per pair, holding the state word, the value slot and
// the release hook (OneshotState<T>, 24 bytes + T, 16-aligned in a
// TaskMemoryPool block):
//   * make_promise_future<T>(scheduler): from scheduler.task_memory(), the
//     size class chosen at compile time; a T that does not fit the largest
//     class (TaskMemoryPool::kMaxBlockSize) is a static_assert;
//   * make_promise_future<T, Sched>(alloc): from a caller-supplied
//     allocator with object_pool::PoolAllocator's allocate / deallocate
//     (of sizeof / alignof the state; taken by reference, so it need not be
//     movable), e.g. a front of the user's own pool; any size.
// The memory source must outlive every state taken from it. Nothing else is
// allocated; the handles are one pointer each.
//
// Consumer API (one consumer, like Oneshot): get() waits (parks a fiber of
// Sched; blocks a plain thread on an atomic, never use that on a worker) and
// consumes the result: the block is freed and the Future becomes empty
// (valid() == false). try_get() never waits; it consumes too once there is
// a result. get() / try_get() on an empty Future fail fast.
//
// Everything sequenced before set() happens-before get() / try_get()
// returning the value.

#include <concepts>
#include <cstddef>
#include <new>
#include <optional>
#include <type_traits>
#include <utility>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fiber/oneshot.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/wait/oneshot_core.hpp"
#include "fibers/wait/result.hpp"

namespace fibers::fiber {

template <class T, FiberScheduler Sched = sched::Scheduler>
class BasicFuture {
public:
    using SyncPolicy = typename Sched::SyncPolicy;
    using State = wait::OneshotState<T, SyncPolicy>;
    using Promise = wait::BasicPromise<T, SyncPolicy>;
    using value_type = T;

    BasicFuture() noexcept = default;

    // For the factories: the consumer side of a pooled `state`.
    explicit BasicFuture(State* state) noexcept : state_(state) {}

    BasicFuture(BasicFuture&& other) noexcept : state_(std::exchange(other.state_, nullptr)) {}

    BasicFuture& operator=(BasicFuture&& other) noexcept {
        if (this != &other) {
            detach();
            state_ = std::exchange(other.state_, nullptr);
        }
        return *this;
    }

    BasicFuture(const BasicFuture&) = delete;
    BasicFuture& operator=(const BasicFuture&) = delete;

    ~BasicFuture() { detach(); }

    // Waits for the result and consumes it (the Future becomes empty).
    Result<T> get() noexcept {
        State* const state = checked("fiber: Future::get() on an empty Future (consumed, detached or moved from)");
        const wait::OneshotStatus s = detail::wait_final<Sched>(*state);
        return consume(state, s);
    }

    // The result if the producer has completed (consumes it), nullopt if
    // not yet. Never waits.
    std::optional<Result<T>> try_get() noexcept {
        State* const state =
            checked("fiber: Future::try_get() on an empty Future (consumed, detached or moved from)");
        const wait::OneshotStatus s = state->status();
        if (s == wait::OneshotStatus::kPending) {
            return std::nullopt;
        }
        return consume(state, s);
    }

    // The producer has completed (set or broken); get() would not wait.
    // False on an empty Future.
    bool ready() const noexcept { return state_ != nullptr && state_->status() != wait::OneshotStatus::kPending; }

    // Holds a state (not consumed, detached or moved from).
    bool valid() const noexcept { return state_ != nullptr; }
    explicit operator bool() const noexcept { return valid(); }

    // Drops the consumer's interest: the result (whenever it comes) is
    // destroyed, the block freed by the side that finishes last. No-op on an
    // empty Future. Never waits.
    void detach() noexcept {
        State* const state = std::exchange(state_, nullptr);
        if (state == nullptr) {
            return;
        }
        const wait::OneshotStatus s = state->detach();
        if (s == wait::OneshotStatus::kPending) {
            return;  // the producer frees it when it completes
        }
        if (s == wait::OneshotStatus::kReady) {
            state->destroy_value();
        }
        state->release();
    }

private:
    State* checked(const char* what) const noexcept {
        if (state_ == nullptr) [[unlikely]] {
            fail::fail_fast(what);
        }
        return state_;
    }

    // After observing the final status `s`: the producer is done with the
    // state; take the result and free the block.
    Result<T> consume(State* state, wait::OneshotStatus s) noexcept {
        state_ = nullptr;
        Result<T> result = state->take(s);
        state->release();
        return result;
    }

    State* state_ = nullptr;
};

template <class T>
using Future = BasicFuture<T, sched::Scheduler>;

// The allocate / deallocate half of object_pool::PoolAllocator: what
// make_promise_future(alloc) calls on the allocator it is given by
// reference (no move-constructibility needed).
template <class A>
concept OneshotAllocator = requires(A& a, void* p, std::size_t n) {
    { a.allocate(n, n) } noexcept -> std::same_as<void*>;
    { a.deallocate(p, n, n) } noexcept;
};

// What make_promise_future returns (structured bindings:
// `auto [promise, future] = ...`).
template <class T, FiberScheduler Sched = sched::Scheduler>
struct PromiseFuture {
    wait::BasicPromise<T, typename Sched::SyncPolicy> promise;
    BasicFuture<T, Sched> future;
};

namespace detail {

template <class Pool, std::size_t BlockSize>
void release_task_memory(void* pool, void* block) noexcept {
    static_cast<Pool*>(pool)->template release<BlockSize>(block);
}

template <class Alloc, class State>
void release_allocated(void* alloc, void* block) noexcept {
    static_cast<Alloc*>(alloc)->deallocate(block, sizeof(State), alignof(State));
}

template <class T, class Sched, class State>
PromiseFuture<T, Sched> adopt(State* state) noexcept {
    return PromiseFuture<T, Sched>{wait::BasicPromise<T, typename Sched::SyncPolicy>(state),
                                   BasicFuture<T, Sched>(state)};
}

}  // namespace detail

// A pair over a block of scheduler.task_memory() (TaskMemoryPool). Any
// thread.
template <class T, FiberScheduler Sched>
    requires requires(Sched& s) { s.task_memory().template acquire<64>(); }
PromiseFuture<T, Sched> make_promise_future(Sched& scheduler) noexcept {
    using State = wait::OneshotState<T, typename Sched::SyncPolicy>;
    using Pool = std::remove_reference_t<decltype(scheduler.task_memory())>;
    constexpr std::size_t kBlock = Pool::block_size_for(sizeof(State));
    static_assert(kBlock != 0,
                  "fiber::make_promise_future<T>(scheduler): T too large for the scheduler's TaskMemoryPool "
                  "(OneshotState<T> = 24 bytes + T must fit into TaskMemoryPool::kMaxBlockSize); pass an "
                  "allocator of your own (make_promise_future<T, Sched>(alloc)) or send a pointer to an "
                  "object you own");
    static_assert(alignof(State) <= Pool::kBlockAlign,
                  "fiber::make_promise_future<T>(scheduler): over-aligned T (TaskMemoryPool blocks are 16-byte "
                  "aligned); pass an allocator of your own (make_promise_future<T, Sched>(alloc))");
    Pool& pool = scheduler.task_memory();
    void* block = pool.template acquire<kBlock>();
    auto* state = ::new (block) State(&detail::release_task_memory<Pool, kBlock>, &pool);
    return detail::adopt<T, Sched>(state);
}

// A pair over memory from a caller-supplied allocator (allocate /
// deallocate of sizeof(OneshotState<T>), alignof(OneshotState<T>)); `alloc`
// must outlive the pair and be callable from whichever thread finishes
// last. Any thread (if the allocator allows it).
template <class T, FiberScheduler Sched = sched::Scheduler, OneshotAllocator Alloc>
PromiseFuture<T, Sched> make_promise_future(Alloc& alloc) noexcept {
    using State = wait::OneshotState<T, typename Sched::SyncPolicy>;
    void* block = alloc.allocate(sizeof(State), alignof(State));
    auto* state = ::new (block) State(&detail::release_allocated<Alloc, State>, &alloc);
    return detail::adopt<T, Sched>(state);
}

}  // namespace fibers::fiber
