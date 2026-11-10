#pragma once

// Pooled<P>: an RAII handle for one object taken from a pool, returned to
// the pool when the handle is destroyed (or reset()).
//
//   object_pool::DefaultPool<Connection> pool;
//   auto c = object_pool::lease(pool);     // or pool.lease()
//   if (!c) { ... }                        // the pool returned nullptr
//   c->stream ...                          // get(), ->, *
//   fiber::go(sched, [c = std::move(c)]() mutable noexcept {
//       serve(*c);
//       c.reset();                         // return now (else: at fiber exit)
//   });
//
// P is any LeasablePool: `value_type`, noexcept acquire() -> value_type*
// (nullptr allowed: "none free") and release(value_type*). The library's
// pools (ShardedPool / DefaultPool, stack::StackPool) qualify and also have
// a lease() member; object_pool::lease(pool) works for any of them,
// including small fixed-slot pools of an application.
//
// The handle is two pointers {pool, object}, move-only, and everything is
// noexcept (nothrow move, so it can be captured by move into fiber
// closures). A moved-from handle is empty and returns nothing. An empty
// handle (default constructed, moved from, reset, disowned, or leased from a
// pool that returned nullptr) has get() == nullptr.
//
// Object state: pools do not destroy objects on release (they are reused
// as they are), and Pooled has no reset-on-return hook. Whatever the object
// holds when the handle returns it is what the next lease gets: the caller
// clears what must not leak to the next user before the handle goes away.
//
// Threads: a handle may be moved to another fiber or thread and return the
// object there, as far as the pool allows releases from other threads (the
// library's pools do).
//
// Lifetime: the pool must outlive its handles. Debug builds (NDEBUG not
// defined) check it for pools that count their leases (ShardedPool,
// StackPool: a detail::LeaseCounter member): destroying such a pool while
// handles still own objects of it fails fast. Release builds count nothing
// and the counter takes no space. Objects acquired by hand (pool.acquire())
// are not counted.
//
// Non-goals (these sites keep raw acquire / release on purpose):
//   - the fiber's stack (fiber/fiber.hpp): the fiber object and its closure
//     live on the very stack they would return, and the trampoline releases
//     it after destroying them; a handle inside the fiber would be destroyed
//     together with (and on) the memory it returns;
//   - TaskMemoryPool blocks (sched/task_memory_pool.hpp): a block travels as
//     a type-erased Task (a raw pointer through queues) and is freed from the
//     Task's call / delete function; there is no scope that owns it.

#include <concepts>
#include <cstddef>
#include <mutex>
#include <utility>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/sync/sync.hpp"

namespace fibers::object_pool {

// What Pooled needs from a pool. (ObjectPool in sharded_object_pool.hpp
// adds capacity() and does not require noexcept.)
template <class P>
concept LeasablePool = requires(P& pool, typename P::value_type* object) {
    { pool.acquire() } noexcept -> std::same_as<typename P::value_type*>;
    { pool.release(object) } noexcept;
};

namespace detail {

// Debug-only count of a pool's outstanding Pooled handles: a member of the
// pool, declared last (so destroyed first) and [[no_unique_address]].
// With NDEBUG it is empty and its functions do nothing.
template <sync::SyncPolicy Sync>
class LeaseCounter {
public:
#ifndef NDEBUG
    LeaseCounter() noexcept = default;
    LeaseCounter(const LeaseCounter&) = delete;
    LeaseCounter& operator=(const LeaseCounter&) = delete;

    ~LeaseCounter() {
        if (leases_.load(std::memory_order_relaxed) != 0) [[unlikely]] {
            fail::fail_fast("object_pool: pool destroyed while Pooled handles still own its objects");
        }
    }

    void add() noexcept { leases_.fetch_add(1, std::memory_order_relaxed); }
    void remove() noexcept { leases_.fetch_sub(1, std::memory_order_relaxed); }

private:
    typename Sync::template Atomic<std::size_t> leases_{0};
#else
    void add() noexcept {}
    void remove() noexcept {}
#endif
};

// Reaches a pool's private `leases_` (a LeaseCounter) if it has one; pools
// with a counter befriend this struct.
struct LeaseAccess {
    template <class P>
    static void add(P& pool) noexcept {
        if constexpr (requires { pool.leases_.add(); }) {
            pool.leases_.add();
        }
    }

    template <class P>
    static void remove(P& pool) noexcept {
        if constexpr (requires { pool.leases_.remove(); }) {
            pool.leases_.remove();
        }
    }
};

}  // namespace detail

template <LeasablePool P>
class Pooled {
public:
    using pool_type = P;
    using value_type = typename P::value_type;

    // Empty.
    Pooled() noexcept = default;

    // Adopts `object`, acquired by hand from `pool` (nullptr: empty handle).
    Pooled(P& pool, value_type* object, std::adopt_lock_t) noexcept
        : pool_(object != nullptr ? &pool : nullptr), object_(object) {
        if (object_ != nullptr) {
            detail::LeaseAccess::add(*pool_);
        }
    }

    Pooled(Pooled&& other) noexcept
        : pool_(std::exchange(other.pool_, nullptr)), object_(std::exchange(other.object_, nullptr)) {}

    Pooled& operator=(Pooled&& other) noexcept {
        if (this != &other) {
            reset();
            pool_ = std::exchange(other.pool_, nullptr);
            object_ = std::exchange(other.object_, nullptr);
        }
        return *this;
    }

    Pooled(const Pooled&) = delete;
    Pooled& operator=(const Pooled&) = delete;

    ~Pooled() { reset(); }

    // Returns the object to its pool now (no-op if empty).
    void reset() noexcept {
        if (value_type* const object = std::exchange(object_, nullptr)) {
            P* const pool = std::exchange(pool_, nullptr);
            detail::LeaseAccess::remove(*pool);
            pool->release(object);
        }
    }

    // Gives up ownership without returning the object; returns it (nullptr
    // if empty). The caller releases it to pool() by hand (read pool()
    // first) or adopts it into another handle.
    value_type* disown() noexcept {
        if (object_ != nullptr) {
            detail::LeaseAccess::remove(*pool_);
        }
        pool_ = nullptr;
        return std::exchange(object_, nullptr);
    }

    value_type* get() const noexcept { return object_; }
    value_type* operator->() const noexcept { return object_; }
    value_type& operator*() const noexcept { return *object_; }

    // The pool the object returns to (nullptr if empty).
    P* pool() const noexcept { return pool_; }

    bool owns() const noexcept { return object_ != nullptr; }
    explicit operator bool() const noexcept { return owns(); }

private:
    P* pool_ = nullptr;
    value_type* object_ = nullptr;
};

// Takes an object from `pool` (empty handle if the pool returns nullptr).
template <LeasablePool P>
[[nodiscard]] Pooled<P> lease(P& pool) noexcept {
    return Pooled<P>(pool, pool.acquire(), std::adopt_lock);
}

}  // namespace fibers::object_pool
