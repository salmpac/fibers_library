#pragma once

#include <algorithm>
#include <bit>
#include <cstddef>
#include <memory>
#include <type_traits>

#include "fibers/sync/sync.hpp"

namespace fibers::queue {

// Bounded lock-free single-producer single-consumer ring buffer.
//
// "Producer" / "consumer" are roles, not fixed threads: a role may migrate
// between threads as long as the hand-off establishes happens-before (e.g. the
// role is guarded by a lock). Role-local state (cached indices) relies on that.
//
// Capacity is rounded up to a power of two.
template <class T, sync::SyncPolicy Sync = sync::StdSync>
    requires std::is_default_constructible_v<T> &&
             std::is_nothrow_move_assignable_v<T>
class SPSCQueue {
public:
    explicit SPSCQueue(std::size_t capacity)
        : capacity_(std::bit_ceil(std::max<std::size_t>(capacity, 1))),
          mask_(capacity_ - 1),
          buffer_(std::make_unique<T[]>(capacity_)) {}

    SPSCQueue(const SPSCQueue&) = delete;
    SPSCQueue& operator=(const SPSCQueue&) = delete;

    std::size_t capacity() const noexcept { return capacity_; }

    // ---------------- producer ----------------

    // Moves from `value` only on success.
    bool try_push(T&& value) noexcept {
        const std::size_t tail = producer_.tail.load(std::memory_order_relaxed);
        if (tail - producer_.cached_head >= capacity_) {
            producer_.cached_head = consumer_.head.load(std::memory_order_acquire);
            if (tail - producer_.cached_head >= capacity_) {
                return false;
            }
        }
        buffer_[tail & mask_] = std::move(value);
        producer_.tail.store(tail + 1, std::memory_order_release);
        return true;
    }

    // Moves the first k items (k is returned) from `items`.
    std::size_t try_push_batch(T* items, std::size_t count) noexcept {
        const std::size_t tail = producer_.tail.load(std::memory_order_relaxed);
        std::size_t free = capacity_ - (tail - producer_.cached_head);
        if (free < count) {
            producer_.cached_head = consumer_.head.load(std::memory_order_acquire);
            free = capacity_ - (tail - producer_.cached_head);
        }
        const std::size_t n = std::min(free, count);
        for (std::size_t i = 0; i < n; ++i) {
            buffer_[(tail + i) & mask_] = std::move(items[i]);
        }
        if (n != 0) {
            producer_.tail.store(tail + n, std::memory_order_release);
        }
        return n;
    }

    // Exact lower bound: free space can only grow until the producer pushes.
    std::size_t free_space() noexcept {
        producer_.cached_head = consumer_.head.load(std::memory_order_acquire);
        return capacity_ - (producer_.tail.load(std::memory_order_relaxed) -
                            producer_.cached_head);
    }

    // ---------------- consumer ----------------

    bool try_pop(T& out) noexcept { return try_pop_batch(&out, 1) == 1; }

    std::size_t try_pop_batch(T* out, std::size_t max) noexcept {
        const std::size_t head = consumer_.head.load(std::memory_order_relaxed);
        std::size_t available = consumer_.cached_tail - head;
        if (available < max) {
            consumer_.cached_tail = producer_.tail.load(std::memory_order_acquire);
            available = consumer_.cached_tail - head;
        }
        const std::size_t n = std::min(available, max);
        for (std::size_t i = 0; i < n; ++i) {
            out[i] = std::move(buffer_[(head + i) & mask_]);
        }
        if (n != 0) {
            consumer_.head.store(head + n, std::memory_order_release);
        }
        return n;
    }

    // Exact lower bound: size can only grow until the consumer pops.
    std::size_t size() noexcept {
        consumer_.cached_tail = producer_.tail.load(std::memory_order_acquire);
        return consumer_.cached_tail - consumer_.head.load(std::memory_order_relaxed);
    }

    // ---------------- any thread ----------------

    std::size_t size_approx() const noexcept {
        // head first (acquire), then tail: guarantees tail >= head.
        const std::size_t head = consumer_.head.load(std::memory_order_acquire);
        const std::size_t tail = producer_.tail.load(std::memory_order_acquire);
        return tail - head;
    }

    bool empty_approx() const noexcept { return size_approx() == 0; }

private:
    struct alignas(sync::kCacheLineSize) ProducerSide {
        typename Sync::template Atomic<std::size_t> tail{0};
        std::size_t cached_head = 0;
    };

    struct alignas(sync::kCacheLineSize) ConsumerSide {
        typename Sync::template Atomic<std::size_t> head{0};
        std::size_t cached_tail = 0;
    };

    const std::size_t capacity_;
    const std::size_t mask_;
    std::unique_ptr<T[]> buffer_;

    ProducerSide producer_;
    ConsumerSide consumer_;
};

}  // namespace fibers::queue
