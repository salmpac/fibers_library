#pragma once

#include <algorithm>
#include <cstddef>
#include <memory>
#include <mutex>
#include <vector>

#include "thread_pool/spin_lock.hpp"
#include "thread_pool/spsc_queue.hpp"

namespace thread_pool {

// Multi-producer multi-consumer queue made of several SPSC shards, each with a
// write spin lock (producers) and a read spin lock (consumers). Producers pick
// a home shard via `hint`; if every producer thread keeps the same hint, a
// shard's write lock is mostly taken by the same thread.
template <class T, SyncPolicy Sync = StdSync>
class ShardedQueue {
public:
    ShardedQueue(std::size_t shards, std::size_t shard_capacity) {
        shards = std::max<std::size_t>(shards, 1);
        shards_.reserve(shards);
        for (std::size_t i = 0; i < shards; ++i) {
            shards_.push_back(std::make_unique<Shard>(shard_capacity));
        }
    }

    std::size_t shard_count() const noexcept { return shards_.size(); }

    // Any thread. Moves from `value` only on success.
    // Tries every shard without blocking, starting at `hint`; if all are busy
    // or full, waits for the home shard once. Fails only if it is full.
    bool try_push(T&& value, std::size_t hint) noexcept {
        const std::size_t n = shards_.size();
        for (std::size_t i = 0; i < n; ++i) {
            Shard& shard = *shards_[(hint + i) % n];
            if (!shard.write_lock.try_lock()) {
                continue;
            }
            const bool ok = shard.queue.try_push(std::move(value));
            shard.write_lock.unlock();
            if (ok) {
                return true;
            }
        }
        Shard& home = *shards_[hint % n];
        std::lock_guard guard(home.write_lock);
        return home.queue.try_push(std::move(value));
    }

    // Any thread. Moves items[0..k) and returns k; same shard policy as
    // try_push, but one lock and one publication per shard for the whole batch.
    std::size_t try_push_batch(T* items, std::size_t count, std::size_t hint) noexcept {
        const std::size_t n = shards_.size();
        std::size_t pushed = 0;
        for (std::size_t i = 0; i < n && pushed < count; ++i) {
            Shard& shard = *shards_[(hint + i) % n];
            if (!shard.write_lock.try_lock()) {
                continue;
            }
            pushed += shard.queue.try_push_batch(items + pushed, count - pushed);
            shard.write_lock.unlock();
        }
        if (pushed < count) {
            Shard& home = *shards_[hint % n];
            std::lock_guard guard(home.write_lock);
            pushed += home.queue.try_push_batch(items + pushed, count - pushed);
        }
        return pushed;
    }

    // Owner. Drains shards round-robin starting at `start`, up to `max` items.
    std::size_t try_pop_batch(T* out, std::size_t max, std::size_t start) noexcept {
        const std::size_t n = shards_.size();
        std::size_t got = 0;
        for (std::size_t i = 0; i < n && got < max; ++i) {
            Shard& shard = *shards_[(start + i) % n];
            if (shard.queue.empty_approx()) {
                continue;
            }
            std::lock_guard guard(shard.read_lock);
            got += shard.queue.try_pop_batch(out + got, max - got);
        }
        return got;
    }

    // Any thread. Never blocks; takes a batch of up to `max` items in total,
    // walking shards starting at `start`.
    std::size_t try_steal_batch(T* out, std::size_t max, std::size_t start) noexcept {
        const std::size_t n = shards_.size();
        std::size_t got = 0;
        for (std::size_t i = 0; i < n && got < max; ++i) {
            Shard& shard = *shards_[(start + i) % n];
            if (shard.queue.empty_approx() || !shard.read_lock.try_lock()) {
                continue;
            }
            got += shard.queue.try_pop_batch(out + got, max - got);
            shard.read_lock.unlock();
        }
        return got;
    }

    bool empty_approx() const noexcept {
        return std::all_of(shards_.begin(), shards_.end(),
                           [](const auto& s) { return s->queue.empty_approx(); });
    }

private:
    struct alignas(kCacheLineSize) Shard {
        explicit Shard(std::size_t capacity) : queue(capacity) {}

        SpinLock<Sync> write_lock;
        alignas(kCacheLineSize) SpinLock<Sync> read_lock;
        SPSCQueue<T, Sync> queue;
    };

    std::vector<std::unique_ptr<Shard>> shards_;
};

}  // namespace thread_pool
