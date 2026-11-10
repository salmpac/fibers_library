#pragma once

// Metric instruments: Counter, Gauge, Histogram. Small copyable handles into
// cells owned by a Registry (registry.hpp); obtain one at startup, keep it,
// update it from any thread or fiber. Updates never allocate, lock, hash or
// look anything up.
//
//   metrics::Counter sent = registry.counter("sms_sent_total", "SMS sent.");
//   sent.inc();                      // one relaxed fetch_add
//   metrics::Histogram latency = ...;
//   latency.observe(elapsed_us);     // two relaxed fetch_adds
//
// Sharded cells
// -------------
// A Counter or Histogram has one row of cells per shard (Registry option
// `shards`, a power of two). An update goes to the row of the calling
// thread's shard: a small per-thread number handed out round-robin when the
// thread first updates a metric (threads 0..shards-1 of a process get rows
// of their own; later ones share). Rows of different shards never share a
// cache line, so updates from different threads do not contend. A scrape
// sums the rows (relaxed loads).
//
// The shard number lives in a thread_local reached only through a
// [[gnu::noinline]] accessor with a compiler barrier (fibers migrate between
// threads: see thread_pool::detail::thread_state()); it is looked up on
// every update, never cached across a park. With shards == 1 the lookup is
// skipped.
//
// Cells of many instruments share a row: a registry's cells come in chunks
// of `shards` rows (CellArena), each instrument taking a few cells of every
// row, so a counter costs 8 bytes per shard, not a cache line per shard.
//
// Exactness: every update is one relaxed RMW on one cell, so once the
// updating threads are joined (or otherwise happen-before the read), value()
// and a scrape see exact totals. While updates run, a scrape is a racy
// snapshot: each cell is read once; a counter never goes down between two
// scrapes (cells only grow, and a later scrape reads every cell later); a
// histogram's _count is the sum of the bucket counts it read (so it always
// equals the +Inf bucket), and _sum may include an observation whose bucket
// it missed or vice versa.
//
// Gauges are not sharded (set() must replace the value): one cell, alone on
// its cache line.

#include <algorithm>
#include <bit>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <new>
#include <span>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/sync/sync.hpp"

namespace fibers::metrics {

// Shards per registry at most.
inline constexpr std::size_t kMaxShards = 256;

namespace detail {

struct ThreadShard {
    std::uint32_t index = 0;  // shard number + 1; 0 = not assigned yet
};

// Process-wide (per Sync policy) round-robin source of shard numbers.
template <class Sync>
struct ShardTickets {
    typename Sync::template Atomic<std::uint32_t> next{0};
};

// The calling thread's shard number (mask it with shards - 1).
template <class Sync>
[[gnu::noinline]] std::uint32_t thread_shard() noexcept {
    asm volatile("" ::: "memory");
    ThreadShard& shard = Sync::template thread_local_instance<ThreadShard>();
    if (shard.index == 0) [[unlikely]] {
        const std::uint32_t ticket =
            Sync::template global_instance<ShardTickets<Sync>>().next.fetch_add(1, std::memory_order_relaxed);
        shard.index = (ticket % static_cast<std::uint32_t>(kMaxShards)) + 1;
    }
    return shard.index - 1;
}

// Where an instrument's cells are: cell i of shard s is base[s * stride + i].
template <class Sync>
struct CellBlock {
    using Cell = typename Sync::template Atomic<std::uint64_t>;

    Cell* base = nullptr;
    std::uint32_t stride = 0;  // cells per row
    std::uint32_t mask = 0;    // shards - 1 (0: one row)

    Cell& row_cell(std::uint32_t shard, std::size_t i) const noexcept {
        return base[static_cast<std::size_t>(shard) * stride + i];
    }

    // The calling thread's row.
    Cell* my_row() const noexcept {
        if (mask == 0) {
            return base;
        }
        return base + static_cast<std::size_t>(thread_shard<Sync>() & mask) * stride;
    }

    // Sum of cell i over all rows (relaxed).
    std::uint64_t sum(std::size_t i) const noexcept {
        std::uint64_t total = 0;
        for (std::uint32_t s = 0; s <= mask; ++s) {
            total += row_cell(s, i).load(std::memory_order_relaxed);
        }
        return total;
    }
};

// Chunks of zeroed cells, `rows` rows of `row_cells` cells each, rows
// cache-line aligned. Not thread-safe (the Registry allocates under its
// mutex); freed with the arena.
template <class Sync>
class CellArena {
public:
    using Cell = typename Sync::template Atomic<std::uint64_t>;
    using Block = CellBlock<Sync>;

    // Cells per row of an ordinary sharded chunk (instruments that need more
    // get a chunk of their own) and of a single-row chunk.
    static constexpr std::size_t kRowCells = 32;
    static constexpr std::size_t kSingleRowCells = 512;
    static constexpr std::size_t kCellsPerLine = std::max<std::size_t>(sync::kCacheLineSize / sizeof(Cell), 1);

    explicit CellArena(std::size_t shards) noexcept : shards_(shards) {
        assert(std::has_single_bit(shards) && shards <= kMaxShards);
    }

    CellArena(const CellArena&) = delete;
    CellArena& operator=(const CellArena&) = delete;

    ~CellArena() {
        while (chunks_ != nullptr) {
            Chunk* next = chunks_->next;
            for (std::size_t i = 0; i < chunks_->count; ++i) {
                chunks_->cells[i].~Cell();
            }
            ::operator delete(static_cast<void*>(chunks_->cells), std::align_val_t{sync::kCacheLineSize});
            delete chunks_;
            chunks_ = next;
        }
    }

    // `n` cells in every shard's row.
    Block sharded(std::size_t n) noexcept { return take(sharded_, shards_, n); }

    // `n` cells in one row, starting on a cache line of their own (rounded up
    // to whole lines, so that nothing else shares them).
    Block single(std::size_t n) noexcept { return take(single_, 1, round_up(n, kCellsPerLine)); }

    std::size_t shards() const noexcept { return shards_; }

private:
    struct Chunk {
        Chunk* next = nullptr;  // all chunks, for the destructor
        Cell* cells = nullptr;
        std::size_t count = 0;      // rows * row_cells
        std::size_t row_cells = 0;
        std::size_t rows = 0;
        std::size_t used = 0;       // cells taken from each row
    };

    static std::size_t round_up(std::size_t n, std::size_t to) noexcept { return (n + to - 1) / to * to; }

    Block take(Chunk*& current, std::size_t rows, std::size_t n) noexcept {
        n = std::max<std::size_t>(n, 1);
        const std::size_t row_cells = rows == 1 ? kSingleRowCells : kRowCells;
        if (n > row_cells) {
            // A big instrument alone in its chunk; `current` keeps its room.
            Chunk* chunk = allocate(rows, round_up(n, kCellsPerLine));
            chunk->used = n;
            return block_of(*chunk, 0);
        }
        if (current == nullptr || current->row_cells - current->used < n) {
            current = allocate(rows, row_cells);
        }
        const std::size_t offset = current->used;
        current->used += n;
        return block_of(*current, offset);
    }

    Block block_of(const Chunk& chunk, std::size_t offset) const noexcept {
        Block block;
        block.base = chunk.cells + offset;
        block.stride = static_cast<std::uint32_t>(chunk.row_cells);
        block.mask = static_cast<std::uint32_t>(chunk.rows - 1);
        return block;
    }

    Chunk* allocate(std::size_t rows, std::size_t row_cells) noexcept {
        auto* chunk = new (std::nothrow) Chunk;
        const std::size_t count = rows * row_cells;
        void* memory = ::operator new(count * sizeof(Cell), std::align_val_t{sync::kCacheLineSize}, std::nothrow);
        if (chunk == nullptr || memory == nullptr) {
            fail::fail_fast("metrics: out of memory");
        }
        chunk->cells = static_cast<Cell*>(memory);
        for (std::size_t i = 0; i < count; ++i) {
            ::new (static_cast<void*>(chunk->cells + i)) Cell(0);
        }
        chunk->count = count;
        chunk->row_cells = row_cells;
        chunk->rows = rows;
        chunk->next = chunks_;
        chunks_ = chunk;
        return chunk;
    }

    const std::size_t shards_;
    Chunk* chunks_ = nullptr;
    Chunk* sharded_ = nullptr;  // being filled
    Chunk* single_ = nullptr;
};

}  // namespace detail

// Monotonic counter. A default-constructed handle is empty (do not update
// it); valid ones come from a Registry and live as long as it does.
template <sync::SyncPolicy Sync = sync::StdSync>
class BasicCounter {
public:
    BasicCounter() noexcept = default;
    explicit BasicCounter(detail::CellBlock<Sync> cells) noexcept : cells_(cells) {}

    void inc() noexcept { add(1); }

    void add(std::uint64_t n) noexcept {
        assert(cells_.base != nullptr);
        cells_.my_row()->fetch_add(n, std::memory_order_relaxed);
    }

    // Sum over the shards (exact once the updaters happen-before the call).
    std::uint64_t value() const noexcept { return cells_.sum(0); }

    explicit operator bool() const noexcept { return cells_.base != nullptr; }

private:
    detail::CellBlock<Sync> cells_;
};

// Gauge: a signed 64-bit value that goes up and down. One cell (not
// sharded), alone on its cache line: set() is a store, add() / sub() are
// relaxed fetch_adds that contend when many threads change one gauge.
template <sync::SyncPolicy Sync = sync::StdSync>
class BasicGauge {
public:
    BasicGauge() noexcept = default;
    explicit BasicGauge(detail::CellBlock<Sync> cells) noexcept : cells_(cells) {}

    void set(std::int64_t v) noexcept { cell().store(static_cast<std::uint64_t>(v), std::memory_order_relaxed); }
    void add(std::int64_t n) noexcept { cell().fetch_add(static_cast<std::uint64_t>(n), std::memory_order_relaxed); }
    void sub(std::int64_t n) noexcept { cell().fetch_sub(static_cast<std::uint64_t>(n), std::memory_order_relaxed); }
    void inc() noexcept { add(1); }
    void dec() noexcept { sub(1); }

    std::int64_t value() const noexcept {
        return static_cast<std::int64_t>(cells_.base->load(std::memory_order_relaxed));
    }

    explicit operator bool() const noexcept { return cells_.base != nullptr; }

private:
    typename detail::CellBlock<Sync>::Cell& cell() const noexcept {
        assert(cells_.base != nullptr);
        return *cells_.base;
    }

    detail::CellBlock<Sync> cells_;
};

// Histogram over integer observations with fixed bucket upper bounds
// (inclusive, ascending; given at registration, shared by every instrument
// of a family). Observations are integers in a unit of your choice (e.g.
// microseconds); the family's `divisor` scales them for the exposition (1e6:
// buckets and _sum in seconds, as Prometheus conventions want).
//
// Row layout (per shard): [ bucket 0 .. bucket n-1 | +Inf bucket | sum ];
// counts are per bucket (made cumulative at scrape time), the sum is the
// two's complement sum of the observations. observe() = bucket search (a
// linear scan for up to 8 bounds, binary search beyond) + two relaxed
// fetch_adds on the thread's row.
template <sync::SyncPolicy Sync = sync::StdSync>
class BasicHistogram {
public:
    BasicHistogram() noexcept = default;
    BasicHistogram(detail::CellBlock<Sync> cells, std::span<const std::int64_t> bounds) noexcept
        : cells_(cells), bounds_(bounds.data()), bound_count_(static_cast<std::uint32_t>(bounds.size())) {}

    void observe(std::int64_t value) noexcept {
        assert(cells_.base != nullptr);
        const std::size_t bucket = bucket_of(value);
        auto* row = cells_.my_row();
        row[bucket].fetch_add(1, std::memory_order_relaxed);
        row[bound_count_ + 1].fetch_add(static_cast<std::uint64_t>(value), std::memory_order_relaxed);
    }

    // Index of the bucket `value` falls into: the first bound >= value, or
    // bounds().size() for +Inf.
    std::size_t bucket_of(std::int64_t value) const noexcept {
        if (bound_count_ <= 8) {
            std::size_t i = 0;
            while (i < bound_count_ && value > bounds_[i]) {
                ++i;
            }
            return i;
        }
        return static_cast<std::size_t>(std::lower_bound(bounds_, bounds_ + bound_count_, value) - bounds_);
    }

    std::span<const std::int64_t> bounds() const noexcept { return {bounds_, bound_count_}; }

    // Observations in bucket i (not cumulative; i == bounds().size(): above
    // the last bound), summed over shards.
    std::uint64_t bucket_count(std::size_t i) const noexcept {
        assert(i <= bound_count_);
        return cells_.sum(i);
    }

    std::uint64_t count() const noexcept {
        std::uint64_t total = 0;
        for (std::size_t i = 0; i <= bound_count_; ++i) {
            total += cells_.sum(i);
        }
        return total;
    }

    std::int64_t sum() const noexcept { return static_cast<std::int64_t>(cells_.sum(bound_count_ + 1)); }

    explicit operator bool() const noexcept { return cells_.base != nullptr; }

private:
    detail::CellBlock<Sync> cells_;
    const std::int64_t* bounds_ = nullptr;
    std::uint32_t bound_count_ = 0;
};

using Counter = BasicCounter<>;
using Gauge = BasicGauge<>;
using Histogram = BasicHistogram<>;

}  // namespace fibers::metrics
