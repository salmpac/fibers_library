#pragma once

// Single-producer / single-consumer ring in caller-provided memory, for talking
// to a separate process (e.g. a DPDK application) through shared memory
// (mmap of a memfd / shm_open object), in either direction.
//
// Layout (Ring<T, StdSync>; fixed, no pointers, little-endian x86-64/aarch64)
//
//   offset  size  field         written by
//        0     8  magic         creator (stored last, release: "initialised")
//        8     4  version       creator
//       12     4  element_size  creator (sizeof(T))
//       16     8  capacity      creator (power of two)
//       24     8  data_offset   creator (= 256)
//       32     8  generation    creator (bumped by whoever re-creates the ring)
//       64     8  tail          producer: next slot to write (monotonic u64)
//      128     8  head          consumer: next slot to read  (monotonic u64)
//      192     4  sleeping      consumer: 1 = "about to sleep, ring the bell"
//      256        slots         capacity * element_size bytes
//
// head, tail and sleeping are naturally aligned 8/4-byte words on separate
// cache lines, accessed only with lock-free atomic instructions; the C side
// uses C11 <stdatomic.h> / rte_atomic on the same offsets. Indices are free
// running: size = tail - head, slot = index & (capacity - 1).
//
// Why not the Sync policy for the shared words: the other process is not ours.
// It runs plain hardware atomics on raw bytes at fixed offsets, while a
// checker's atomics (twist::ed::std::atomic) are process-local objects with
// their own size, layout and bookkeeping that no other process could see, let
// alone a C program. So the cross-process instantiation is Ring<T, StdSync>
// (std::atomic, required to be always lock-free => address-free, and
// layout-checked below). The algorithm is nonetheless a template over the
// policy, and Ring<T, sync::TwistSync> runs the very same code in-process
// under Twist (plus happens-before probes on the slots).
//
// Sleep / wake protocol (Dekker, same shape as worker parking)
//
//   consumer arm():                      producer after a batch push:
//     C1 sleeping = 1          seq_cst     P1 tail = t + n            release
//     C2 tail != head ?        seq_cst     P2 fence                   seq_cst
//        yes: sleeping = 0, "work"         P3 if (sleeping) write(eventfd, 1)
//        no:  sleep on the eventfd
//   after waking: sleeping = 0, drain the eventfd.
//
//   By the fence rules of the C++ memory model (and the x86 / ARM fences
//   they map to) C2 misses P1 only if P3 sees C1: no lost wakeup. The producer
//   pays one fence + one load per batch; the eventfd write (a syscall) only
//   happens while the consumer actually sleeps. Stale writes (the consumer
//   woke up for another reason) cause at most one spurious wakeup.
//
// eventfd sharing: create it before fork() (inherited; the child must not be
// exec'ed with O_CLOEXEC set) or pass it over a unix socket with SCM_RIGHTS
// (see ipc/fd_passing.hpp). Each direction needs its own eventfd.
//
// Peer death: register a pidfd (pidfd_open(peer_pid)) as a poller::Epollable;
// it becomes readable when the peer exits, and its callback tears the ring
// down. Where the peer's pid is unknown or reused, the `generation` word lets a
// restarted peer re-create the ring and the survivor notice the change. A
// heartbeat counter can be added in a later version of the layout; nothing in
// the ring itself depends on the peer being alive.
//
// Backpressure: try_push returns how many items fit; the producer retries
// later (there is no "space available" wakeup in this version).

#include <atomic>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <new>
#include <type_traits>

#include "fibers/sync/sync.hpp"

namespace fibers::ipc {


inline constexpr std::uint64_t kRingMagic = 0x4353505352424946ull;  // "FIBRSPSC" (LE)
inline constexpr std::uint32_t kRingVersion = 1;
inline constexpr std::size_t kRingHeaderBytes = 256;

static_assert(std::atomic<std::uint64_t>::is_always_lock_free);
static_assert(std::atomic<std::uint32_t>::is_always_lock_free);
static_assert(sizeof(std::atomic<std::uint64_t>) == 8);
static_assert(sizeof(std::atomic<std::uint32_t>) == 4);

template <sync::SyncPolicy Sync>
struct RingHeader {
    template <class U>
    using Atomic = typename Sync::template Atomic<U>;

    Atomic<std::uint64_t> magic{0};
    std::uint32_t version = 0;
    std::uint32_t element_size = 0;
    std::uint64_t capacity = 0;
    std::uint64_t data_offset = 0;
    std::uint64_t generation = 0;

    alignas(64) Atomic<std::uint64_t> tail{0};
    alignas(64) Atomic<std::uint64_t> head{0};
    alignas(64) Atomic<std::uint32_t> sleeping{0};
};

namespace detail {

// Optional checker hook (see queue::LockFreeSPMCQueue): a plain cell
// per slot, written after the producer's copy and read before the consumer's,
// so a missing release / acquire shows up as a data race under Twist.
template <class Sync>
concept HasHbProbe = requires { typename Sync::HbProbe; };

template <class T, class Sync>
struct SlotOf {
    using type = T;
    static void written(type&) noexcept {}
    static void reading(const type&) noexcept {}
    static T& value(type& slot) noexcept { return slot; }
};

template <class T, HasHbProbe Sync>
struct SlotOf<T, Sync> {
    struct type {
        T value;
        typename Sync::HbProbe probe;
    };
    static void written(type& slot) noexcept { slot.probe.write(); }
    static void reading(const type& slot) noexcept { slot.probe.read(); }
    static T& value(type& slot) noexcept { return slot.value; }
};

}  // namespace detail

// A view of a ring placed in some memory. Cheap to copy.
template <class T, sync::SyncPolicy Sync = sync::StdSync>
class Ring {
    static_assert(std::is_trivially_copyable_v<T>, "ring elements are copied as bytes");
    static_assert(alignof(T) <= 64);

public:
    using Header = RingHeader<Sync>;
    using SlotTraits = detail::SlotOf<T, Sync>;
    using Slot = typename SlotTraits::type;

    static constexpr std::size_t kDataOffset =
        sizeof(Header) <= kRingHeaderBytes ? kRingHeaderBytes : (sizeof(Header) + 63) / 64 * 64;

    // Bytes needed for a ring of `capacity` (a power of two) elements.
    static constexpr std::size_t bytes_for(std::size_t capacity) noexcept {
        return kDataOffset + capacity * sizeof(Slot);
    }

    Ring() noexcept = default;

    // Initialises a ring in [memory, memory + bytes) (64-byte aligned). The
    // magic word is published last, so a concurrent attach() either fails or
    // sees a complete header. Invalid ring if the arguments do not fit.
    static Ring create(void* memory, std::size_t bytes, std::size_t capacity,
                       std::uint64_t generation = 0) noexcept {
        if (memory == nullptr || reinterpret_cast<std::uintptr_t>(memory) % 64 != 0 ||
            capacity == 0 || !std::has_single_bit(capacity) || bytes < bytes_for(capacity)) {
            return Ring{};
        }
        check_layout();
        auto* header = ::new (memory) Header{};
        header->version = kRingVersion;
        header->element_size = static_cast<std::uint32_t>(sizeof(T));
        header->capacity = capacity;
        header->data_offset = kDataOffset;
        header->generation = generation;
        Slot* slots = slots_of(memory);
        if constexpr (!std::is_trivially_default_constructible_v<Slot>) {
            for (std::size_t i = 0; i < capacity; ++i) {
                ::new (slots + i) Slot{};
            }
        }
        header->magic.store(kRingMagic, std::memory_order_release);
        return Ring(header, slots);
    }

    // Validates a ring created (possibly by another process) in `memory`.
    // Invalid ring on any mismatch (not initialised yet, other version or
    // element size, does not fit in `bytes`).
    static Ring attach(void* memory, std::size_t bytes) noexcept {
        if (memory == nullptr || bytes < kDataOffset) {
            return Ring{};
        }
        check_layout();
        // The creator constructed the header; we only read it.
        auto* header = std::launder(static_cast<Header*>(memory));
        if (header->magic.load(std::memory_order_acquire) != kRingMagic ||
            header->version != kRingVersion || header->element_size != sizeof(T) ||
            header->data_offset != kDataOffset || header->capacity == 0 ||
            !std::has_single_bit(header->capacity) || bytes < bytes_for(header->capacity)) {
            return Ring{};
        }
        return Ring(header, slots_of(memory));
    }

    // In-process rings only (e.g. under Twist): runs the destructors of the
    // header and slot objects. Shared-memory rings need nothing (std::atomic
    // is trivially destructible); just unmap.
    void destroy() noexcept {
        if (header_ == nullptr) {
            return;
        }
        if constexpr (!std::is_trivially_destructible_v<Slot>) {
            for (std::size_t i = 0; i < header_->capacity; ++i) {
                slots_[i].~Slot();
            }
        }
        header_->~Header();
        header_ = nullptr;
        slots_ = nullptr;
    }

    bool valid() const noexcept { return header_ != nullptr; }
    std::size_t capacity() const noexcept { return static_cast<std::size_t>(header_->capacity); }
    Header* header() const noexcept { return header_; }
    Slot* slots() const noexcept { return slots_; }

private:
    Ring(Header* header, Slot* slots) noexcept : header_(header), slots_(slots) {}

    static Slot* slots_of(void* memory) noexcept {
        return std::launder(reinterpret_cast<Slot*>(static_cast<std::byte*>(memory) + kDataOffset));
    }

    static constexpr void check_layout() noexcept {
        if constexpr (std::is_same_v<typename Header::template Atomic<std::uint64_t>,
                                     std::atomic<std::uint64_t>>) {
            // The cross-process layout documented above.
            static_assert(sizeof(Header) <= kRingHeaderBytes);
            static_assert(kDataOffset == kRingHeaderBytes);
            static_assert(std::is_standard_layout_v<Header>);
            static_assert(offsetof(Header, magic) == 0);
            static_assert(offsetof(Header, version) == 8);
            static_assert(offsetof(Header, element_size) == 12);
            static_assert(offsetof(Header, capacity) == 16);
            static_assert(offsetof(Header, data_offset) == 24);
            static_assert(offsetof(Header, generation) == 32);
            static_assert(offsetof(Header, tail) == 64);
            static_assert(offsetof(Header, head) == 128);
            static_assert(offsetof(Header, sleeping) == 192);
            static_assert(sizeof(Slot) == sizeof(T));
        }
    }

    Header* header_ = nullptr;
    Slot* slots_ = nullptr;
};

// Producer side. Exactly one Producer per ring at a time (across processes).
template <class T, sync::SyncPolicy Sync = sync::StdSync>
class Producer {
public:
    explicit Producer(Ring<T, Sync> ring) noexcept
        : ring_(ring),
          mask_(ring.capacity() - 1),
          tail_(ring.header()->tail.load(std::memory_order_relaxed)),
          cached_head_(ring.header()->head.load(std::memory_order_acquire)) {}

    // Copies up to `count` items in and publishes them with one release store.
    // Returns how many fit. Does not wake the consumer: see notify().
    std::size_t try_push(const T* items, std::size_t count) noexcept {
        auto* header = ring_.header();
        const std::size_t capacity = mask_ + 1;
        std::size_t free = capacity - static_cast<std::size_t>(tail_ - cached_head_);
        if (free < count) {
            cached_head_ = header->head.load(std::memory_order_acquire);  // slots consumed
            free = capacity - static_cast<std::size_t>(tail_ - cached_head_);
        }
        const std::size_t n = count < free ? count : free;
        if (n == 0) {
            return 0;
        }
        for (std::size_t i = 0; i < n; ++i) {
            auto& slot = ring_.slots()[(tail_ + i) & mask_];
            std::memcpy(&Ring<T, Sync>::SlotTraits::value(slot), &items[i], sizeof(T));
            Ring<T, Sync>::SlotTraits::written(slot);
        }
        tail_ += n;
        header->tail.store(tail_, std::memory_order_release);  // P1
        return n;
    }

    // The producer's half of the sleep protocol: call after a batch of pushes.
    // Rings the doorbell iff the consumer announced it is going to sleep.
    template <class Doorbell>
    bool notify(Doorbell& doorbell) noexcept {
        Sync::atomic_thread_fence(std::memory_order_seq_cst);                  // P2
        if (ring_.header()->sleeping.load(std::memory_order_seq_cst) != 0) {  // P3
            doorbell.ring();
            return true;
        }
        return false;
    }

    // try_push + notify (if anything was pushed).
    template <class Doorbell>
    std::size_t push(const T* items, std::size_t count, Doorbell& doorbell) noexcept {
        const std::size_t n = try_push(items, count);
        if (n != 0) {
            notify(doorbell);
        }
        return n;
    }

    std::size_t free_space() noexcept {
        cached_head_ = ring_.header()->head.load(std::memory_order_acquire);
        return mask_ + 1 - static_cast<std::size_t>(tail_ - cached_head_);
    }

private:
    Ring<T, Sync> ring_;
    std::size_t mask_;
    std::uint64_t tail_;         // own copy of header->tail
    std::uint64_t cached_head_;  // last seen header->head
};

// Consumer side. Exactly one Consumer per ring at a time.
template <class T, sync::SyncPolicy Sync = sync::StdSync>
class Consumer {
public:
    explicit Consumer(Ring<T, Sync> ring) noexcept
        : ring_(ring),
          mask_(ring.capacity() - 1),
          head_(ring.header()->head.load(std::memory_order_relaxed)),
          cached_tail_(ring.header()->tail.load(std::memory_order_acquire)) {}

    // Copies out up to `max` items, frees their slots with one release store.
    std::size_t try_pop(T* out, std::size_t max) noexcept {
        auto* header = ring_.header();
        std::size_t available = static_cast<std::size_t>(cached_tail_ - head_);
        if (available == 0) {
            cached_tail_ = header->tail.load(std::memory_order_acquire);
            available = static_cast<std::size_t>(cached_tail_ - head_);
        }
        const std::size_t n = available < max ? available : max;
        if (n == 0) {
            return 0;
        }
        for (std::size_t i = 0; i < n; ++i) {
            auto& slot = ring_.slots()[(head_ + i) & mask_];
            Ring<T, Sync>::SlotTraits::reading(slot);
            std::memcpy(&out[i], &Ring<T, Sync>::SlotTraits::value(slot), sizeof(T));
        }
        head_ += n;
        header->head.store(head_, std::memory_order_release);
        return n;
    }

    // Announces "sleeping" and re-checks. true: the consumer may sleep on the
    // doorbell; false: items arrived meanwhile (the flag is cleared again).
    bool arm() noexcept {
        auto* header = ring_.header();
        header->sleeping.store(1, std::memory_order_seq_cst);                // C1
        const std::uint64_t tail = header->tail.load(std::memory_order_seq_cst);  // C2
        if (tail != head_) {
            cached_tail_ = tail;
            header->sleeping.store(0, std::memory_order_relaxed);
            return false;
        }
        return true;
    }

    // After waking (the caller then drains the doorbell).
    void disarm() noexcept { ring_.header()->sleeping.store(0, std::memory_order_relaxed); }

    bool empty() noexcept {
        if (cached_tail_ != head_) {
            return false;
        }
        cached_tail_ = ring_.header()->tail.load(std::memory_order_acquire);
        return cached_tail_ == head_;
    }

private:
    Ring<T, Sync> ring_;
    std::size_t mask_;
    std::uint64_t head_;         // own copy of header->head
    std::uint64_t cached_tail_;  // last seen header->tail
};

}  // namespace fibers::ipc
