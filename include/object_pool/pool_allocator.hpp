#pragma once

// Memory source of the object pools (sharded_pool.hpp).
//
// A pool allocator hands out raw, aligned bytes:
//
//   void* allocate(std::size_t bytes, std::size_t alignment);
//       returns storage for `bytes` bytes aligned to `alignment` (a power of
//       two, possibly above __STDCPP_DEFAULT_NEW_ALIGNMENT__), or throws;
//       never returns nullptr.
//   void deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept;
//       `p`, `bytes` and `alignment` are exactly those of one earlier
//       allocate() call on the same allocator object.
//
// These are the semantics of std::pmr::memory_resource::allocate/deallocate,
// but resolved statically: the allocator is a template parameter of the pool,
// stored once in it ([[no_unique_address]], so an empty allocator costs
// nothing) and called directly, without a virtual call. Stateful allocators
// are passed to the pool constructor; the pool keeps that very instance (it is
// moved in, never copied afterwards).
//
// The pool allocates only at construction and when it grows, and frees
// everything at destruction (objects are type-stable), so the allocator is
// never called on the acquire/release fast path.
//
// Ready-made allocators:
//   NewDeleteAllocator - aligned ::operator new / ::operator delete (default);
//   PmrAllocator       - adapter for a std::pmr::memory_resource*.

#include <concepts>
#include <cstddef>
#include <memory_resource>
#include <new>

namespace object_pool {

template <class A>
concept PoolAllocator =
    std::move_constructible<A> && requires(A& a, void* p, std::size_t n) {
        { a.allocate(n, n) } -> std::same_as<void*>;
        { a.deallocate(p, n, n) } noexcept;
    };

// Global aligned operator new / delete.
struct NewDeleteAllocator {
    static void* allocate(std::size_t bytes, std::size_t alignment) {
        return ::operator new(bytes, std::align_val_t{alignment});
    }
    static void deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept {
        ::operator delete(p, bytes, std::align_val_t{alignment});
    }
};

using DefaultAllocator = NewDeleteAllocator;

// Adapter for a std::pmr::memory_resource. The resource is not owned and must
// outlive the pool. Default: std::pmr::get_default_resource() at construction.
class PmrAllocator {
public:
    PmrAllocator() noexcept : resource_(std::pmr::get_default_resource()) {}
    PmrAllocator(std::pmr::memory_resource* resource) noexcept  // NOLINT: implicit on purpose
        : resource_(resource) {}

    void* allocate(std::size_t bytes, std::size_t alignment) {
        return resource_->allocate(bytes, alignment);
    }
    void deallocate(void* p, std::size_t bytes, std::size_t alignment) noexcept {
        resource_->deallocate(p, bytes, alignment);
    }

    std::pmr::memory_resource* resource() const noexcept { return resource_; }

private:
    std::pmr::memory_resource* resource_;
};

static_assert(PoolAllocator<NewDeleteAllocator>);
static_assert(PoolAllocator<PmrAllocator>);

}  // namespace object_pool
