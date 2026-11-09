#pragma once

// Stack: an owning fiber stack, an mmap'ed region with a guard page.
//
//   high  +--------------------+  view().data() + view().size()
//         |  usable (RW)       |  stack grows down from here
//         +--------------------+  view().data()
//         |  guard page (NONE) |  an overflow faults here (SIGSEGV)
//   low   +--------------------+
//
// MAP_NORESERVE: pages are committed when first touched, so a large stack
// costs address space, not memory, until the fiber uses it. MAP_STACK marks
// it as a stack for the kernel. Out of memory (mmap or mprotect failing)
// is a fail_fast.
//
// The guard page catches running off the end in steps smaller than a page;
// a single frame larger than a page could skip it (compilers probe large
// frames with -fstack-clash-protection, on in the nix toolchain).

#include <sys/mman.h>
#include <unistd.h>

#include <algorithm>
#include <cstddef>
#include <utility>

#include "fibers/context/stack_view.hpp"
#include "fibers/fail/fail_fast.hpp"

namespace fibers::stack {

using context::StackView;

class Stack {
public:
    static constexpr std::size_t kDefaultSize = 64 * 1024;

    // An empty stack (no mapping): empty() is true, view() is empty.
    Stack() noexcept = default;

    // Maps a stack with at least `size` usable bytes (rounded up to whole
    // pages) and a guard page below them.
    static Stack allocate(std::size_t size = kDefaultSize) noexcept {
        const std::size_t page = page_size();
        const std::size_t usable = (std::max<std::size_t>(size, 1) + page - 1) / page * page;
        const std::size_t length = usable + page;
        void* memory = ::mmap(nullptr, length, PROT_READ | PROT_WRITE,
                              MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE | MAP_STACK, -1, 0);
        if (memory == MAP_FAILED) [[unlikely]] {
            fail::fail_fast("stack: mmap of a fiber stack failed");
        }
        if (::mprotect(memory, page, PROT_NONE) != 0) [[unlikely]] {
            fail::fail_fast("stack: mprotect of a stack guard page failed");
        }
        return Stack(static_cast<std::byte*>(memory), length, page);
    }

    Stack(Stack&& other) noexcept
        : mapping_(std::exchange(other.mapping_, nullptr)),
          length_(std::exchange(other.length_, 0)),
          guard_(std::exchange(other.guard_, 0)) {}

    Stack& operator=(Stack&& other) noexcept {
        if (this != &other) {
            unmap();
            mapping_ = std::exchange(other.mapping_, nullptr);
            length_ = std::exchange(other.length_, 0);
            guard_ = std::exchange(other.guard_, 0);
        }
        return *this;
    }

    Stack(const Stack&) = delete;
    Stack& operator=(const Stack&) = delete;

    ~Stack() { unmap(); }

    bool empty() const noexcept { return mapping_ == nullptr; }

    // The usable part (without the guard page).
    StackView view() const noexcept {
        return empty() ? StackView{} : StackView{mapping_ + guard_, length_ - guard_};
    }

    // Usable bytes.
    std::size_t size() const noexcept { return length_ - guard_; }

    // The whole mapping, guard page included (e.g. to check a faulting
    // address in a signal handler).
    const std::byte* mapping() const noexcept { return mapping_; }
    std::size_t guard_size() const noexcept { return guard_; }

    static std::size_t page_size() noexcept {
        return static_cast<std::size_t>(::sysconf(_SC_PAGESIZE));
    }

private:
    Stack(std::byte* mapping, std::size_t length, std::size_t guard) noexcept
        : mapping_(mapping), length_(length), guard_(guard) {}

    void unmap() noexcept {
        if (mapping_ != nullptr) {
            ::munmap(mapping_, length_);
            mapping_ = nullptr;
            length_ = 0;
            guard_ = 0;
        }
    }

    std::byte* mapping_ = nullptr;
    std::size_t length_ = 0;  // whole mapping
    std::size_t guard_ = 0;   // guard bytes at the low end
};

}  // namespace fibers::stack
