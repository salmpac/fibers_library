#pragma once

// A plain heap buffer as a context stack (no guard page): the context tests
// do not depend on fibers/stack.

#include <cstddef>
#include <memory>

#include "fibers/context/stack_view.hpp"

namespace context_test {

class TestStack {
public:
    static constexpr std::size_t kSize = 64 * 1024;

    fibers::context::StackView view() const noexcept { return {memory_.get(), kSize}; }
    std::size_t size() const noexcept { return kSize; }

private:
    std::unique_ptr<std::byte[]> memory_ = std::make_unique<std::byte[]>(kSize);
};

}  // namespace context_test
