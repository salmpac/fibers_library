#pragma once

#include <cstddef>
#include <span>

namespace fibers::context {

// The memory a context runs on: [data(), data() + size()), growing down from
// the end. Not owned (see fibers/stack for owning stacks). Same type as
// ::sure::StackView, so it is handed to the Twist adapter unchanged.
using StackView = std::span<std::byte>;

}  // namespace fibers::context
