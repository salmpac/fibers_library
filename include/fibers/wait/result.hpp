#pragma once

// Result<T>: a value or an error code, without exceptions (std::expected's
// shape, C++20). Returned by the one-shot hand-offs (fiber::Oneshot,
// fiber::Future) and meant for later waiting primitives too.
//
//   wait::Result<Reply> r = future.get();
//   if (!r) { ... r.error() == wait::Error::kBroken ... }
//   use(*r);  // or r.value(), r->field
//
// Accessing the value of an error (or the error of a value) fails fast.
// T must be nothrow-move-constructible and nothrow-destructible (Result is
// moved around in noexcept code).
//
// Result<void>: success or an error, no value (what Oneshot<void> /
// Future<void> return): has_value() / operator bool / error(); value()
// only checks (fails fast on an error).
//
//   wait::Result<void> r = done.get();
//   if (!r) { ... r.error() ... }

#include <cstdint>
#include <optional>
#include <type_traits>
#include <utility>

#include "fibers/fail/fail_fast.hpp"

namespace fibers::wait {

enum class Error : std::uint8_t {
    // The producer side went away without delivering a value (a Promise
    // destroyed or abandon()ed before set()).
    kBroken = 1,
    // The wait's context was cancelled (fiber::BasicContext::cancel(), or
    // a parent context's cancellation).
    kCancelled = 2,
    // The wait's context reached its deadline.
    kTimedOut = 3,
};

inline const char* to_string(Error error) noexcept {
    switch (error) {
        case Error::kBroken:
            return "broken promise";
        case Error::kCancelled:
            return "cancelled";
        case Error::kTimedOut:
            return "timed out";
    }
    return "unknown error";
}

template <class T>
class [[nodiscard]] Result {
    static_assert(std::is_object_v<T> && !std::is_array_v<T>, "wait::Result<T>: T must be an object type");
    static_assert(std::is_nothrow_move_constructible_v<T>, "wait::Result<T>: T must be nothrow-move-constructible");
    static_assert(std::is_nothrow_destructible_v<T>, "wait::Result<T>: T must be nothrow-destructible");

public:
    using value_type = T;

    template <class... Args>
        requires std::is_nothrow_constructible_v<T, Args&&...>
    explicit Result(std::in_place_t, Args&&... args) noexcept : value_(std::in_place, std::forward<Args>(args)...) {}

    // An error (implicit: `return Error::kBroken;`).
    Result(Error error) noexcept : error_(error) {}  // NOLINT(google-explicit-constructor)

    Result(Result&&) noexcept = default;
    Result& operator=(Result&&) noexcept = default;
    Result(const Result&) = default;
    Result& operator=(const Result&) = default;

    bool has_value() const noexcept { return value_.has_value(); }
    explicit operator bool() const noexcept { return has_value(); }

    T& value() & noexcept { return checked(); }
    const T& value() const& noexcept { return const_cast<Result*>(this)->checked(); }
    T&& value() && noexcept { return std::move(checked()); }

    T& operator*() & noexcept { return checked(); }
    const T& operator*() const& noexcept { return const_cast<Result*>(this)->checked(); }
    T&& operator*() && noexcept { return std::move(checked()); }
    T* operator->() noexcept { return &checked(); }
    const T* operator->() const noexcept { return &const_cast<Result*>(this)->checked(); }

    // The error; fails fast if there is a value.
    Error error() const noexcept {
        if (value_.has_value()) [[unlikely]] {
            fail::fail_fast("wait: Result::error() on a value");
        }
        return error_;
    }

private:
    T& checked() noexcept {
        if (!value_.has_value()) [[unlikely]] {
            fail::fail_fast("wait: Result::value() on an error");
        }
        return *value_;
    }

    std::optional<T> value_;
    Error error_ = Error::kBroken;  // meaningful only without a value
};

template <>
class [[nodiscard]] Result<void> {
public:
    using value_type = void;

    // Success.
    Result() noexcept = default;
    explicit Result(std::in_place_t) noexcept {}

    // An error (implicit: `return Error::kBroken;`).
    Result(Error error) noexcept : ok_(false), error_(error) {}  // NOLINT(google-explicit-constructor)

    bool has_value() const noexcept { return ok_; }
    explicit operator bool() const noexcept { return has_value(); }

    // Fails fast on an error; nothing to return otherwise.
    void value() const noexcept {
        if (!ok_) [[unlikely]] {
            fail::fail_fast("wait: Result::value() on an error");
        }
    }
    void operator*() const noexcept { value(); }

    // The error; fails fast on success.
    Error error() const noexcept {
        if (ok_) [[unlikely]] {
            fail::fail_fast("wait: Result::error() on a value");
        }
        return error_;
    }

private:
    bool ok_ = true;
    Error error_ = Error::kBroken;  // meaningful only without a value
};

}  // namespace fibers::wait
