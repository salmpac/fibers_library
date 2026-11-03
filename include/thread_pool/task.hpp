#pragma once

#include <concepts>
#include <type_traits>
#include <utility>

namespace thread_pool {

// Type-erased unit of work: { memo, call_ptr, delete_ptr }.
//
// Contract:
//   * call_ptr(memo)   executes the task AND releases memo. Must not throw.
//   * delete_ptr(memo) releases memo of a task that was never executed
//                      (e.g. dropped on ThreadPool shutdown). Must not throw.
//                      May be nullptr if there is nothing to release.
//   * A Task is empty iff call_ptr == nullptr. memo itself may be nullptr.
//
// Task is move-only; destroying a non-empty Task calls delete_ptr.
class Task {
public:
    using CallFn = void (*)(void*) noexcept;
    using DeleteFn = void (*)(void*) noexcept;

    Task() noexcept = default;

    Task(void* memo, CallFn call_ptr, DeleteFn delete_ptr) noexcept
        : memo_(memo), call_ptr_(call_ptr), delete_ptr_(delete_ptr) {}

    // Heap-allocates a copy of `f`. If `f` throws, std::terminate is called.
    template <class F>
        requires(!std::same_as<std::decay_t<F>, Task>) &&
                std::invocable<std::decay_t<F>&>
    static Task make(F&& f) {
        using Fn = std::decay_t<F>;
        return Task(new Fn(std::forward<F>(f)), &call_impl<Fn>, &delete_impl<Fn>);
    }

    Task(const Task&) = delete;
    Task& operator=(const Task&) = delete;

    Task(Task&& other) noexcept
        : memo_(other.memo_),
          call_ptr_(std::exchange(other.call_ptr_, nullptr)),
          delete_ptr_(other.delete_ptr_) {}

    Task& operator=(Task&& other) noexcept {
        if (this != &other) {
            reset();
            memo_ = other.memo_;
            call_ptr_ = std::exchange(other.call_ptr_, nullptr);
            delete_ptr_ = other.delete_ptr_;
        }
        return *this;
    }

    ~Task() { reset(); }

    explicit operator bool() const noexcept { return call_ptr_ != nullptr; }

    // Executes the task; the Task becomes empty. Precondition: non-empty.
    void run() && noexcept { std::exchange(call_ptr_, nullptr)(memo_); }

    // Drops the task without executing it; the Task becomes empty.
    void reset() noexcept {
        if (std::exchange(call_ptr_, nullptr) != nullptr && delete_ptr_ != nullptr) {
            delete_ptr_(memo_);
        }
    }

private:
    template <class Fn>
    static void call_impl(void* memo) noexcept {
        Fn* fn = static_cast<Fn*>(memo);
        (*fn)();
        delete fn;
    }

    template <class Fn>
    static void delete_impl(void* memo) noexcept {
        delete static_cast<Fn*>(memo);
    }

    void* memo_ = nullptr;
    CallFn call_ptr_ = nullptr;
    DeleteFn delete_ptr_ = nullptr;
};

}  // namespace thread_pool
