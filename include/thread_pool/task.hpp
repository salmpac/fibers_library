#pragma once

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
// The pool never allocates: where memo lives (object pools, fiber structs,
// ...) is entirely the caller's business.
class Task {
public:
    using CallFn = void (*)(void*) noexcept;
    using DeleteFn = void (*)(void*) noexcept;

    // The three fields as plain data, without ownership semantics. Lets
    // lock-free containers keep a task in atomic words (see
    // LockFreeSPMCQueue): release() hands ownership out as a Raw, from_raw()
    // takes it back. Copying a Raw does not copy ownership; exactly one copy
    // may be turned back into a Task.
    struct Raw {
        void* memo = nullptr;
        CallFn call_ptr = nullptr;
        DeleteFn delete_ptr = nullptr;
    };

    Task() noexcept = default;

    Task(void* memo, CallFn call_ptr, DeleteFn delete_ptr) noexcept
        : memo_(memo), call_ptr_(call_ptr), delete_ptr_(delete_ptr) {}

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

    // Gives up ownership WITHOUT calling delete_ptr; the Task becomes empty.
    // The caller now owns the returned fields (call_ptr == nullptr iff the
    // Task was empty).
    [[nodiscard]] Raw release() noexcept {
        return Raw{memo_, std::exchange(call_ptr_, nullptr), delete_ptr_};
    }

    // Takes ownership of `raw` (typically obtained from release()).
    static Task from_raw(Raw raw) noexcept { return Task(raw.memo, raw.call_ptr, raw.delete_ptr); }

    // Drops the task without executing it; the Task becomes empty.
    void reset() noexcept {
        if (std::exchange(call_ptr_, nullptr) != nullptr && delete_ptr_ != nullptr) {
            delete_ptr_(memo_);
        }
    }

private:
    void* memo_ = nullptr;
    CallFn call_ptr_ = nullptr;
    DeleteFn delete_ptr_ = nullptr;
};

}  // namespace thread_pool
