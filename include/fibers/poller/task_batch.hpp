#pragma once

// Delivery of work from reactor callbacks to the outside world (normally a
// ThreadPool).
//
// Callbacks run on the poller thread and must be tiny: typically they turn
// readiness into a Task (e.g. "resume this fiber") and push() it into the
// TaskBatch they are handed. The poller flushes the batch with ONE
// submit_batch per loop iteration (and whenever it fills up), so a burst of
// readiness costs one pool submission, not one per event.

#include <cstddef>
#include <utility>

#include "fibers/thread_pool/task.hpp"

namespace fibers::poller {

using thread_pool::Task;

// Type-erased sink: anything with submit_batch(Task*, std::size_t) noexcept
// that moves from (i.e. takes ownership of) all `count` tasks.
struct TaskSink {
    using SubmitFn = void (*)(void* target, Task* tasks, std::size_t count) noexcept;

    void* target = nullptr;
    SubmitFn submit = nullptr;

    // TaskSink::of(pool) for a ThreadPool (or any type with submit_batch).
    template <class T>
    static TaskSink of(T& target) noexcept {
        return TaskSink{&target, [](void* t, Task* tasks, std::size_t count) noexcept {
                            static_cast<T*>(t)->submit_batch(tasks, count);
                        }};
    }
};

// Fixed-size batch of tasks headed for a TaskSink. Not thread-safe: owned by
// the poller thread.
class TaskBatch {
public:
    static constexpr std::size_t kCapacity = 64;

    explicit TaskBatch(TaskSink sink) noexcept : sink_(sink) {}

    TaskBatch(const TaskBatch&) = delete;
    TaskBatch& operator=(const TaskBatch&) = delete;

    ~TaskBatch() { flush(); }

    // Appends `task` (empty tasks are ignored); flushes first when full.
    void push(Task task) noexcept {
        if (!task) {
            return;
        }
        if (size_ == kCapacity) {
            flush();
        }
        tasks_[size_++] = std::move(task);
    }

    // Hands everything collected so far to the sink.
    void flush() noexcept {
        if (size_ == 0) {
            return;
        }
        const std::size_t count = std::exchange(size_, 0);
        sink_.submit(sink_.target, tasks_, count);
        for (std::size_t i = 0; i < count; ++i) {
            tasks_[i].reset();  // no-op unless the sink left something behind
        }
    }

    std::size_t size() const noexcept { return size_; }

private:
    TaskSink sink_;
    std::size_t size_ = 0;
    Task tasks_[kCapacity];
};

}  // namespace fibers::poller
