#pragma once

// Blocking operations with completion delivery: caller-owned objects that
// carry a call's parameters and result plus a continuation Task and a Sink.
//
//   PreadOp op(fd, buf, len, offset);
//   op.set_continuation(resume_task, Sink::of(thread_pool));
//   if (!try_submit_op(blocking_pool, op)) { /* full: retry later */ }
//   ... a pool thread runs pread, stores op.result(), then hands
//   resume_task to thread_pool.submit(); resume_task reads op.result().
//
// Nothing is allocated: the op lives wherever the caller puts it (a fiber's
// frame, an object pool). Lifetime: from submission until its continuation
// runs (or is dropped). The pool thread does not touch the op after handing
// the continuation to the sink, so the continuation may free it.
//
// Memory: the result is written by the pool thread before the sink's submit;
// the sink (e.g. ThreadPool::submit) publishes the continuation with release
// / acquire, so the continuation sees the result.
//
// Dropped ops: if the pool drops the op's Task (stop() with the op still
// queued), the op drops its continuation (Task::reset, i.e. its delete_ptr);
// the continuation never runs. A fiber layer that must always resume can give
// its continuation a delete_ptr that resumes with a "cancelled" status.
//
// User-defined blocking calls: derive from Op<YourOp> and give it
//   IoResult execute() noexcept;
// or wrap a callable in CallOp<F> (getaddrinfo, a blocking library, ...).
//
// Fiber integration (intended flow; the fiber layer does not exist yet)
// ----------------------------------------------------------------------
// A fiber F running on a worker of the main ThreadPool calls, say,
// read(fd, buf, len, offset):
//  1. Fast path (nowait.hpp): PreadOp op on F's stack; try_complete_inline(op)
//     with preadv2(RWF_NOWAIT). Page-cache hit: return op.result() at once,
//     F never suspends.
//  2. Would block: op.set_continuation(<Task that resumes F>,
//     Sink::of(thread_pool)) and suspend F. Only AFTER the context switch
//     (in the scheduler's post-switch hook, on the worker's own stack)
//     try_submit_op(blocking_pool, op): publishing before F is fully
//     suspended would let a fast completion resume F while it still runs.
//     The op stays valid: it lives on F's (suspended) stack.
//  3. A BlockingPool thread runs pread (it may sleep on the disk), stores the
//     result and calls thread_pool.submit(resume F) (an external submit: it
//     never blocks; see ThreadPool).
//  4. Some worker runs the continuation, F resumes and reads op.result().
//  * Queue full (try_submit_op returned false in step 2): resume F with a
//    "retry" status; F yields to the scheduler and tries again. Its worker
//    keeps running other fibers: nothing blocks.
//  * Pool stopped with the op queued: the continuation is dropped (its
//    delete_ptr); the fiber layer decides whether that resumes F with
//    ECANCELED or destroys it.

#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <type_traits>
#include <utility>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/thread_pool/task.hpp"

namespace fibers::blocking {

using thread_pool::Task;

// Syscall-style result: value >= 0 and error == 0 on success, value == -1 and
// error == errno on failure.
struct IoResult {
    ssize_t value = 0;
    int error = 0;

    bool ok() const noexcept { return error == 0; }
    bool would_block() const noexcept { return error == EAGAIN; }

    static IoResult success(ssize_t value) noexcept { return IoResult{value, 0}; }
    static IoResult failure(int error) noexcept { return IoResult{-1, error}; }
    // From a syscall's return value (reads errno on failure).
    static IoResult from_syscall(ssize_t ret) noexcept { return ret >= 0 ? success(ret) : failure(errno); }
};

// Where a completed op hands its continuation, one Task at a time.
struct Sink {
    using SubmitFn = void (*)(void* target, Task task) noexcept;

    void* target = nullptr;
    SubmitFn submit = nullptr;

    explicit operator bool() const noexcept { return submit != nullptr; }

    // Sink::of(pool) for a ThreadPool (or any type with submit(Task) noexcept).
    template <class T>
    static Sink of(T& target) noexcept {
        return Sink{&target, [](void* t, Task task) noexcept { static_cast<T*>(t)->submit(std::move(task)); }};
    }

    // Runs the continuation right on the pool thread (tests, or continuations
    // that are themselves tiny and thread-agnostic, e.g. "signal an eventfd").
    static Sink run_inline() noexcept {
        return Sink{nullptr, [](void*, Task task) noexcept { std::move(task).run(); }};
    }
};

// Base of every op. Not movable: the pool holds a pointer to it.
class BlockingOp {
public:
    using ExecuteFn = IoResult (*)(BlockingOp&) noexcept;

    BlockingOp(const BlockingOp&) = delete;
    BlockingOp& operator=(const BlockingOp&) = delete;

    // What to deliver, and where, once the call has returned. An empty
    // continuation means fire-and-forget. Set before submission.
    void set_continuation(Task continuation, Sink sink) noexcept {
        continuation_ = std::move(continuation);
        sink_ = sink;
    }

    // Takes the continuation back (e.g. after the op completed inline).
    Task take_continuation() noexcept { return std::move(continuation_); }

    // Valid once the continuation runs (or after an inline completion).
    const IoResult& result() const noexcept { return result_; }

    // Records a result without running the call (inline fast paths).
    void set_result(IoResult result) noexcept { result_ = result; }

    // The Task to hand to a BlockingPool (memo = this op). Running it runs
    // the call, stores the result and delivers the continuation; dropping it
    // drops the continuation. Prefer try_submit_op(), which does not drop the
    // op's continuation when the pool refuses the task.
    Task task() noexcept { return Task(this, &call, &drop); }

protected:
    explicit BlockingOp(ExecuteFn execute) noexcept : execute_(execute) {}
    ~BlockingOp() = default;

private:
    static void call(void* memo) noexcept {
        auto* op = static_cast<BlockingOp*>(memo);
        op->result_ = op->execute_(*op);
        Task continuation = std::move(op->continuation_);
        if (!continuation) {
            return;
        }
        const Sink sink = op->sink_;
        if (!sink) {
            fail::fail_fast("BlockingOp: continuation without a sink");
        }
        // From here on the op may be gone: the continuation may free it.
        sink.submit(sink.target, std::move(continuation));
    }

    static void drop(void* memo) noexcept { static_cast<BlockingOp*>(memo)->continuation_.reset(); }

    ExecuteFn execute_;
    Task continuation_;
    Sink sink_;
    IoResult result_;
};

// CRTP helper: Derived provides `IoResult execute() noexcept`.
template <class Derived>
class Op : public BlockingOp {
protected:
    Op() noexcept
        : BlockingOp([](BlockingOp& self) noexcept -> IoResult { return static_cast<Derived&>(self).execute(); }) {}
};

// Submits `op` to `pool` (anything with try_submit(Task&&) -> bool). On false
// (full / stopped) nothing happened: the op and its continuation are intact.
template <class Pool>
[[nodiscard]] bool try_submit_op(Pool& pool, BlockingOp& op) noexcept {
    Task task = op.task();
    if (pool.try_submit(std::move(task))) {
        return true;
    }
    static_cast<void>(task.release());  // the op stays with the caller: do not drop it
    return false;
}

// ---------------------------------------------------------------------------
// Ready-made ops. Each runs ONE syscall (retried on EINTR) with its usual
// semantics: a read may be short (at EOF, or for pipes / sockets).
//
// `transferred`: bytes of the request already done before the op ran (the
// RWF_NOWAIT fast path may have done a prefix inline, see nowait.hpp). The
// call covers the rest, [buf + transferred, len) at offset + transferred,
// and result().value reports the whole request: transferred + n, or just
// transferred if the call failed after a non-empty prefix (pread semantics).

namespace detail {
inline IoResult with_prefix(std::size_t transferred, ssize_t ret, int error) noexcept {
    if (ret >= 0) {
        return IoResult::success(static_cast<ssize_t>(transferred) + ret);
    }
    return transferred > 0 ? IoResult::success(static_cast<ssize_t>(transferred)) : IoResult::failure(error);
}
}  // namespace detail

class PreadOp final : public Op<PreadOp> {
public:
    PreadOp() noexcept = default;
    PreadOp(int fd_, void* buf_, std::size_t len_, off_t offset_) noexcept
        : fd(fd_), buf(buf_), len(len_), offset(offset_) {}

    int fd = -1;
    void* buf = nullptr;
    std::size_t len = 0;
    off_t offset = 0;
    std::size_t transferred = 0;

    IoResult execute() noexcept {
        ssize_t ret;
        do {
            ret = ::pread(fd, static_cast<char*>(buf) + transferred, len - transferred,
                          offset + static_cast<off_t>(transferred));
        } while (ret < 0 && errno == EINTR);
        return detail::with_prefix(transferred, ret, ret < 0 ? errno : 0);
    }
};

class PwriteOp final : public Op<PwriteOp> {
public:
    PwriteOp() noexcept = default;
    PwriteOp(int fd_, const void* buf_, std::size_t len_, off_t offset_) noexcept
        : fd(fd_), buf(buf_), len(len_), offset(offset_) {}

    int fd = -1;
    const void* buf = nullptr;
    std::size_t len = 0;
    off_t offset = 0;
    std::size_t transferred = 0;

    IoResult execute() noexcept {
        ssize_t ret;
        do {
            ret = ::pwrite(fd, static_cast<const char*>(buf) + transferred, len - transferred,
                           offset + static_cast<off_t>(transferred));
        } while (ret < 0 && errno == EINTR);
        return detail::with_prefix(transferred, ret, ret < 0 ? errno : 0);
    }
};

// fsync, or fdatasync with datasync = true. result().value is 0 on success.
class FsyncOp final : public Op<FsyncOp> {
public:
    FsyncOp() noexcept = default;
    explicit FsyncOp(int fd_, bool datasync_ = false) noexcept : fd(fd_), datasync(datasync_) {}

    int fd = -1;
    bool datasync = false;

    IoResult execute() noexcept {
        int ret;
        do {
            ret = datasync ? ::fdatasync(fd) : ::fsync(fd);
        } while (ret < 0 && errno == EINTR);
        return IoResult::from_syscall(ret);
    }
};

// Any blocking callable: F() -> IoResult, or F() -> void (result {0, 0}).
// Inputs and outputs beyond IoResult live in the callable's captures, i.e.
// in caller-owned memory, e.g. for getaddrinfo:
//   CallOp op([&] { return IoResult{0, ::getaddrinfo(host, port, &hints, &res)}; });
// (getaddrinfo's EAI_* codes are not errno values: interpret them yourself.)
// F must not throw (the call is noexcept: a throw terminates).
template <class F>
class CallOp final : public Op<CallOp<F>> {
    static_assert(std::is_invocable_v<F&>, "CallOp: F must be callable without arguments");

public:
    explicit CallOp(F fn) noexcept(std::is_nothrow_move_constructible_v<F>) : fn_(std::move(fn)) {}

    IoResult execute() noexcept {
        if constexpr (std::is_same_v<std::invoke_result_t<F&>, IoResult>) {
            return fn_();
        } else {
            fn_();
            return IoResult{};
        }
    }

private:
    F fn_;
};

}  // namespace fibers::blocking
