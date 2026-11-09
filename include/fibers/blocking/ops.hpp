#pragma once

// Blocking operations with completion delivery: caller-owned objects that
// carry a call's parameters and result, a continuation Task and a Sink, plus
// the intrusive link by which BlockingPool queues them.
//
//   PreadOp op(fd, buf, len, offset);
//   op.set_continuation(resume_task, Sink::of(thread_pool));
//   blocking_pool.submit(op);
//   ... a pool thread runs pread, stores op.result(), then hands
//   resume_task to thread_pool.submit(); resume_task reads op.result().
//
// Nothing is allocated, and the pool's queue has no capacity: the op itself
// is the queue node. It lives wherever the caller puts it (a fiber's frame,
// an object pool). Lifetime: from submit() until its continuation runs (or
// is dropped). The pool does not touch the op after handing the continuation
// to the sink, so the continuation may free or resubmit it. An op must not
// be submitted again while it is still queued or running. A fire-and-forget
// op (no continuation) has no completion signal: the pool still stores its
// result after execute() returns, so such an op must stay valid until the
// pool is stopped (signalling from inside execute() is too early).
//
// Memory: the op's fields are published to the pool thread by the queue
// (release / acquire on the link). The result is written by the pool thread
// before the sink's submit; the sink (e.g. ThreadPool::submit) publishes the
// continuation with release / acquire, so the continuation sees the result.
//
// Dropped ops: if the pool drops the op (stop() with the op still queued, or
// a submit() after stop(), see BlockingPool), the op drops its continuation
// (Task::reset, i.e. its delete_ptr); the continuation never runs. A fiber
// layer that must always resume can give its continuation a delete_ptr that
// resumes with a "cancelled" status.
//
// User-defined blocking calls: derive from Op<YourOp> and give it
//   IoResult execute() noexcept;
// or wrap a callable in CallOp<F> (getaddrinfo, a blocking library, ...).
// There is no plain-Task submission: arbitrary work is a CallOp, which is
// caller-owned like every op, so the queue never needs storage.
//
// Sync policy: ops carry a Sync::Atomic link, so they are templates on the
// policy (Basic*Op<Sync>, Op<Derived, Sync>, CallOp<F, Sync>); the plain
// names (BlockingOp, PreadOp, ...) are the StdSync ones.
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
//     blocking_pool.submit(op): publishing before F is fully suspended would
//     let a fast completion resume F while it still runs. The op stays valid:
//     it lives on F's (suspended) stack. submit() never blocks and never
//     fails: no retry loop, no "queue full" status.
//  3. A BlockingPool thread runs pread (it may sleep on the disk), stores the
//     result and calls thread_pool.submit(resume F) (an external submit: it
//     never blocks; see ThreadPool).
//  4. Some worker runs the continuation, F resumes and reads op.result().
//  * Pool stopped (before or after the submit): the continuation is dropped
//    (its delete_ptr); the fiber layer decides whether that resumes F with
//    ECANCELED or destroys it. It is the same code path in both cases.

#include <sys/types.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <type_traits>
#include <utility>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/sync/sync.hpp"
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

template <sync::ParkingSyncPolicy Sync>
class BlockingPool;

namespace detail {

// The intrusive link. A base of every op, and the queue's stub node.
template <sync::SyncPolicy Sync>
struct OpLink {
    typename Sync::template Atomic<OpLink*> next{nullptr};
};

template <sync::SyncPolicy Sync>
class OpQueue;

}  // namespace detail

// Base of every op. Not movable: the pool holds a pointer to it.
template <sync::SyncPolicy Sync = sync::StdSync>
class BasicBlockingOp : private detail::OpLink<Sync> {
public:
    using ExecuteFn = IoResult (*)(BasicBlockingOp&) noexcept;

    BasicBlockingOp(const BasicBlockingOp&) = delete;
    BasicBlockingOp& operator=(const BasicBlockingOp&) = delete;

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

protected:
    explicit BasicBlockingOp(ExecuteFn execute) noexcept : execute_(execute) {}
    ~BasicBlockingOp() = default;

private:
    friend class detail::OpQueue<Sync>;
    template <sync::ParkingSyncPolicy S>
    friend class BlockingPool;

    // Runs the call, stores the result, delivers the continuation. The op
    // may be gone when this returns.
    void run() noexcept {
        result_ = execute_(*this);
        Task continuation = std::move(continuation_);
        if (!continuation) {
            return;
        }
        const Sink sink = sink_;
        if (!sink) {
            fail::fail_fast("BlockingOp: continuation without a sink");
        }
        // From here on the op may be gone: the continuation may free it.
        sink.submit(sink.target, std::move(continuation));
    }

    // The pool will not run the op: drop its continuation (delete_ptr).
    void drop() noexcept { continuation_.reset(); }

    ExecuteFn execute_;
    Task continuation_;
    Sink sink_;
    IoResult result_;
};

using BlockingOp = BasicBlockingOp<sync::StdSync>;

// CRTP helper: Derived provides `IoResult execute() noexcept`.
template <class Derived, sync::SyncPolicy Sync = sync::StdSync>
class Op : public BasicBlockingOp<Sync> {
protected:
    Op() noexcept
        : BasicBlockingOp<Sync>([](BasicBlockingOp<Sync>& self) noexcept -> IoResult {
              return static_cast<Derived&>(self).execute();
          }) {}
};

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

template <sync::SyncPolicy Sync = sync::StdSync>
class BasicPreadOp final : public Op<BasicPreadOp<Sync>, Sync> {
public:
    BasicPreadOp() noexcept = default;
    BasicPreadOp(int fd_, void* buf_, std::size_t len_, off_t offset_) noexcept
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

template <sync::SyncPolicy Sync = sync::StdSync>
class BasicPwriteOp final : public Op<BasicPwriteOp<Sync>, Sync> {
public:
    BasicPwriteOp() noexcept = default;
    BasicPwriteOp(int fd_, const void* buf_, std::size_t len_, off_t offset_) noexcept
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
template <sync::SyncPolicy Sync = sync::StdSync>
class BasicFsyncOp final : public Op<BasicFsyncOp<Sync>, Sync> {
public:
    BasicFsyncOp() noexcept = default;
    explicit BasicFsyncOp(int fd_, bool datasync_ = false) noexcept : fd(fd_), datasync(datasync_) {}

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

using PreadOp = BasicPreadOp<sync::StdSync>;
using PwriteOp = BasicPwriteOp<sync::StdSync>;
using FsyncOp = BasicFsyncOp<sync::StdSync>;

// Any blocking callable: F() -> IoResult, or F() -> void (result {0, 0}).
// Inputs and outputs beyond IoResult live in the callable's captures, i.e.
// in caller-owned memory, e.g. for getaddrinfo:
//   CallOp op([&] { return IoResult{0, ::getaddrinfo(host, port, &hints, &res)}; });
// (getaddrinfo's EAI_* codes are not errno values: interpret them yourself.)
// F must not throw (the call is noexcept: a throw terminates).
template <class F, sync::SyncPolicy Sync = sync::StdSync>
class CallOp final : public Op<CallOp<F, Sync>, Sync> {
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

template <class F>
CallOp(F) -> CallOp<F>;

}  // namespace fibers::blocking
