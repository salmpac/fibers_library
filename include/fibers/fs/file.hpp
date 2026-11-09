#pragma once

// File I/O for fibers: the usual syscalls, with the fiber parked instead of
// the worker thread whenever the call could block.
//
//   fiber::go(scheduler, [&]() noexcept {
//       fs::File file;
//       if (IoResult r = file.open("data.bin", O_RDWR | O_CREAT | O_CLOEXEC, 0644); !r.ok()) {
//           ... r.error is the errno ...
//       }
//       IoResult n = file.pread(buf, 4096, 0);   // page cache hit: inline
//       file.pwrite(buf, 4096, 8192);
//       file.fdatasync();
//   });                                          // ~File closes (offloaded)
//
// Errors are values (blocking::IoResult: value >= 0 and error == 0, or
// value == -1 and error == errno), never exceptions; nothing allocates.
//
// Which calls run where
// ---------------------
//   pread      RWF_NOWAIT first, inline on the worker (nowait.hpp): served
//              from the page cache it never leaves the fiber; a short nowait
//              read is continued inline while it does not block. What would
//              block (or a filesystem / kernel without nowait reads) is
//              offloaded, only the remainder. The result is complete unless
//              EOF or an error cut it short.
//   pwrite     RWF_NOWAIT first only with a TRACKED filesystem handle (File
//              has one; free functions take it as `fs`) whose filesystem has
//              not answered "unsupported" yet; otherwise offloaded at once.
//              Rationale: on Linux 7.2 ext4 and tmpfs reject buffered nowait
//              writes (EOPNOTSUPP); with a tracked handle that costs one failed
//              syscall per filesystem, then one relaxed load per write, and
//              filesystems that do support them (xfs, btrfs) get the inline
//              path. Untracked, every write would pay a doomed syscall
//              (~0.2 us; see benchmarks/fs_bench.cpp), so it is skipped.
//              Like pwrite(2) the result may be short.
//   preadv / pwritev, fsync / fdatasync, open / close / fstat
//              always offloaded: vectored nowait would need a copy of the
//              iovec array to continue a short transfer, and the others have
//              no nowait form (open and close can block too: NFS, FUSE,
//              FIFOs, the last close of a file with dirty pages on some
//              filesystems; fstat on network filesystems).
//
// Outside a fiber of Sched, and when the scheduler's BlockingPool has been
// stopped, everything runs inline on the calling thread (fiber/blocking.hpp,
// this_fiber::blocking_op): the result is always the call's own result,
// never "cancelled".
//
// EINTR is retried (by the ops: pread, pwrite, fsync, open, ...), except for
// close: Linux releases the fd even when close() reports EINTR, so a retry
// could close an fd that another thread has just been given.

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#include <cerrno>
#include <cstddef>
#include <utility>

#include "fibers/blocking/nowait.hpp"
#include "fibers/blocking/ops.hpp"
#include "fibers/fiber/blocking.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fibers::fs {

using blocking::IoResult;
using fiber::BlockingScheduler;
using fiber::this_fiber::blocking_op;
using blocking::NowaitFs;
using blocking::NowaitSupport;

namespace detail {

// The write counterpart of blocking::try_complete_inline: true when
// op.result() is final, false when the rest (from op.transferred) must be
// offloaded.
template <class Sync>
bool try_write_inline(blocking::BasicPwriteOp<Sync>& op, NowaitFs fs, NowaitSupport& support) noexcept {
    for (;;) {
        if (op.transferred == op.len) {
            op.set_result(IoResult::success(static_cast<ssize_t>(op.len)));
            return true;
        }
        const IoResult r = support.write(op.fd, static_cast<const char*>(op.buf) + op.transferred,
                                         op.len - op.transferred, op.offset + static_cast<off_t>(op.transferred), fs);
        if (r.would_block() || (r.ok() && r.value == 0)) {
            return false;  // no progress inline: the pool does the rest
        }
        if (!r.ok()) {
            op.set_result(blocking::detail::with_prefix(op.transferred, -1, r.error));
            return true;
        }
        op.transferred += static_cast<std::size_t>(r.value);
    }
}

// preadv / pwritev (one call, EINTR retried).
struct VectorCall {
    int fd;
    const struct iovec* iov;
    int iovcnt;
    off_t offset;
    bool write;

    IoResult operator()() const noexcept {
        ssize_t ret;
        do {
            ret = write ? ::pwritev(fd, iov, iovcnt, offset) : ::preadv(fd, iov, iovcnt, offset);
        } while (ret < 0 && errno == EINTR);
        return IoResult::from_syscall(ret);
    }
};

struct OpenCall {
    const char* path;
    int flags;
    mode_t mode;
    NowaitFs* nowait;  // if set: the new file's filesystem (NowaitSupport::filesystem)

    IoResult operator()() const noexcept {
        int fd;
        do {
            fd = ::open(path, flags, mode);
        } while (fd < 0 && errno == EINTR);
        if (fd >= 0 && nowait != nullptr) {
            *nowait = blocking::process_nowait_support().filesystem(fd);
        }
        return IoResult::from_syscall(fd);
    }
};

struct CloseCall {
    int fd;
    IoResult operator()() const noexcept { return IoResult::from_syscall(::close(fd)); }  // no EINTR retry
};

struct FstatCall {
    int fd;
    struct stat* st;
    IoResult operator()() const noexcept { return IoResult::from_syscall(::fstat(fd, st)); }
};

template <class Sched, class Call>
IoResult blocking_call(Call call) noexcept {
    blocking::CallOp<Call, typename Sched::SyncPolicy> op(call);
    blocking_op<Sched>(op);
    return op.result();
}

}  // namespace detail

// ---- free functions on fds --------------------------------------------------
//
// `fs`: the file's NowaitSupport::Fs (support.filesystem(fd), once per open
// file), default untracked; `support`: whose verdicts to use (default the
// process-wide one). See nowait.hpp.

// pread(2), page cache first (see the top comment).
template <BlockingScheduler Sched = sched::Scheduler>
IoResult pread(int fd, void* buf, std::size_t len, off_t offset, NowaitFs fs = {},
               NowaitSupport& support = blocking::process_nowait_support()) noexcept {
    blocking::BasicPreadOp<typename Sched::SyncPolicy> op(fd, buf, len, offset);
    if (fiber::BasicFiber<Sched>::current() != nullptr && blocking::try_complete_inline(op, fs, support)) {
        return op.result();
    }
    blocking_op<Sched>(op);  // the rest (op.transferred); inline outside a fiber
    return op.result();
}

// pwrite(2): nowait inline only with a tracked `fs` (see the top comment).
template <BlockingScheduler Sched = sched::Scheduler>
IoResult pwrite(int fd, const void* buf, std::size_t len, off_t offset, NowaitFs fs = {},
                NowaitSupport& support = blocking::process_nowait_support()) noexcept {
    blocking::BasicPwriteOp<typename Sched::SyncPolicy> op(fd, buf, len, offset);
    if (fs.tracked() && support.write_supported(fs) && fiber::BasicFiber<Sched>::current() != nullptr &&
        detail::try_write_inline(op, fs, support)) {
        return op.result();
    }
    blocking_op<Sched>(op);
    return op.result();
}

// preadv(2) / pwritev(2): offloaded. The iovec array and the buffers stay
// the caller's (the fiber is parked meanwhile).
template <BlockingScheduler Sched = sched::Scheduler>
IoResult preadv(int fd, const struct iovec* iov, int iovcnt, off_t offset) noexcept {
    return detail::blocking_call<Sched>(detail::VectorCall{fd, iov, iovcnt, offset, false});
}

template <BlockingScheduler Sched = sched::Scheduler>
IoResult pwritev(int fd, const struct iovec* iov, int iovcnt, off_t offset) noexcept {
    return detail::blocking_call<Sched>(detail::VectorCall{fd, iov, iovcnt, offset, true});
}

// fsync(2) / fdatasync(2): offloaded; value 0 on success.
template <BlockingScheduler Sched = sched::Scheduler>
IoResult fsync(int fd) noexcept {
    blocking::BasicFsyncOp<typename Sched::SyncPolicy> op(fd, false);
    blocking_op<Sched>(op);
    return op.result();
}

template <BlockingScheduler Sched = sched::Scheduler>
IoResult fdatasync(int fd) noexcept {
    blocking::BasicFsyncOp<typename Sched::SyncPolicy> op(fd, true);
    blocking_op<Sched>(op);
    return op.result();
}

// open(2): offloaded; value is the new fd.
template <BlockingScheduler Sched = sched::Scheduler>
IoResult open(const char* path, int flags, mode_t mode = 0) noexcept {
    return detail::blocking_call<Sched>(detail::OpenCall{path, flags, mode, nullptr});
}

// close(2): offloaded, never retried (see the top comment).
template <BlockingScheduler Sched = sched::Scheduler>
IoResult close(int fd) noexcept {
    return detail::blocking_call<Sched>(detail::CloseCall{fd});
}

// fstat(2): offloaded; value 0 on success.
template <BlockingScheduler Sched = sched::Scheduler>
IoResult fstat(int fd, struct stat& st) noexcept {
    return detail::blocking_call<Sched>(detail::FstatCall{fd, &st});
}

// ---- File ------------------------------------------------------------------

// An owned fd plus its filesystem's RWF_NOWAIT handle (resolved by open() on
// the pool thread, so the fast paths use per-filesystem verdicts). Move-only.
// Every member is noexcept and returns errors as values. The destructor
// closes the fd (offloaded inside a fiber, so it may park), ignoring errors:
// call close() to see them. Like an fd, one File may serve concurrent
// pread / pwrite / fsync calls from several fibers; open, close, release and
// moves must not race with any other call on it.
template <BlockingScheduler Sched = sched::Scheduler>
class BasicFile {
public:
    BasicFile() noexcept = default;

    // Adopts `fd` (closed by this File). `nowait`: its filesystem handle if
    // known (process_nowait_support().filesystem(fd)), else untracked.
    explicit BasicFile(int fd, NowaitFs nowait = {}) noexcept : fd_(fd), nowait_(nowait) {}

    BasicFile(BasicFile&& other) noexcept
        : fd_(std::exchange(other.fd_, -1)), nowait_(std::exchange(other.nowait_, NowaitFs{})) {}

    BasicFile& operator=(BasicFile&& other) noexcept {
        if (this != &other) {
            static_cast<void>(close());
            fd_ = std::exchange(other.fd_, -1);
            nowait_ = std::exchange(other.nowait_, NowaitFs{});
        }
        return *this;
    }

    BasicFile(const BasicFile&) = delete;
    BasicFile& operator=(const BasicFile&) = delete;

    ~BasicFile() { static_cast<void>(close()); }

    // Closes the current file (if any), then opens `path` (offloaded, with
    // the filesystem lookup for the nowait paths). value: the fd.
    [[nodiscard]] IoResult open(const char* path, int flags, mode_t mode = 0) noexcept {
        static_cast<void>(close());
        NowaitFs nowait;
        const IoResult r = detail::blocking_call<Sched>(detail::OpenCall{path, flags, mode, &nowait});
        if (r.ok()) {
            fd_ = static_cast<int>(r.value);
            nowait_ = nowait;
        }
        return r;
    }

    // Offloaded. The File is closed afterwards whatever the result (the fd
    // is released even on error). No-op (success) if not open.
    IoResult close() noexcept {
        if (fd_ < 0) {
            return IoResult::success(0);
        }
        const int fd = std::exchange(fd_, -1);
        nowait_ = NowaitFs{};
        return fs::close<Sched>(fd);
    }

    IoResult pread(void* buf, std::size_t len, off_t offset) noexcept {
        return fs::pread<Sched>(fd_, buf, len, offset, nowait_);
    }
    IoResult pwrite(const void* buf, std::size_t len, off_t offset) noexcept {
        return fs::pwrite<Sched>(fd_, buf, len, offset, nowait_);
    }
    IoResult preadv(const struct iovec* iov, int iovcnt, off_t offset) noexcept {
        return fs::preadv<Sched>(fd_, iov, iovcnt, offset);
    }
    IoResult pwritev(const struct iovec* iov, int iovcnt, off_t offset) noexcept {
        return fs::pwritev<Sched>(fd_, iov, iovcnt, offset);
    }
    IoResult fsync() noexcept { return fs::fsync<Sched>(fd_); }
    IoResult fdatasync() noexcept { return fs::fdatasync<Sched>(fd_); }
    IoResult stat(struct stat& st) noexcept { return fs::fstat<Sched>(fd_, st); }

    // Gives up ownership: returns the fd (-1 if none); the File is closed.
    [[nodiscard]] int release() noexcept {
        nowait_ = NowaitFs{};
        return std::exchange(fd_, -1);
    }

    int fd() const noexcept { return fd_; }
    bool is_open() const noexcept { return fd_ >= 0; }
    NowaitFs nowait_fs() const noexcept { return nowait_; }

private:
    int fd_ = -1;
    NowaitFs nowait_;
};

using File = BasicFile<sched::Scheduler>;

}  // namespace fibers::fs
