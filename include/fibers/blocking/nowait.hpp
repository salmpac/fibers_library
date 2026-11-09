#pragma once

// RWF_NOWAIT fast path: try a file read on the calling (worker) thread
// without blocking, and offload to the BlockingPool only if it would block.
//
// preadv2(..., RWF_NOWAIT) (Linux >= 4.14) copies what is in the page cache
// and returns EAGAIN instead of waiting for the disk. A 4 KiB read from the
// page cache costs about a microsecond inline; the round trip through the
// pool (two thread wakeups) costs tens of microseconds.
//
// Unsupported: ENOSYS (no preadv2), EOPNOTSUPP (kernel without RWF_NOWAIT, or
// a file / filesystem that does not support it) and EINVAL (some kernels and
// emulators reject unknown flags; we pre-check our own arguments, so EINVAL
// from a valid request means the flag) mark the NowaitSupport "unsupported"
// in a relaxed atomic. From then on every attempt returns "would block" at
// once (one atomic load, no syscall): the caller offloads, as it would on
// EAGAIN. The default NowaitSupport is process-wide, so one file on a
// filesystem without RWF_NOWAIT disables the fast path for every file. That
// is common: tmpfs (/tmp, /dev/shm) answers EOPNOTSUPP to RWF_NOWAIT reads
// (observed on Linux 7.2) while ext4 supports them. Callers that mix
// filesystems should keep a NowaitSupport per filesystem (st_dev) or per file
// and pass it explicitly. (Not detectable from the error alone: the
// O_DIRECT misalignment EINVAL also marks it; do not use the fast path with
// misaligned O_DIRECT buffers.)
//
// The atomic is a hint, not synchronisation: nothing is published through it
// and any value is safe (a stale "supported" costs one failed syscall), so it
// is a plain std::atomic rather than a Sync-policy one.
//
// Results: a nowait read may be SHORT before EOF (the first pages were cached,
// the next ones were not). try_read_nowait returns that short count as is;
// try_complete_inline / read_nowait_or_offload continue it (inline while it
// does not block, else offload the rest), so their result is a normal pread
// result.
//
// pwritev2(RWF_NOWAIT) (try_write_nowait): for buffered writes most kernels
// and filesystems return EAGAIN (or EOPNOTSUPP on older ones) whenever the
// write would need block allocation, a page that is not cached, a contended
// inode lock or writeback throttling, and many always do (ext4 and tmpfs
// answer EOPNOTSUPP to buffered RWF_NOWAIT writes on Linux 7.2, which marks
// writes unsupported: every write is then offloaded). The write may be
// short. A successful nowait write only dirties the page cache: durability
// still needs fsync / fdatasync (FsyncOp), which always blocks. Reads and
// writes are tracked separately.

#include <sys/types.h>
#include <sys/uio.h>

#include <atomic>
#include <cerrno>
#include <climits>
#include <cstddef>

#include "fibers/blocking/ops.hpp"

namespace fibers::blocking {

class NowaitSupport {
public:
    using Preadv2Fn = ssize_t (*)(int, const struct iovec*, int, off_t, int);
    using Pwritev2Fn = ssize_t (*)(int, const struct iovec*, int, off_t, int);

    constexpr NowaitSupport() noexcept = default;

    // Test hook: replaces the syscalls (e.g. to simulate ENOSYS or a short
    // read). Production code uses the default constructor.
    constexpr NowaitSupport(Preadv2Fn preadv2_fn, Pwritev2Fn pwritev2_fn) noexcept
        : preadv2_(preadv2_fn), pwritev2_(pwritev2_fn) {}

    NowaitSupport(const NowaitSupport&) = delete;
    NowaitSupport& operator=(const NowaitSupport&) = delete;

    bool read_supported() const noexcept { return !read_unsupported_.load(std::memory_order_relaxed); }
    bool write_supported() const noexcept { return !write_unsupported_.load(std::memory_order_relaxed); }

    void mark_read_unsupported() noexcept { read_unsupported_.store(true, std::memory_order_relaxed); }
    void mark_write_unsupported() noexcept { write_unsupported_.store(true, std::memory_order_relaxed); }

    // Forget what was learnt (tests; or after remounting / switching files).
    void reset() noexcept {
        read_unsupported_.store(false, std::memory_order_relaxed);
        write_unsupported_.store(false, std::memory_order_relaxed);
    }

    // One nowait syscall attempt; see try_read_nowait / try_write_nowait.
    IoResult read(int fd, void* buf, std::size_t len, off_t offset) noexcept {
        return attempt(preadv2_, read_unsupported_, fd, buf, len, offset);
    }
    IoResult write(int fd, const void* buf, std::size_t len, off_t offset) noexcept {
        return attempt(pwritev2_, write_unsupported_, fd, const_cast<void*>(buf), len, offset);
    }

private:
    static IoResult attempt(Preadv2Fn fn, std::atomic<bool>& unsupported, int fd, void* buf, std::size_t len,
                            off_t offset) noexcept {
        // pread semantics: no "current position" (-1), no oversized length.
        // Checked here so that EINVAL from the kernel can only mean the flag.
        if (offset < 0 || len > static_cast<std::size_t>(SSIZE_MAX)) {
            return IoResult::failure(EINVAL);
        }
        if (unsupported.load(std::memory_order_relaxed)) {
            return IoResult::failure(EAGAIN);
        }
        const struct iovec iov{buf, len};
        for (;;) {
            const ssize_t ret = fn(fd, &iov, 1, offset, RWF_NOWAIT);
            if (ret >= 0) {
                return IoResult::success(ret);
            }
            const int error = errno;
            switch (error) {
                case EINTR:
                    continue;
                case EAGAIN:
                    return IoResult::failure(EAGAIN);
                case ENOSYS:
                case EOPNOTSUPP:
                case EINVAL:
                    unsupported.store(true, std::memory_order_relaxed);
                    return IoResult::failure(EAGAIN);
                default:
                    return IoResult::failure(error);
            }
        }
    }

    std::atomic<bool> read_unsupported_{false};
    std::atomic<bool> write_unsupported_{false};
    Preadv2Fn preadv2_ = &::preadv2;
    Pwritev2Fn pwritev2_ = &::pwritev2;
};

namespace detail {
inline constinit NowaitSupport process_nowait_support{};
}  // namespace detail

// The process-wide default.
inline NowaitSupport& process_nowait_support() noexcept { return detail::process_nowait_support; }

// One preadv2(RWF_NOWAIT) on the calling thread, pread semantics otherwise
// (offset >= 0). Returns:
//   value > 0     bytes read from the page cache (possibly short: see above)
//   value == 0    EOF (or len == 0)
//   would_block() not cached, or RWF_NOWAIT unsupported: offload the read
//   other error   a real error (EBADF, EISDIR, EFAULT, EINVAL for offset < 0)
inline IoResult try_read_nowait(int fd, void* buf, std::size_t len, off_t offset,
                                NowaitSupport& support = process_nowait_support()) noexcept {
    return support.read(fd, buf, len, offset);
}

// The same with pwritev2(RWF_NOWAIT); see the header comment for what it
// means for buffered writes. A short write is possible.
inline IoResult try_write_nowait(int fd, const void* buf, std::size_t len, off_t offset,
                                 NowaitSupport& support = process_nowait_support()) noexcept {
    return support.write(fd, buf, len, offset);
}

// Tries to complete `op` (fd, buf, len, offset, transferred) inline with
// nowait reads. Returns true when op.result() is final (all read, EOF, or an
// error): no offload needed, the continuation is untouched. Returns false
// when the rest would block: op.transferred says how much was read inline,
// and the op is ready for try_submit_op() to read the rest.
inline bool try_complete_inline(PreadOp& op, NowaitSupport& support = process_nowait_support()) noexcept {
    for (;;) {
        if (op.transferred == op.len) {
            op.set_result(IoResult::success(static_cast<ssize_t>(op.len)));
            return true;
        }
        const IoResult r = support.read(op.fd, static_cast<char*>(op.buf) + op.transferred, op.len - op.transferred,
                                        op.offset + static_cast<off_t>(op.transferred));
        if (r.would_block()) {
            return false;
        }
        if (!r.ok() || r.value == 0) {  // error or EOF: report like pread would
            op.set_result(detail::with_prefix(op.transferred, r.ok() ? 0 : -1, r.error));
            return true;
        }
        op.transferred += static_cast<std::size_t>(r.value);
    }
}

enum class ReadPath {
    kInline,     // done on the calling thread; op.result() is final and the
                 // continuation was NOT delivered (it is still in the op)
    kOffloaded,  // submitted to the pool; the continuation will be delivered
    kRejected,   // would block and the pool refused it (full / stopped); the
                 // op is ready to be submitted again with try_submit_op()
};

// "Read: try nowait, else offload with continuation". Set the op's
// continuation first (it is used only on kOffloaded).
//
// A fiber layer that suspends the fiber before publishing it splits this in
// two: try_complete_inline() while running; if it returns false, suspend,
// and after the context switch try_submit_op() (see "Fiber integration" in
// ops.hpp).
template <class Pool>
[[nodiscard]] ReadPath read_nowait_or_offload(Pool& pool, PreadOp& op,
                                              NowaitSupport& support = process_nowait_support()) noexcept {
    if (try_complete_inline(op, support)) {
        return ReadPath::kInline;
    }
    return try_submit_op(pool, op) ? ReadPath::kOffloaded : ReadPath::kRejected;
}

}  // namespace fibers::blocking
