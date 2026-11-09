#pragma once

// RWF_NOWAIT fast path: try a file read on the calling (worker) thread
// without blocking, and offload to the BlockingPool only if it would block.
//
// preadv2(..., RWF_NOWAIT) (Linux >= 4.14) copies what is in the page cache
// and returns EAGAIN instead of waiting for the disk. A 4 KiB read from the
// page cache costs about a microsecond inline; the round trip through the
// pool (two thread wakeups) costs tens of microseconds.
//
// Support is per filesystem, not per process
// ------------------------------------------
// Whether RWF_NOWAIT works depends on the kernel and on the filesystem: on
// Linux 7.2 ext4 serves nowait reads while tmpfs (/tmp, /dev/shm) answers
// EOPNOTSUPP, and both are commonly used by one process. So NowaitSupport
// remembers "unsupported" per st_dev, in a small fixed table:
//
// * NowaitSupport::filesystem(fd) resolves the fd's filesystem once (one
//   fstat) to an Fs handle: its entry in an open-addressing table of
//   kMaxFilesystems slots of atomics, filled on first use, never allocated,
//   never evicted. Keep the handle with the open file (the fiber layer's
//   file object) and pass it to every call.
// * With a tracked Fs: EOPNOTSUPP / EINVAL from a nowait call mark that
//   filesystem unsupported (reads and writes separately); from then on calls
//   for it return "would block" at once (one relaxed load, no syscall) and
//   the caller offloads, as it would on EAGAIN. Other filesystems are not
//   affected: a tmpfs file does not disable the fast path for ext4 files.
// * Without one (Fs{}: the fd-only calls, or filesystem() could not track
//   it because fstat failed or the table is full): conservative, every call
//   tries the syscall and a failure is remembered for that call only (it
//   returns "would block"). A doomed nowait syscall costs ~0.1-0.3 us, small
//   next to the offload that follows; consulting the table without a handle
//   would need an fstat per call, which costs as much and would also tax the
//   files that do support RWF_NOWAIT. So fd-only calls never fstat.
// * ENOSYS (no preadv2 / pwritev2 at all) is the kernel's: remembered
//   process-wide (per NowaitSupport), tracked or not.
// * EINVAL: some kernels and emulators reject unknown flags with it; we
//   pre-check our own arguments, so EINVAL from a valid request means the
//   flag. Not distinguishable from the O_DIRECT misalignment EINVAL: do not
//   use the fast path with misaligned O_DIRECT buffers.
// * Keyed by st_dev, so files of one filesystem share the verdict. A stacked
//   filesystem whose files differ in support (overlayfs over mixed layers,
//   FUSE) gets the verdict of the first file that failed.
//
// The atomics are hints, not synchronisation: nothing is published through
// them and any value is safe (a stale "supported" costs one failed syscall),
// so they are plain std::atomic rather than Sync-policy ones.
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
// answer EOPNOTSUPP to buffered RWF_NOWAIT writes on Linux 7.2). The write
// may be short. A successful nowait write only dirties the page cache:
// durability still needs fsync / fdatasync (FsyncOp), which always blocks.

#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>

#include <atomic>
#include <cerrno>
#include <climits>
#include <cstddef>
#include <cstdint>

#include "fibers/blocking/ops.hpp"

namespace fibers::blocking {

class NowaitSupport;

namespace detail {
struct NowaitEntry {
    std::atomic<std::uint64_t> key{0};    // st_dev + 1 (0: empty); written once
    std::atomic<std::uint32_t> flags{0};  // NowaitSupport::kRead / kWrite: unsupported
};
}  // namespace detail

// A filesystem's entry in the table of the NowaitSupport that made it (use it
// only with that one). Default: untracked (per-call behaviour). Trivially
// copyable; keep one per open file.
class NowaitFs {
public:
    NowaitFs() noexcept = default;
    bool tracked() const noexcept { return entry_ != nullptr; }

private:
    friend class NowaitSupport;
    explicit NowaitFs(detail::NowaitEntry* entry) noexcept : entry_(entry) {}
    detail::NowaitEntry* entry_ = nullptr;
};

class NowaitSupport {
    using Entry = detail::NowaitEntry;

public:
    using Preadv2Fn = ssize_t (*)(int, const struct iovec*, int, off_t, int);
    using Pwritev2Fn = ssize_t (*)(int, const struct iovec*, int, off_t, int);
    using FstatFn = int (*)(int, struct stat*);

    // Distinct filesystems (st_dev values) tracked per NowaitSupport.
    static constexpr std::size_t kMaxFilesystems = 32;

    using Fs = NowaitFs;

    constexpr NowaitSupport() noexcept = default;

    // Test hook: replaces the syscalls (e.g. to simulate ENOSYS, a short read
    // or several filesystems). Production code uses the default constructor.
    constexpr NowaitSupport(Preadv2Fn preadv2_fn, Pwritev2Fn pwritev2_fn, FstatFn fstat_fn = &::fstat) noexcept
        : preadv2_(preadv2_fn), pwritev2_(pwritev2_fn), fstat_(fstat_fn) {}

    NowaitSupport(const NowaitSupport&) = delete;
    NowaitSupport& operator=(const NowaitSupport&) = delete;

    // Resolves fd's filesystem (one fstat) to its table entry, inserting it on
    // first use. Untracked if fstat fails or all kMaxFilesystems entries hold
    // other filesystems. Lock-free; any thread.
    Fs filesystem(int fd) noexcept {
        struct stat st{};
        if (fstat_(fd, &st) != 0) {
            return Fs{};
        }
        return Fs{find_or_insert(static_cast<std::uint64_t>(st.st_dev))};
    }

    // One nowait syscall attempt; see try_read_nowait / try_write_nowait.
    IoResult read(int fd, void* buf, std::size_t len, off_t offset, Fs fs = {}) noexcept {
        return attempt(preadv2_, kRead, fs, fd, buf, len, offset);
    }
    IoResult write(int fd, const void* buf, std::size_t len, off_t offset, Fs fs = {}) noexcept {
        return attempt(pwritev2_, kWrite, fs, fd, const_cast<void*>(buf), len, offset);
    }

    // False once nowait reads (writes) are known not to work on `fs` (or in
    // this kernel). An untracked Fs reports only the kernel-wide verdict.
    bool read_supported(Fs fs = {}) const noexcept { return (flags(fs) & kRead) == 0; }
    bool write_supported(Fs fs = {}) const noexcept { return (flags(fs) & kWrite) == 0; }

    // Diagnostics: filesystems in the table.
    std::size_t tracked_filesystems() const noexcept {
        std::size_t n = 0;
        for (const Entry& entry : table_) {
            n += entry.key.load(std::memory_order_relaxed) != kEmptyKey ? 1 : 0;
        }
        return n;
    }

    // Forgets every verdict (tests; or after remounts). Entries stay assigned
    // to their filesystems, so outstanding Fs handles remain valid.
    void reset() noexcept {
        kernel_flags_.store(0, std::memory_order_relaxed);
        for (Entry& entry : table_) {
            entry.flags.store(0, std::memory_order_relaxed);
        }
    }

private:
    static constexpr std::uint32_t kRead = 1;
    static constexpr std::uint32_t kWrite = 2;
    static constexpr std::uint64_t kEmptyKey = 0;

    // Linear probing from the hash; claims an empty slot with a CAS.
    Entry* find_or_insert(std::uint64_t dev) noexcept {
        if (dev == ~std::uint64_t{0}) {
            return nullptr;  // no key for it (key = dev + 1, 0 = empty): untracked
        }
        const std::uint64_t key = dev + 1;
        const std::size_t start = static_cast<std::size_t>((key * 0x9E3779B97F4A7C15ull) >> 32) % kMaxFilesystems;
        for (std::size_t i = 0; i < kMaxFilesystems; ++i) {
            Entry& entry = table_[(start + i) % kMaxFilesystems];
            std::uint64_t seen = entry.key.load(std::memory_order_relaxed);
            if (seen == kEmptyKey) {
                if (entry.key.compare_exchange_strong(seen, key, std::memory_order_relaxed)) {
                    return &entry;
                }
                // Lost the slot: `seen` is the winner's key.
            }
            if (seen == key) {
                return &entry;
            }
        }
        return nullptr;  // full
    }

    std::uint32_t flags(Fs fs) const noexcept {
        std::uint32_t f = kernel_flags_.load(std::memory_order_relaxed);
        if (fs.entry_ != nullptr) {
            f |= fs.entry_->flags.load(std::memory_order_relaxed);
        }
        return f;
    }

    IoResult attempt(Preadv2Fn fn, std::uint32_t kind, Fs fs, int fd, void* buf, std::size_t len,
                     off_t offset) noexcept {
        // pread semantics: no "current position" (-1), no oversized length.
        // Checked here so that EINVAL from the kernel can only mean the flag.
        if (offset < 0 || len > static_cast<std::size_t>(SSIZE_MAX)) {
            return IoResult::failure(EINVAL);
        }
        if ((flags(fs) & kind) != 0) {
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
                    kernel_flags_.fetch_or(kind, std::memory_order_relaxed);
                    return IoResult::failure(EAGAIN);
                case EOPNOTSUPP:
                case EINVAL:
                    if (fs.entry_ != nullptr) {
                        fs.entry_->flags.fetch_or(kind, std::memory_order_relaxed);
                    }
                    return IoResult::failure(EAGAIN);
                default:
                    return IoResult::failure(error);
            }
        }
    }

    Entry table_[kMaxFilesystems]{};
    std::atomic<std::uint32_t> kernel_flags_{0};
    Preadv2Fn preadv2_ = &::preadv2;
    Pwritev2Fn pwritev2_ = &::pwritev2;
    FstatFn fstat_ = &::fstat;
};

namespace detail {
inline constinit NowaitSupport process_nowait_support{};
}  // namespace detail

// The process-wide default.
inline NowaitSupport& process_nowait_support() noexcept { return detail::process_nowait_support; }

// One preadv2(RWF_NOWAIT) on the calling thread, pread semantics otherwise
// (offset >= 0). `fs`: the file's NowaitSupport::Fs (see the header comment;
// default: untracked). Returns:
//   value > 0     bytes read from the page cache (possibly short: see above)
//   value == 0    EOF (or len == 0)
//   would_block() not cached, or RWF_NOWAIT unsupported: offload the read
//   other error   a real error (EBADF, EISDIR, EFAULT, EINVAL for offset < 0)
inline IoResult try_read_nowait(int fd, void* buf, std::size_t len, off_t offset, NowaitSupport::Fs fs = {},
                                NowaitSupport& support = process_nowait_support()) noexcept {
    return support.read(fd, buf, len, offset, fs);
}

// The same with pwritev2(RWF_NOWAIT); see the header comment for what it
// means for buffered writes. A short write is possible.
inline IoResult try_write_nowait(int fd, const void* buf, std::size_t len, off_t offset, NowaitSupport::Fs fs = {},
                                 NowaitSupport& support = process_nowait_support()) noexcept {
    return support.write(fd, buf, len, offset, fs);
}

// Tries to complete `op` (fd, buf, len, offset, transferred) inline with
// nowait reads. Returns true when op.result() is final (all read, EOF, or an
// error): no offload needed, the continuation is untouched. Returns false
// when the rest would block: op.transferred says how much was read inline,
// and the op is ready for BlockingPool::submit() to read the rest.
template <class Sync>
bool try_complete_inline(BasicPreadOp<Sync>& op, NowaitSupport::Fs fs = {},
                         NowaitSupport& support = process_nowait_support()) noexcept {
    for (;;) {
        if (op.transferred == op.len) {
            op.set_result(IoResult::success(static_cast<ssize_t>(op.len)));
            return true;
        }
        const IoResult r = support.read(op.fd, static_cast<char*>(op.buf) + op.transferred,
                                        op.len - op.transferred, op.offset + static_cast<off_t>(op.transferred), fs);
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
                 // (or dropped, if the pool is stopped)
};

// "Read: try nowait, else offload with continuation". Set the op's
// continuation first (it is used only on kOffloaded).
//
// A fiber layer that suspends the fiber before publishing it splits this in
// two: try_complete_inline() while running; if it returns false, suspend,
// and after the context switch pool.submit(op) (see "Fiber integration" in
// ops.hpp).
template <class Pool, class Sync>
[[nodiscard]] ReadPath read_nowait_or_offload(Pool& pool, BasicPreadOp<Sync>& op, NowaitSupport::Fs fs = {},
                                              NowaitSupport& support = process_nowait_support()) noexcept {
    if (try_complete_inline(op, fs, support)) {
        return ReadPath::kInline;
    }
    pool.submit(op);
    return ReadPath::kOffloaded;
}

}  // namespace fibers::blocking
