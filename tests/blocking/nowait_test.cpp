// RWF_NOWAIT fast path: real files and simulated kernels.

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <vector>

#include "fibers/blocking/blocking_pool.hpp"
#include "fibers/blocking/nowait.hpp"
#include "fibers/blocking/ops.hpp"
#include "test_util.hpp"

namespace blocking = fibers::blocking;
using blocking::BlockingPool;
using blocking::IoResult;
using blocking::NowaitSupport;
using blocking::PreadOp;
using blocking::ReadPath;
using blocking::Sink;
using namespace blocking::test;

namespace {

constexpr std::size_t kFileSize = 64 * 1024;

std::vector<char> pattern(std::size_t n) {
    std::vector<char> data(n);
    for (std::size_t i = 0; i < n; ++i) data[i] = static_cast<char>(i * 7 + i / 251);
    return data;
}

void fill(TempFile& file, const std::vector<char>& data) {
    ASSERT_EQ(::pwrite(file.fd, data.data(), data.size(), 0), static_cast<ssize_t>(data.size()));
}

// ---- simulated kernels (NowaitSupport's test hook) ----

std::atomic<int> g_calls{0};
std::atomic<int> g_errno{0};

ssize_t fail_with_errno(int, const iovec*, int, off_t, int) {
    g_calls.fetch_add(1);
    errno = g_errno.load();
    return -1;
}

// Page cache holding only the first 1000 bytes of every request: the first
// call returns a short read, the next one EAGAIN.
ssize_t short_then_eagain(int fd, const iovec* iov, int, off_t offset, int flags) {
    EXPECT_EQ(flags, RWF_NOWAIT);
    if (g_calls.fetch_add(1) % 2 == 1) {
        errno = EAGAIN;
        return -1;
    }
    return ::pread(fd, iov->iov_base, std::min<std::size_t>(iov->iov_len, 1000), offset);
}

// Everything cached, but at most 1000 bytes per call.
ssize_t chunky(int fd, const iovec* iov, int, off_t offset, int) {
    g_calls.fetch_add(1);
    return ::pread(fd, iov->iov_base, std::min<std::size_t>(iov->iov_len, 1000), offset);
}

struct ResetCalls {
    ResetCalls() { g_calls.store(0); }
};

}  // namespace

TEST(Nowait, ProcessWideInstance) {
    EXPECT_EQ(&blocking::process_nowait_support(), &blocking::process_nowait_support());
}

TEST(Nowait, CachedReadSucceedsInline) {
    TempFile file;
    const auto data = pattern(kFileSize);
    fill(file, data);  // just written: in the page cache
    NowaitSupport support;
    char buf[4096];
    const IoResult r = blocking::try_read_nowait(file.fd, buf, sizeof(buf), 8192, support);
    if (r.would_block() && !support.read_supported()) {
        GTEST_SKIP() << "RWF_NOWAIT reads not supported here (kernel or filesystem); fast path always offloads";
    }
    ASSERT_TRUE(r.ok()) << std::strerror(r.error);
    EXPECT_EQ(r.value, 4096);
    EXPECT_EQ(std::memcmp(buf, data.data() + 8192, sizeof(buf)), 0);

    // At EOF: short, then 0.
    EXPECT_EQ(blocking::try_read_nowait(file.fd, buf, sizeof(buf), kFileSize - 100, support).value, 100);
    EXPECT_EQ(blocking::try_read_nowait(file.fd, buf, sizeof(buf), kFileSize, support).value, 0);
    EXPECT_TRUE(support.read_supported());
}

TEST(Nowait, EvictedReadWouldBlock) {
    TempFile file;
    fill(file, pattern(kFileSize));
    ASSERT_EQ(::fsync(file.fd), 0);
    ASSERT_EQ(::posix_fadvise(file.fd, 0, 0, POSIX_FADV_DONTNEED), 0);
    NowaitSupport support;
    char buf[4096];
    const IoResult r = blocking::try_read_nowait(file.fd, buf, sizeof(buf), 8192, support);
    if (!support.read_supported()) {
        GTEST_SKIP() << "RWF_NOWAIT reads not supported here; nothing to evict";
    }
    if (r.ok()) {
        GTEST_SKIP() << "the filesystem kept the pages after fsync + POSIX_FADV_DONTNEED "
                        "(e.g. tmpfs, or pages mapped elsewhere): cannot observe a cache miss here";
    }
    EXPECT_TRUE(r.would_block()) << std::strerror(r.error);
}

TEST(Nowait, UnsupportedIsRememberedAndSkipsTheSyscall) {
    for (const int error : {ENOSYS, EOPNOTSUPP, EINVAL}) {
        ResetCalls reset;
        g_errno.store(error);
        NowaitSupport support(&fail_with_errno, &fail_with_errno);
        char buf[16];
        EXPECT_TRUE(support.read_supported());
        EXPECT_TRUE(blocking::try_read_nowait(3, buf, sizeof(buf), 0, support).would_block()) << error;
        EXPECT_FALSE(support.read_supported()) << error;
        EXPECT_TRUE(support.write_supported()) << "reads and writes are tracked separately";
        EXPECT_EQ(g_calls.load(), 1);
        for (int i = 0; i < 3; ++i) {
            EXPECT_TRUE(blocking::try_read_nowait(3, buf, sizeof(buf), 0, support).would_block());
        }
        EXPECT_EQ(g_calls.load(), 1) << "no syscall once known unsupported";

        EXPECT_TRUE(blocking::try_write_nowait(3, buf, sizeof(buf), 0, support).would_block());
        EXPECT_FALSE(support.write_supported());
        support.reset();
        EXPECT_TRUE(support.read_supported());
        EXPECT_TRUE(support.write_supported());
    }
}

TEST(Nowait, EagainAndRealErrorsDoNotMarkUnsupported) {
    ResetCalls reset;
    char buf[16];
    {
        g_errno.store(EAGAIN);
        NowaitSupport support(&fail_with_errno, &fail_with_errno);
        EXPECT_TRUE(blocking::try_read_nowait(3, buf, sizeof(buf), 0, support).would_block());
        EXPECT_TRUE(blocking::try_read_nowait(3, buf, sizeof(buf), 0, support).would_block());
        EXPECT_TRUE(support.read_supported());
        EXPECT_EQ(g_calls.load(), 2);
    }
    {
        NowaitSupport support;  // real syscall
        const IoResult r = blocking::try_read_nowait(-1, buf, sizeof(buf), 0, support);
        EXPECT_EQ(r.value, -1);
        EXPECT_EQ(r.error, EBADF);
        EXPECT_TRUE(support.read_supported());
    }
    {
        g_calls.store(0);
        NowaitSupport support(&fail_with_errno, &fail_with_errno);
        // Our own argument check: pread semantics, no syscall, not "unsupported".
        const IoResult r = blocking::try_read_nowait(3, buf, sizeof(buf), -1, support);
        EXPECT_EQ(r.error, EINVAL);
        EXPECT_EQ(g_calls.load(), 0);
        EXPECT_TRUE(support.read_supported());
    }
}

TEST(Nowait, CompleteInlineContinuesShortReads) {
    TempFile file;
    const auto data = pattern(kFileSize);
    fill(file, data);
    ResetCalls reset;
    NowaitSupport support(&chunky, &chunky);
    std::vector<char> buf(8192);
    PreadOp op(file.fd, buf.data(), buf.size(), 4096);
    ASSERT_TRUE(blocking::try_complete_inline(op, support));
    EXPECT_EQ(op.result().value, 8192);
    EXPECT_EQ(g_calls.load(), 9);  // 8 x 1000 + 192
    EXPECT_EQ(std::memcmp(buf.data(), data.data() + 4096, buf.size()), 0);

    // EOF in the middle: a normal short pread result.
    PreadOp tail(file.fd, buf.data(), buf.size(), kFileSize - 2500);
    ASSERT_TRUE(blocking::try_complete_inline(tail, support));
    EXPECT_EQ(tail.result().value, 2500);
}

TEST(Nowait, ShortThenWouldBlockOffloadsTheRest) {
    TempFile file;
    const auto data = pattern(kFileSize);
    fill(file, data);
    ResetCalls reset;
    NowaitSupport support(&short_then_eagain, &short_then_eagain);
    BlockingPool<> pool({.num_threads = 2});
    std::vector<char> buf(8192);
    PreadOp op(file.fd, buf.data(), buf.size(), 4096);
    Counts k;
    op.set_continuation(k.task(), Sink::run_inline());
    ASSERT_EQ(blocking::read_nowait_or_offload(pool, op, support), ReadPath::kOffloaded);
    ASSERT_TRUE(wait_for([&] { return k.ran.load() == 1; }));
    EXPECT_EQ(op.transferred, 1000u);
    EXPECT_EQ(op.result().value, 8192);
    EXPECT_EQ(std::memcmp(buf.data(), data.data() + 4096, buf.size()), 0);
}

TEST(Nowait, ReadNowaitOrOffloadPaths) {
    TempFile file;
    const auto data = pattern(kFileSize);
    fill(file, data);
    std::vector<char> buf(4096);

    {  // kInline: continuation not delivered, still in the op
        ResetCalls reset;
        NowaitSupport support(&chunky, &chunky);
        Counts k;
        BlockingPool<> pool({.num_threads = 1});
        PreadOp op(file.fd, buf.data(), buf.size(), 0);
        op.set_continuation(k.task(), Sink::run_inline());
        EXPECT_EQ(blocking::read_nowait_or_offload(pool, op, support), ReadPath::kInline);
        EXPECT_EQ(op.result().value, 4096);
        EXPECT_EQ(k.ran.load(), 0);
        Task back = op.take_continuation();
        EXPECT_TRUE(static_cast<bool>(back));
    }
    {  // kRejected, then submitted again elsewhere
        g_errno.store(EAGAIN);
        NowaitSupport support(&fail_with_errno, &fail_with_errno);
        Counts k;
        BlockingPool<> stopped({.num_threads = 1});
        stopped.stop();
        PreadOp op(file.fd, buf.data(), buf.size(), 100);
        op.set_continuation(k.task(), Sink::run_inline());
        EXPECT_EQ(blocking::read_nowait_or_offload(stopped, op, support), ReadPath::kRejected);
        EXPECT_EQ(k.dropped.load(), 0);
        BlockingPool<> pool({.num_threads = 1});
        ASSERT_TRUE(blocking::try_submit_op(pool, op));
        ASSERT_TRUE(wait_for([&] { return k.ran.load() == 1; }));
        EXPECT_EQ(op.result().value, 4096);
        EXPECT_EQ(std::memcmp(buf.data(), data.data() + 100, buf.size()), 0);
    }
    {  // real kernel: inline if supported, else offloaded; same data either way
        NowaitSupport support;
        Counts k;  // outlives the op: an undelivered continuation is dropped with it
        BlockingPool<> pool({.num_threads = 1});
        PreadOp op(file.fd, buf.data(), buf.size(), 200);
        op.set_continuation(k.task(), Sink::run_inline());
        const ReadPath path = blocking::read_nowait_or_offload(pool, op, support);
        ASSERT_NE(path, ReadPath::kRejected);
        if (path == ReadPath::kOffloaded) {
            ASSERT_TRUE(wait_for([&] { return k.ran.load() == 1; }));
        }
        EXPECT_EQ(op.result().value, 4096);
        EXPECT_EQ(std::memcmp(buf.data(), data.data() + 200, buf.size()), 0);
    }
}

TEST(Nowait, WriteNowait) {
    TempFile file;
    fill(file, pattern(kFileSize));
    NowaitSupport support;
    const char msg[] = "nowait write";
    const IoResult r = blocking::try_write_nowait(file.fd, msg, sizeof(msg), 100, support);
    if (r.would_block()) {
        GTEST_SKIP() << "buffered RWF_NOWAIT write " << (support.write_supported() ? "would block" : "unsupported")
                     << " here; offload with PwriteOp";
    }
    ASSERT_TRUE(r.ok()) << std::strerror(r.error);
    ASSERT_GT(r.value, 0);
    char back[sizeof(msg)] = {};
    ASSERT_EQ(::pread(file.fd, back, static_cast<std::size_t>(r.value), 100), r.value);
    EXPECT_EQ(std::memcmp(back, msg, static_cast<std::size_t>(r.value)), 0);
}
