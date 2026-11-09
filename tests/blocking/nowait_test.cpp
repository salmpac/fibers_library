// RWF_NOWAIT fast path: real files and simulated kernels / filesystems.

#include <gtest/gtest.h>

#include <fcntl.h>
#include <linux/magic.h>
#include <sys/stat.h>
#include <sys/statfs.h>
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
using Fs = NowaitSupport::Fs;

namespace {

constexpr std::size_t kFileSize = 64 * 1024;

std::vector<char> pattern(std::size_t n) {
    std::vector<char> data(n);
    for (std::size_t i = 0; i < n; ++i) data[i] = static_cast<char>(i * 7 + i / 251);
    return data;
}

void fill(TempFile& file, const std::vector<char>& data) {
    ASSERT_TRUE(file.ok());
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

// Fake filesystems: fd N lives on st_dev N / 100. Filesystem 1 rejects
// RWF_NOWAIT (EOPNOTSUPP, like tmpfs), every other one serves the read.
// fd 666 cannot be fstat'ed.
std::atomic<int> g_fstats{0};
int fake_fstat(int fd, struct stat* st) {
    g_fstats.fetch_add(1);
    if (fd == 666) {
        errno = EBADF;
        return -1;
    }
    *st = {};
    st->st_dev = static_cast<dev_t>(fd / 100);
    return 0;
}
ssize_t fake_fs_rw(int fd, const iovec* iov, int, off_t, int) {
    g_calls.fetch_add(1);
    if (fd / 100 == 1) {
        errno = EOPNOTSUPP;
        return -1;
    }
    return static_cast<ssize_t>(iov->iov_len);
}

struct ResetCalls {
    ResetCalls() {
        g_calls.store(0);
        g_fstats.store(0);
    }
};

bool is_tmpfs(int fd) {
    struct statfs sfs{};
    return ::fstatfs(fd, &sfs) == 0 && sfs.f_type == TMPFS_MAGIC;
}

dev_t dev_of(int fd) {
    struct stat st{};
    return ::fstat(fd, &st) == 0 ? st.st_dev : 0;
}

}  // namespace

TEST(Nowait, ProcessWideInstance) {
    EXPECT_EQ(&blocking::process_nowait_support(), &blocking::process_nowait_support());
}

TEST(Nowait, CachedReadSucceedsInline) {
    TempFile file;
    const auto data = pattern(kFileSize);
    fill(file, data);  // just written: in the page cache
    NowaitSupport support;
    const Fs fs = support.filesystem(file.fd);
    ASSERT_TRUE(fs.tracked());
    char buf[4096];
    const IoResult r = blocking::try_read_nowait(file.fd, buf, sizeof(buf), 8192, fs, support);
    if (r.would_block() && !support.read_supported(fs)) {
        GTEST_SKIP() << "RWF_NOWAIT reads not supported on this filesystem; fast path always offloads";
    }
    ASSERT_TRUE(r.ok()) << std::strerror(r.error);
    EXPECT_EQ(r.value, 4096);
    EXPECT_EQ(std::memcmp(buf, data.data() + 8192, sizeof(buf)), 0);

    // At EOF: short, then 0.
    EXPECT_EQ(blocking::try_read_nowait(file.fd, buf, sizeof(buf), kFileSize - 100, fs, support).value, 100);
    EXPECT_EQ(blocking::try_read_nowait(file.fd, buf, sizeof(buf), kFileSize, fs, support).value, 0);
    EXPECT_TRUE(support.read_supported(fs));
}

TEST(Nowait, EvictedReadWouldBlock) {
    TempFile file;
    fill(file, pattern(kFileSize));
    ASSERT_EQ(::fsync(file.fd), 0);
    ASSERT_EQ(::posix_fadvise(file.fd, 0, 0, POSIX_FADV_DONTNEED), 0);
    NowaitSupport support;
    const Fs fs = support.filesystem(file.fd);
    char buf[4096];
    const IoResult r = blocking::try_read_nowait(file.fd, buf, sizeof(buf), 8192, fs, support);
    if (!support.read_supported(fs)) {
        GTEST_SKIP() << "RWF_NOWAIT reads not supported here; nothing to evict";
    }
    if (r.ok()) {
        GTEST_SKIP() << "the filesystem kept the pages after fsync + POSIX_FADV_DONTNEED "
                        "(e.g. pages mapped elsewhere): cannot observe a cache miss here";
    }
    EXPECT_TRUE(r.would_block()) << std::strerror(r.error);
    EXPECT_TRUE(support.read_supported(fs)) << "EAGAIN is not 'unsupported'";
}

// The point of the per-st_dev table: one filesystem without RWF_NOWAIT does
// not disable the fast path for another.
TEST(Nowait, VerdictsArePerFilesystem) {
    ResetCalls reset;
    NowaitSupport support(&fake_fs_rw, &fake_fs_rw, &fake_fstat);
    const Fs tmp = support.filesystem(101);   // filesystem 1: no RWF_NOWAIT
    const Fs disk = support.filesystem(201);  // filesystem 2: supported
    ASSERT_TRUE(tmp.tracked());
    ASSERT_TRUE(disk.tracked());
    EXPECT_EQ(support.tracked_filesystems(), 2u);
    EXPECT_EQ(support.filesystem(102).tracked(), true) << "same st_dev, same entry";
    EXPECT_EQ(support.tracked_filesystems(), 2u);

    char buf[64];
    EXPECT_TRUE(support.read(101, buf, sizeof(buf), 0, tmp).would_block());
    EXPECT_FALSE(support.read_supported(tmp));
    EXPECT_TRUE(support.write_supported(tmp)) << "reads and writes are tracked separately";
    EXPECT_TRUE(support.read_supported(disk));
    EXPECT_EQ(g_calls.load(), 1);

    // The unsupported filesystem: no more syscalls, for any of its files.
    const Fs tmp_other_file = support.filesystem(150);
    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(support.read(101, buf, sizeof(buf), 0, tmp).would_block());
        EXPECT_TRUE(support.read(150, buf, sizeof(buf), 0, tmp_other_file).would_block());
    }
    EXPECT_EQ(g_calls.load(), 1) << "no syscall once the filesystem is known unsupported";

    // The other filesystem keeps its fast path.
    for (int i = 0; i < 3; ++i) {
        const IoResult r = support.read(201, buf, sizeof(buf), 0, disk);
        ASSERT_TRUE(r.ok());
        EXPECT_EQ(r.value, static_cast<ssize_t>(sizeof(buf)));
    }
    EXPECT_TRUE(support.read_supported(disk));
    EXPECT_EQ(g_calls.load(), 4);

    // Writes on filesystem 1 learn separately.
    EXPECT_TRUE(support.write(101, buf, sizeof(buf), 0, tmp).would_block());
    EXPECT_FALSE(support.write_supported(tmp));
    EXPECT_TRUE(support.write_supported(disk));

    support.reset();
    EXPECT_TRUE(support.read_supported(tmp));
    EXPECT_TRUE(support.write_supported(tmp));
    EXPECT_EQ(support.tracked_filesystems(), 2u) << "reset keeps entries (handles stay valid)";
}

// Untracked (fd-only calls, fstat failure, full table): try every time,
// remember nothing beyond the call; and never fstat on that path.
TEST(Nowait, UntrackedIsConservativePerCall) {
    ResetCalls reset;
    NowaitSupport support(&fake_fs_rw, &fake_fs_rw, &fake_fstat);
    char buf[64];
    for (int i = 0; i < 3; ++i) {
        EXPECT_TRUE(support.read(101, buf, sizeof(buf), 0).would_block());
        EXPECT_TRUE(blocking::try_read_nowait(101, buf, sizeof(buf), 0, {}, support).would_block());
        EXPECT_TRUE(support.read(201, buf, sizeof(buf), 0).ok());
    }
    EXPECT_EQ(g_calls.load(), 9) << "untracked: a syscall every time";
    EXPECT_EQ(g_fstats.load(), 0) << "fd-only calls never fstat";
    EXPECT_TRUE(support.read_supported()) << "nothing remembered kernel-wide for EOPNOTSUPP";
    EXPECT_EQ(support.tracked_filesystems(), 0u);

    // fstat failure: untracked.
    EXPECT_FALSE(support.filesystem(666).tracked());
}

TEST(Nowait, FullTableFallsBackToUntracked) {
    ResetCalls reset;
    NowaitSupport support(&fake_fs_rw, &fake_fs_rw, &fake_fstat);
    std::vector<Fs> handles;
    for (std::size_t dev = 2; dev < 2 + NowaitSupport::kMaxFilesystems; ++dev) {
        handles.push_back(support.filesystem(static_cast<int>(dev * 100)));
        ASSERT_TRUE(handles.back().tracked()) << dev;
    }
    EXPECT_EQ(support.tracked_filesystems(), NowaitSupport::kMaxFilesystems);
    // Filesystem 1 does not fit: untracked, i.e. tried every time.
    const Fs overflow = support.filesystem(101);
    EXPECT_FALSE(overflow.tracked());
    char buf[64];
    g_calls.store(0);
    for (int i = 0; i < 3; ++i) EXPECT_TRUE(support.read(101, buf, sizeof(buf), 0, overflow).would_block());
    EXPECT_EQ(g_calls.load(), 3);
    // Known filesystems are still found.
    EXPECT_TRUE(support.filesystem(300).tracked());
    for (const Fs& fs : handles) EXPECT_TRUE(support.read_supported(fs));
}

TEST(Nowait, KernelWithoutPreadv2IsRememberedProcessWide) {
    ResetCalls reset;
    g_errno.store(ENOSYS);
    NowaitSupport support(&fail_with_errno, &fail_with_errno, &fake_fstat);
    const Fs a = support.filesystem(201);
    char buf[16];
    EXPECT_TRUE(support.read(201, buf, sizeof(buf), 0, a).would_block());
    EXPECT_EQ(g_calls.load(), 1);
    // Every filesystem, tracked or not, skips the syscall now.
    const Fs b = support.filesystem(301);
    EXPECT_TRUE(support.read(301, buf, sizeof(buf), 0, b).would_block());
    EXPECT_TRUE(support.read(401, buf, sizeof(buf), 0).would_block());
    EXPECT_EQ(g_calls.load(), 1);
    EXPECT_FALSE(support.read_supported());
    EXPECT_FALSE(support.read_supported(b));
    EXPECT_TRUE(support.write_supported()) << "reads and writes are tracked separately";
}

TEST(Nowait, UnsupportedErrnosMarkTheFilesystem) {
    for (const int error : {EOPNOTSUPP, EINVAL}) {
        ResetCalls reset;
        g_errno.store(error);
        NowaitSupport support(&fail_with_errno, &fail_with_errno, &fake_fstat);
        const Fs fs = support.filesystem(201);
        char buf[16];
        EXPECT_TRUE(blocking::try_read_nowait(201, buf, sizeof(buf), 0, fs, support).would_block()) << error;
        EXPECT_FALSE(support.read_supported(fs)) << error;
        EXPECT_TRUE(support.read_supported()) << "not kernel-wide";
        EXPECT_TRUE(blocking::try_read_nowait(201, buf, sizeof(buf), 0, fs, support).would_block());
        EXPECT_EQ(g_calls.load(), 1) << "no syscall once known unsupported";
    }
}

TEST(Nowait, EagainAndRealErrorsDoNotMarkUnsupported) {
    ResetCalls reset;
    char buf[16];
    {
        g_errno.store(EAGAIN);
        NowaitSupport support(&fail_with_errno, &fail_with_errno, &fake_fstat);
        const Fs fs = support.filesystem(201);
        EXPECT_TRUE(blocking::try_read_nowait(201, buf, sizeof(buf), 0, fs, support).would_block());
        EXPECT_TRUE(blocking::try_read_nowait(201, buf, sizeof(buf), 0, fs, support).would_block());
        EXPECT_TRUE(support.read_supported(fs));
        EXPECT_EQ(g_calls.load(), 2);
    }
    {
        NowaitSupport support;  // real syscall
        const IoResult r = blocking::try_read_nowait(-1, buf, sizeof(buf), 0, {}, support);
        EXPECT_EQ(r.value, -1);
        EXPECT_EQ(r.error, EBADF);
        EXPECT_TRUE(support.read_supported());
    }
    {
        g_calls.store(0);
        NowaitSupport support(&fail_with_errno, &fail_with_errno, &fake_fstat);
        const Fs fs = support.filesystem(201);
        // Our own argument check: pread semantics, no syscall, not "unsupported".
        const IoResult r = blocking::try_read_nowait(201, buf, sizeof(buf), -1, fs, support);
        EXPECT_EQ(r.error, EINVAL);
        EXPECT_EQ(g_calls.load(), 0);
        EXPECT_TRUE(support.read_supported(fs));
    }
}

// Real kernel: a tmpfs file and a disk file in one process are judged
// independently. Skips unless both kinds are at hand and they differ.
TEST(Nowait, TmpfsAndDiskFilesInOneProcess) {
    TempFile disk(".");
    TempFile tmp("/dev/shm");
    if (!tmp.ok() || !is_tmpfs(tmp.fd)) {
        TempFile tmp2("/tmp");
        if (!tmp2.ok() || !is_tmpfs(tmp2.fd)) GTEST_SKIP() << "no tmpfs at /dev/shm or /tmp";
        std::swap(tmp.fd, tmp2.fd);
    }
    ASSERT_TRUE(disk.ok());
    if (is_tmpfs(disk.fd)) GTEST_SKIP() << "the build tree is on tmpfs: no disk filesystem at hand";
    ASSERT_NE(dev_of(disk.fd), dev_of(tmp.fd));
    const auto data = pattern(kFileSize);
    fill(disk, data);
    fill(tmp, data);

    NowaitSupport support;
    const Fs disk_fs = support.filesystem(disk.fd);
    const Fs tmp_fs = support.filesystem(tmp.fd);
    ASSERT_TRUE(disk_fs.tracked());
    ASSERT_TRUE(tmp_fs.tracked());
    char buf[4096];

    // tmpfs first: if it rejects RWF_NOWAIT, that must not leak to the disk.
    const IoResult t = support.read(tmp.fd, buf, sizeof(buf), 4096, tmp_fs);
    const IoResult d = support.read(disk.fd, buf, sizeof(buf), 4096, disk_fs);
    const bool tmp_supported = support.read_supported(tmp_fs);
    const bool disk_supported = support.read_supported(disk_fs);
    std::printf("RWF_NOWAIT reads: tmpfs %s, disk %s\n", tmp_supported ? "supported" : "unsupported",
                disk_supported ? "supported" : "unsupported");
    if (tmp_supported == disk_supported) {
        GTEST_SKIP() << "tmpfs and the disk filesystem agree here (both "
                     << (tmp_supported ? "support" : "reject") << " RWF_NOWAIT): no mixed case to check";
    }
    if (!tmp_supported) {
        EXPECT_TRUE(t.would_block());
        ASSERT_TRUE(d.ok()) << std::strerror(d.error);
        EXPECT_EQ(d.value, 4096);
        EXPECT_EQ(std::memcmp(buf, data.data() + 4096, sizeof(buf)), 0);
        // And it stays so: the disk file keeps reading inline, repeatedly.
        for (int i = 0; i < 3; ++i) {
            EXPECT_TRUE(support.read(tmp.fd, buf, sizeof(buf), 0, tmp_fs).would_block());
            EXPECT_EQ(support.read(disk.fd, buf, sizeof(buf), 0, disk_fs).value, 4096);
        }
        EXPECT_TRUE(support.read_supported(disk_fs));
    } else {
        EXPECT_TRUE(t.ok());
        EXPECT_TRUE(d.would_block());
        EXPECT_TRUE(support.read_supported(tmp_fs));
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
    ASSERT_TRUE(blocking::try_complete_inline(op, {}, support));
    EXPECT_EQ(op.result().value, 8192);
    EXPECT_EQ(g_calls.load(), 9);  // 8 x 1000 + 192
    EXPECT_EQ(std::memcmp(buf.data(), data.data() + 4096, buf.size()), 0);

    // EOF in the middle: a normal short pread result.
    PreadOp tail(file.fd, buf.data(), buf.size(), kFileSize - 2500);
    ASSERT_TRUE(blocking::try_complete_inline(tail, {}, support));
    EXPECT_EQ(tail.result().value, 2500);
}

TEST(Nowait, ShortThenWouldBlockOffloadsTheRest) {
    TempFile file;
    const auto data = pattern(kFileSize);
    fill(file, data);
    ResetCalls reset;
    NowaitSupport support(&short_then_eagain, &short_then_eagain);
    BlockingPool<> pool;
    std::vector<char> buf(8192);
    PreadOp op(file.fd, buf.data(), buf.size(), 4096);
    Counts k;
    op.set_continuation(k.task(), Sink::run_inline());
    ASSERT_EQ(blocking::read_nowait_or_offload(pool, op, {}, support), ReadPath::kOffloaded);
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
        EXPECT_EQ(blocking::read_nowait_or_offload(pool, op, {}, support), ReadPath::kInline);
        EXPECT_EQ(op.result().value, 4096);
        EXPECT_EQ(k.ran.load(), 0);
        Task back = op.take_continuation();
        EXPECT_TRUE(static_cast<bool>(back));
    }
    {  // would block into a stopped pool: offloaded, and the continuation dropped
        g_errno.store(EAGAIN);
        NowaitSupport support(&fail_with_errno, &fail_with_errno);
        Counts k;
        BlockingPool<> stopped({.num_threads = 1});
        stopped.stop();
        PreadOp op(file.fd, buf.data(), buf.size(), 100);
        op.set_continuation(k.task(), Sink::run_inline());
        EXPECT_EQ(blocking::read_nowait_or_offload(stopped, op, {}, support), ReadPath::kOffloaded);
        EXPECT_EQ(k.dropped.load(), 1);
        EXPECT_EQ(k.ran.load(), 0);
    }
    {  // real kernel, tracked fs: inline if supported, else offloaded; same data either way
        NowaitSupport support;
        const Fs fs = support.filesystem(file.fd);
        Counts k;  // outlives the op: an undelivered continuation is dropped with it
        BlockingPool<> pool({.num_threads = 1});
        PreadOp op(file.fd, buf.data(), buf.size(), 200);
        op.set_continuation(k.task(), Sink::run_inline());
        const ReadPath path = blocking::read_nowait_or_offload(pool, op, fs, support);
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
    const Fs fs = support.filesystem(file.fd);
    const char msg[] = "nowait write";
    const IoResult r = blocking::try_write_nowait(file.fd, msg, sizeof(msg), 100, fs, support);
    if (r.would_block()) {
        GTEST_SKIP() << "buffered RWF_NOWAIT write " << (support.write_supported(fs) ? "would block" : "unsupported")
                     << " here; offload with PwriteOp";
    }
    ASSERT_TRUE(r.ok()) << std::strerror(r.error);
    ASSERT_GT(r.value, 0);
    char back[sizeof(msg)] = {};
    ASSERT_EQ(::pread(file.fd, back, static_cast<std::size_t>(r.value), 100), r.value);
    EXPECT_EQ(std::memcmp(back, msg, static_cast<std::size_t>(r.value)), 0);
}
