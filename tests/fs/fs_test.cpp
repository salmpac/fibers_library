// fs: the file functions on the full Scheduler (StdSync, real files in the
// build tree); this_fiber::blocking itself is tested in tests/fiber.
//
// Fibers never use gtest assertions themselves: they record into atomics or
// plain fields that the test thread reads after a Latch (whose acq_rel
// count_down / acquire wait order them).

#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "fibers/blocking/nowait.hpp"
#include "fibers/blocking/ops.hpp"
#include "fibers/fiber/fiber.hpp"
#include "fibers/fs/file.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace fs = fibers::fs;
namespace blocking = fibers::blocking;
namespace sched = fibers::sched;
namespace this_fiber = fibers::fiber::this_fiber;
using blocking::IoResult;
using blocking::NowaitSupport;
using fibers::sched::Scheduler;
using namespace std::chrono_literals;

namespace {

// Joins fibers from the test thread. Declare it BEFORE the scheduler.
class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}

    void count_down() noexcept { count_.fetch_sub(1, std::memory_order_acq_rel); }

    bool wait(std::chrono::milliseconds timeout = 30s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (count_.load(std::memory_order_acquire) != 0) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(100us);
        }
        return true;
    }

private:
    std::atomic<std::int64_t> count_;
};

sched::SchedulerOptions options(std::size_t workers, std::size_t blocking_threads = 2) {
    sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 1;
    o.blocking.num_threads = blocking_threads;
    o.stacks.max_stacks = 1024;
    return o;
}

// A file in the current directory (the build tree: a disk filesystem, so
// POSIX_FADV_DONTNEED can evict its pages), unlinked when done.
class TempPath {
public:
    TempPath() {
        char name[] = "./fs_test_XXXXXX";
        const int fd = ::mkstemp(name);
        if (fd >= 0) {
            ::close(fd);
            path_ = name;
        }
    }
    ~TempPath() {
        if (!path_.empty()) ::unlink(path_.c_str());
    }
    TempPath(const TempPath&) = delete;
    TempPath& operator=(const TempPath&) = delete;

    const char* c_str() const { return path_.c_str(); }
    bool ok() const { return !path_.empty(); }

private:
    std::string path_;
};

std::vector<char> pattern(std::size_t n, unsigned seed = 0) {
    std::vector<char> data(n);
    for (std::size_t i = 0; i < n; ++i) data[i] = static_cast<char>(i * 7 + i / 251 + seed * 13);
    return data;
}

void fill(const TempPath& path, const std::vector<char>& data) {
    const int fd = ::open(path.c_str(), O_WRONLY | O_TRUNC);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::pwrite(fd, data.data(), data.size(), 0), static_cast<ssize_t>(data.size()));
    ASSERT_EQ(::fsync(fd), 0);
    ::close(fd);
}

// Asks the kernel to drop the file's pages from the page cache (it may keep
// some: callers check what the nowait reads saw).
bool evict(int fd) { return ::fsync(fd) == 0 && ::posix_fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) == 0; }

// ---- counting / simulated nowait syscalls (NowaitSupport's test hook) ----

std::atomic<int> g_nowait_ok{0};
std::atomic<int> g_nowait_eagain{0};
std::atomic<int> g_nowait_other{0};

void reset_counts() {
    g_nowait_ok.store(0);
    g_nowait_eagain.store(0);
    g_nowait_other.store(0);
}

ssize_t count_result(ssize_t ret) {
    if (ret >= 0) {
        g_nowait_ok.fetch_add(1);
    } else if (errno == EAGAIN) {
        g_nowait_eagain.fetch_add(1);
    } else {
        g_nowait_other.fetch_add(1);
    }
    return ret;
}

// The real syscalls, counted.
ssize_t counting_preadv2(int fd, const iovec* iov, int cnt, off_t off, int flags) {
    return count_result(::preadv2(fd, iov, cnt, off, flags));
}
ssize_t counting_pwritev2(int fd, const iovec* iov, int cnt, off_t off, int flags) {
    return count_result(::pwritev2(fd, iov, cnt, off, flags));
}

// Nothing is ever cached: every nowait call would block.
ssize_t always_eagain(int, const iovec*, int, off_t, int) {
    g_nowait_eagain.fetch_add(1);
    errno = EAGAIN;
    return -1;
}

// A filesystem with nowait writes: the first call writes at most 1000
// bytes, the next one would block.
std::atomic<int> g_write_calls{0};
ssize_t short_write_then_eagain(int fd, const iovec* iov, int, off_t off, int) {
    if (g_write_calls.fetch_add(1) % 2 == 1) {
        errno = EAGAIN;
        return -1;
    }
    return ::pwrite(fd, iov->iov_base, std::min<std::size_t>(iov->iov_len, 1000), off);
}

// ------------------------------------------------------------------- files

// The blocking pool has stopped: every file call (offloaded or not) runs
// inline on the fiber's worker, with its normal result.
TEST(File, WorksAfterBlockingPoolStop) {
    TempPath path;
    ASSERT_TRUE(path.ok());
    fill(path, pattern(8192));
    Latch done(1);
    IoResult opened{}, read{}, synced{}, closed{};
    char buf[4096] = {};
    Scheduler scheduler(options(1));
    scheduler.blocking().stop();
    fiber::go(scheduler, [&]() noexcept {
        fs::File file;
        opened = file.open(path.c_str(), O_RDWR);
        const iovec iov{buf, sizeof(buf)};
        read = file.preadv(&iov, 1, 4096);
        synced = file.fsync();
        closed = file.close();
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_TRUE(opened.ok());
    EXPECT_EQ(read.value, 4096);
    EXPECT_EQ(std::memcmp(buf, pattern(8192).data() + 4096, 4096), 0);
    EXPECT_TRUE(synced.ok());
    EXPECT_TRUE(closed.ok());
}

TEST(File, EveryOpOnARealFile) {
    TempPath path;
    ASSERT_TRUE(path.ok());
    const std::vector<char> data = pattern(3 * 4096 + 100);
    Latch done(1);
    IoResult opened{}, wrote{}, wrotev{}, synced{}, dsynced{}, stated{}, readall{}, readv{}, at_eof{}, closed{};
    struct stat st{};
    std::vector<char> back(data.size());
    char v1[100], v2[200];
    Scheduler scheduler(options(2));
    fiber::go(scheduler, [&]() noexcept {
        fs::File file;
        opened = file.open(path.c_str(), O_RDWR | O_TRUNC | O_CLOEXEC);
        wrote = file.pwrite(data.data(), 3 * 4096, 0);
        const iovec out[2] = {{const_cast<char*>(data.data()) + 3 * 4096, 40},
                              {const_cast<char*>(data.data()) + 3 * 4096 + 40, 60}};
        wrotev = file.pwritev(out, 2, 3 * 4096);
        synced = file.fsync();
        dsynced = file.fdatasync();
        stated = file.stat(st);
        readall = file.pread(back.data(), back.size(), 0);
        const iovec in[2] = {{v1, sizeof(v1)}, {v2, sizeof(v2)}};
        readv = file.preadv(in, 2, 50);
        char byte;
        at_eof = file.pread(&byte, 1, static_cast<off_t>(data.size()));
        closed = file.close();
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    ASSERT_TRUE(opened.ok()) << std::strerror(opened.error);
    EXPECT_GE(opened.value, 0);
    EXPECT_EQ(wrote.value, 3 * 4096);
    EXPECT_EQ(wrotev.value, 100);
    EXPECT_TRUE(synced.ok());
    EXPECT_TRUE(dsynced.ok());
    EXPECT_TRUE(stated.ok());
    EXPECT_EQ(st.st_size, static_cast<off_t>(data.size()));
    EXPECT_EQ(readall.value, static_cast<ssize_t>(data.size()));
    EXPECT_EQ(back, data);
    EXPECT_EQ(readv.value, 300);
    EXPECT_EQ(std::memcmp(v1, data.data() + 50, 100), 0);
    EXPECT_EQ(std::memcmp(v2, data.data() + 150, 200), 0);
    EXPECT_EQ(at_eof.value, 0);
    EXPECT_TRUE(at_eof.ok());
    EXPECT_TRUE(closed.ok());
}

TEST(File, FreeFunctionsAndMoves) {
    TempPath path;
    ASSERT_TRUE(path.ok());
    fill(path, pattern(4096));
    Latch done(1);
    IoResult opened{}, read{}, st_ok{}, closed{}, moved_read{};
    bool moved_from_closed = false;
    char buf[4096];
    char buf2[16];
    struct stat st{};
    Scheduler scheduler(options(1));
    fiber::go(scheduler, [&]() noexcept {
        opened = fs::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (opened.ok()) {
            const int fd = static_cast<int>(opened.value);
            read = fs::pread(fd, buf, sizeof(buf), 0);
            st_ok = fs::fstat(fd, st);
            fs::File a(fd);
            fs::File b(std::move(a));
            moved_from_closed = !a.is_open();
            moved_read = b.pread(buf2, sizeof(buf2), 100);
            closed = fs::close(b.release());
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    ASSERT_TRUE(opened.ok());
    EXPECT_EQ(read.value, 4096);
    EXPECT_EQ(std::memcmp(buf, pattern(4096).data(), 4096), 0);
    EXPECT_TRUE(st_ok.ok());
    EXPECT_EQ(st.st_size, 4096);
    EXPECT_TRUE(moved_from_closed);
    EXPECT_EQ(moved_read.value, 16);
    EXPECT_EQ(std::memcmp(buf2, pattern(4096).data() + 100, 16), 0);
    EXPECT_TRUE(closed.ok());
}

TEST(File, Errors) {
    Latch done(1);
    IoResult missing{}, bad_read{}, bad_write{}, bad_sync{}, bad_close{}, bad_stat{}, bad_readv{}, closed_read{},
        dir_write{};
    Scheduler scheduler(options(1));
    fiber::go(scheduler, [&]() noexcept {
        fs::File file;
        missing = file.open("./fs_test_does_not_exist/x", O_RDONLY);
        char buf[16];
        closed_read = file.pread(buf, sizeof(buf), 0);  // never opened: fd -1
        bad_read = fs::pread(-1, buf, sizeof(buf), 0);
        bad_write = fs::pwrite(-1, buf, sizeof(buf), 0);
        bad_sync = fs::fsync(-1);
        bad_close = fs::close(-1);
        struct stat st{};
        bad_stat = fs::fstat(-1, st);
        const iovec iov{buf, sizeof(buf)};
        bad_readv = fs::preadv(-1, &iov, 1, 0);
        dir_write = fs::open(".", O_WRONLY);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(missing.error, ENOENT);
    EXPECT_EQ(missing.value, -1);
    EXPECT_EQ(closed_read.error, EBADF);
    EXPECT_EQ(bad_read.error, EBADF);
    EXPECT_EQ(bad_write.error, EBADF);
    EXPECT_EQ(bad_sync.error, EBADF);
    EXPECT_EQ(bad_close.error, EBADF);
    EXPECT_EQ(bad_stat.error, EBADF);
    EXPECT_EQ(bad_readv.error, EBADF);
    EXPECT_EQ(dir_write.error, EISDIR);
}

TEST(File, OutsideAFiberRunsInline) {
    TempPath path;
    ASSERT_TRUE(path.ok());
    fill(path, pattern(4096));
    fs::File file;
    ASSERT_TRUE(file.open(path.c_str(), O_RDWR).ok());
    char buf[4096];
    EXPECT_EQ(file.pread(buf, sizeof(buf), 0).value, 4096);
    EXPECT_EQ(std::memcmp(buf, pattern(4096).data(), 4096), 0);
    EXPECT_EQ(file.pwrite("abc", 3, 0).value, 3);
    EXPECT_TRUE(file.fdatasync().ok());
    EXPECT_TRUE(file.close().ok());
    EXPECT_EQ(fs::open("./fs_test_does_not_exist/x", O_RDONLY).error, ENOENT);
}

// Hot page cache: the read is served by preadv2(RWF_NOWAIT) on the worker,
// no offload.
TEST(FileNowait, HotCacheReadIsInline) {
    TempPath path;
    ASSERT_TRUE(path.ok());
    const std::vector<char> data = pattern(64 * 1024);
    fill(path, data);
    const int fd = ::open(path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    std::vector<char> warm(data.size());
    ASSERT_EQ(::pread(fd, warm.data(), warm.size(), 0), static_cast<ssize_t>(warm.size()));
    NowaitSupport support(&counting_preadv2, &counting_pwritev2);
    const NowaitSupport::Fs nowait = support.filesystem(fd);
    if (!support.read_supported(nowait)) {
        ::close(fd);
        GTEST_SKIP() << "no RWF_NOWAIT reads here";
    }
    reset_counts();
    Latch done(1);
    std::vector<char> back(data.size());
    int full = 0;
    Scheduler scheduler(options(1));
    fiber::go(scheduler, [&]() noexcept {
        for (int i = 0; i < 16; ++i) {
            const IoResult r = fs::pread(fd, back.data() + i * 4096, 4096, i * 4096, nowait, support);
            full += r.value == 4096 ? 1 : 0;
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    ::close(fd);
    EXPECT_EQ(full, 16);
    EXPECT_EQ(back, data);
    if (g_nowait_ok.load() != 16) {
        GTEST_SKIP() << "pages left the cache meanwhile (memory pressure?)";
    }
    EXPECT_EQ(g_nowait_eagain.load(), 0);
}

// Cold page cache (POSIX_FADV_DONTNEED): the nowait attempt says EAGAIN, the
// read is offloaded and still returns the data.
TEST(FileNowait, EvictedReadIsOffloaded) {
    TempPath path;
    ASSERT_TRUE(path.ok());
    const std::vector<char> data = pattern(256 * 1024, 3);
    fill(path, data);
    const int fd = ::open(path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    NowaitSupport support(&counting_preadv2, &counting_pwritev2);
    const NowaitSupport::Fs nowait = support.filesystem(fd);
    if (!support.read_supported(nowait) || !evict(fd)) {
        ::close(fd);
        GTEST_SKIP() << "no RWF_NOWAIT reads or no eviction here";
    }
    reset_counts();
    Latch done(1);
    std::vector<char> back(data.size());
    IoResult r{};
    Scheduler scheduler(options(1));
    fiber::go(scheduler, [&]() noexcept {
        r = fs::pread(fd, back.data(), back.size(), 0, nowait, support);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    ::close(fd);
    EXPECT_EQ(r.value, static_cast<ssize_t>(data.size()));
    EXPECT_EQ(back, data);
    if (g_nowait_eagain.load() == 0) {
        GTEST_SKIP() << "the pages stayed (or came back) in the page cache: no miss to offload";
    }
    EXPECT_TRUE(support.read_supported(nowait)) << "EAGAIN is not 'unsupported'";
}

// Every nowait read would block: always offloaded, results intact, from
// many fibers at once (the default 2 blocking threads).
TEST(FileNowait, ForcedOffloadManyFibers) {
    TempPath path;
    ASSERT_TRUE(path.ok());
    constexpr int kFibers = 64;
    const std::vector<char> data = pattern(kFibers * 4096, 5);
    fill(path, data);
    const int fd = ::open(path.c_str(), O_RDONLY);
    ASSERT_GE(fd, 0);
    NowaitSupport support(&always_eagain, &always_eagain);
    const NowaitSupport::Fs nowait = support.filesystem(fd);
    reset_counts();
    Latch done(kFibers);
    std::vector<char> back(data.size());
    std::atomic<int> bad{0};
    Scheduler scheduler(options(2));
    for (int f = 0; f < kFibers; ++f) {
        fiber::go(scheduler, [&, f]() noexcept {
            for (int r = 0; r < 4; ++r) {
                if (fs::pread(fd, back.data() + f * 4096, 4096, f * 4096, nowait, support).value != 4096) {
                    bad.fetch_add(1);
                }
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    ::close(fd);
    EXPECT_EQ(bad.load(), 0);
    EXPECT_EQ(back, data);
    EXPECT_EQ(g_nowait_eagain.load(), kFibers * 4);
}

// Writes: a tracked filesystem that rejects nowait writes (ext4 / tmpfs on
// current kernels) is learnt once, then writes go straight to the pool; one
// that serves them gets the inline path, and a short nowait write has its
// rest offloaded.
TEST(FileNowait, WritePaths) {
    TempPath path;
    ASSERT_TRUE(path.ok());
    const int fd = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    const std::vector<char> data = pattern(8192, 7);
    Latch done(1);
    IoResult real1{}, real2{}, fake{}, untracked{};
    bool real_supported_after = true;
    NowaitSupport real(&counting_preadv2, &counting_pwritev2);
    const NowaitSupport::Fs real_fs = real.filesystem(fd);
    NowaitSupport sim(&counting_preadv2, &short_write_then_eagain);
    const NowaitSupport::Fs sim_fs = sim.filesystem(fd);
    reset_counts();
    g_write_calls.store(0);
    int real_calls_after_first = 0;
    Scheduler scheduler(options(1));
    fiber::go(scheduler, [&]() noexcept {
        real1 = fs::pwrite(fd, data.data(), 4096, 0, real_fs, real);
        const int calls_after_first = g_nowait_ok.load() + g_nowait_eagain.load() + g_nowait_other.load();
        real_supported_after = real.write_supported(real_fs);
        real2 = fs::pwrite(fd, data.data() + 4096, 4096, 4096, real_fs, real);
        real_calls_after_first =
            g_nowait_ok.load() + g_nowait_eagain.load() + g_nowait_other.load() - calls_after_first;
        fake = fs::pwrite(fd, data.data(), 4096, 0, sim_fs, sim);
        untracked = fs::pwrite(fd, data.data(), 10, 0, NowaitSupport::Fs{}, sim);
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(real1.value, 4096);
    EXPECT_EQ(real2.value, 4096);
    if (!real_supported_after) {
        EXPECT_EQ(real_calls_after_first, 0) << "an unsupported filesystem costs no further syscalls";
    }
    EXPECT_EQ(fake.value, 4096);
    EXPECT_EQ(g_write_calls.load(), 2) << "one short nowait write, one EAGAIN, the rest offloaded";
    EXPECT_EQ(untracked.value, 10);
    std::vector<char> back(8192);
    ASSERT_EQ(::pread(fd, back.data(), back.size(), 0), 8192);
    EXPECT_EQ(back, data);
    ::close(fd);
}

// Many fibers on two workers write and read back their own blocks of one
// file, with fdatasync, then read again after eviction (offloaded reads).
TEST(FileNowait, ConcurrentWritesAndReads) {
    TempPath path;
    ASSERT_TRUE(path.ok());
    constexpr int kFibers = 128;
    constexpr std::size_t kBlock = 4096;
    fs::File* shared = nullptr;
    std::atomic<int> bad{0};
    Scheduler scheduler(options(2));
    {
        Latch opened(1);
        fs::File file;
        fiber::go(scheduler, [&]() noexcept {
            if (file.open(path.c_str(), O_RDWR | O_CLOEXEC).ok()) shared = &file;
            opened.count_down();
        });
        ASSERT_TRUE(opened.wait());
        ASSERT_NE(shared, nullptr);
        Latch written(kFibers);
        for (int f = 0; f < kFibers; ++f) {
            fiber::go(scheduler, [&, f]() noexcept {
                const std::vector<char> mine = pattern(kBlock, static_cast<unsigned>(f));
                std::vector<char> back(kBlock);
                const off_t at = static_cast<off_t>(f) * kBlock;
                if (shared->pwrite(mine.data(), kBlock, at).value != static_cast<ssize_t>(kBlock)) bad.fetch_add(1);
                if (f % 16 == 0 && !shared->fdatasync().ok()) bad.fetch_add(1);
                if (shared->pread(back.data(), kBlock, at).value != static_cast<ssize_t>(kBlock) || back != mine) {
                    bad.fetch_add(1);
                }
                written.count_down();
            });
        }
        ASSERT_TRUE(written.wait());
        EXPECT_EQ(bad.load(), 0);
        const bool evicted = evict(file.fd());
        Latch reread(kFibers);
        for (int f = 0; f < kFibers; ++f) {
            fiber::go(scheduler, [&, f]() noexcept {
                std::vector<char> back(kBlock);
                const off_t at = static_cast<off_t>(f) * kBlock;
                if (shared->pread(back.data(), kBlock, at).value != static_cast<ssize_t>(kBlock) ||
                    back != pattern(kBlock, static_cast<unsigned>(f))) {
                    bad.fetch_add(1);
                }
                reread.count_down();
            });
        }
        ASSERT_TRUE(reread.wait());
        EXPECT_EQ(bad.load(), 0) << (evicted ? "after eviction" : "pages stayed cached");
        Latch closed(1);
        fiber::go(scheduler, [&]() noexcept {
            if (!file.close().ok()) bad.fetch_add(1);
            closed.count_down();
        });
        ASSERT_TRUE(closed.wait());
    }
    EXPECT_EQ(bad.load(), 0);
}

}  // namespace
