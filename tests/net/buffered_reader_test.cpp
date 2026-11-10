// net::BufferedReader over a scripted fake stream (exact control of how the
// bytes arrive and fail) and over socketpairs on a real Scheduler (split
// delivery, deadlines).
//
// Fibers never use gtest assertions themselves: they record into atomics
// that the test thread checks after a Latch.

#include <gtest/gtest.h>

#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "fibers/fiber/fiber.hpp"
#include "fibers/net/buffered_reader.hpp"
#include "fibers/net/socket.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fiber = fibers::fiber;
namespace net = fibers::net;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::sched::Scheduler;
using net::BufferedReader;
using net::IoResult;
using net::LineResult;
using net::Stream;
using namespace std::chrono_literals;

namespace {

// ---- a scripted stream -----------------------------------------------------------

// Each step is one read_some's worth (cut to the caller's size; the rest of
// a chunk stays for the next call), an error, or end of stream. Records the
// sizes asked for.
class FakeStream {
public:
    struct Step {
        std::string data;
        int error = 0;
        bool eof = false;
    };

    void chunk(std::string data) { steps_.push_back(Step{std::move(data), 0, false}); }
    void error(int e) { steps_.push_back(Step{{}, e, false}); }
    void eof() { steps_.push_back(Step{{}, 0, true}); }

    IoResult read_some(void* buffer, std::size_t size) noexcept {
        reads.push_back(size);
        if (steps_.empty()) {
            return IoResult{0, EAGAIN};  // the script ran out: a test bug
        }
        Step& step = steps_.front();
        if (step.eof) {
            return IoResult{0, 0};  // sticky
        }
        if (step.error != 0) {
            const int e = step.error;
            steps_.pop_front();
            return IoResult{0, e};
        }
        const std::size_t n = std::min(size, step.data.size());
        std::memcpy(buffer, step.data.data(), n);
        step.data.erase(0, n);
        if (step.data.empty()) {
            steps_.pop_front();
        }
        return IoResult{n, 0};
    }

    std::vector<std::size_t> reads;

private:
    std::deque<Step> steps_;
};

static_assert(net::ReadStream<FakeStream>);
static_assert(net::ReadStream<Stream>);
static_assert(net::ReadStream<BufferedReader<Stream>>);  // readers stack

std::string_view str(std::span<const std::byte> bytes) {
    return std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size());
}

TEST(BufferedReader, LinesSplitAcrossReads) {
    FakeStream s;
    s.chunk("GET / HT");
    s.chunk("TP/1.1\r\nHost: a\r");
    s.chunk("\n\r\nBODY");
    s.eof();
    std::byte storage[64];
    BufferedReader reader(s, storage);
    LineResult l = reader.read_line();
    ASSERT_TRUE(l);
    EXPECT_EQ(l.line, "GET / HTTP/1.1\r\n");
    l = reader.read_line();
    EXPECT_EQ(l.line, "Host: a\r\n");
    l = reader.read_line();
    EXPECT_EQ(l.line, "\r\n");
    l = reader.read_line();  // the stream ends without a delimiter
    ASSERT_TRUE(l);
    EXPECT_EQ(l.line, "BODY");
    l = reader.read_line();
    ASSERT_TRUE(l);
    EXPECT_TRUE(l.line.empty());  // end of stream
    EXPECT_EQ(reader.buffered(), 0u);
}

TEST(BufferedReader, ManyLinesInOneRead) {
    FakeStream s;
    s.chunk("a\nbb\nccc\n");
    s.eof();
    std::byte storage[32];
    BufferedReader reader(s, storage);
    EXPECT_EQ(reader.read_line().line, "a\n");
    EXPECT_EQ(reader.read_line().line, "bb\n");
    EXPECT_EQ(reader.read_line().line, "ccc\n");
    EXPECT_EQ(s.reads.size(), 1u);  // one syscall for all three
    EXPECT_TRUE(reader.read_line().line.empty());
}

TEST(BufferedReader, CustomDelimiter) {
    FakeStream s;
    s.chunk(std::string("key\0value\0", 10));
    s.eof();
    std::byte storage[16];
    BufferedReader reader(s, storage);
    EXPECT_EQ(reader.read_line('\0').line, std::string_view("key\0", 4));
    EXPECT_EQ(reader.read_line('\0').line, std::string_view("value\0", 6));
}

// A line longer than `max` (or than the buffer): EMSGSIZE, the first `max`
// bytes left buffered; the caller can skip them and go on.
TEST(BufferedReader, LineLongerThanMax) {
    FakeStream s;
    s.chunk("0123456789");
    s.chunk("abcdef\nok\n");
    s.eof();
    std::byte storage[64];
    BufferedReader reader(s, storage);
    LineResult l = reader.read_line('\n', 8);
    EXPECT_EQ(l.error, EMSGSIZE);
    EXPECT_EQ(l.line, "01234567");
    EXPECT_EQ(reader.buffered(), 10u);  // nothing consumed
    reader.consume(l.line.size());  // skip it, 8 bytes at a time
    l = reader.read_line('\n', 8);
    EXPECT_EQ(l.error, EMSGSIZE);  // "89abcdef\n" is 9 bytes
    EXPECT_EQ(l.line, "89abcdef");
    reader.consume(l.line.size());
    l = reader.read_line('\n', 8);
    ASSERT_TRUE(l);
    EXPECT_EQ(l.line, "\n");  // the end of the long line
    l = reader.read_line('\n', 8);
    ASSERT_TRUE(l);
    EXPECT_EQ(l.line, "ok\n");
}

TEST(BufferedReader, LineLongerThanBuffer) {
    FakeStream s;
    s.chunk(std::string(40, 'x') + "\n");
    s.eof();
    std::byte storage[16];
    BufferedReader reader(s, storage);
    const LineResult l = reader.read_line();  // max clamped to the capacity
    EXPECT_EQ(l.error, EMSGSIZE);
    EXPECT_EQ(l.line.size(), 16u);
    EXPECT_EQ(reader.buffered(), 16u);
}

// The delimiter exactly at position max: accepted.
TEST(BufferedReader, LineOfExactlyMax) {
    FakeStream s;
    s.chunk("abc");
    s.chunk("d\n");
    s.eof();
    std::byte storage[8];
    BufferedReader reader(s, storage);
    const LineResult l = reader.read_line('\n', 5);
    ASSERT_TRUE(l);
    EXPECT_EQ(l.line, "abcd\n");
}

// An error from the stream keeps what was buffered: the call can be
// repeated and picks up the partial line.
TEST(BufferedReader, ErrorKeepsPartialLine) {
    FakeStream s;
    s.chunk("hel");
    s.error(ETIMEDOUT);
    s.chunk("lo\n");
    s.eof();
    std::byte storage[16];
    BufferedReader reader(s, storage);
    LineResult l = reader.read_line();
    EXPECT_EQ(l.error, ETIMEDOUT);
    EXPECT_TRUE(l.line.empty());
    EXPECT_EQ(reader.buffered(), 3u);
    l = reader.read_line();
    ASSERT_TRUE(l);
    EXPECT_EQ(l.line, "hello\n");
}

// Bytes are moved to the front only when a fill needs room at the end.
TEST(BufferedReader, CompactsWhenFull) {
    FakeStream s;
    s.chunk("aaaaaa\nbbbb");  // 11 of 12 bytes
    s.chunk("bbbbbb\n");
    s.eof();
    std::byte storage[12];
    BufferedReader reader(s, storage);
    EXPECT_EQ(reader.read_line().line, "aaaaaa\n");
    EXPECT_EQ(reader.buffered(), 4u);
    const LineResult l = reader.read_line();  // needs room: moves "bbbb" to the front
    ASSERT_TRUE(l);
    EXPECT_EQ(l.line, "bbbbbbbbbb\n");
}

TEST(BufferedReader, ReadExactMixesBufferedAndDirect) {
    FakeStream s;
    std::string big(100, '\0');
    for (std::size_t i = 0; i < big.size(); ++i) {
        big[i] = static_cast<char>('A' + i % 26);
    }
    s.chunk("hdr:" + big.substr(0, 4));  // the header plus the start of the body
    s.chunk(big.substr(4));
    s.eof();
    std::byte storage[16];
    BufferedReader reader(s, storage);
    char hdr[4];
    IoResult r = reader.read_exact(hdr, 4);
    ASSERT_TRUE(r);
    EXPECT_EQ(std::string_view(hdr, 4), "hdr:");
    std::string body(100, '\0');
    r = reader.read_exact(body.data(), body.size());
    ASSERT_TRUE(r);
    EXPECT_EQ(r.bytes, 100u);
    EXPECT_EQ(body, big);
    // 4 buffered bytes, then 96 >= capacity: straight into the caller's buffer.
    EXPECT_EQ(s.reads.back(), 96u);
    r = reader.read_exact(hdr, 1);
    EXPECT_TRUE(r);
    EXPECT_EQ(r.bytes, 0u);  // end of stream
}

TEST(BufferedReader, ReadExactSmallPiecesThroughTheBuffer) {
    FakeStream s;
    s.chunk("ab");
    s.chunk("cd");
    s.chunk("ef");
    s.eof();
    std::byte storage[16];
    BufferedReader reader(s, storage);
    char out[5];
    const IoResult r = reader.read_exact(out, 5);
    ASSERT_TRUE(r);
    EXPECT_EQ(std::string_view(out, 5), "abcde");
    EXPECT_EQ(reader.buffered(), 1u);
    for (const std::size_t asked : s.reads) {
        EXPECT_EQ(asked, 16u);  // every read went through the buffer
    }
}

TEST(BufferedReader, ReadExactShortAtEofAndError) {
    {
        FakeStream s;
        s.chunk("abc");
        s.eof();
        std::byte storage[8];
        BufferedReader reader(s, storage);
        char out[8];
        const IoResult r = reader.read_exact(out, 6);
        EXPECT_TRUE(r);
        EXPECT_EQ(r.bytes, 3u);
    }
    {
        FakeStream s;
        s.chunk("abc");
        s.error(ECONNRESET);
        std::byte storage[8];
        BufferedReader reader(s, storage);
        char out[8];
        const IoResult r = reader.read_exact(out, 6);
        EXPECT_EQ(r.error, ECONNRESET);
        EXPECT_EQ(r.bytes, 3u);  // delivered before the error
        EXPECT_EQ(reader.buffered(), 0u);
    }
}

TEST(BufferedReader, ReadSome) {
    FakeStream s;
    s.chunk("hello world");
    s.chunk(std::string(32, 'z'));
    s.eof();
    std::byte storage[16];
    BufferedReader reader(s, storage);
    char out[64];
    IoResult r = reader.read_some(out, 5);
    ASSERT_TRUE(r);
    EXPECT_EQ(std::string_view(out, r.bytes), "hello");
    r = reader.read_some(out, sizeof out);  // the buffered rest only, no read
    EXPECT_EQ(std::string_view(out, r.bytes), " world");
    r = reader.read_some(out, sizeof out);  // empty buffer, big request: direct
    EXPECT_EQ(r.bytes, 32u);
    EXPECT_EQ(s.reads.back(), sizeof out);
    r = reader.read_some(out, sizeof out);
    EXPECT_TRUE(r);
    EXPECT_EQ(r.bytes, 0u);
    EXPECT_EQ(reader.read_some(out, 0).bytes, 0u);
}

TEST(BufferedReader, PeekAndConsume) {
    FakeStream s;
    s.chunk(std::string("\x01\x00", 2));
    s.chunk(std::string("\x05" "abcdeXY", 8));
    s.eof();
    std::byte storage[16];
    BufferedReader reader(s, storage);
    net::PeekResult p = reader.peek(3);  // a 3-byte frame header, delivered in two reads
    ASSERT_TRUE(p);
    ASSERT_GE(p.data.size(), 3u);
    EXPECT_EQ(static_cast<int>(p.data[2]), 5);
    const auto len = static_cast<std::size_t>(p.data[2]);
    reader.consume(3);
    p = reader.peek(len);
    ASSERT_TRUE(p);
    EXPECT_EQ(str(p.data.first(len)), "abcde");
    reader.consume(len);
    EXPECT_EQ(str(reader.view()), "XY");
    p = reader.peek(4);  // the stream ends first
    EXPECT_TRUE(p);
    EXPECT_EQ(str(p.data), "XY");
    EXPECT_EQ(reader.peek(17).error, EMSGSIZE);  // more than fits
}

TEST(BufferedReader, TakeBufferedHandsOverTheOvershoot) {
    FakeStream s;
    s.chunk("STARTTLS\n\x16\x03\x01rest-of-hello");
    std::byte storage[64];
    BufferedReader reader(s, storage);
    ASSERT_EQ(reader.read_line().line, "STARTTLS\n");
    const std::span<const std::byte> rest = reader.take_buffered();
    EXPECT_EQ(str(rest), "\x16\x03\x01rest-of-hello");
    EXPECT_EQ(reader.buffered(), 0u);
    EXPECT_TRUE(reader.take_buffered().empty());
}

TEST(BufferedReader, FillFullBuffer) {
    FakeStream s;
    s.chunk("12345678");
    std::byte storage[8];
    BufferedReader reader(s, storage);
    EXPECT_EQ(reader.fill().bytes, 8u);
    EXPECT_EQ(reader.fill().error, ENOBUFS);
    reader.clear();
    EXPECT_EQ(reader.buffered(), 0u);
}

TEST(BufferedReaderDeathTest, ConsumePastBuffered) {
    GTEST_FLAG_SET(death_test_style, "threadsafe");
    EXPECT_DEATH(
        {
            FakeStream s;
            std::byte storage[8];
            BufferedReader reader(s, storage);
            reader.consume(1);
        },
        "past the buffered bytes");
}

// ---- over sockets ---------------------------------------------------------------

class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}

    void count_down() noexcept { count_.fetch_sub(1, std::memory_order_acq_rel); }

    bool wait(std::chrono::milliseconds timeout = 60s) {
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

fibers::sched::SchedulerOptions options(std::size_t workers) {
    fibers::sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 1;
    o.blocking.num_threads = 1;
    o.stacks.max_stacks = 256;
    return o;
}

struct Pair {
    int fd[2] = {-1, -1};
    Pair() {
        if (::socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, fd) != 0) {
            fd[0] = fd[1] = -1;
        }
    }
};

// A writer sends lines and length-prefixed frames in odd-sized pieces with
// yields in between (split delivery: parked reads, lines across reads); the
// reader reassembles them with read_line / peek / read_exact.
TEST(BufferedReaderSocket, SplitDeliveryLinesAndFrames) {
    constexpr int kMessages = 2000;
    Latch done(2);
    std::atomic<int> errors{0};
    std::atomic<int> received{0};
    Scheduler scheduler(options(2));
    Pair pair;
    Stream a;
    Stream b;
    ASSERT_EQ(a.adopt(scheduler, pair.fd[0]), 0);
    ASSERT_EQ(b.adopt(scheduler, pair.fd[1]), 0);
    auto line_of = [](int i) { return "line " + std::to_string(i) + std::string(static_cast<std::size_t>(i % 50), '.'); };
    fiber::go(scheduler, [&]() noexcept {
        std::string out;
        for (int i = 0; i < kMessages; ++i) {
            out += line_of(i) + "\n";
            const std::string body(static_cast<std::size_t>(i % 300), static_cast<char>('a' + i % 26));
            out += static_cast<char>(body.size() >> 8);
            out += static_cast<char>(body.size() & 0xFF);
            out += body;
        }
        std::size_t pos = 0;
        std::size_t piece = 1;
        while (pos < out.size()) {
            const std::size_t n = std::min(piece, out.size() - pos);
            if (!a.write_all(out.data() + pos, n)) {
                errors.fetch_add(1);
                break;
            }
            pos += n;
            piece = piece * 7 % 977 + 1;
            if (piece % 3 == 0) {
                this_fiber::yield();
            }
        }
        static_cast<void>(a.shutdown(SHUT_WR));
        done.count_down();
    });
    fiber::go(scheduler, [&]() noexcept {
        std::byte storage[512];
        BufferedReader reader(b, storage);
        std::string body;
        for (int i = 0; i < kMessages; ++i) {
            const LineResult l = reader.read_line('\n', 128);
            if (!l || l.line != line_of(i) + "\n") {
                errors.fetch_add(1);
                break;
            }
            const net::PeekResult p = reader.peek(2);
            if (!p || p.data.size() < 2) {
                errors.fetch_add(1);
                break;
            }
            const std::size_t len =
                (static_cast<std::size_t>(p.data[0]) << 8) | static_cast<std::size_t>(p.data[1]);
            reader.consume(2);
            body.resize(len);
            const IoResult r = reader.read_exact(body.data(), len);
            if (!r || r.bytes != len || body != std::string(len, static_cast<char>('a' + i % 26))) {
                errors.fetch_add(1);
                break;
            }
            received.fetch_add(1);
        }
        if (!reader.read_line().line.empty()) {  // end of stream
            errors.fetch_add(1);
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(errors.load(), 0);
    EXPECT_EQ(received.load(), kMessages);
    EXPECT_EQ(a.close(), 0);
    EXPECT_EQ(b.close(), 0);
}

// The stream's read deadline: read_line fails with ETIMEDOUT and keeps the
// partial line; once the deadline is moved and the rest arrives, the same
// call completes it.
TEST(BufferedReaderSocket, DeadlineKeepsPartialLine) {
    Latch done(1);
    std::atomic<int> first_error{-1};
    std::atomic<std::size_t> kept{0};
    std::atomic<bool> completed{false};
    Scheduler scheduler(options(2));
    Pair pair;
    Stream b;
    ASSERT_EQ(b.adopt(scheduler, pair.fd[1]), 0);
    ASSERT_EQ(::write(pair.fd[0], "partial", 7), 7);
    fiber::go(scheduler, [&]() noexcept {
        std::byte storage[64];
        BufferedReader reader(b, storage);
        static_cast<void>(b.set_read_deadline(scheduler.now() + 30ms));
        LineResult l = reader.read_line();
        first_error.store(l.error);
        kept.store(reader.buffered());
        static_cast<void>(b.set_read_deadline(scheduler.now() + 10s));
        if (::write(pair.fd[0], " line\n", 6) == 6) {
            l = reader.read_line();
            completed.store(l && l.line == "partial line\n");
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(first_error.load(), ETIMEDOUT);
    EXPECT_EQ(kept.load(), 7u);
    EXPECT_TRUE(completed.load());
    EXPECT_EQ(b.close(), 0);
    ::close(pair.fd[0]);
}

// Lines longer than max over a socket: EMSGSIZE, skipped by the caller,
// then the following line reads fine.
TEST(BufferedReaderSocket, LongLineSkipped) {
    Latch done(1);
    std::atomic<int> errors{0};
    std::atomic<int> too_long{0};
    Scheduler scheduler(options(2));
    Pair pair;
    Stream b;
    ASSERT_EQ(b.adopt(scheduler, pair.fd[1]), 0);
    const std::string payload = std::string(1000, 'L') + "\nshort\n";
    ASSERT_EQ(::write(pair.fd[0], payload.data(), payload.size()), static_cast<ssize_t>(payload.size()));
    ::close(pair.fd[0]);
    fiber::go(scheduler, [&]() noexcept {
        std::byte storage[256];
        BufferedReader reader(b, storage);
        LineResult l = reader.read_line('\n', 64);
        while (!l && l.error == EMSGSIZE) {
            too_long.fetch_add(1);
            // Skip up to (and including) a delimiter inside what we have.
            const std::string_view seen = l.line;
            const std::size_t nl = seen.find('\n');
            reader.consume(nl == std::string_view::npos ? seen.size() : nl + 1);
            if (nl != std::string_view::npos) {
                break;
            }
            l = reader.read_line('\n', 64);
        }
        l = reader.read_line('\n', 64);
        if (!l || l.line != "short\n") {
            errors.fetch_add(1);
        }
        if (!reader.read_line().line.empty()) {
            errors.fetch_add(1);
        }
        done.count_down();
    });
    ASSERT_TRUE(done.wait());
    EXPECT_EQ(errors.load(), 0);
    EXPECT_GE(too_long.load(), 1000 / 64);
    EXPECT_EQ(b.close(), 0);
}

}  // namespace
