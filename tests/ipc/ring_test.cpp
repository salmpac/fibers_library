// Shared-memory SPSC ring: layout, single-process stress, cross-process via
// fork() with the consumer sleeping in the poller and woken through an eventfd.

#include <gtest/gtest.h>

#include <poll.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>

#include "ipc/doorbell.hpp"
#include "ipc/fd_passing.hpp"
#include "ipc/ring_checkable.hpp"
#include "ipc/spsc_ring.hpp"
#include "poller/eventfd.hpp"
#include "poller/poller.hpp"

namespace {

using namespace std::chrono_literals;
using Clock = std::chrono::steady_clock;

struct Item {
    std::uint64_t seq;
    std::uint64_t payload;
};

using Ring = ipc::Ring<Item>;

// MAP_SHARED anonymous memory: shared with fork()ed children.
struct SharedMemory {
    explicit SharedMemory(std::size_t size) : size(size) {
        memory = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_ANONYMOUS, -1, 0);
        if (memory == MAP_FAILED) memory = nullptr;
    }
    ~SharedMemory() {
        if (memory != nullptr) munmap(memory, size);
    }
    void* memory = nullptr;
    std::size_t size;
};

template <class Pred>
bool wait_until(Pred pred, std::chrono::milliseconds timeout = 20s) {
    const auto deadline = Clock::now() + timeout;
    while (!pred()) {
        if (Clock::now() > deadline) return false;
        std::this_thread::sleep_for(50us);
    }
    return true;
}

// Waits for the poller-less consumer's doorbell. false: timed out (1 s) --
// with items in the ring afterwards that is a lost wakeup.
bool sleep_on(int fd) {
    pollfd pfd{fd, POLLIN, 0};
    int ready;
    while ((ready = poll(&pfd, 1, 1000)) < 0 && errno == EINTR) {
    }
    return ready > 0;
}

}  // namespace

TEST(IpcRing, CreateAttachValidates) {
    SharedMemory shm(Ring::bytes_for(64));
    ASSERT_NE(shm.memory, nullptr);
    EXPECT_FALSE(Ring::attach(shm.memory, shm.size).valid()) << "not initialised yet";
    EXPECT_FALSE(Ring::create(shm.memory, shm.size, 48).valid()) << "not a power of two";
    EXPECT_FALSE(Ring::create(shm.memory, shm.size, 128).valid()) << "does not fit";
    Ring ring = Ring::create(shm.memory, shm.size, 64, /*generation=*/7);
    ASSERT_TRUE(ring.valid());
    EXPECT_EQ(ring.capacity(), 64u);
    EXPECT_EQ(Ring::kDataOffset, 256u);
    Ring attached = Ring::attach(shm.memory, shm.size);
    ASSERT_TRUE(attached.valid());
    EXPECT_EQ(attached.header()->generation, 7u);
    EXPECT_FALSE((ipc::Ring<std::uint32_t>::attach(shm.memory, shm.size).valid())) << "element size";
    EXPECT_FALSE(Ring::attach(shm.memory, Ring::bytes_for(32)).valid()) << "region too small";
    // Raw layout as the C side sees it.
    auto* bytes = static_cast<unsigned char*>(shm.memory);
    std::uint64_t magic;
    std::memcpy(&magic, bytes, 8);
    EXPECT_EQ(magic, ipc::kRingMagic);
    EXPECT_EQ(std::memcmp(bytes, "FIBRSPSC", 8), 0);
}

TEST(IpcRing, PushPopWrapAround) {
    SharedMemory shm(Ring::bytes_for(8));
    Ring ring = Ring::create(shm.memory, shm.size, 8);
    ipc::Producer<Item> producer(ring);
    ipc::Consumer<Item> consumer(ring);
    std::uint64_t next_in = 0, next_out = 0;
    Item in[5], out[5];
    for (int round = 0; round < 100; ++round) {
        for (std::size_t expected : {5u, 3u}) {  // 5 fit, then only 3 of 5
            for (std::size_t i = 0; i < 5; ++i) in[i] = Item{next_in + i, (next_in + i) * 3};
            const std::size_t pushed = producer.try_push(in, 5);
            ASSERT_EQ(pushed, expected);
            next_in += pushed;
        }
        EXPECT_EQ(producer.free_space(), 0u);
        while (const std::size_t n = consumer.try_pop(out, 5)) {
            for (std::size_t i = 0; i < n; ++i) {
                ASSERT_EQ(out[i].seq, next_out);
                ASSERT_EQ(out[i].payload, next_out * 3);
                ++next_out;
            }
        }
        EXPECT_EQ(next_out, next_in);
        EXPECT_TRUE(consumer.empty());
    }
}

TEST(IpcRing, ArmProtocolSingleThread) {
    SharedMemory shm(Ring::bytes_for(8));
    Ring ring = Ring::create(shm.memory, shm.size, 8);
    ipc::Producer<Item> producer(ring);
    ipc::Consumer<Item> consumer(ring);
    ipc::EventFdDoorbell bell{poller::make_eventfd()};
    Item item{1, 2};
    // Not sleeping: no doorbell.
    EXPECT_EQ(producer.push(&item, 1, bell), 1u);
    EXPECT_EQ(poller::eventfd_drain(bell.fd), 0u);
    EXPECT_FALSE(consumer.arm()) << "item pending: must not sleep";
    EXPECT_EQ(ring.header()->sleeping.load(), 0u);
    Item out;
    EXPECT_EQ(consumer.try_pop(&out, 1), 1u);
    EXPECT_TRUE(consumer.arm());
    EXPECT_EQ(ring.header()->sleeping.load(), 1u);
    EXPECT_EQ(producer.push(&item, 1, bell), 1u);
    EXPECT_EQ(poller::eventfd_drain(bell.fd), 1u) << "sleeping consumer gets the doorbell";
    consumer.disarm();
    EXPECT_EQ(ring.header()->sleeping.load(), 0u);
    close(bell.fd);
}

// One producer thread, one consumer thread that sleeps on the eventfd
// whenever arm() allows it. Exact counts and order.
TEST(IpcRing, SingleProcessStress) {
    constexpr std::uint64_t kItems = 2'000'000;
    SharedMemory shm(Ring::bytes_for(256));
    Ring ring = Ring::create(shm.memory, shm.size, 256);
    ipc::EventFdDoorbell bell{poller::make_eventfd()};
    std::atomic<std::uint64_t> rings{0};
    std::thread producer_thread([&] {
        ipc::Producer<Item> producer(ring);
        Item batch[16];
        std::uint64_t next = 0;
        while (next < kItems) {
            const std::size_t n = std::min<std::uint64_t>(16, kItems - next);
            for (std::size_t i = 0; i < n; ++i) batch[i] = Item{next + i, ~(next + i)};
            std::size_t pushed = producer.try_push(batch, n);
            if (pushed != 0 && producer.notify(bell)) rings.fetch_add(1, std::memory_order_relaxed);
            next += pushed;
            if (next % 100'000 < 16) std::this_thread::sleep_for(100us);  // let it sleep
        }
    });
    ipc::Consumer<Item> consumer(ring);
    Item out[32];
    std::uint64_t expected = 0, sleeps = 0, lost_wakeups = 0;
    while (expected < kItems) {
        const std::size_t n = consumer.try_pop(out, 32);
        for (std::size_t i = 0; i < n; ++i) {
            ASSERT_EQ(out[i].seq, expected);
            ASSERT_EQ(out[i].payload, ~expected);
            ++expected;
        }
        if (n == 0 && consumer.arm()) {
            ++sleeps;
            if (!sleep_on(bell.fd) && !consumer.empty()) ++lost_wakeups;
            consumer.disarm();
            poller::eventfd_drain(bell.fd);
        }
    }
    producer_thread.join();
    EXPECT_EQ(lost_wakeups, 0u) << "slept through items: lost wakeup";
    std::printf("[ ring ] %llu consumer sleeps, %llu doorbells\n",
                static_cast<unsigned long long>(sleeps), static_cast<unsigned long long>(rings.load()));
    EXPECT_GT(sleeps, 0u);
    close(bell.fd);
}

// Hammers the arm / notify race. One item in flight at a time: the producer
// pushes, spins a random sub-microsecond gap (so its publication overlaps the
// consumer arming), then waits for the item to be consumed; the consumer arms
// as soon as the ring is empty. With a correct protocol a sleeping consumer
// with an item in the ring always has its doorbell rung, so a poll TIMEOUT
// (100 ms) followed by a non-empty ring is exactly a lost wakeup -- however
// slowly the threads get scheduled (TSan, loaded build machines). The
// one-in-flight design means no later push can mask it.
TEST(IpcRing, ArmRaceStress) {
    constexpr std::uint64_t kItems = 50'000;
    SharedMemory shm(Ring::bytes_for(64));
    Ring ring = Ring::create(shm.memory, shm.size, 64);
    ipc::EventFdDoorbell bell{poller::make_eventfd()};
    std::thread producer_thread([&] {
        ipc::Producer<Item> producer(ring);
        std::uint64_t rng = 88172645463325252ull;
        for (std::uint64_t next = 0; next < kItems; ++next) {
            const Item item{next, ~next};
            producer.push(&item, 1, bell);
            rng ^= rng << 13, rng ^= rng >> 7, rng ^= rng << 17;
            const auto gap_end = Clock::now() + std::chrono::nanoseconds(rng % 2000);
            while (Clock::now() < gap_end) {
            }
            while (ring.header()->head.load(std::memory_order_acquire) <= next) {
                std::this_thread::yield();
            }
        }
    });
    ipc::Consumer<Item> consumer(ring);
    Item out[16];
    std::uint64_t expected = 0, sleeps = 0, lost_wakeups = 0;
    while (expected < kItems) {
        const std::size_t n = consumer.try_pop(out, 16);
        for (std::size_t i = 0; i < n; ++i) ASSERT_EQ(out[i].seq, expected++);
        if (n == 0 && consumer.arm()) {
            ++sleeps;
            pollfd pfd{bell.fd, POLLIN, 0};
            int ready;
            while ((ready = poll(&pfd, 1, 100)) < 0 && errno == EINTR) {
            }
            if (ready == 0 && !consumer.empty()) ++lost_wakeups;
            consumer.disarm();
            poller::eventfd_drain(bell.fd);
        }
    }
    producer_thread.join();
    std::printf("[ arm race ] %llu sleeps, %llu lost wakeups\n", static_cast<unsigned long long>(sleeps),
                static_cast<unsigned long long>(lost_wakeups));
    EXPECT_GT(sleeps, 1000u);
    EXPECT_EQ(lost_wakeups, 0u);
    close(bell.fd);
}

namespace {

struct ChildStats {
    std::atomic<std::uint64_t> rings{0};
    std::atomic<std::uint64_t> pushed{0};
};

// Child producer: kItems in bursts, pausing so the consumer falls asleep.
[[noreturn]] void run_child_producer(void* ring_memory, std::size_t bytes, int bell_fd,
                                     ChildStats* stats, std::uint64_t items, std::uint64_t burst) {
    Ring ring = Ring::attach(ring_memory, bytes);
    if (!ring.valid()) _exit(2);
    ipc::Producer<Item> producer(ring);
    ipc::EventFdDoorbell bell{bell_fd};
    Item batch[32];
    std::uint64_t next = 0;
    while (next < items) {
        const std::size_t n = static_cast<std::size_t>(std::min<std::uint64_t>(32, items - next));
        for (std::size_t i = 0; i < n; ++i) batch[i] = Item{next + i, (next + i) * 7};
        const std::size_t pushed = producer.try_push(batch, n);
        if (pushed != 0 && producer.notify(bell)) stats->rings.fetch_add(1);
        next += pushed;
        stats->pushed.store(next, std::memory_order_relaxed);
        if (pushed != 0 && next % burst < pushed) usleep(2000);  // consumer goes to sleep
    }
    _exit(0);
}

struct ConsumerState {
    std::uint64_t expected = 0;
    std::atomic<std::uint64_t> received{0};
    std::atomic<bool> bad{false};
    std::atomic<bool> peer_exited{false};
};

void on_items(void* context, const Item* items, std::size_t count, poller::TaskBatch&) noexcept {
    auto* state = static_cast<ConsumerState*>(context);
    for (std::size_t i = 0; i < count; ++i) {
        if (items[i].seq != state->expected || items[i].payload != state->expected * 7) {
            state->bad.store(true);
        }
        ++state->expected;
    }
    state->received.fetch_add(count, std::memory_order_relaxed);
}

struct NoSink {
    void submit_batch(poller::Task* tasks, std::size_t count) noexcept {
        for (std::size_t i = 0; i < count; ++i) std::move(tasks[i]).run();
    }
};

}  // namespace

// Child = producer process, parent = poller consuming through RingCheckable;
// child exit observed through a pidfd Epollable.
TEST(IpcRing, CrossProcessForkConsumerSleepsAndWakes) {
    constexpr std::uint64_t kItems = 300'000;
    constexpr std::size_t kCapacity = 1024;
    const std::size_t ring_bytes = Ring::bytes_for(kCapacity);
    SharedMemory shm(ring_bytes + 4096);
    ASSERT_NE(shm.memory, nullptr);
    Ring ring = Ring::create(shm.memory, ring_bytes, kCapacity, 1);
    ASSERT_TRUE(ring.valid());
    auto* stats = new (static_cast<char*>(shm.memory) + ring_bytes) ChildStats{};
    const int bell_fd = poller::make_eventfd();  // inherited by the child

    // No threads exist yet: fork is safe.
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        usleep(50'000);  // let the parent's poller start and fall asleep first
        run_child_producer(shm.memory, ring_bytes, bell_fd, stats, kItems, 20'000);
    }

    ConsumerState state;
    NoSink sink;
    poller::Poller<> p(poller::TaskSink::of(sink), poller::PollerOptions{.spin_budget = 2000});
    ipc::RingCheckable<Item> consumer(ring, ipc::EventFdDoorbell{bell_fd}, &on_items, &state);
    p.add(consumer.checkable());

    // Peer death detection: pidfd readable when the child exits.
    const int pidfd = static_cast<int>(syscall(SYS_pidfd_open, child, 0));
    poller::Epollable death{pidfd, &state, [](void* memo, std::uint32_t, poller::TaskBatch&) noexcept {
                                static_cast<ConsumerState*>(memo)->peer_exited.store(true);
                            }};
    if (pidfd >= 0) {
        ASSERT_EQ(p.watch(death, EPOLLIN), 0);
    }

    p.start();
    const bool all = wait_until([&] { return state.received.load() == kItems; }, 60s);
    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    ASSERT_TRUE(all) << "received " << state.received.load() << " of " << kItems << " (child pushed "
                     << stats->pushed.load() << ")";
    if (pidfd >= 0) {
        EXPECT_TRUE(wait_until([&] { return state.peer_exited.load(); }, 5s));
    }
    p.stop();
    EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0) << status;
    EXPECT_FALSE(state.bad.load()) << "order / payload mismatch";
    EXPECT_EQ(state.received.load(), kItems);
    const auto poller_stats = p.stats();
    std::printf("[ cross-process ] %llu doorbells from the child, %llu poller sleeps\n",
                static_cast<unsigned long long>(stats->rings.load()),
                static_cast<unsigned long long>(poller_stats.sleeps));
    EXPECT_GT(stats->rings.load(), 0u) << "the consumer never slept / was never woken";
    EXPECT_GT(poller_stats.sleeps, 0u);
    EXPECT_EQ(ring.header()->sleeping.load(), 0u) << "flag cleared after waking";
    p.remove(consumer.checkable());
    if (pidfd >= 0) {
        p.unwatch(death);
        close(pidfd);
    }
    close(bell_fd);
}

// Reverse direction: parent produces, child consumes and sleeps on its own
// eventfd (poll(2), no poller); exact count reported back via shared memory.
TEST(IpcRing, CrossProcessReverseDirection) {
    constexpr std::uint64_t kItems = 200'000;
    const std::size_t ring_bytes = Ring::bytes_for(512);
    SharedMemory shm(ring_bytes + 4096);
    Ring ring = Ring::create(shm.memory, ring_bytes, 512);
    ASSERT_TRUE(ring.valid());
    auto* result = new (static_cast<char*>(shm.memory) + ring_bytes) std::atomic<std::uint64_t>[2]{};
    const int bell_fd = poller::make_eventfd();
    const pid_t child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        Ring r = Ring::attach(shm.memory, ring_bytes);
        if (!r.valid()) _exit(2);
        ipc::Consumer<Item> consumer(r);
        Item out[64];
        std::uint64_t expected = 0, sleeps = 0;
        while (expected < kItems) {
            const std::size_t n = consumer.try_pop(out, 64);
            for (std::size_t i = 0; i < n; ++i) {
                if (out[i].seq != expected++) _exit(3);
            }
            if (n == 0 && consumer.arm()) {
                ++sleeps;
                if (!sleep_on(bell_fd) && !consumer.empty()) _exit(4);  // lost wakeup
                consumer.disarm();
                poller::eventfd_drain(bell_fd);
            }
        }
        result[0].store(expected);
        result[1].store(sleeps);
        _exit(0);
    }
    ipc::Producer<Item> producer(ring);
    ipc::EventFdDoorbell bell{bell_fd};
    std::uint64_t next = 0;
    while (next < kItems) {
        Item batch[16];
        const std::size_t n = static_cast<std::size_t>(std::min<std::uint64_t>(16, kItems - next));
        for (std::size_t i = 0; i < n; ++i) batch[i] = Item{next + i, 0};
        const std::size_t pushed = producer.push(batch, n, bell);
        next += pushed;
        if (pushed != 0 && next % 25'000 < pushed) std::this_thread::sleep_for(2ms);
    }
    int status = 0;
    ASSERT_EQ(waitpid(child, &status, 0), child);
    EXPECT_TRUE(WIFEXITED(status) && WEXITSTATUS(status) == 0) << status;
    EXPECT_EQ(result[0].load(), kItems);
    EXPECT_GT(result[1].load(), 0u) << "child consumer never slept";
    close(bell_fd);
}

TEST(IpcFdPassing, EventFdOverUnixSocket) {
    int sv[2];
    ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0, sv), 0);
    const int original = poller::make_eventfd();
    ASSERT_EQ(ipc::send_fd(sv[0], original), 0);
    const int received = ipc::recv_fd(sv[1]);
    ASSERT_GE(received, 0);
    EXPECT_NE(received, original);
    poller::eventfd_signal(received, 5);
    EXPECT_EQ(poller::eventfd_drain(original), 5u) << "same eventfd";
    close(sv[0]);
    EXPECT_EQ(ipc::recv_fd(sv[1]), -ECONNRESET);
    close(received);
    close(original);
    close(sv[1]);
}
