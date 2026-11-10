// object_pool::Pooled: the RAII lease of a pool object, over ShardedPool
// (DefaultPool), stack::StackPool and a small fixed-slot pool; in plain code
// and moved into fibers.
//
// Fibers never use gtest assertions themselves: they record into atomics
// that the test thread checks after joining them (test-local Latch).

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <set>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "fibers/fiber/fiber.hpp"
#include "fibers/object_pool/pooled.hpp"
#include "fibers/object_pool/sharded_pool.hpp"
#include "fibers/sched/scheduler.hpp"
#include "fibers/stack/stack_pool.hpp"

namespace fiber = fibers::fiber;
namespace op = fibers::object_pool;
namespace sched = fibers::sched;
namespace this_fiber = fibers::fiber::this_fiber;
using fibers::sched::Scheduler;
using fibers::stack::Stack;
using fibers::stack::StackPool;
using fibers::stack::StackPoolOptions;
using namespace std::chrono_literals;

namespace {

struct Object {
    std::atomic<int> owner{0};  // 0 = free, else the holder's id
    int value = 0;
};

using Pool = op::DefaultPool<Object>;

static_assert(op::LeasablePool<Pool>);
static_assert(op::LeasablePool<StackPool<>>);
static_assert(sizeof(op::Pooled<Pool>) == 2 * sizeof(void*));
static_assert(std::is_nothrow_move_constructible_v<op::Pooled<Pool>>);
static_assert(std::is_nothrow_move_assignable_v<op::Pooled<Pool>>);
static_assert(std::is_nothrow_destructible_v<op::Pooled<Pool>>);
static_assert(!std::is_copy_constructible_v<op::Pooled<Pool>>);
static_assert(!std::is_copy_assignable_v<op::Pooled<Pool>>);
static_assert(std::is_same_v<decltype(std::declval<Pool&>().lease()), op::Pooled<Pool>>);
static_assert(std::is_same_v<decltype(std::declval<StackPool<>&>().lease()), op::Pooled<StackPool<>>>);

// One shard, no caches: LIFO, so "the same object comes back" is checkable,
// and nothing is stranded in per-thread caches.
op::PoolOptions plain(std::size_t initial) {
    return {.shards = 1, .initial_objects = initial, .grow_chunk = 1, .thread_cache_capacity = 0};
}

// Counts what goes through it (any LeasablePool inside).
template <class Inner>
struct Counting {
    using value_type = typename Inner::value_type;

    explicit Counting(Inner& p) noexcept : inner(p) {}

    value_type* acquire() noexcept {
        value_type* object = inner.acquire();
        if (object != nullptr) {
            acquired.fetch_add(1, std::memory_order_relaxed);
        }
        return object;
    }
    void release(value_type* object) noexcept {
        released.fetch_add(1, std::memory_order_relaxed);
        inner.release(object);
    }
    std::int64_t in_use() const noexcept { return acquired.load() - released.load(); }

    Inner& inner;
    std::atomic<std::int64_t> acquired{0};
    std::atomic<std::int64_t> released{0};
};

// A fixed array of N objects: acquire() returns nullptr when all are out
// (like the examples' connection slots).
template <std::size_t N>
struct Slots {
    using value_type = Object;

    Slots() noexcept {
        for (std::size_t i = 0; i < N; ++i) {
            free[i] = &objects[N - 1 - i];
        }
    }
    Object* acquire() noexcept {
        std::lock_guard guard(lock);
        return count == 0 ? nullptr : free[--count];
    }
    void release(Object* object) noexcept {
        std::lock_guard guard(lock);
        free[count++] = object;
    }
    std::size_t available() noexcept {
        std::lock_guard guard(lock);
        return count;
    }

    Object objects[N];
    Object* free[N] = {};
    std::size_t count = N;
    std::mutex lock;
};

static_assert(op::LeasablePool<Slots<1>>);
static_assert(op::LeasablePool<Counting<Pool>>);

// Joins fibers from a non-fiber thread. Declare it BEFORE the scheduler.
class Latch {
public:
    explicit Latch(std::int64_t count) : count_(count) {}
    void count_down() noexcept { count_.fetch_sub(1, std::memory_order_acq_rel); }
    bool wait(std::chrono::milliseconds timeout = 300s) {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (count_.load(std::memory_order_acquire) != 0) {
            if (std::chrono::steady_clock::now() > deadline) {
                return false;
            }
            std::this_thread::sleep_for(100us);
        }
        return true;
    }
    std::int64_t remaining() const noexcept { return count_.load(); }

private:
    std::atomic<std::int64_t> count_;
};

template <class Pred>
bool eventually(Pred pred, std::chrono::milliseconds timeout = 120s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) {
            return false;
        }
        std::this_thread::sleep_for(50us);
    }
    return true;
}

sched::SchedulerOptions options(std::size_t workers) {
    sched::SchedulerOptions o;
    o.pool.num_workers = workers;
    o.pollers.num_pollers = 1;
    o.blocking.num_threads = 1;
    o.stacks.stack_size = 64 * 1024;
    o.stacks.max_stacks = 8192;
    return o;
}

TEST(Pooled, LeaseAndReturn) {
    Pool pool(plain(1));
    Object* first = nullptr;
    {
        op::Pooled<Pool> a = op::lease(pool);
        ASSERT_TRUE(a);
        EXPECT_TRUE(a.owns());
        EXPECT_EQ(a.pool(), &pool);
        first = a.get();
        EXPECT_EQ(&*a, first);
        EXPECT_EQ(a.operator->(), first);
        a->value = 42;  // objects keep their state across leases
    }
    {
        auto b = pool.lease();  // the member, same thing
        EXPECT_EQ(b.get(), first);
        EXPECT_EQ(b->value, 42);
    }
    EXPECT_EQ(pool.capacity(), 1u);  // returned each time, never grew
    Object* raw = pool.acquire();
    EXPECT_EQ(raw, first);
    pool.release(raw);
}

TEST(Pooled, EmptyLease) {
    op::Pooled<Slots<1>> none;
    EXPECT_FALSE(none);
    EXPECT_FALSE(none.owns());
    EXPECT_EQ(none.get(), nullptr);
    EXPECT_EQ(none.pool(), nullptr);

    Slots<1> slots;
    Counting counting(slots);
    {
        auto a = op::lease(counting);
        ASSERT_TRUE(a);
        auto b = op::lease(counting);  // the pool is out of objects: empty
        EXPECT_FALSE(b);
        EXPECT_EQ(b.get(), nullptr);
        EXPECT_EQ(b.pool(), nullptr);
        b.reset();  // no-op
        EXPECT_EQ(b.disown(), nullptr);
    }
    EXPECT_EQ(counting.acquired.load(), 1);
    EXPECT_EQ(counting.released.load(), 1);  // only the real one
    EXPECT_EQ(slots.available(), 1u);
}

TEST(Pooled, MoveSemantics) {
    Pool pool(plain(4));
    Counting counting(pool);
    {
        auto a = op::lease(counting);
        Object* const object = a.get();
        auto b = std::move(a);
        EXPECT_FALSE(a);  // NOLINT(bugprone-use-after-move): moved-from is empty
        EXPECT_EQ(a.pool(), nullptr);
        EXPECT_EQ(b.get(), object);
        EXPECT_EQ(counting.released.load(), 0);

        // Move-assigning over an owning handle returns its object first.
        auto c = op::lease(counting);
        c = std::move(b);
        EXPECT_EQ(counting.released.load(), 1);
        EXPECT_EQ(c.get(), object);
        EXPECT_FALSE(b);  // NOLINT(bugprone-use-after-move)

        // Self-move keeps the object.
        auto& alias = c;
        c = std::move(alias);
        EXPECT_EQ(c.get(), object);
        EXPECT_EQ(counting.released.load(), 1);

        // Assigning an empty handle returns the object.
        c = op::Pooled<Counting<Pool>>();
        EXPECT_FALSE(c);
        EXPECT_EQ(counting.released.load(), 2);
    }
    // a, b, c destroyed empty: no double release.
    EXPECT_EQ(counting.acquired.load(), 2);
    EXPECT_EQ(counting.released.load(), 2);
}

TEST(Pooled, DisownResetAdopt) {
    Pool pool(plain(2));
    Counting counting(pool);
    Object* raw = nullptr;
    {
        auto a = op::lease(counting);
        Object* const object = a.get();
        Counting<Pool>* const owner = a.pool();
        raw = a.disown();
        EXPECT_EQ(raw, object);
        EXPECT_FALSE(a);
        EXPECT_EQ(a.pool(), nullptr);
        EXPECT_EQ(owner, &counting);
    }
    EXPECT_EQ(counting.released.load(), 0);  // disowned: not returned
    {
        op::Pooled<Counting<Pool>> adopted(counting, raw, std::adopt_lock);
        EXPECT_EQ(adopted.get(), raw);
        adopted.reset();  // returns now
        EXPECT_FALSE(adopted);
        EXPECT_EQ(counting.released.load(), 1);
        adopted.reset();  // idempotent
        EXPECT_EQ(counting.released.load(), 1);
    }
    EXPECT_EQ(counting.released.load(), 1);
    {
        op::Pooled<Counting<Pool>> none(counting, nullptr, std::adopt_lock);
        EXPECT_FALSE(none);
    }
    EXPECT_EQ(counting.in_use(), 0);
}

TEST(Pooled, StackPool) {
    StackPool<> stacks(StackPoolOptions{.stack_size = 16 * 1024, .max_stacks = 4});
    Stack* first = nullptr;
    for (int i = 0; i < 10; ++i) {
        auto s = stacks.lease();
        ASSERT_TRUE(s);
        EXPECT_FALSE(s->empty());
        EXPECT_GE(s->size(), 16u * 1024);
        if (first == nullptr) {
            first = s.get();
        }
        EXPECT_EQ(s.get(), first);  // back via the thread cache every time
    }
    EXPECT_EQ(stacks.mapped_stacks(), 1u);
    {
        std::vector<op::Pooled<StackPool<>>> all;
        for (int i = 0; i < 4; ++i) {
            all.push_back(op::lease(stacks));
        }
        std::set<Stack*> distinct;
        for (const auto& s : all) {
            distinct.insert(s.get());
        }
        EXPECT_EQ(distinct.size(), 4u);
    }
    EXPECT_EQ(stacks.mapped_stacks(), 4u);
    // All four back: four more leases need no new stack (max_stacks = 4
    // would fail fast otherwise).
    std::vector<op::Pooled<StackPool<>>> again;
    for (int i = 0; i < 4; ++i) {
        again.push_back(stacks.lease());
    }
    EXPECT_EQ(stacks.mapped_stacks(), 4u);
}

// Captured by move into a fiber's closure: returned when the closure is
// destroyed at fiber exit, or earlier by reset().
TEST(Pooled, MovedIntoFiberClosure) {
    Pool pool(plain(4));
    Counting counting(pool);
    Latch done(2);
    std::atomic<int> seen{0};
    Scheduler scheduler(options(2));
    {
        auto at_exit = op::lease(counting);
        at_exit->value = 7;
        fiber::go(scheduler, [&seen, &done, h = std::move(at_exit)]() noexcept {
            seen.fetch_add(h->value);
            done.count_down();  // h still owned here; returned after the body
        });
        EXPECT_FALSE(at_exit);  // NOLINT(bugprone-use-after-move)

        auto early = op::lease(counting);
        early->value = 100;
        fiber::go(scheduler, [&seen, &done, &counting, h = std::move(early)]() mutable noexcept {
            seen.fetch_add(h->value);
            const std::int64_t before = counting.released.load();
            h.reset();
            if (counting.released.load() > before) {
                seen.fetch_add(1000);
            }
            done.count_down();
        });
    }
    ASSERT_TRUE(done.wait());
    EXPECT_TRUE(eventually([&] { return counting.in_use() == 0; }));
    EXPECT_EQ(seen.load(), 1107);
    EXPECT_EQ(counting.acquired.load(), 2);
    EXPECT_EQ(counting.released.load(), 2);
}

// Many fibers x workers churn leases (moved between handles and into child
// fibers, disowned and adopted, reset early), with every object held by one
// fiber at a time; at the end every object is back and the pools did not
// grow. Bounded by iterations only (no timing assumptions); each fiber has
// at most one child alive, so at most 2 objects / stacks per fiber are out.
TEST(Pooled, ChurnManyFibers) {
    constexpr int kFibers = 256;
    constexpr int kRounds = 1000;
    // >= the most ever out at once: per fiber its own, its child's and (in
    // the moment a child's closure is being destroyed) the previous child's.
    constexpr std::size_t kObjects = 3 * kFibers + 16;
    Pool pool(op::PoolOptions{.shards = 4,
                              .initial_objects = kObjects,
                              .grow_chunk = 1,
                              .thread_cache_capacity = 0});
    Counting counting(pool);
    op::DefaultPool<Object> cached({.initial_objects = 64});  // with per-thread caches
    Counting counting_cached(cached);
    StackPool<> stacks(StackPoolOptions{.stack_size = 16 * 1024,
                                        .max_stacks = 3 * kFibers + 64 * 8 + 16,
                                        .thread_cache_capacity = 8});
    Counting counting_stacks(stacks);
    std::atomic<int> violations{0};
    Latch done(kFibers);
    constexpr int kChildrenPerFiber = (kRounds + 15) / 16;  // i % 16 == 0
    Latch children(std::int64_t{kFibers} * kChildrenPerFiber);
    std::vector<std::atomic<int>> child_alive(kFibers + 1);
    {
        Scheduler scheduler(options(4));
        for (int f = 1; f <= kFibers; ++f) {
            fiber::go(scheduler, [&, f]() noexcept {
                auto take = [&](auto& p) {
                    auto h = op::lease(p);
                    int expected = 0;
                    if (!h || !h->owner.compare_exchange_strong(expected, f)) {
                        violations.fetch_add(1);
                    }
                    return h;
                };
                auto give_back = [&](auto& h) {
                    int expected = f;
                    if (!h->owner.compare_exchange_strong(expected, 0)) {
                        violations.fetch_add(1);
                    }
                };
                for (int i = 0; i < kRounds; ++i) {
                    auto a = take(counting);
                    auto b = std::move(a);
                    if (i % 5 == 0) {
                        this_fiber::yield();  // hold it across a reschedule
                    }
                    switch (i % 4) {
                        case 0:
                            give_back(b);
                            b.reset();
                            break;
                        case 1: {
                            give_back(b);
                            Counting<Pool>* const owner = b.pool();
                            Object* raw = b.disown();
                            op::Pooled<Counting<Pool>> adopted(*owner, raw, std::adopt_lock);
                            break;  // adopted returns it
                        }
                        default:
                            give_back(b);
                            break;  // b's destructor returns it
                    }
                    auto c = take(counting_cached);
                    give_back(c);
                    auto s = op::lease(counting_stacks);
                    if (!s || s->empty()) {
                        violations.fetch_add(1);
                    }
                    if (i % 16 == 0) {
                        // A child fiber takes the stack and an object along
                        // and returns them when it exits, on any worker.
                        while (child_alive[f].load(std::memory_order_acquire) != 0) {
                            this_fiber::yield();  // the previous child is still running
                        }
                        child_alive[f].store(1, std::memory_order_relaxed);
                        auto o = take(counting);
                        fiber::go([&, f, s = std::move(s), o = std::move(o)]() noexcept {
                            int expected = f;
                            if (s->empty() || !o->owner.compare_exchange_strong(expected, 0)) {
                                violations.fetch_add(1);
                            }
                            children.count_down();
                            // s and o go back when the closure is destroyed,
                            // right after this; the parent's next child may
                            // overlap that by one object and one stack.
                            child_alive[f].store(0, std::memory_order_release);
                        });
                    }
                }
                done.count_down();
            });
        }
        ASSERT_TRUE(done.wait()) << done.remaining() << " fibers stuck";
        ASSERT_TRUE(children.wait()) << children.remaining() << " children stuck";
        ASSERT_TRUE(eventually([&] {
            return counting.in_use() == 0 && counting_cached.in_use() == 0 &&
                   counting_stacks.in_use() == 0;
        })) << counting.in_use() << " " << counting_cached.in_use() << " " << counting_stacks.in_use();
    }  // workers joined
    EXPECT_EQ(violations.load(), 0);
    EXPECT_EQ(counting.acquired.load(), counting.released.load());
    EXPECT_GT(counting.acquired.load(), std::int64_t{kFibers} * kRounds);
    EXPECT_EQ(counting_cached.acquired.load(), counting_cached.released.load());
    EXPECT_EQ(counting_stacks.acquired.load(), counting_stacks.released.load());

    // The uncached pool is back to its initial objects: it never grew and all
    // of them can be taken again, distinct and free.
    EXPECT_EQ(pool.capacity(), kObjects);
    std::set<Object*> all;
    std::vector<Object*> raw;
    for (std::size_t i = 0; i < kObjects; ++i) {
        Object* o = pool.acquire();
        raw.push_back(o);
        all.insert(o);
        EXPECT_EQ(o->owner.load(), 0);
    }
    EXPECT_EQ(all.size(), kObjects);
    EXPECT_EQ(pool.capacity(), kObjects);
    for (Object* o : raw) {
        pool.release(o);
    }
}

// Debug builds: destroying a pool while a handle still owns one of its
// objects fails fast (the handle would return it into freed memory).
TEST(PooledDeathTest, PoolDestroyedWithOutstandingLease) {
#ifdef NDEBUG
    GTEST_SKIP() << "the lease counter exists in debug builds only";
#else
    EXPECT_DEATH(
        {
            auto* pool = new Pool(plain(1));
            auto* leaked = new op::Pooled<Pool>(pool->lease());
            static_cast<void>(leaked);
            delete pool;
        },
        "Pooled handles still own");
    EXPECT_DEATH(
        {
            auto* stacks = new StackPool<>(StackPoolOptions{.max_stacks = 2});
            auto* leaked = new op::Pooled<StackPool<>>(stacks->lease());
            static_cast<void>(leaked);
            delete stacks;
        },
        "Pooled handles still own");
#endif
}

// Disowned or returned leases are not outstanding (no death), nor are raw
// acquires.
TEST(Pooled, DisownedAndRawAreNotCounted) {
    auto* pool = new Pool(plain(2));
    auto a = pool->lease();
    Object* raw = a.disown();
    Object* other = pool->acquire();
    delete pool;  // fine: no handle owns anything
    static_cast<void>(raw);
    static_cast<void>(other);
}

}  // namespace
