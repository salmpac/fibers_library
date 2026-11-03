#include <gtest/gtest.h>

#include "thread_pool/task.hpp"

using thread_pool::Task;

namespace {

struct Counters {
    int called = 0;
    int deleted = 0;
};

Task raw_task(Counters& c) {
    return Task(
        &c,
        [](void* m) noexcept { ++static_cast<Counters*>(m)->called; },
        [](void* m) noexcept { ++static_cast<Counters*>(m)->deleted; });
}

}  // namespace

TEST(Task, DefaultIsEmpty) {
    Task t;
    EXPECT_FALSE(t);
}

TEST(Task, RunCallsOnlyCallPtr) {
    Counters c;
    Task t = raw_task(c);
    ASSERT_TRUE(t);
    std::move(t).run();
    EXPECT_FALSE(t);
    EXPECT_EQ(c.called, 1);
    EXPECT_EQ(c.deleted, 0);
}

TEST(Task, DestroyingUnrunTaskCallsDeletePtr) {
    Counters c;
    { Task t = raw_task(c); }
    EXPECT_EQ(c.called, 0);
    EXPECT_EQ(c.deleted, 1);
}

TEST(Task, MoveTransfersOwnership) {
    Counters c;
    Task a = raw_task(c);
    Task b = std::move(a);
    EXPECT_FALSE(a);
    EXPECT_TRUE(b);
    Task d;
    d = std::move(b);
    EXPECT_FALSE(b);
    std::move(d).run();
    EXPECT_EQ(c.called, 1);
    EXPECT_EQ(c.deleted, 0);
}

TEST(Task, MoveAssignDropsPrevious) {
    Counters c1, c2;
    Task a = raw_task(c1);
    a = raw_task(c2);
    EXPECT_EQ(c1.deleted, 1);
    std::move(a).run();
    EXPECT_EQ(c2.called, 1);
}

TEST(Task, NullDeletePtrIsAllowed) {
    int called = 0;
    { Task t(&called, [](void* m) noexcept { ++*static_cast<int*>(m); }, nullptr); }
    EXPECT_EQ(called, 0);
}

