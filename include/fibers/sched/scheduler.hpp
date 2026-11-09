#pragma once

// Scheduler: the runtime that fibers and plain callbacks run on.
//
//   Scheduler {
//       ThreadPool      workers (one per core): run Tasks
//       Poller          reactor thread: fds, timers (sleep_for), polling sources
//       BlockingPool    a few threads for calls that block in the kernel
//       StackPool       fiber stacks
//       TaskMemoryPool  blocks for plain-callback tasks
//   }
//
// The Scheduler owns its parts and exposes them; it adds the glue: plain
// callbacks (submit(f): closure in a TaskMemoryPool block behind a
// TaskHeader), and the poller's and the blocking pool's output goes into the
// ThreadPool. Fibers are spawned with fiber::go(scheduler, f)
// (fibers/fiber/fiber.hpp).
//
// One Poller: timers of every fiber (this_fiber::sleep_for) go to it. Should
// the scheduler ever own several, the choice must stay deterministic per
// fiber (e.g. by worker index), see fiber::BasicFiber::sleep_until.
//
// Lifecycle, like ThreadPool: the constructor starts every thread (workers,
// the reactor, the blocking threads); stop() stops and joins them all
// (idempotent, not from a thread of this scheduler); the destructor stops.
// Stop order: the poller (no new timer wake-ups), the workers, the blocking
// threads. Members are destroyed in reverse declaration order, so the
// queue owners drop their leftovers while the memory pools still exist:
//   ~Poller        delivers kCancelled to pending timers (sleeping fibers
//                  get their wake Task, into the stopped ThreadPool's queues)
//   ~BlockingPool  drops queued ops (their continuation's delete_ptr)
//   ~ThreadPool    drops queued Tasks (delete_ptr): a callback's closure is
//                  destroyed and its block returned; a fiber that never ran
//                  is destroyed with its closure; a fiber that has started
//                  cannot be unwound (no exceptions), so its stack goes back
//                  to the StackPool without running the destructors of its
//                  frames
//   ~StackPool     unmaps every stack, including those of fibers that were
//                  still parked (abandoned the same way)
// So: let fibers finish before stopping if their frames own resources.
//
// Sync policy: ThreadPool and BlockingPool park threads (ParkingSyncPolicy),
// fibers switch through Sync::ExecutionContext (ContextSyncPolicy). Under
// Twist use FakeBackend<TwistSync> as the poller backend.

#include <cstddef>
#include <new>
#include <type_traits>
#include <utility>

#include "fibers/blocking/blocking_pool.hpp"
#include "fibers/poller/epoll_backend.hpp"
#include "fibers/poller/poller.hpp"
#include "fibers/sched/task_header.hpp"
#include "fibers/sched/task_memory_pool.hpp"
#include "fibers/stack/stack_pool.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/thread_pool/task.hpp"
#include "fibers/thread_pool/thread_pool.hpp"

namespace fibers::sched {

using thread_pool::Task;

template <class S>
concept SchedulerSyncPolicy = sync::ParkingSyncPolicy<S> && sync::ContextSyncPolicy<S>;

struct SchedulerOptions {
    thread_pool::ThreadPoolOptions pool{};
    poller::PollerOptions poller{};
    blocking::BlockingPoolOptions blocking{};
    stack::StackPoolOptions stacks{};
    TaskMemoryPoolOptions task_memory{};
};

template <SchedulerSyncPolicy Sync = sync::StdSync, poller::Backend B = poller::EpollBackend>
class BasicScheduler {
public:
    using SyncPolicy = Sync;
    using Backend = B;
    using ThreadPool = thread_pool::ThreadPool<Sync>;
    using Poller = poller::Poller<B, Sync>;
    using BlockingPool = blocking::BlockingPool<Sync>;
    using StackPool = stack::StackPool<Sync>;
    using TaskMemory = TaskMemoryPool<Sync>;
    using TaskHeader = BasicTaskHeader<BasicScheduler>;

    // Starts every thread. `backend_args` construct the poller's backend.
    // Fail fast if threads or memory cannot be had.
    template <class... BackendArgs>
    explicit BasicScheduler(SchedulerOptions options = {}, BackendArgs&&... backend_args) noexcept
        : stacks_(options.stacks),
          task_memory_(options.task_memory),
          pool_(options.pool),
          blocking_(options.blocking),
          poller_(poller::TaskSink::of(pool_), options.poller, std::forward<BackendArgs>(backend_args)...) {
        poller_.start();
    }

    BasicScheduler(const BasicScheduler&) = delete;
    BasicScheduler& operator=(const BasicScheduler&) = delete;

    ~BasicScheduler() { stop(); }

    // Stops and joins every thread; idempotent. Not from a thread of this
    // scheduler (a worker, the reactor, a blocking thread). What is still
    // queued is dropped on destruction (see the top comment).
    void stop() noexcept {
        poller_.stop();
        pool_.stop();
        blocking_.stop();
    }

    // ---- plain callbacks --------------------------------------------------

    // Runs f() on a worker. The closure is moved into a TaskMemoryPool block
    // behind a TaskHeader (size class chosen at compile time), destroyed
    // right after the call (or when the task is dropped), and the block goes
    // back to the pool. Any thread.
    template <class F>
        requires std::is_nothrow_invocable_v<std::decay_t<F>&> &&
                 std::is_nothrow_constructible_v<std::decay_t<F>, F&&>
    void submit(F&& f) noexcept {
        pool_.submit(make_task(std::forward<F>(f)));
    }

    // The same as a Task, for batching or for handing to another sink.
    template <class F>
        requires std::is_nothrow_invocable_v<std::decay_t<F>&> &&
                 std::is_nothrow_constructible_v<std::decay_t<F>, F&&>
    Task make_task(F&& f) noexcept {
        using Node = CallbackNode<std::decay_t<F>>;
        static_assert(TaskMemory::block_size_for(sizeof(Node)) != 0,
                      "Scheduler::submit: closure too large for TaskMemoryPool (TaskHeader + closure "
                      "must fit into TaskMemoryPool::kMaxBlockSize bytes); keep the state in an "
                      "object of your own (e.g. from your own object pool) and capture a pointer to it");
        static_assert(alignof(Node) <= TaskMemory::kBlockAlign,
                      "Scheduler::submit: over-aligned closure (TaskMemoryPool blocks are 16-byte "
                      "aligned); capture a pointer to the over-aligned object instead");
        void* block = task_memory_.template acquire<Node::block_size()>();
        Node* node = ::new (block) Node{TaskHeader{this, nullptr}, std::forward<F>(f)};
        return Task(node, &Node::run, &Node::drop);
    }

    // Raw tasks (memo, call_ptr, delete_ptr), e.g. intrusive user objects.
    void submit(Task task) noexcept { pool_.submit(std::move(task)); }

    // ---- parts --------------------------------------------------------------

    ThreadPool& pool() noexcept { return pool_; }
    Poller& poller() noexcept { return poller_; }
    BlockingPool& blocking() noexcept { return blocking_; }
    StackPool& stacks() noexcept { return stacks_; }
    TaskMemory& task_memory() noexcept { return task_memory_; }

    // Whether the calling thread is a worker of this scheduler's pool.
    bool in_worker_thread() const noexcept { return pool_.in_worker_thread(); }

private:
    // [ TaskHeader | closure ] in one block.
    template <class F>
    struct CallbackNode {
        TaskHeader header;
        F f;

        // A function: the class is complete in its body.
        static constexpr std::size_t block_size() noexcept {
            return TaskMemory::block_size_for(sizeof(CallbackNode));
        }

        static void run(void* memo) noexcept {
            auto* node = static_cast<CallbackNode*>(memo);
            BasicScheduler* scheduler = node->header.scheduler;
            node->f();
            node->~CallbackNode();
            scheduler->task_memory_.template release<block_size()>(node);
        }

        static void drop(void* memo) noexcept {
            auto* node = static_cast<CallbackNode*>(memo);
            BasicScheduler* scheduler = node->header.scheduler;
            node->~CallbackNode();
            scheduler->task_memory_.template release<block_size()>(node);
        }
    };

    // Declaration order = construction order; destruction runs backwards
    // (see the top comment): pools outlive the queues that hold their memory.
    StackPool stacks_;
    TaskMemory task_memory_;
    ThreadPool pool_;
    BlockingPool blocking_;
    Poller poller_;
};

using Scheduler = BasicScheduler<>;

}  // namespace fibers::sched
