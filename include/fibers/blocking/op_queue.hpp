#pragma once

// OpQueue: BlockingPool's submission queue. Intrusive (the ops are the
// nodes), unbounded, allocation-free; Dmitry Vyukov's intrusive MPSC queue
// with a stub node, made multi-consumer by a short consumer-side lock.
//
//   push (any thread, wait-free):  op.next = null              (relaxed)
//                                  prev = tail.exchange(op)    (acq_rel)
//                                  prev.next = op              (release)
//   pop  (under consumer_lock_):   walk from head_ along `next` (acquire)
//
// * The exchange is acq_rel: release publishes op.next = null (and the op's
//   fields) to the next producer, which links behind it; acquire orders that
//   producer's prev.next store after our null store in prev.next's
//   modification order (without it our null store could overwrite its link).
// * The link store is release, the consumer's load of it acquire: the op's
//   fields (parameters, continuation, sink) are published to the pool thread
//   that runs it.
//
// In transit. Between a producer's exchange and its link store, the node it
// pushed (and every node pushed behind it) is not reachable from head_ yet,
// although tail_ already shows it. pop() reports this state (kInTransit),
// distinct from kEmpty: a node is published only when it is linked. The
// window is two instructions of the producer; it is longer only if that
// thread is preempted right there. pop() waits it out (spins) and never
// reports it as empty: see BlockingPool ("The in-transit window") for the
// lost wakeup that giving up would cause.
//
// Consumers. Vyukov's pop is single-consumer (head_ is a plain pointer and
// the stub is re-pushed by the consumer), so pops are serialised with a
// Sync::Mutex: the critical section is a few loads and stores (plus a stub
// push when the queue drains), consumers are the pool's few threads (plus
// drains after stop()), and producers never take it. Uncontended, a mutex
// costs what a spin lock does (one CAS to lock, one RMW to unlock). It is
// not a spin lock because pool threads routinely share the cores with the
// main pool's workers (workers = cores, plus these threads): a spin lock
// whose holder is preempted makes every other pool thread burn a time slice
// (in a saturated benchmark on 10 CPUs this showed up as a ~1 ms p99), a
// mutex puts them to sleep. A lock-free multi-consumer claim (CAS on head_)
// would have to solve the stub re-push and node reuse (ABA) for the same few
// contenders.

#include <atomic>
#include <cstddef>
#include <mutex>

#include "fibers/blocking/ops.hpp"
#include "fibers/sync/sync.hpp"

namespace fibers::blocking::detail {

template <sync::SyncPolicy Sync>
class OpQueue {
public:
    using Op = BasicBlockingOp<Sync>;
    using Link = OpLink<Sync>;

    enum class Pop { kOp, kEmpty, kInTransit };

    OpQueue() noexcept : tail_(&stub_), head_(&stub_) {}

    OpQueue(const OpQueue&) = delete;
    OpQueue& operator=(const OpQueue&) = delete;

    // Any thread. Wait-free; never fails, never blocks.
    void push(Op& op) noexcept { push_link(static_cast<Link*>(&op)); }

    // Any thread. Pops the oldest op, or returns nullptr when the queue is
    // empty. While it is in transit, spins (Sync::spin_hint) until the
    // producer's link appears: the producer is wait-free and never takes
    // the consumer lock, so this terminates.
    Op* pop() noexcept {
        std::lock_guard<typename Sync::Mutex> lock(consumer_lock_);
        for (;;) {
            Op* op = nullptr;
            const Pop result = pop_locked(op);
            if (result == Pop::kOp) {
                return op;
            }
            if (result == Pop::kEmpty) {
                return nullptr;
            }
            Sync::spin_hint();
        }
    }

private:
    void push_link(Link* node) noexcept {
        node->next.store(nullptr, std::memory_order_relaxed);
        Link* prev = tail_.exchange(node, std::memory_order_acq_rel);
        prev->next.store(node, std::memory_order_release);
    }

    static Op* to_op(Link* link) noexcept { return static_cast<Op*>(link); }

    // Vyukov's pop, telling "empty" and "in transit" apart. Under the lock.
    Pop pop_locked(Op*& out) noexcept {
        Link* head = head_;
        Link* next = head->next.load(std::memory_order_acquire);
        if (head == &stub_) {
            if (next == nullptr) {
                // Nothing linked behind the stub: empty, unless a producer
                // has exchanged tail_ and not linked yet.
                return tail_.load(std::memory_order_relaxed) == &stub_ ? Pop::kEmpty : Pop::kInTransit;
            }
            head_ = next;  // skip the stub
            head = next;
            next = next->next.load(std::memory_order_acquire);
        }
        if (next != nullptr) {
            head_ = next;
            out = to_op(head);
            return Pop::kOp;
        }
        // `head` is a real op without a visible successor. Unless it is the
        // last node (tail_), its successor is in transit.
        if (tail_.load(std::memory_order_relaxed) != head) {
            return Pop::kInTransit;
        }
        // Last node: re-push the stub behind it so that it can be unlinked.
        push_link(&stub_);
        next = head->next.load(std::memory_order_acquire);
        if (next != nullptr) {
            head_ = next;
            out = to_op(head);
            return Pop::kOp;
        }
        // A producer exchanged between our tail_ check and the stub push;
        // its link into `head` is in transit (the stub is queued behind it).
        return Pop::kInTransit;
    }

    // Producers' end: exchanged by every push.
    alignas(sync::kCacheLineSize) typename Sync::template Atomic<Link*> tail_;
    // Consumers' end, guarded by consumer_lock_.
    alignas(sync::kCacheLineSize) typename Sync::Mutex consumer_lock_;
    Link* head_;
    Link stub_;
};

}  // namespace fibers::blocking::detail
