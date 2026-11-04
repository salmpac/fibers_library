#pragma once

// Timers of the poller: caller-owned intrusive nodes, an intrusive MPSC queue
// (Vyukov) that carries them from any thread to the poller, and the poller's
// private pairing heap.
//
// Ownership of a node (BasicTimer):
//
//   idle      owned by the caller: fields may be written, the node may be
//             destroyed or reused. Initial state.
//   pending   Poller::schedule() handed it to the poller. The caller must not
//             touch deadline / memo / callback (only Poller::cancel() is
//             allowed) until the callback is invoked.
//   callback  The poller invokes callback(memo, event, out) EXACTLY ONCE per
//             schedule(): with kFired when the deadline passed, or with
//             kCancelled after a successful cancel(). The node is idle again
//             (owned by the caller) from the moment the callback is entered;
//             the poller copies memo / callback beforehand and never touches
//             the node afterwards, so the callback may reschedule it (periodic
//             timer), return it to an object pool, or free it.
//
//   cancel()  true  -> the timer will not fire; the poller still references
//                      the node until it delivers kCancelled (lazy removal).
//             false -> too late: kFired has been / is being delivered.
//
// Lazy removal: a cancelled node is reclaimed when it reaches the top of the
// heap, when it is still in the MPSC queue at drain time, or by a compaction
// pass that runs as soon as cancelled nodes make up >= 1/4 of the heap (Go's
// runtime uses the same rule). Each compaction is O(n) and happens after
// >= n/4 cancellations, i.e. amortised O(1) per cancel. A cancellation does not
// wake a sleeping poller: reclamation then waits for its next iteration.

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "poller/sources.hpp"
#include "poller/sync.hpp"

namespace poller {

template <class Backend, SyncPolicy Sync>
class Poller;

template <class Timer>
class TimerHeap;

namespace detail {

template <SyncPolicy Sync>
struct MpscNode {
    typename Sync::template Atomic<MpscNode*> next{nullptr};
};

// Intrusive MPSC queue (Dmitry Vyukov's non-intrusive-stub variant).
//
//   push (any thread): n.next = null; prev = head.exchange(n); prev.next = n
//   pop  (consumer)  : walks from tail via `next`.
//
// Between a producer's exchange and its link store the queue is "in transit":
// the consumer cannot reach that node (nor the ones behind it) yet; pop()
// returns nullptr and maybe_nonempty() stays true until the link appears.
//
// The exchange is seq_cst: it is the producer's half of the poller's Dekker
// handshake (see Poller: push; load sleep_until vs. store sleep_until; load
// head).
template <SyncPolicy Sync>
class MpscQueue {
public:
    using Node = MpscNode<Sync>;

    MpscQueue() noexcept : head_(&stub_), tail_(&stub_) {}

    MpscQueue(const MpscQueue&) = delete;
    MpscQueue& operator=(const MpscQueue&) = delete;

    void push(Node* node) noexcept {
        node->next.store(nullptr, std::memory_order_relaxed);
        Node* prev = head_.exchange(node, std::memory_order_seq_cst);
        prev->next.store(node, std::memory_order_release);  // publishes the node's fields
    }

    // Consumer only. nullptr: empty, or a push is in transit.
    Node* pop() noexcept {
        Node* tail = tail_;
        Node* next = tail->next.load(std::memory_order_acquire);
        if (tail == &stub_) {
            if (next == nullptr) {
                return nullptr;
            }
            tail_ = next;
            tail = next;
            next = next->next.load(std::memory_order_acquire);
        }
        if (next != nullptr) {
            tail_ = next;
            return tail;
        }
        if (tail != head_.load(std::memory_order_acquire)) {
            return nullptr;  // in transit
        }
        push(&stub_);
        next = tail->next.load(std::memory_order_acquire);
        if (next != nullptr) {
            tail_ = next;
            return tail;
        }
        return nullptr;  // in transit (someone pushed before our stub)
    }

    // Consumer only, after pop() returned nullptr: true if something was
    // pushed (or is in transit). seq_cst: the poller's half of the Dekker
    // handshake.
    bool maybe_nonempty() const noexcept {
        return head_.load(std::memory_order_seq_cst) != tail_;
    }

private:
    alignas(kCacheLineSize) typename Sync::template Atomic<Node*> head_;  // producers
    alignas(kCacheLineSize) Node* tail_;                                  // consumer
    Node stub_;
};

}  // namespace detail

template <SyncPolicy Sync = StdSync>
class BasicTimer : public detail::MpscNode<Sync> {
public:
    using Callback = void (*)(void* memo, TimerEvent event, TaskBatch& out) noexcept;

    BasicTimer() noexcept = default;
    BasicTimer(void* memo, Callback callback) noexcept : memo(memo), callback(callback) {}

    BasicTimer(const BasicTimer&) = delete;
    BasicTimer& operator=(const BasicTimer&) = delete;

    // Written by the owner while the node is idle.
    TimePoint deadline{};
    void* memo = nullptr;
    Callback callback = nullptr;

    // True when the node belongs to the caller (acquire: everything the poller
    // did with it happens-before).
    bool idle() const noexcept { return state_.load(std::memory_order_acquire) == kIdle; }

private:
    template <class Backend, SyncPolicy S>
    friend class Poller;
    template <class Timer>
    friend class TimerHeap;

    enum : std::uint32_t { kIdle = 0, kPending = 1, kCancelled = 2 };

    typename Sync::template Atomic<std::uint32_t> state_{kIdle};
    // Pairing heap links (poller thread only).
    BasicTimer* child_ = nullptr;
    BasicTimer* sibling_ = nullptr;
};

using Timer = BasicTimer<StdSync>;

// Intrusive pairing heap ordered by deadline. Allocation-free, unbounded,
// O(1) insert, O(log n) amortised pop. Poller thread only.
template <class Timer>
class TimerHeap {
public:
    bool empty() const noexcept { return root_ == nullptr; }
    std::size_t size() const noexcept { return size_; }
    Timer* top() const noexcept { return root_; }

    void push(Timer* timer) noexcept {
        timer->child_ = nullptr;
        timer->sibling_ = nullptr;
        root_ = meld(root_, timer);
        ++size_;
    }

    Timer* pop() noexcept {
        Timer* top = root_;
        root_ = merge_pairs(top->child_);
        top->child_ = nullptr;
        --size_;
        return top;
    }

    // Removes every node for which pred(node) holds and hands it to
    // `removed(node)` (which may push into the MPSC queue, not into this
    // heap). O(n), no allocation: the tree is flattened through the sibling
    // links and the survivors are melded back.
    template <class Pred, class Removed>
    void remove_if(Pred pred, Removed removed) noexcept {
        Timer* stack = root_;  // the root never has siblings
        root_ = nullptr;
        size_ = 0;
        while (stack != nullptr) {
            Timer* node = stack;
            stack = node->sibling_;
            if (Timer* child = node->child_; child != nullptr) {
                Timer* last = child;
                while (last->sibling_ != nullptr) {
                    last = last->sibling_;
                }
                last->sibling_ = stack;
                stack = child;
            }
            node->child_ = nullptr;
            node->sibling_ = nullptr;
            if (pred(node)) {
                removed(node);
            } else {
                root_ = meld(root_, node);
                ++size_;
            }
        }
    }

private:
    // Both arguments are roots (their sibling links are ignored).
    static Timer* meld(Timer* a, Timer* b) noexcept {
        if (a == nullptr) {
            return b;
        }
        if (b == nullptr) {
            return a;
        }
        if (b->deadline < a->deadline) {
            std::swap(a, b);
        }
        b->sibling_ = a->child_;
        a->child_ = b;
        a->sibling_ = nullptr;
        return a;
    }

    // Standard two-pass merge of a sibling list, iteratively.
    static Timer* merge_pairs(Timer* first) noexcept {
        Timer* pairs = nullptr;  // melded pairs, in reverse order (via sibling)
        while (first != nullptr) {
            Timer* a = first;
            Timer* b = a->sibling_;
            if (b == nullptr) {
                a->sibling_ = pairs;
                pairs = a;
                break;
            }
            first = b->sibling_;
            Timer* pair = meld(a, b);
            pair->sibling_ = pairs;
            pairs = pair;
        }
        Timer* result = nullptr;
        while (pairs != nullptr) {
            Timer* next = pairs->sibling_;
            result = meld(result, pairs);
            pairs = next;
        }
        return result;
    }

    Timer* root_ = nullptr;
    std::size_t size_ = 0;
};

}  // namespace poller
