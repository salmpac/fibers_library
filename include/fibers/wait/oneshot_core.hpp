#pragma once

// OneshotCore / OneshotState / BasicPromise: a one-shot value hand-off
// (promise / future) over WaitNode, waiter-agnostic. Front-ends
// (fibers/fiber/oneshot.hpp: fiber::Oneshot; fibers/fiber/future.hpp: the
// pooled fiber::Future) add only "how to wait for the node's wake" and where
// the state lives.
//
// One producer, ONE consumer. The producer delivers a value (set) or gives
// up (abandon, or its Promise is destroyed): the result is then "broken".
// The consumer waits for either, or detaches (drops its interest; only a
// state whose memory is freed by the last side, i.e. a pooled one).
//
// State word (OneshotCore)
// ------------------------
// One consumer, so no wait list: a single word, like net::PollDesc's
// readiness words.
//
//   EMPTY    (0)   nothing yet, nobody waits
//   READY    (1)   final: the value is in the slot
//   BROKEN   (2)   final: the producer gave up, no value
//   DETACHED (3)   the consumer has gone; the producer frees the state
//   WaitNode*      the consumer waits (nodes are aligned to >= 4, so never
//                  0..3)
//
//   producer  complete(READY | BROKEN):   (set() first writes the value)
//     old = exchange(final, acq_rel)
//       EMPTY    -> done: never touch the state again
//       node     -> node->notify() (reads wake / self first), done; the
//                   state is not touched after the exchange, only the node
//       DETACHED -> the caller owns the state now: destroy the value, free
//       final    -> fail_fast (completed twice)
//   consumer  wait_or_enqueue(node):
//     s = load(acquire)
//       final    -> READY / BROKEN: do not wait
//       EMPTY    -> CAS(EMPTY -> &node, release; failure: acquire)
//                   ok -> PENDING: wait for the node's wake (exactly one:
//                         the producer completes once), then status()
//                   failed -> only the producer writes the word while it is
//                         EMPTY, so it is final now: READY / BROKEN
//       node / DETACHED -> fail_fast (one consumer, one wait at a time)
//   consumer  status(): load(acquire): PENDING / READY / BROKEN
//   consumer  detach():
//     old = exchange(DETACHED, acq_rel)
//       EMPTY    -> PENDING: the producer frees the state when it completes
//       final    -> READY / BROKEN: the producer is done with the state, the
//                   caller frees it (and destroys a value it did not take)
//       node     -> fail_fast (withdraw the waiter first, see "Later")
//
// Why nothing is lost: every transition out of EMPTY is an RMW on the one
// word, so the producer's exchange and the consumer's CAS / exchange are
// totally ordered. Exchange first: the consumer reads the final value and
// does not wait (or frees the state after detach()). Consumer first: the
// exchange reads its node (and wakes it; a wake that arrives before a fiber
// has left its stack is absorbed by its state word) or DETACHED (and frees).
//
// Lifetime: the producer's last touch of the state is its exchange (then
// only the node, which lives with the consumer, or, after DETACHED, the
// state it now owns). So the consumer may destroy / free the state as soon
// as it has observed a final word (status(), a returning wait_or_enqueue, or
// the node's wake followed by status()). Who frees a pooled state: the side
// that RMWs second (the consumer after observing final, or the producer
// after reading DETACHED): no reference count beyond the word itself.
//
// Memory orders
// -------------
//   * value: producer -> consumer. The value is constructed in the slot
//     before the exchange (release); the consumer reads the slot only after
//     it has observed READY with an acquire (status(), the load or the
//     failing CAS of wait_or_enqueue, whose result the front-ends use
//     without reading the word again; every later write of the word is the
//     consumer's own), so the construction happens-before the read. Also
//     orders all of the producer's earlier writes before the consumer's
//     return (a response's side data).
//   * node: consumer -> producer. node.wake / node.self (and whatever the
//     front-end put next to them) are written before the publishing CAS
//     (release); the exchange is acquire and reads that node.
//   * wake: the notify is sequenced after the exchange; the wake itself
//     (front-end) provides the release / acquire to the woken consumer
//     (WaitNode contract); the consumer then reads the word (status(),
//     acquire, redundant after such a wake).
//   * free by the producer (DETACHED): the consumer's detach exchange is a
//     release after its last touch of the state (its reads of the word:
//     the block is reused with plain writes once freed); the producer's
//     exchange (acquire) reads it, so the consumer's accesses
//     happen-before the free.
//   * free by the consumer: detach()'s exchange is also an acquire; it (or
//     the consumer's load) reads the producer's final exchange (release), so
//     the producer's value construction happens-before the consumer destroys
//     it and returns the block.
// No fence anywhere: the only race is the producer's exchange against the
// consumer's CAS / exchange on the SAME word (no Dekker pattern).
//
// Checked under Twist (tests/oneshot/twist): weakening any one of the
// orders above to relaxed (or either half of an acq_rel) makes a simulator
// test report a race, on a payload cell, on a cell next to the node, or on
// the state's probes (detail::ProbeOf: value slot, block).
//
// Later (timeouts / cancellation, not implemented): a consumer that stops
// waiting takes its node back out with CAS(&node -> EMPTY), as PollDesc's
// deadline expiry does. Success: the producer never saw the node, the
// consumer may leave (and then detach()). Failure: the producer's exchange
// took the node, its wake is on the way and must still be consumed (park
// once) before the node may go. The word needs no other change for that.
//
// OneshotState<T>: the core plus the value slot and, for a pooled state,
// how to return its block (release(context, block), a plain function
// pointer: no virtual calls). BasicPromise<T>: the producer's move-only
// handle over a state, the same for in-place (fiber::Oneshot) and pooled
// (fiber::Future) states.
//
// T = void: a completion signal without a value (Oneshot<void>,
// Promise<void> / Future<void>): the slot holds an empty tag (detail::Unit),
// set() takes no arguments and the consumer gets a Result<void>. The core
// and the protocol are the same.

#include <cstdint>
#include <new>
#include <type_traits>
#include <utility>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/wait/result.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::wait {

enum class OneshotStatus : std::uint8_t { kPending, kReady, kBroken };

namespace detail {

// Checker hook (Twist simulator: Sync::HbProbe, a plain cell checked for
// happens-before), so a missing release / acquire shows up as a race even
// though the simulated atomics are never stale. OneshotState has two:
//   value probe: the slot (written by emplace / destroy, read by take);
//   block probe: the state as a whole, i.e. each side's last touch of it
//     (read by the producer before its completion and by the consumer
//     before detach), written by release() (the free).
struct NoProbe {
    void write() noexcept {}
    void read() const noexcept {}
};

template <class Sync>
struct ProbeOf {
    using type = NoProbe;
};

template <class Sync>
    requires requires { typename Sync::HbProbe; }
struct ProbeOf<Sync> {
    using type = typename Sync::HbProbe;
};

// What the value slot holds: T, or an empty tag for T = void.
struct Unit {};

template <class T>
using SlotOf = std::conditional_t<std::is_void_v<T>, Unit, T>;

// BasicPromise<T>::set(T value) takes exactly this argument list.
template <class T, class... Args>
inline constexpr bool kConvertsTo = false;

template <class T, class Arg>
inline constexpr bool kConvertsTo<T, Arg> = std::is_convertible_v<Arg, T>;

template <class...>
inline constexpr bool kAlwaysFalse = false;

}  // namespace detail

template <sync::SyncPolicy Sync = sync::StdSync>
class OneshotCore {
public:
    OneshotCore() noexcept = default;
    OneshotCore(const OneshotCore&) = delete;
    OneshotCore& operator=(const OneshotCore&) = delete;

    ~OneshotCore() {
        if (is_node(state_.load(std::memory_order_relaxed))) [[unlikely]] {
            fail::fail_fast("wait: destroying a oneshot with an enqueued waiter");
        }
    }

    // Producer, exactly once: outcome is kReady (the value is in place) or
    // kBroken. Wakes the waiting consumer, if any. true: the consumer had
    // detached and the caller now owns the state (destroys the value, frees
    // the memory); false: the caller must not touch the state any more.
    [[nodiscard]] bool complete(OneshotStatus outcome) noexcept {
        const std::uintptr_t word = outcome == OneshotStatus::kReady ? kReady : kBroken;
        const std::uintptr_t old = state_.exchange(word, std::memory_order_acq_rel);
        if (old == kEmpty) {
            return false;
        }
        if (old == kDetached) {
            return true;
        }
        if (!is_node(old)) [[unlikely]] {
            fail::fail_fast("wait: a oneshot completed twice");
        }
        reinterpret_cast<WaitNode*>(old)->notify();  // reads wake / self first; last touch
        return false;
    }

    // Consumer. kReady / kBroken: the result is there (final, read with
    // acquire), do not wait. kPending: `node` (self / wake set) was
    // published; the caller must not touch it until its wake, which comes
    // exactly once, then reads status(). After kPending the node may be
    // woken (and its owner resumed) before this returns.
    OneshotStatus wait_or_enqueue(WaitNode& node) noexcept {
        std::uintptr_t s = state_.load(std::memory_order_acquire);
        if (s == kEmpty) {
            if (state_.compare_exchange_strong(s, reinterpret_cast<std::uintptr_t>(&node), std::memory_order_release,
                                               std::memory_order_acquire)) {
                return OneshotStatus::kPending;
            }
            // Only the producer writes an EMPTY word: s is final now.
        }
        if (s == kReady || s == kBroken) {
            return status_of(s);
        }
        fail::fail_fast(s == kDetached ? "wait: waiting on a detached oneshot"
                                       : "wait: a oneshot has one consumer (second waiter)");
    }

    // Consumer (acquire): kReady / kBroken order the caller after the
    // producer's completion.
    OneshotStatus status() const noexcept { return status_of(state_.load(std::memory_order_acquire)); }

    // Consumer gives up. kPending: the producer has not completed and will
    // free the state when it does. kReady / kBroken: the producer is done
    // with the state; the caller frees it (and destroys an untaken value).
    // Not while the consumer's node is enqueued; once.
    OneshotStatus detach() noexcept {
        const std::uintptr_t old = state_.exchange(kDetached, std::memory_order_acq_rel);
        if (old == kEmpty) {
            return OneshotStatus::kPending;
        }
        if (old == kReady || old == kBroken) {
            return status_of(old);
        }
        fail::fail_fast(old == kDetached ? "wait: a oneshot detached twice"
                                         : "wait: detaching a oneshot with an enqueued waiter");
    }

    // Racy snapshot (tests, diagnostics).
    bool has_waiter_approx() const noexcept { return is_node(state_.load(std::memory_order_relaxed)); }

private:
    static constexpr std::uintptr_t kEmpty = 0;
    static constexpr std::uintptr_t kReady = 1;
    static constexpr std::uintptr_t kBroken = 2;
    static constexpr std::uintptr_t kDetached = 3;
    static_assert(alignof(WaitNode) >= 4, "the words 0..3 must not be WaitNode addresses");

    static constexpr bool is_node(std::uintptr_t s) noexcept { return s > kDetached; }

    static constexpr OneshotStatus status_of(std::uintptr_t s) noexcept {
        if (s == kReady) {
            return OneshotStatus::kReady;
        }
        return s == kBroken ? OneshotStatus::kBroken : OneshotStatus::kPending;
    }

    typename Sync::template Atomic<std::uintptr_t> state_{kEmpty};
};

// The core, the value slot and (pooled) the release hook. The value is
// managed explicitly by the handles: emplace() by the producer, take() /
// destroy_value() by whoever owns the state after its final word.
template <class T, sync::SyncPolicy Sync = sync::StdSync>
class OneshotState {
    static_assert(std::is_void_v<T> || (std::is_object_v<T> && !std::is_array_v<T> && !std::is_const_v<T>),
                  "wait::OneshotState<T>: T must be void or a non-const object type (not a reference or array)");
    static_assert(std::is_void_v<T> || std::is_nothrow_move_constructible_v<T>,
                  "wait::OneshotState<T>: T must be nothrow-move-constructible (no exceptions in the library)");
    static_assert(std::is_void_v<T> || std::is_nothrow_destructible_v<T>,
                  "wait::OneshotState<T>: T must be nothrow-destructible");

public:
    using ReleaseFn = void (*)(void* context, void* block) noexcept;
    // What the slot holds: T, or detail::Unit for T = void.
    using Slot = detail::SlotOf<T>;

    // In place (fiber::Oneshot): never detached, never released.
    OneshotState() noexcept {}  // NOLINT: the union member stays unconstructed

    // In a block of its own: release(context, this) returns it.
    OneshotState(ReleaseFn release, void* context) noexcept : release_(release), context_(context) {}

    OneshotState(const OneshotState&) = delete;
    OneshotState& operator=(const OneshotState&) = delete;

    ~OneshotState() {}  // NOLINT: the value is destroyed explicitly (destroy_value / take)

    // ---- producer ----------------------------------------------------------

    template <class... Args>
    void emplace(Args&&... args) noexcept {
        value_probe_.write();
        ::new (static_cast<void*>(&value_)) Slot(std::forward<Args>(args)...);
    }

    [[nodiscard]] bool complete(OneshotStatus outcome) noexcept {
        block_probe_.read();  // the producer's last touch (unless DETACHED)
        return core_.complete(outcome);
    }

    // ---- consumer ----------------------------------------------------------

    OneshotStatus wait_or_enqueue(WaitNode& node) noexcept { return core_.wait_or_enqueue(node); }
    OneshotStatus status() const noexcept { return core_.status(); }

    OneshotStatus detach() noexcept {
        block_probe_.read();  // the consumer's last touch (unless final)
        return core_.detach();
    }

    // After the consumer observed `final` (kReady / kBroken): the result; a
    // value is moved out and the slot destroyed.
    Result<T> take(OneshotStatus final) noexcept {
        if (final != OneshotStatus::kReady) {
            return Error::kBroken;
        }
        value_probe_.read();
        if constexpr (std::is_void_v<T>) {
            destroy_value();
            return Result<void>(std::in_place);
        } else {
            Result<T> result(std::in_place, std::move(value_));
            destroy_value();
            return result;
        }
    }

    // ---- whoever owns the state at the end ----------------------------------

    void destroy_value() noexcept {
        value_probe_.write();
        value_.~Slot();
    }

    bool pooled() const noexcept { return release_ != nullptr; }

    // Pooled only: destroys the state and returns its block.
    void release() noexcept {
        block_probe_.write();
        const ReleaseFn release = release_;
        void* const context = context_;
        this->~OneshotState();
        release(context, this);
    }

    bool has_waiter_approx() const noexcept { return core_.has_waiter_approx(); }

private:
    OneshotCore<Sync> core_;
    ReleaseFn release_ = nullptr;
    void* context_ = nullptr;
    [[no_unique_address]] typename detail::ProbeOf<Sync>::type value_probe_{};
    [[no_unique_address]] typename detail::ProbeOf<Sync>::type block_probe_{};
    union {
        Slot value_;
    };
};

// The producer's handle: move-only, one per state. set() delivers the value
// (BasicPromise<void>: set() with no arguments, just the completion);
// destroying (or abandon()ing) an unset Promise breaks it: the consumer gets
// Error::kBroken. Any thread, never waits; set() may wake the consumer from
// the calling thread (a fiber wake: one ThreadPool submit).
//
// set() never throws, so how the value is built depends on whether that can
// throw:
//   * set(T value): by value. Whatever converts to T implicitly (a literal,
//     a copy, a braced list) is turned into T at the CALL SITE, in the
//     caller's context (where a throwing constructor is the caller's
//     business); set() only moves it into the state (T is
//     nothrow-move-constructible). Promise<std::string> p; p.set("pong").
//   * set(args...): T(args...) constructed in place in the state, only if
//     that is noexcept (no extra move).
//   * anything else (a construction that may throw and is not one implicit
//     conversion: an explicit constructor, several arguments) is a
//     static_assert: build the value yourself, set(T(args...)).
template <class T, sync::SyncPolicy Sync = sync::StdSync>
class BasicPromise {
public:
    using State = OneshotState<T, Sync>;
    using value_type = T;

    BasicPromise() noexcept = default;

    // For the front-ends: the producer side of `state`.
    explicit BasicPromise(State* state) noexcept : state_(state) {}

    BasicPromise(BasicPromise&& other) noexcept : state_(std::exchange(other.state_, nullptr)) {}

    BasicPromise& operator=(BasicPromise&& other) noexcept {
        if (this != &other) {
            abandon();
            state_ = std::exchange(other.state_, nullptr);
        }
        return *this;
    }

    BasicPromise(const BasicPromise&) = delete;
    BasicPromise& operator=(const BasicPromise&) = delete;

    ~BasicPromise() { abandon(); }

    // Moves `value` (built at the call site) into the state and completes
    // it. The Promise is empty afterwards; set() on an empty Promise fails
    // fast (as for every set() below).
    void set(detail::SlotOf<T> value) noexcept
        requires(!std::is_void_v<T>)
    {
        complete_with(std::move(value));
    }

    // Constructs T(args...) in the state (noexcept construction only) and
    // completes it.
    template <class... Args>
        requires(!std::is_void_v<T>) && std::is_nothrow_constructible_v<T, Args&&...>
    void set(Args&&... args) noexcept {
        complete_with(std::forward<Args>(args)...);
    }

    // Neither of the two: a compile-time error with directions.
    template <class... Args>
        requires(!std::is_void_v<T>) && (!std::is_nothrow_constructible_v<T, Args&&...>) &&
                (!detail::kConvertsTo<T, Args&&...>)
    void set(Args&&...) noexcept {
        static_assert(detail::kAlwaysFalse<Args...>,
                      "wait::Promise<T>::set(args...): T is not nothrow-constructible from these arguments, and "
                      "they are not one argument that converts to T implicitly. set() never throws: build the "
                      "value yourself at the call site, set(T(args...)), and it is moved in");
    }

    // BasicPromise<void>: completes it (no value).
    void set() noexcept
        requires std::is_void_v<T>
    {
        complete_with();
    }

    // Breaks the promise (the consumer gets Error::kBroken); no-op on an
    // empty Promise.
    void abandon() noexcept {
        State* const state = std::exchange(state_, nullptr);
        if (state != nullptr && state->complete(OneshotStatus::kBroken)) {
            state->release();
        }
    }

    // Not yet set / abandoned / moved from.
    bool valid() const noexcept { return state_ != nullptr; }
    explicit operator bool() const noexcept { return valid(); }

private:
    template <class... Args>
    void complete_with(Args&&... args) noexcept {
        State* const state = state_;
        if (state == nullptr) [[unlikely]] {
            fail::fail_fast("wait: Promise::set() on an empty Promise (already set, abandoned or moved from)");
        }
        state_ = nullptr;
        state->emplace(std::forward<Args>(args)...);
        if (state->complete(OneshotStatus::kReady)) {  // the consumer has gone
            state->destroy_value();
            state->release();
        }
    }

    State* state_ = nullptr;
};

}  // namespace fibers::wait
