#pragma once

// WaitGroupCore: the waiter-agnostic algorithm of the fiber WaitGroup, over
// WaitNode and EventCore. Front-ends (fibers/fiber/wait_group.hpp for
// stackful fibers) add only "how to wait for the node's wake".
//
// A one-shot WAIT, not a one-shot counter: the count may go up and down and
// cross zero any number of times before anyone waits (tracking in-flight
// requests, say). The first wait() closes the group (JOINING): from then on
// the group finishes the moment the count reaches zero, once and for all.
//
// State
// -----
//   state_ = [ count : 63 | JOINING (bit 0) ]   (count in units of kOne = 2)
//   event_ = EventCore: set when the count reaches zero while JOINING
//
//   add(n):  s = state.fetch_add(n, relaxed)
//            JOINING(s) && count(s) == 0 -> fail_fast (the group has finished)
//   done():  s = state.fetch_sub(1, acq_rel)
//            count(s) == 0               -> fail_fast (more done() than add())
//            count(s) == 1 && JOINING(s) -> event.set()   (the last one)
//   wait_or_enqueue(node):
//            s = state.fetch_or(JOINING, acquire)
//            count(s) == 0 -> return true (finished)
//            else          -> event.wait_or_enqueue(node)
//   try_wait(): the fetch_or alone (true: finished)
//
// Why nothing is lost: the fetch_or and the last fetch_sub are RMWs of one
// word, so one of them comes first. Last done() first: the waiter reads
// count 0 and does not wait (the event is never set, nobody needs it).
// fetch_or first: the last done() reads JOINING and sets the event; a waiter
// that enqueues after that finds the event set and does not wait.
//
// Rules (fail fast where it can be detected cheaply):
//   * add(n): n > 0 adds n; n == 0 is a no-op (also after the group has
//     finished: add(batch.size()) with an empty batch is fine); n < 0 fails
//     fast (use done()).
//   * add() while JOINING is allowed as long as the count is > 0 (children
//     spawning children during a graceful shutdown). It must be done by
//     someone who holds a unit of the count, i.e. before that unit's done():
//     then the count cannot reach zero in between. add() after the count
//     has reached zero while JOINING fails fast ("add after the group
//     finished"); a racing add() that is not covered by a unit is a bug that
//     is only detected when it loses the race.
//   * done() more often than add(): fails fast (underflow).
//   * wait() may be called by any number of waiters, any number of times:
//     all of them return once the group has finished.
//   * Lifetime: done() touches the group after its fetch_sub only when it is
//     the last one, and then only the event, whose set() lets go of it with
//     its exchange. That done() only runs set() because some waiter's
//     fetch_or found count > 0, and that waiter cannot return before the
//     exchange. So the group may be destroyed once every wait() has
//     returned (with no add() / done() still to come); a later waiter may
//     return earlier (it reads count 0) while the first one is still
//     waiting. Destroying the group with enqueued waiters fails fast.
//
// Memory orders
// -------------
// Required: everything sequenced before every done() happens-before every
// return from wait() (and anything after the matching add() precedes the
// done() by the program's own ordering).
//   * done() is a release RMW: its writes travel in the release sequence of
//     state_ (every write of state_ is an RMW, so the sequence never breaks).
//   * waiter reads count 0 (fetch_or, acquire): it reads the value of the
//     last done() or of a later RMW (a fetch_or), so it synchronises with
//     every done(): each is earlier in state_'s modification order and its
//     release sequence includes the value read.
//   * waiter waits on the event: the last done()'s fetch_sub is also an
//     acquire, so it synchronises with every earlier done() (same argument);
//     its event.set() (a release exchange, sequenced after) passes all of it
//     on to every waiter that leaves the event (EventCore's memory orders).
//   * add() is relaxed: it publishes nothing; a later wait() sees its
//     increment by coherence when the add happens-before the wait (the
//     caller's ordering), and a racing one is caught by the JOINING check.
//   * the fetch_or needs no release: done() only reads the JOINING bit to
//     decide whether to set the event; the waiter's node is published by the
//     event's own push CAS (release).

#include <cstdint>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/sync/sync.hpp"
#include "fibers/wait/event_core.hpp"
#include "fibers/wait/wait_node.hpp"

namespace fibers::wait {

template <sync::SyncPolicy Sync = sync::StdSync>
class WaitGroupCore {
public:
    // Largest count the group can hold.
    static constexpr std::uint64_t kMaxCount = (std::uint64_t{1} << 63) - 1;

    WaitGroupCore() noexcept = default;
    WaitGroupCore(const WaitGroupCore&) = delete;
    WaitGroupCore& operator=(const WaitGroupCore&) = delete;

    // ~EventCore fails fast if waiters are enqueued.
    ~WaitGroupCore() = default;

    // n > 0: count += n. n == 0: no-op. n < 0: fails fast. Any thread.
    void add(std::int64_t n = 1) noexcept {
        if (n <= 0) [[unlikely]] {
            if (n == 0) {
                return;
            }
            fail::fail_fast("wait: WaitGroup::add() with a negative count (use done())");
        }
        const auto delta = static_cast<std::uint64_t>(n);
        const std::uint64_t s = state_.fetch_add(delta * kOne, std::memory_order_relaxed);
        if ((s & kJoining) != 0 && count_of(s) == 0) [[unlikely]] {
            fail::fail_fast("wait: WaitGroup::add() after the group finished");
        }
        if (count_of(s) > kMaxCount - delta) [[unlikely]] {
            fail::fail_fast("wait: WaitGroup count overflow");
        }
    }

    // count -= 1; the last done() of a closed (JOINING) group wakes every
    // waiter. Any thread.
    void done() noexcept {
        const std::uint64_t s = state_.fetch_sub(kOne, std::memory_order_acq_rel);
        if (count_of(s) == 0) [[unlikely]] {
            fail::fail_fast("wait: WaitGroup::done() without a matching add()");
        }
        if (s == (kOne | kJoining)) {
            if (!event_.set()) [[unlikely]] {
                fail::fail_fast("wait: WaitGroup finished twice (corrupt state)");
            }
        }
    }

    // Closes the group (JOINING). true: it has finished, do not wait.
    // false: `node` (next is overwritten, self / wake must be set) was
    // enqueued; the caller must not touch it until its wake, which comes
    // exactly once, when the group finishes. After a false the node may be
    // woken (and its owner resumed) before this returns.
    bool wait_or_enqueue(WaitNode& node) noexcept {
        if (try_wait()) {
            return true;
        }
        return event_.wait_or_enqueue(node);
    }

    // Closes the group (JOINING); true: it has finished (count zero), with
    // the same ordering as a returning wait. Never waits; the bit is sticky,
    // so calling it again (or wait_or_enqueue after it) is fine.
    bool try_wait() noexcept {
        return count_of(state_.fetch_or(kJoining, std::memory_order_acquire)) == 0;
    }

    // Racy snapshots (tests, diagnostics).
    std::uint64_t count_approx() const noexcept { return count_of(state_.load(std::memory_order_relaxed)); }
    bool joining_approx() const noexcept { return (state_.load(std::memory_order_relaxed) & kJoining) != 0; }

private:
    static constexpr std::uint64_t kJoining = 1;
    static constexpr std::uint64_t kOne = 2;

    static constexpr std::uint64_t count_of(std::uint64_t s) noexcept { return s / kOne; }

    typename Sync::template Atomic<std::uint64_t> state_{0};
    EventCore<Sync> event_;
};

}  // namespace fibers::wait
