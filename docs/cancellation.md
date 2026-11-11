# Cancellation and deadlines: `Context`

A request arrives with a deadline of 200 ms. Its handler reads sockets,
calls upstream services, borrows pooled connections, fans out to helper
fibers. When the deadline passes, or the client goes away, every network wait
in that tree of work should return `ETIMEDOUT` / `ECANCELED` so the handler
unwinds normally (RAII) and returns. That is what a `fibers::Context` is for.

```cpp
fibers::Context ctx = fibers::Context::with_deadline(sched, sched.now() + 200ms);
fiber::go(sched, ctx, [&]() noexcept {          // the fiber runs under ctx
    fiber::WaitGroup wg;
    wg.go([&]() noexcept { ... });               // go(f) / wg.go(f) inherit ctx
    IoResult r = conn.read_some(buf, n);         // ETIMEDOUT / ECANCELED when ctx ends
    if (fiber::this_fiber::cancelled()) return;  // cooperative check for CPU loops
    wg.wait();                                   // join: NOT interrupted (see below)
});
ctx.cancel();                                    // wakes every network wait under ctx
```

Cancellation is **cooperative**: nothing is killed and nothing throws. A
cancellable wait returns an error code; user code returns normally.

## 1. What is cancellable

| Wait | Cancellable | Under a cancelled / expired context |
|---|---|---|
| `net::PollDesc` waits: `Stream::read_some` / `write_some` / `write_all`, `Listener::accept`, `connect`, everything built on them (TLS handshake / read / write, `net::Connection`, `net::BufferedReader`, HTTP/1.1 client and server I/O, the HTTP/2 client's socket I/O) | yes | `-ECANCELED` / `-ETIMEDOUT` (the PollDesc convention), surfaced by each layer as its usual errno |
| `net::ConnectionPool::acquire` | yes | `ECANCELED` / `ETIMEDOUT` |
| `http2::Client::request` (the caller's wait for its stream) | yes | `ECANCELED` / `ETIMEDOUT`; the stream is reset (RST_STREAM CANCEL) |
| `fiber::Mutex::lock` | no | held briefly by design (like Go's `sync.Mutex`) |
| `fiber::Event::wait`, `fiber::WaitGroup::wait` | no | see "Why not the sync primitives" |
| `fiber::Oneshot::get`, `fiber::Future::get` | no | |
| `fiber::Semaphore::acquire` | no | |
| `this_fiber::sleep_for` / `sleep_until` | no | |
| `this_fiber::blocking(f)`, `fs::` file ops | no | the op lives on the fiber's stack; a pool thread is running it |
| `net::resolve` (getaddrinfo in the blocking pool) | no | same as `blocking` |
| `Stream::close`, `PollDesc::close`, `ConnectionPool::close` | no | teardown must complete |

**Rule for cancellable waits:** an operation that would *not* wait completes
normally even under a cancelled context (data already in the socket buffer
is read, an idle pooled connection is handed out); an operation that *would*
wait fails at once. So a cancelled handler still makes progress on ready
data, but never blocks.

**Waits that are not cancellable.** A user who needs a deadline on them has
two tools:
* poll `this_fiber::cancelled()` (one load) between steps of CPU work, or in
  a loop around `try_*` variants (`Future::try_get`, `Semaphore::try_acquire`);
* make the wait network-bound: the thing being waited for is usually a fiber
  doing network I/O under the same context, which *is* cancelled and then
  completes the `Future` / sets the `Event` / calls `done()`, so the waiter
  returns promptly anyway. This is the structured-concurrency pattern the
  example (`examples/sync/context_deadline.cpp`) uses.

### Why not the sync primitives (scope decision)

The waiter structures of `Event` / `WaitGroup` (a lock-free Treiber stack),
`Semaphore` (a FIFO under a spin lock) and `Oneshot` stay as they are: making
them cancellable needs removable waiter nodes in their cores (a list from
which a node can be taken out of the middle), which changes their fast paths
and their verified memory-order arguments. The mechanism below is designed so
that adding them later is local: a primitive only has to provide "take this
waiter out if it is still in" (a CAS or an unlink under its lock) as the
interrupt hook; nothing in `Context` changes.

`WaitGroup::wait()` should stay a join even then: the structured pattern
`wg.go(...); wg.wait(); return;` relies on the children being done before
the parent's frame (which they reference) goes away, and the children run
under the very context being cancelled.

`Mutex::lock` will stay uncancellable: it is held for short critical
sections; a cancellable lock would make every `lock_guard` fallible.

`blocking(f)` / file ops: the op, its result slot and the waiter live on the
fiber's stack, and a blocking-pool thread is inside the syscall. The fiber
cannot leave before that thread is done, and reporting `kCancelled` after the
fact would throw away a result that already happened (an opened fd, written
bytes). So they complete normally; the next cancellable wait reports.

## 2. API

```cpp
namespace fibers::fiber {
template <class Sched> class BasicContext;      // Sched = sched::Scheduler for fibers::Context
}
namespace fibers { using Context = fiber::BasicContext<sched::Scheduler>; }

// Roots
Context::cancellable();                           // no deadline
Context::with_deadline(sched, deadline);          // any thread
Context::with_deadline(deadline);                 // in a fiber: its scheduler
Context::with_timeout(sched, d) / with_timeout(d);
// Children: cancelled with the parent; deadline = min(own, parent's)
Context::child_of(parent);
Context::child_of(parent, deadline);

bool ctx.cancel();                 // kCancelled; false if already ended
bool ctx.cancelled();              // ended (cancelled or deadline)
std::optional<wait::Error> ctx.reason();   // kCancelled / kTimedOut
int  ctx.error_code();             // 0 / ECANCELED / ETIMEDOUT
TimePoint ctx.deadline();          // effective (TimePoint::max(): none)

fiber::go(sched, ctx, f);          // a fiber under ctx
fiber::go(f);  wg.go(f);           // in a fiber: inherit the current context
fiber::go(sched, f);  wg.go(sched, f);   // explicit scheduler: NO context
this_fiber::context();             // BasicContext* or nullptr
this_fiber::cancelled();           // context() && context()->cancelled()
this_fiber::context_error();       // 0 / ECANCELED / ETIMEDOUT
fiber::ContextScope scope(ctx_or_nullptr);  // run a section under another
                                            // context (a child with a tighter
                                            // deadline; nullptr = shield)
```

Library-internal background fibers (HTTP/2 reader / writer, the
ConnectionPool reaper, the HTTP server's connection fibers) are spawned with
an explicit scheduler and therefore never inherit a caller's context: a
cancelled request must not tear down a shared connection.

`wait::Error` gains `kCancelled` and `kTimedOut` (they were reserved).

## 3. Mechanism

### 3.1 Propagation

`TaskHeader` gets `context` (a `BasicContext<Sched>*`; nullptr for plain
callbacks and for fibers without one). The fiber writes it at spawn (before
the submit that publishes the fiber) and changes it only on its own stack
(`ContextScope`), so it is a plain field. Every lookup is the fiber's own.

### 3.2 `wait::ContextCore<Sync>`: state + interrupt list

```
state_   atomic: 0 (live) | kCancelled | kTimedOut           first cancel wins
lock_    SpinLock: guards the list and the links' `linked` flags
list     intrusive doubly linked list of Interrupt nodes
parent_  ContextCore* (child) + link_: this child's Interrupt in the parent

struct Interrupt {           // "how to interrupt me", on the waiter's stack
    Interrupt *prev, *next;  // under the context's lock
    bool linked;             // under the context's lock
    void* self;  void (*fn)(void* self, wait::Error reason) noexcept;
    atomic<uint32_t> done;   // canceller -> waiter handshake
};
```

A waiter (a fiber parked in a cancellable primitive) and a child context are
the same thing to a context: an `Interrupt` in its list. For a waiter `fn`
claims its wait and wakes it; for a child `fn` cancels the child with the
same reason (recursively).

```
enlist(h):  lock; if state != 0: unlock, return false
            h.linked = true; h.done = 0; append; unlock; return true
delist(h):  lock; if h.linked: unlink, unlock, return      (never popped)
            unlock; spin until h.done (acquire)             (popped by a canceller:
                                                             wait until it has
                                                             finished with h)
cancel(r):  lock; if state != 0: unlock, return false
            state = r (release)
            loop: h = head; if none: break
                  unlink h; h.linked = false; unlock
                  h.fn(h.self, r)                   // outside the lock
                  h.done = 1 (release)              // last touch of h
                  lock
            unlock; return true
```

* The canceller never holds the lock while it runs a hook: no lock is ever
  nested (a child's cancel takes the child's lock only), and fibers woken by
  the hooks do not spin behind a long walk; each waits (in `delist`) only for
  its *own* hook to return.
* Exactly one canceller walks a context (the `state_` transition under the
  lock); every node is popped at most once.
* After `state_` is set, `enlist` refuses: a waiter that publishes itself
  after the cancel sees it under the lock and interrupts itself (below).

### 3.3 The claim: who wakes the waiter

A wait has one winner word that the event path and the cancel path both race
on with an RMW; the winner wakes the fiber, the loser does nothing. For
`PollDesc` it is the direction's word (`rd_` / `wr_`) — the very CAS the
socket-deadline expiry already uses: `CAS(&waiter -> EMPTY)`. For
`ConnectionPool` it is the waiter's `linked` flag under the pool's lock (the
same test the deadline timer's callback makes). For the HTTP/2 client it is
the call's `DeadlineNode::claimed` flag (shared with the per-request
deadline timer and the stream's completion).

Waiter side (a fiber with a context `ctx`):

```
publish the wait (arm the word / queue the node)             -- as before
if (!ctx.enlist(h))                   // cancelled meanwhile
    h.fn(h.self, ctx.reason())        //   interrupt ourselves: claim; if we
                                      //   win, our own wake sets NOTIFIED
park()                                // exactly one wake: event or cancel
if (enlisted) ctx.delist(h)           // before h (on this stack) goes away
```

Why this is exactly-once and leak-free:
* **one wake per park**: the event path and `fn` both need the claim RMW to
  succeed before they wake; the word is taken once.
* **no lost cancel**: `enlist` and `cancel` serialise on the lock. Enlist
  first: the walk finds `h` and calls `fn`, whose claim sees the published
  waiter (the arm happened before the enlist's unlock, the walk after its
  lock) unless the event path took it, which then wakes the fiber. Cancel
  first: `enlist` fails and the fiber runs `fn` itself.
* **no use of a dead frame**: the fiber can leave the wait (and its stack
  frame with `h` and the waiter) only after `delist`; a popped `h` makes
  `delist` wait for `done`, which the canceller stores after `fn` returned.
  The claim loser's `fn` may still read the waiter's word; that is before
  `done`.
* **no ABA on the word**: a fiber re-arms the same waiter (`PollDesc::io`
  loops) only after `delist`, i.e. after any `fn` that could CAS on it.

### 3.4 Deadlines: one timer per context

A deadline is not a per-wait timer: the context owns **one** poller timer
that calls `cancel(kTimedOut)`. Individual waits need no timer; the
effective deadline of a socket wait is `min(socket deadline, context
deadline)` simply because two independent mechanisms can end it (the
socket's own `set_read_deadline` timer and the context's).

* `with_deadline(t)`: `t <= now()` cancels at once (kTimedOut, no timer);
  otherwise the timer is scheduled in the constructor (the object is built
  in place: factories return a prvalue, the type is neither copyable nor
  movable).
* `child_of(parent, t)`: effective deadline `min(t, parent.deadline())`; a
  timer of its own only if `t` is earlier than the parent's, otherwise the
  parent's cancellation reaches it (with the parent's reason, kTimedOut).
* the timer node does not live in the context but in a **deadline record**
  (`detail::DeadlineRecord`): a block of the scheduler's `TaskMemoryPool`
  (128 bytes: the poller's timer node, a back-pointer to the context, the
  pool, a state word), owned jointly by the context and by the poller until
  both have let go; the second one returns the block. So the context can go
  while the poller still references the node, and destroying a context
  whose deadline has not come yet (the common case: the request finished
  in time) is a lazy `Poller::cancel()` and one exchange, with no poller
  round trip and no waiting.

#### The deadline record protocol

```
state: kArmed (both own it) | kContextGone | kTimerDone

~Context (timer armed):
    if poller.cancel(timer)                      // pending -> cancelled: it will not fire
        if state.exchange(kContextGone) == kTimerDone: free the record
        return                                   // the callback (kCancelled, later) frees it
    else                                         // fire won: kFired delivered / being delivered
        wait until state == kTimerDone (acquire)
        free the record
timer callback (poller thread):
    if kFired: context.cancel(kTimedOut)         // the destructor waits for us: it lives
    if state.exchange(kTimerDone) == kContextGone: free the record
```

* **The claim is the poller's own timer word.** Firing (pending -> idle)
  and `cancel()` (pending -> cancelled) are CASes on the timer's state in
  the poller: exactly one wins. A kFired callback therefore always runs
  against a live context, and a kCancelled one never touches it. No
  separate claim word (ARMED -> ORPHANED / FIRING) is needed, and the
  context does not have to clear a back-pointer.
* **Cancel before letting go.** The destructor calls `cancel()` while the
  record is still certainly alive: the callback frees it only after
  `kContextGone`, which the destructor stores after `cancel()`. (Orphaning
  first and cancelling afterwards would race with a callback that frees
  the record in between.)
* **The wait is rare and short.** The destructor waits only if the deadline
  fired at the very moment of destruction; then the callback is running
  its cancel walk on the poller thread (short, never blocks), so it spins
  (as `delist` does). On the poller's own thread the callback has returned
  already (one thread), so a context may be destroyed from a poller
  callback.
* **Reclamation.** A cancelled node stays in the poller's queue / heap until
  the poller's next iteration (drain, compaction, or the top of the heap),
  and so does its record. A poller that sleeps until a far deadline would
  let them pile up while requests keep coming and finishing early, so every
  `Poller::kCancelsPerWake`-th (256) cancel since the poller's last
  iteration wakes it if it sleeps (best effort); busy pollers (sockets)
  iterate anyway. In the heap, compaction keeps cancelled nodes to about a
  third of the live ones.
* **Poller destruction.** `~Poller` hands every timer still pending back
  with kCancelled. A record the context has let go of (cancelled) is freed
  there; the callback of a record whose context still lives sees `kArmed`
  and leaves it to the context, whose `cancel()` then fails and which finds
  `kTimerDone`. The `TaskMemoryPool` outlives the pollers in `Scheduler`.

Cost: create + destroy a context with a live deadline is ~0.2 us (was
~11 us with the posted retire node and the wait for it), most of it the
record's trip to the poller thread and back to the pool; a context with a
deadline that has fired, or without a deadline, is destroyed with no
round trip at all.

### 3.5 Lifetimes (fail fast)

A context must outlive every fiber and every wait that uses it. The intended
shape is structured: create it on the handler's stack, run children under it,
join them (`wg.wait()`, which is deliberately not interrupted), return.

* `~BasicContext` fails fast if any Interrupt is still enlisted (a fiber is
  parked under it, or a child context still exists).
* A child context unlinks itself from its parent in its destructor
  (`delist`: if the parent's canceller is cancelling it right now, it waits
  until that finished).
* `cancel()` called by user code must have returned before the destructor
  runs (like `Event::set`). Cancels the library issues itself (the deadline
  timer, a parent's walk) are covered by the deadline record protocol and
  by `delist`.
* Fibers running under a context are not counted: a counter decremented at
  fiber exit would race with the parent's join (the child signals `done()`
  inside its body, before it exits). A fiber that touches a destroyed
  context is a bug the destructor cannot see unless it is parked.
* Destroy contexts before the scheduler is **destroyed** (the record
  belongs to its `TaskMemoryPool`, and `cancel()` reaches its poller).
  Stopping the scheduler first is fine: `cancel()` works on a stopped
  poller, and `~Poller` hands the node back later. (The former rule,
  "destroy before stopping the pollers", came from the posted retire node
  and is gone with it.) Destroying a context from a poller callback is
  fine as well.

### 3.6 Memory orders

| Operation | Order | Why |
|---|---|---|
| `cancel`: `state_` store | release | `cancelled()` / `reason()` readers (no lock) see what the canceller did before `cancel()` |
| `cancelled()`, `reason()` | acquire | pairs with the store above |
| `enlist`: load of `state_` | relaxed | under the lock, which orders it against `cancel`'s store (also under the lock) |
| hook pop / `linked` / list links | plain, under `lock_` | the SpinLock's acquire / release |
| `done` store (canceller) | release | the canceller's last accesses of the hook and of the waiter it claimed (or failed to claim) happen-before the waiter's `delist` returns and its frame is reused |
| `done` load (`delist`) | acquire | pairs with the store above |
| PollDesc claim `CAS(&waiter -> EMPTY)` | relaxed | the waiter's fields were published by the arm CAS (release) *and* by the enlist's unlock, which the canceller's lock acquires before the claim; the result and the wake go to the fiber through its wake (state word / queue) |
| deadline record: `state.exchange` (destructor, callback) | acq_rel | release: the side that lets go first has finished with the record (the callback also with the context); acquire: the side that frees the record has seen those accesses |
| deadline record: `state` load (the destructor's wait) | acquire | the callback's cancel walk (its accesses of the context) and its reads of the record happen-before the destructor frees both |
| poller timer word: fire CAS / `cancel()` CAS | acq_rel (the poller's) | only decides who wins; no data of the context rides on it |

The Twist tests (tests/fiber/twist/context_twist_test.cpp,
tests/net/twist/net_twist_test.cpp) check each non-relaxed order: weakening
it makes a simulator test fail (see the tables in the commit messages).

## 4. Cost when unused

* `TaskHeader` grows by one pointer (24 bytes): plain-callback closures get
  8 bytes less per TaskMemoryPool size class.
* Spawning: `go(f)` copies the parent's context pointer (one load);
  `go(sched, f)` stores nullptr.
* `PollDesc::io`: the context is fetched once per call, only when the
  syscall returned EAGAIN (the fiber is about to park anyway); without a
  context the park path is unchanged after one null check.
* No change at all to Mutex / Event / WaitGroup / Oneshot / Semaphore / sleep.
* A context without a deadline of its own holds no record (a null
  pointer); the context object itself got smaller (it held two timer
  nodes before).

## 5. Later

* Removable waiter nodes in the sync cores (`Event`, `Semaphore`,
  `Oneshot`), then hook them up (section 1).
* `http::Server`: a per-request context with the request deadline
  (`grpc-timeout` in grpc_sansio), visible to handlers through
  `this_fiber::context()`.
