// Not built by CMake: run with GenMC (https://github.com/MPI-SWS/genmc), e.g.
//   docker run --rm -v $PWD:/w:ro genmc/genmc genmc -rc11 -- [-DNO_F1 ...] /w/park.c
// Baseline: "No errors were detected" under -rc11 and -tso (v0.20). Every
// mutant -D flag listed below is a safety violation under -rc11.
// GenMC model of BlockingPool's wake protocol (one producer, one parking
// consumer, empty queue at start). Mirrors blocking_pool.hpp / op_queue.hpp:
//   producer: n.next = 0; prev = tail.xchg(n) acq_rel; prev->next = n release;
//             F1 = fence(seq_cst); idle.load relaxed; if idle != 0:
//             state.load relaxed != RUNNING -> state.xchg(RUNNING) seq_cst,
//             PARKED -> notify (woke = 1)
//   consumer: state = PARKING relaxed; idle.fetch_add seq_cst; F2 = fence;
//             re-check (pop: stub.next acquire; in transit: wait for it);
//             nothing -> CAS PARKING -> PARKED seq_cst (sleeps)
// Lost wakeup: the consumer sleeps (CAS succeeded), the op is never seen by
// it, and the producer never notified.
//
// The re-check waits for an in-transit link (GenMC's assume models "spins
// until the link is visible"), as pop() does.
// -DNO_F1 / -DNO_F2: drop a fence. -DLOAD_STORE: unpark with load + store.
// -DNO_SPIN: the re-check treats an in-transit link as nothing. The one
// exception to "every mutant fails": with a single op nothing is stranded
// behind the in-transit link (the multi-op stranding is what Twist's
// EachPushWakesAThread catches); -DNO_SPIN -DNO_F1 fails under -tso too.
#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <genmc.h>

#define RUNNING 0
#define PARKING 1
#define PARKED 2

struct node { _Atomic(struct node*) next; };

struct node stub;
struct node n1;
_Atomic(struct node*) tail = &stub;
atomic_uint_fast64_t idle;
atomic_uint state;
int woke;
int parked;
int found;

void* producer(void* arg) {
    atomic_store_explicit(&n1.next, 0, memory_order_relaxed);
    struct node* prev = atomic_exchange_explicit(&tail, &n1, memory_order_acq_rel);
    atomic_store_explicit(&prev->next, &n1, memory_order_release);
#ifndef NO_F1
    atomic_thread_fence(memory_order_seq_cst);
#endif
    if (atomic_load_explicit(&idle, memory_order_relaxed) != 0) {
        if (atomic_load_explicit(&state, memory_order_relaxed) != RUNNING) {
#ifdef LOAD_STORE
            unsigned p = atomic_load_explicit(&state, memory_order_seq_cst);
            atomic_store_explicit(&state, RUNNING, memory_order_seq_cst);
#else
            unsigned p = atomic_exchange_explicit(&state, RUNNING, memory_order_seq_cst);
#endif
            if (p == PARKED) woke = 1;
        }
    }
    return 0;
}

void* consumer(void* arg) {
    atomic_store_explicit(&state, PARKING, memory_order_relaxed);
    atomic_fetch_add_explicit(&idle, 1, memory_order_seq_cst);
#ifndef NO_F2
    atomic_thread_fence(memory_order_seq_cst);
#endif
    struct node* next = atomic_load_explicit(&stub.next, memory_order_acquire);
#ifndef NO_SPIN
    if (next == 0 && atomic_load_explicit(&tail, memory_order_relaxed) != &stub) {
        next = atomic_load_explicit(&stub.next, memory_order_acquire);
        __VERIFIER_assume(next != 0);
    }
#endif
    if (next != 0) {
        found = 1;
        return 0;
    }
    unsigned expected = PARKING;
    if (atomic_compare_exchange_strong_explicit(&state, &expected, PARKED, memory_order_seq_cst,
                                                memory_order_acquire)) {
        parked = 1;
    }
    return 0;
}

int main() {
    pthread_t p, c;
    pthread_create(&p, 0, producer, 0);
    pthread_create(&c, 0, consumer, 0);
    pthread_join(p, 0);
    pthread_join(c, 0);
    // A sleeping consumer must have been notified (or must not sleep).
    assert(!(parked && !woke));
    return 0;
}
