// Not built by CMake: run with GenMC (https://github.com/MPI-SWS/genmc), e.g.
//   docker run --rm -v $PWD:/w:ro genmc/genmc genmc -rc11 -- [-DNO_F1 ...] /w/stop.c
// Baseline: "No errors were detected" under -rc11 and -tso (v0.20). Every
// mutant -D flag listed below is a safety violation under -rc11.
// GenMC model of submit() vs stop(): the op must be dropped by stop()'s
// drain or by the submitter's own drain (stop bit seen after F1); never by
// both (the drains are serialised by a lock and pop at most once), never by
// neither. Pool threads are already gone (stop() joined them).
//   submit: push (xchg tail acq_rel; link release); F1; idle.load relaxed;
//           stop bit -> drain
//   stop:   idle.fetch_or(STOP) seq_cst; F3; (join); drain
//   drain:  lock; pop: stub.next acquire; in transit (tail != stub, link not
//           visible) -> spin until linked (assume); unlock
// -DNO_F1 / -DNO_F3: drop a fence. -DNO_SELF_DRAIN: submit ignores the bit.
#include <assert.h>
#include <genmc.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>

#define STOP (1ull << 63)

struct node { _Atomic(struct node*) next; };
struct node stub;
struct node n1;
_Atomic(struct node*) tail = &stub;
atomic_uint_fast64_t idle;
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
int popped;   // under lock
int dropped;  // under lock

static void drain(void) {
    pthread_mutex_lock(&lock);
    if (!popped) {
        struct node* next = atomic_load_explicit(&stub.next, memory_order_acquire);
        if (next == 0 && atomic_load_explicit(&tail, memory_order_relaxed) != &stub) {
            next = atomic_load_explicit(&stub.next, memory_order_acquire);
            __VERIFIER_assume(next != 0);  // spin until linked
        }
        if (next != 0) {
            popped = 1;
            dropped++;
        }
    }
    pthread_mutex_unlock(&lock);
}

void* submitter(void* arg) {
    atomic_store_explicit(&n1.next, 0, memory_order_relaxed);
    struct node* prev = atomic_exchange_explicit(&tail, &n1, memory_order_acq_rel);
    atomic_store_explicit(&prev->next, &n1, memory_order_release);
#ifndef NO_F1
    atomic_thread_fence(memory_order_seq_cst);
#endif
    if (atomic_load_explicit(&idle, memory_order_relaxed) & STOP) {
#ifndef NO_SELF_DRAIN
        drain();
#endif
    }
    return 0;
}

void* stopper(void* arg) {
    atomic_fetch_or_explicit(&idle, STOP, memory_order_seq_cst);
#ifndef NO_F3
    atomic_thread_fence(memory_order_seq_cst);
#endif
    drain();
    return 0;
}

int main() {
    pthread_t a, b;
    pthread_create(&a, 0, submitter, 0);
    pthread_create(&b, 0, stopper, 0);
    pthread_join(a, 0);
    pthread_join(b, 0);
    assert(dropped == 1);
    return 0;
}
