#pragma once

// TaskHeader: metadata at the start of every memo the Scheduler issues.
//
// A thread_pool::Task stays { memo, call_ptr, delete_ptr } (24 bytes, it
// lives in the slots of lock-free queues). What a running task needs to know
// about itself sits at the start of the object its memo points to:
//
//   plain callback (Scheduler::submit(f)): a TaskMemoryPool block
//       [ TaskHeader | closure ]
//   stackful fiber: the Fiber object at the top of its own stack
//       [ TaskHeader | state word | coroutine ... ]   (fibers/fiber/fiber.hpp)
//
// Room for later (each a field here, no change to Task): cancel token,
// deadline, fiber-local slots, trace id.

namespace fibers::sched {

template <class Scheduler>
struct BasicTaskHeader {
    // The scheduler that runs the task: a task / fiber reschedules itself or
    // spawns siblings through it, and returns its memory to its pools.
    Scheduler* scheduler = nullptr;
    // The fiber object if the memo is a fiber (memo == fiber), nullptr for
    // a plain callback.
    void* fiber = nullptr;
};

}  // namespace fibers::sched
