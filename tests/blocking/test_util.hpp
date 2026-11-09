#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "fibers/thread_pool/task.hpp"

namespace fibers::blocking::test {

using thread_pool::Task;
using namespace std::chrono_literals;

// Polls `pred` until it holds or `timeout` passes; returns the final value.
template <class Pred>
bool wait_for(Pred pred, std::chrono::milliseconds timeout = 10s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return pred();
        }
        std::this_thread::sleep_for(100us);
    }
    return true;
}

// Counts runs and drops of tasks that share it as memo.
struct Counts {
    std::atomic<int> ran{0};
    std::atomic<int> dropped{0};

    Task task() {
        return Task(
            this, [](void* m) noexcept { static_cast<Counts*>(m)->ran.fetch_add(1); },
            [](void* m) noexcept { static_cast<Counts*>(m)->dropped.fetch_add(1); });
    }
};

// A task that blocks its pool thread until open() is called.
struct Gate {
    std::atomic<bool> entered{false};
    std::atomic<bool> open_{false};
    std::atomic<int> ran{0};

    Task task() {
        return Task(this, [](void* m) noexcept {
            auto* g = static_cast<Gate*>(m);
            g->entered.store(true);
            while (!g->open_.load()) std::this_thread::sleep_for(100us);
            g->ran.fetch_add(1);
        }, nullptr);
    }
    void open() { open_.store(true); }
};

// A temporary file in the current directory (the build tree: a real disk
// filesystem, unlike a tmpfs /tmp, so page-cache eviction is observable).
// Unlinked at once: a killed test leaves nothing behind.
struct TempFile {
    int fd = -1;

    TempFile() {
        char name[] = "./blocking_test_XXXXXX";
        fd = ::mkstemp(name);
        if (fd < 0) {
            std::perror("mkstemp");
            std::abort();
        }
        ::unlink(name);
    }
    ~TempFile() { ::close(fd); }
    TempFile(const TempFile&) = delete;
    TempFile& operator=(const TempFile&) = delete;
};

}  // namespace fibers::blocking::test
