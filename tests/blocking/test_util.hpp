#pragma once

#include <fcntl.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <thread>

#include "fibers/blocking/ops.hpp"
#include "fibers/thread_pool/task.hpp"

namespace fibers::blocking::test {

using thread_pool::Task;
using namespace std::chrono_literals;

// Polls `pred` until it holds or `timeout` passes; returns the final value.
template <class Pred>
bool wait_for(Pred pred, std::chrono::milliseconds timeout = 30s) {
    const auto deadline = std::chrono::steady_clock::now() + timeout;
    while (!pred()) {
        if (std::chrono::steady_clock::now() >= deadline) {
            return pred();
        }
        std::this_thread::sleep_for(100us);
    }
    return true;
}

// Counts runs and drops of continuation tasks that share it as memo.
struct Counts {
    std::atomic<int> ran{0};
    std::atomic<int> dropped{0};

    Task task() {
        return Task(
            this, [](void* m) noexcept { static_cast<Counts*>(m)->ran.fetch_add(1); },
            [](void* m) noexcept { static_cast<Counts*>(m)->dropped.fetch_add(1); });
    }
};

// An op that counts its executions (and optionally a shared total).
struct CountingOp final : Op<CountingOp> {
    std::atomic<int> executed{0};
    std::atomic<int>* total = nullptr;

    IoResult execute() noexcept {
        executed.fetch_add(1);
        if (total != nullptr) total->fetch_add(1);
        return IoResult::success(1);
    }
};

// An op that blocks its pool thread until open() (bounded: 30 s, then abort,
// so a broken test cannot hang the pool's join forever).
struct GateOp final : Op<GateOp> {
    std::atomic<bool> entered{false};
    std::atomic<bool> open_{false};
    std::atomic<int> ran{0};

    IoResult execute() noexcept {
        entered.store(true);
        const auto deadline = std::chrono::steady_clock::now() + 30s;
        while (!open_.load()) {
            if (std::chrono::steady_clock::now() >= deadline) {
                std::fprintf(stderr, "GateOp: never opened\n");
                std::abort();
            }
            std::this_thread::sleep_for(100us);
        }
        ran.fetch_add(1);
        return IoResult{};
    }
    void open() { open_.store(true); }
};

// A temporary file in the current directory (the build tree: a real disk
// filesystem, unlike a tmpfs /tmp, so page-cache eviction is observable), or
// in `dir`. Unlinked at once: a killed test leaves nothing behind.
struct TempFile {
    int fd = -1;

    explicit TempFile(const char* dir = ".") {
        char name[256];
        std::snprintf(name, sizeof(name), "%s/blocking_test_XXXXXX", dir);
        fd = ::mkstemp(name);
        if (fd >= 0) {
            ::unlink(name);
        }
    }
    ~TempFile() {
        if (fd >= 0) ::close(fd);
    }
    bool ok() const { return fd >= 0; }
    TempFile(const TempFile&) = delete;
    TempFile& operator=(const TempFile&) = delete;
};

}  // namespace fibers::blocking::test
