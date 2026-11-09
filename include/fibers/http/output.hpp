#pragma once

// detail::ConnectionIo: a server connection's write buffer and its socket
// deadlines (transport-agnostic: the stream behind a StreamRef).
//
// Writes are buffered: response heads and small bodies are copied into the
// write buffer and go out with one write_all() when the connection is about
// to wait for more input (so pipelined responses share a write), when the
// buffer is full, or when a handler streams. Large bodies are written
// directly after flushing what is buffered. The first write error is sticky:
// later writes are dropped and the connection closes after the request.
//
// Deadlines and graceful shutdown: every deadline the connection sets is
// clamped to the server's shutdown deadline (ShutdownSignal). The server
// publishes that deadline and then lowers the deadlines of every open
// connection; a connection that set a later one meanwhile re-reads the
// shutdown deadline after setting its own and lowers it again. Both sides go
// through the stream's deadline lock, so one of them sees the other.

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <string_view>

#include "fibers/http/options.hpp"
#include "fibers/http/stream.hpp"
#include "fibers/poller/sources.hpp"

namespace fibers::http::detail {

inline poller::TimePoint steady_now() noexcept { return std::chrono::steady_clock::now(); }

// Published by the server when it starts shutting down.
struct ShutdownSignal {
    static constexpr std::int64_t kNone = std::numeric_limits<std::int64_t>::max();

    std::atomic<bool> draining{false};
    std::atomic<std::int64_t> deadline_ns{kNone};

    bool is_draining() const noexcept { return draining.load(std::memory_order_seq_cst); }

    // TimePoint::max() if none.
    poller::TimePoint deadline() const noexcept {
        const std::int64_t ns = deadline_ns.load(std::memory_order_seq_cst);
        if (ns == kNone) {
            return poller::TimePoint::max();
        }
        return poller::TimePoint(std::chrono::nanoseconds(ns));
    }

    void publish(poller::TimePoint deadline) noexcept {
        deadline_ns.store(std::chrono::duration_cast<std::chrono::nanoseconds>(deadline.time_since_epoch()).count(),
                          std::memory_order_seq_cst);
        draining.store(true, std::memory_order_seq_cst);
    }
};

class ConnectionIo {
public:
    void bind(StreamRef stream, char* buffer, std::size_t capacity, const ServerOptions* options,
              const ShutdownSignal* signal) noexcept {
        stream_ = stream;
        buf_ = buffer;
        cap_ = capacity;
        len_ = 0;
        error_ = 0;
        options_ = options;
        signal_ = signal;
    }

    StreamRef stream() const noexcept { return stream_; }
    const ServerOptions& options() const noexcept { return *options_; }
    bool draining() const noexcept { return signal_ != nullptr && signal_->is_draining(); }

    // 0 or the first write error.
    int error() const noexcept { return error_; }
    std::size_t pending() const noexcept { return len_; }

    // ---- deadlines (clamped to the shutdown deadline, see the top comment) --

    int set_read_deadline(poller::TimePoint t) noexcept { return set_deadline(t, stream_.ops->set_read_deadline); }
    int set_write_deadline(poller::TimePoint t) noexcept { return set_deadline(t, stream_.ops->set_write_deadline); }

    // ---- output ------------------------------------------------------------

    // Writes out what is buffered (write deadline: now + write_timeout).
    int flush() noexcept {
        if (len_ == 0 || error_ != 0) {
            len_ = 0;
            return error_;
        }
        arm_write_deadline();
        const net::IoResult r = stream_.write_all(buf_, len_);
        len_ = 0;
        if (!r) {
            error_ = r.error;
        }
        return error_;
    }

    // `n` contiguous bytes of buffer space (flushing first if needed), or
    // nullptr if n exceeds the whole buffer or after a write error. Fill
    // them, then commit() how many were used.
    char* reserve(std::size_t n) noexcept {
        if (error_ != 0 || n > cap_) {
            return nullptr;
        }
        if (cap_ - len_ < n && flush() != 0) {
            return nullptr;
        }
        return buf_ + len_;
    }

    void commit(std::size_t n) noexcept { len_ += n; }

    // Buffers small data, writes large data directly (after the buffered
    // bytes, so the order is kept).
    int append(const void* data, std::size_t n) noexcept {
        if (error_ != 0) {
            return error_;
        }
        if (n <= cap_ - len_) {
            std::memcpy(buf_ + len_, data, n);
            len_ += n;
            return 0;
        }
        if (flush() != 0) {
            return error_;
        }
        if (n <= cap_ / 2) {
            std::memcpy(buf_, data, n);
            len_ = n;
            return 0;
        }
        arm_write_deadline();
        const net::IoResult r = stream_.write_all(data, n);
        if (!r) {
            error_ = r.error;
        }
        return error_;
    }

    int append(std::string_view s) noexcept { return append(s.data(), s.size()); }

    // Drops buffered output (a connection that is being abandoned).
    void discard() noexcept { len_ = 0; }

    // Records an error (e.g. a failed read while writing is still pending).
    void fail(int error) noexcept {
        if (error_ == 0) {
            error_ = error;
        }
    }

private:
    void arm_write_deadline() noexcept {
        static_cast<void>(set_write_deadline(steady_now() + options_->write_timeout));
    }

    int set_deadline(poller::TimePoint t, int (*set)(void*, poller::TimePoint) noexcept) noexcept {
        if (signal_ != nullptr) {
            if (const poller::TimePoint sd = signal_->deadline(); sd < t) {
                t = sd;
            }
        }
        int e = set(stream_.stream, t);
        if (signal_ != nullptr) {
            // The server may have lowered the deadline before our set: lower
            // it again (see the top comment).
            if (const poller::TimePoint sd = signal_->deadline(); sd < t) {
                e = set(stream_.stream, sd);
            }
        }
        return e;
    }

    StreamRef stream_;
    char* buf_ = nullptr;
    std::size_t cap_ = 0;
    std::size_t len_ = 0;
    int error_ = 0;
    const ServerOptions* options_ = nullptr;
    const ShutdownSignal* signal_ = nullptr;
};

}  // namespace fibers::http::detail
