#pragma once

// The transport HTTP runs over: any byte stream with net::Stream's I/O
// surface (plain net::Stream today, a TLS stream with the same member
// functions later). The server's connection code and the client are written
// against this concept only.
//
//   read_some(buf, n)        -> net::IoResult  (bytes == 0, error == 0: EOF)
//   write_all(buf, n)        -> net::IoResult
//   set_read_deadline(t)     -> int (0 / errno), t on steady_clock
//   set_write_deadline(t)    -> int
//   shutdown()               -> int (half-close the sending side)
//   close()                  -> int
//
// All of them noexcept, errors as errno values (ETIMEDOUT once a deadline
// has passed, ECANCELED when closed meanwhile), like net::Stream.
//
// Inside, the connection engine calls the stream through a table of function
// pointers (StreamOps, one per stream type): one engine for every transport,
// an indirect call next to a syscall.

#include <cstddef>
#include <concepts>

#include "fibers/net/socket.hpp"
#include "fibers/poller/sources.hpp"

namespace fibers::http {

template <class S>
concept ByteStream = requires(S& s, void* buffer, const void* data, std::size_t size, poller::TimePoint t) {
    { s.read_some(buffer, size) } noexcept -> std::same_as<net::IoResult>;
    { s.write_all(data, size) } noexcept -> std::same_as<net::IoResult>;
    { s.set_read_deadline(t) } noexcept -> std::same_as<int>;
    { s.set_write_deadline(t) } noexcept -> std::same_as<int>;
    { s.shutdown() } noexcept -> std::same_as<int>;
    { s.close() } noexcept -> std::same_as<int>;
};

static_assert(ByteStream<net::Stream>);

namespace detail {

// Type-erased ByteStream operations (void* = the stream).
struct StreamOps {
    net::IoResult (*read_some)(void* stream, void* buffer, std::size_t size) noexcept;
    net::IoResult (*write_all)(void* stream, const void* data, std::size_t size) noexcept;
    int (*set_read_deadline)(void* stream, poller::TimePoint deadline) noexcept;
    int (*set_write_deadline)(void* stream, poller::TimePoint deadline) noexcept;
    int (*shutdown)(void* stream) noexcept;
    int (*close)(void* stream) noexcept;
};

template <ByteStream S>
inline constexpr StreamOps kStreamOps{
    [](void* s, void* b, std::size_t n) noexcept { return static_cast<S*>(s)->read_some(b, n); },
    [](void* s, const void* b, std::size_t n) noexcept { return static_cast<S*>(s)->write_all(b, n); },
    [](void* s, poller::TimePoint t) noexcept { return static_cast<S*>(s)->set_read_deadline(t); },
    [](void* s, poller::TimePoint t) noexcept { return static_cast<S*>(s)->set_write_deadline(t); },
    [](void* s) noexcept { return static_cast<S*>(s)->shutdown(); },
    [](void* s) noexcept { return static_cast<S*>(s)->close(); },
};

// A stream and its operations.
struct StreamRef {
    void* stream = nullptr;
    const StreamOps* ops = nullptr;

    template <ByteStream S>
    static StreamRef of(S& s) noexcept {
        return StreamRef{&s, &kStreamOps<S>};
    }

    net::IoResult read_some(void* b, std::size_t n) const noexcept { return ops->read_some(stream, b, n); }
    net::IoResult write_all(const void* b, std::size_t n) const noexcept { return ops->write_all(stream, b, n); }
    int set_read_deadline(poller::TimePoint t) const noexcept { return ops->set_read_deadline(stream, t); }
    int set_write_deadline(poller::TimePoint t) const noexcept { return ops->set_write_deadline(stream, t); }
    int shutdown() const noexcept { return ops->shutdown(stream); }
    int close() const noexcept { return ops->close(stream); }
};

}  // namespace detail

}  // namespace fibers::http
