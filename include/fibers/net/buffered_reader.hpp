#pragma once

// BufferedReader<S>: a read buffer in front of any byte stream with
// net::Stream's read_some (net::Stream, tls::TlsStream, an http::ByteStream,
// a test fake), for protocols that read lines, fixed-size headers and
// length-prefixed frames without a syscall per field.
//
//   std::byte storage[8192];                        // caller's memory: a
//   net::BufferedReader reader(stream, storage);    // local, a pool block...
//   net::LineResult line = reader.read_line();      // "...\n", a view into storage
//   if (!line) { ... line.error ... }               // ETIMEDOUT, ECONNRESET, EMSGSIZE
//   if (line.line.empty()) { ... end of stream ... }
//   std::byte header[5];
//   net::IoResult r = reader.read_exact(header, sizeof header);
//
// No allocation: the buffer is supplied by the caller (on the fiber's stack,
// from an object pool, inside the connection object) and its capacity is
// fixed. The reader holds a reference to the stream and a view of the
// buffer; both must outlive it. One reader per stream, used by one fiber at
// a time (like the stream's own read side).
//
// Errors are the stream's, passed through as IoResult / LineResult errors
// (errno values): deadlines (set them on the stream: set_read_deadline),
// ECANCELED, resets. A failed fill never loses data: whatever was buffered
// stays buffered, so after, say, ETIMEDOUT the caller may move the deadline
// and call read_line() again, which picks up where it stopped. End of
// stream is error 0 with nothing (more) to return, as with read_some.
//
// Views returned by read_line(), peek(), view() and take_buffered() point
// into the buffer and are valid until the next call that reads from the
// stream or consumes (any non-const member function).
//
// Switching protocols: take_buffered() hands over the bytes read past the
// current message (the reader overshot), e.g. into
// TlsStream::upgrade_client / upgrade_server(ctx, ..., already_received)
// after a plaintext STARTTLS / Postgres SSLRequest exchange; the reader is
// then empty and the stream may be used directly (or a new reader made over
// the TLS stream, with the same buffer).
//
// Buffer layout: [consumed | buffered: begin_..end_ | free]. Bytes are moved
// to the front only when a fill needs room at the end (and the buffer is
// rewound for free whenever it runs empty), so a line or a frame header is
// usually read with one syscall and no copy until the caller's own.

#include <algorithm>
#include <concepts>
#include <cstddef>
#include <cstring>
#include <span>
#include <string_view>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/net/socket.hpp"

namespace fibers::net {

// What BufferedReader needs from a stream: net::Stream's read_some
// (IoResult; bytes == 0 with error 0: end of stream).
template <class S>
concept ReadStream = requires(S& s, void* buffer, std::size_t size) {
    { s.read_some(buffer, size) } noexcept -> std::same_as<IoResult>;
};

// read_line()'s result. error == 0: `line` is a whole line including its
// delimiter, or the last bytes of the stream without one (it ended first),
// or empty at end of stream. error != 0: the stream's error (line empty),
// or EMSGSIZE (no delimiter within `max` bytes; line = those bytes, NOT
// consumed).
struct LineResult {
    std::string_view line;
    int error = 0;

    explicit operator bool() const noexcept { return error == 0; }
};

// peek()'s result: the buffered bytes (at least the requested count, unless
// the stream ended first or failed) and 0 or an error.
struct PeekResult {
    std::span<const std::byte> data;
    int error = 0;

    explicit operator bool() const noexcept { return error == 0; }
};

template <ReadStream S>
class BufferedReader {
public:
    // No limit for read_line() beyond the buffer's capacity.
    static constexpr std::size_t kNoLimit = static_cast<std::size_t>(-1);

    // `buffer` must not be empty (fails fast).
    BufferedReader(S& stream, std::span<std::byte> buffer) noexcept
        : stream_(&stream), data_(buffer.data()), capacity_(buffer.size()) {
        if (capacity_ == 0) [[unlikely]] {
            fail::fail_fast("net: BufferedReader needs a non-empty buffer");
        }
    }

    BufferedReader(S& stream, void* data, std::size_t size) noexcept
        : BufferedReader(stream, std::span<std::byte>(static_cast<std::byte*>(data), size)) {}

    BufferedReader(const BufferedReader&) = delete;
    BufferedReader& operator=(const BufferedReader&) = delete;

    S& stream() noexcept { return *stream_; }

    std::size_t capacity() const noexcept { return capacity_; }

    // Bytes read from the stream and not consumed yet.
    std::size_t buffered() const noexcept { return end_ - begin_; }

    // The buffered bytes (no read).
    std::span<const std::byte> view() const noexcept { return {data_ + begin_, buffered()}; }

    // ---- reading ----------------------------------------------------------------

    // Up to `size` bytes: the buffered ones if there are any, else one read
    // (straight into `buffer` when it is at least the capacity, else
    // through the buffer). {0, 0}: end of stream.
    IoResult read_some(void* buffer, std::size_t size) noexcept {
        if (size == 0) {
            return IoResult{0, 0};
        }
        if (buffered() == 0) {
            if (size >= capacity_) {
                return stream_->read_some(buffer, size);
            }
            if (const IoResult r = fill(); !r || r.bytes == 0) {
                return r;
            }
        }
        return IoResult{copy_out(static_cast<std::byte*>(buffer), size), 0};
    }

    // Exactly `size` bytes (the buffered ones first; large remainders are
    // read straight into `buffer`). A short count with error 0: the stream
    // ended first. On an error, `bytes` were delivered (and consumed) before
    // it, as with Stream::read_exact.
    IoResult read_exact(void* buffer, std::size_t size) noexcept {
        auto* out = static_cast<std::byte*>(buffer);
        std::size_t done = copy_out(out, size);
        while (done < size) {
            const std::size_t remaining = size - done;
            IoResult r;
            if (remaining >= capacity_) {
                r = stream_->read_some(out + done, remaining);
                if (r && r.bytes != 0) {
                    done += r.bytes;
                    continue;
                }
            } else {
                r = fill();
                if (r && r.bytes != 0) {
                    done += copy_out(out + done, remaining);
                    continue;
                }
            }
            return IoResult{done, r.error};  // end of stream (error 0) or an error
        }
        return IoResult{done, 0};
    }

    // The next line, delimiter included, as a view into the buffer (see
    // LineResult). `max`: the longest line accepted, delimiter included
    // (clamped to [1, capacity()]); a longer one is EMSGSIZE with its first
    // `max` bytes left buffered (skip them with consume(), or give up).
    LineResult read_line(char delim = '\n', std::size_t max = kNoLimit) noexcept {
        max = std::clamp<std::size_t>(max, 1, capacity_);
        std::size_t scanned = 0;  // relative to begin_ (a fill may move the bytes)
        for (;;) {
            const std::size_t limit = std::min(buffered(), max);
            const char* const base = chars() + begin_;
            if (const void* hit = std::memchr(base + scanned, delim, limit - scanned)) {
                const std::size_t len = static_cast<std::size_t>(static_cast<const char*>(hit) - base) + 1;
                const std::string_view line(base, len);
                consume(len);
                return LineResult{line, 0};
            }
            scanned = limit;
            if (buffered() >= max) {
                return LineResult{std::string_view(base, max), EMSGSIZE};
            }
            const IoResult r = fill();
            if (!r) {
                return LineResult{{}, r.error};  // what we have stays buffered
            }
            if (r.bytes == 0) {
                // End of stream: the unterminated rest, or empty.
                const std::string_view rest(chars() + begin_, buffered());
                consume(buffered());
                return LineResult{rest, 0};
            }
        }
    }

    // Waits until at least `size` bytes are buffered (or the stream ends or
    // fails) and returns ALL buffered bytes, without consuming them. size >
    // capacity(): EMSGSIZE at once (data = what is buffered). A shorter
    // result with error 0: the stream ended first.
    PeekResult peek(std::size_t size) noexcept {
        if (size > capacity_) {
            return PeekResult{view(), EMSGSIZE};
        }
        while (buffered() < size) {
            const IoResult r = fill();
            if (!r) {
                return PeekResult{view(), r.error};
            }
            if (r.bytes == 0) {
                break;
            }
        }
        return PeekResult{view(), 0};
    }

    // Drops `size` buffered bytes (fails fast if fewer are buffered).
    void consume(std::size_t size) noexcept {
        if (size > buffered()) [[unlikely]] {
            fail::fail_fast("net: BufferedReader::consume() past the buffered bytes");
        }
        begin_ += size;
        if (begin_ == end_) {
            begin_ = end_ = 0;  // rewind for free (the bytes stay in place until the next fill)
        }
    }

    // One read_some() into the free space at the end (moving the buffered
    // bytes to the front first if needed). bytes: how many were added, {0,
    // 0} at end of stream; ENOBUFS if the buffer is full.
    IoResult fill() noexcept {
        if (end_ == capacity_) {
            if (begin_ == 0) {
                return IoResult{0, ENOBUFS};
            }
            const std::size_t n = buffered();
            std::memmove(data_, data_ + begin_, n);
            begin_ = 0;
            end_ = n;
        }
        const IoResult r = stream_->read_some(data_ + end_, capacity_ - end_);
        if (r) {
            end_ += r.bytes;
        }
        return r;
    }

    // Hands over everything buffered and empties the reader: e.g. the
    // ciphertext that arrived behind a STARTTLS / SSLRequest, for
    // upgrade_*(..., already_received). Valid until the next call.
    std::span<const std::byte> take_buffered() noexcept {
        const std::span<const std::byte> taken = view();
        begin_ = end_ = 0;
        return taken;
    }

    // Drops everything buffered.
    void clear() noexcept { begin_ = end_ = 0; }

private:
    const char* chars() const noexcept { return reinterpret_cast<const char*>(data_); }

    // Moves up to `size` buffered bytes to `out`; how many.
    std::size_t copy_out(std::byte* out, std::size_t size) noexcept {
        const std::size_t n = std::min(size, buffered());
        if (n != 0) {
            std::memcpy(out, data_ + begin_, n);
            consume(n);
        }
        return n;
    }

    S* stream_;
    std::byte* data_;
    std::size_t capacity_;
    std::size_t begin_ = 0;
    std::size_t end_ = 0;
};

template <class S>
BufferedReader(S&, std::span<std::byte>) -> BufferedReader<S>;

static_assert(ReadStream<Stream>);

}  // namespace fibers::net
