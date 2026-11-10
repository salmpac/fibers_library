#pragma once

// The protocol half of an HTTP/2 client connection: an nghttp2 client
// session, the per-stream records, and the hand-off of results to the
// waiting callers. No I/O and no waiting here: the connection (client.hpp)
// feeds received bytes in (recv), takes bytes to send out (fill), and calls
// everything under its one mutex.
//
// A request's way through it:
//   submit()        the caller (under the mutex): a StreamRecord from the
//                   session's pool, nghttp2_submit_request2 (nghttp2 copies
//                   the header array; the body is read from the caller's
//                   memory by read_body as flow control allows).
//   callbacks       inside recv() / fill(): headers and DATA go straight
//                   into the caller's Response; END_STREAM is noted.
//   on_stream_close the stream is done: the result (0 / errno) completes the
//                   caller's Oneshot (complete()), the record goes back to
//                   the pool. nghttp2 calls it for every submitted request
//                   it got to send or refuse: also for HEADERS still queued
//                   behind MAX_CONCURRENT_STREAMS when a GOAWAY arrives
//                   (REFUSED_STREAM) or when we cancelled them.
//   lost()          the connection is gone: every record still live
//                   completes with the connection's error.
//
// Completion vs. the deadline timer (a poller callback, which must not take
// the mutex): the record's Call points at a DeadlineNode whose `claimed`
// flag decides who completes the caller. complete() claims first; if the
// timer has claimed (completed the caller with ETIMEDOUT), complete() only
// detaches. The caller, woken by the timer, then locks the mutex and
// cancel()s: detaches the record (if the stream is still live) and queues
// RST_STREAM(CANCEL). So the caller's frame (Call, Response, request body)
// is touched under the mutex only, and only until it is detached; a
// detached record that nghttp2 still asks for body bytes resets the stream.
//
// Memory: records are pooled per session (they grow to the number of
// streams live at once and are reused); the Response is the caller's
// (bounded by max_response_header_bytes / max_response_body_bytes). nghttp2
// allocates internally (per stream state, header copies, HPACK tables,
// frame buffers) with malloc.

#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "fibers/fail/fail_fast.hpp"
#include "fibers/fiber/oneshot.hpp"
#include "fibers/http2/message.hpp"
#include "fibers/http2/options.hpp"
#include "fibers/poller/sources.hpp"
#include "fibers/poller/task_batch.hpp"
#include "fibers/sched/scheduler.hpp"

namespace fibers::http2 {

// Counters of a client (relaxed atomics; for tests and monitoring).
struct ClientStats {
    std::uint64_t connects = 0;          // connections established
    std::uint64_t connect_failures = 0;  // connect attempts that failed
    std::uint64_t requests = 0;          // requests submitted on a connection
    std::uint64_t goaways_received = 0;
    std::uint64_t streams_reset = 0;     // RST_STREAM sent by us (deadline, limits)
    std::uint64_t pings_sent = 0;        // keepalive PINGs
    std::uint64_t ping_acks = 0;
    std::uint64_t keepalive_failures = 0;  // connections dropped for a missing PING ACK
};

namespace detail {

template <class Sync>
struct Counters {
    template <class T>
    using Atomic = typename Sync::template Atomic<T>;

    Atomic<std::uint64_t> connects{0};
    Atomic<std::uint64_t> connect_failures{0};
    Atomic<std::uint64_t> requests{0};
    Atomic<std::uint64_t> goaways_received{0};
    Atomic<std::uint64_t> streams_reset{0};
    Atomic<std::uint64_t> pings_sent{0};
    Atomic<std::uint64_t> ping_acks{0};
    Atomic<std::uint64_t> keepalive_failures{0};

    static void bump(Atomic<std::uint64_t>& c) noexcept { c.fetch_add(1, std::memory_order_relaxed); }

    ClientStats snapshot() const noexcept {
        ClientStats s;
        s.connects = connects.load(std::memory_order_relaxed);
        s.connect_failures = connect_failures.load(std::memory_order_relaxed);
        s.requests = requests.load(std::memory_order_relaxed);
        s.goaways_received = goaways_received.load(std::memory_order_relaxed);
        s.streams_reset = streams_reset.load(std::memory_order_relaxed);
        s.pings_sent = pings_sent.load(std::memory_order_relaxed);
        s.ping_acks = ping_acks.load(std::memory_order_relaxed);
        s.keepalive_failures = keepalive_failures.load(std::memory_order_relaxed);
        return s;
    }
};

template <class Sched>
struct Call;

// The deadline of one request: a poller timer from the scheduler's
// TaskMemoryPool (pooled, no malloc), shared by the caller and the timer
// callback (refs), with the claim flag that decides who completes the call.
template <class Sched>
struct DeadlineNode {
    using Sync = typename Sched::SyncPolicy;

    typename Sched::Timer timer;
    typename Sync::template Atomic<std::uint32_t> refs{2};  // the caller + the timer callback
    typename Sync::template Atomic<bool> claimed{false};
    Call<Sched>* call = nullptr;
    Sched* scheduler = nullptr;

    static constexpr std::size_t kBlock = Sched::TaskMemory::block_size_for(sizeof(DeadlineNode<Sched>));

    static DeadlineNode* create(Sched& scheduler, Call<Sched>* call) noexcept {
        static_assert(kBlock != 0, "DeadlineNode does not fit the scheduler's TaskMemoryPool");
        void* block = scheduler.task_memory().template acquire<kBlock>();
        auto* node = ::new (block) DeadlineNode();
        node->call = call;
        node->scheduler = &scheduler;
        node->timer.memo = node;
        node->timer.callback = &on_timer;
        return node;
    }

    // true: this party completes the call.
    bool claim() noexcept { return !claimed.exchange(true, std::memory_order_acq_rel); }

    void unref() noexcept {
        if (refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            Sched* s = scheduler;
            this->~DeadlineNode();
            s->task_memory().template release<kBlock>(this);
        }
    }

    // Poller thread.
    static void on_timer(void* memo, poller::TimerEvent event, poller::TaskBatch&) noexcept;
};

// The caller's state of one request, in its own frame.
template <class Sched>
struct Call {
    using Oneshot = fiber::BasicOneshot<int, Sched>;
    typename Oneshot::Promise promise;
    Response* response = nullptr;
    DeadlineNode<Sched>* deadline = nullptr;  // nullptr: no deadline
    void* record = nullptr;                   // the live StreamRecord (under the connection mutex)
};

template <class Sched>
void DeadlineNode<Sched>::on_timer(void* memo, poller::TimerEvent event, poller::TaskBatch&) noexcept {
    auto* node = static_cast<DeadlineNode*>(memo);
    if (event == poller::TimerEvent::kFired && node->claim()) {
        // The caller is parked on its Oneshot: its frame is alive until this
        // set() completes it (our last touch of it).
        node->call->promise.set(ETIMEDOUT);
    }
    node->unref();
}

template <class Sched>
class Session {
public:
    using CallT = Call<Sched>;
    using CountersT = Counters<typename Sched::SyncPolicy>;

    Session(const ClientOptions& options, CountersT& counters) noexcept : options_(options), counters_(counters) {}

    Session(const Session&) = delete;
    Session& operator=(const Session&) = delete;

    ~Session() {
        if (session_ != nullptr) {
            nghttp2_session_del(session_);
        }
    }

    // Creates the nghttp2 session and queues our SETTINGS (and the
    // connection window). 0 or ENOMEM.
    int init() noexcept {
        nghttp2_session_callbacks* cbs = nullptr;
        if (nghttp2_session_callbacks_new(&cbs) != 0) {
            return ENOMEM;
        }
        nghttp2_session_callbacks_set_on_begin_headers_callback(cbs, &on_begin_headers);
        nghttp2_session_callbacks_set_on_header_callback(cbs, &on_header);
        nghttp2_session_callbacks_set_on_frame_recv_callback(cbs, &on_frame_recv);
        nghttp2_session_callbacks_set_on_data_chunk_recv_callback(cbs, &on_data_chunk_recv);
        nghttp2_session_callbacks_set_on_stream_close_callback(cbs, &on_stream_close);
        nghttp2_session_callbacks_set_on_invalid_frame_recv_callback(cbs, &on_invalid_frame_recv);
        nghttp2_session_callbacks_set_on_frame_send_callback(cbs, &on_frame_send);
        nghttp2_option* option = nullptr;
        if (nghttp2_option_new(&option) != 0) {
            nghttp2_session_callbacks_del(cbs);
            return ENOMEM;
        }
        nghttp2_option_set_peer_max_concurrent_streams(option, options_.assumed_max_concurrent_streams);
        const int rv = nghttp2_session_client_new3(&session_, cbs, this, option, nullptr);
        nghttp2_option_del(option);
        nghttp2_session_callbacks_del(cbs);
        if (rv != 0) {
            session_ = nullptr;
            return ENOMEM;
        }
        const nghttp2_settings_entry iv[] = {
            {NGHTTP2_SETTINGS_ENABLE_PUSH, 0},
            {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, options_.initial_window_size},
            {NGHTTP2_SETTINGS_MAX_HEADER_LIST_SIZE, static_cast<std::uint32_t>(options_.max_response_header_bytes)},
        };
        if (nghttp2_submit_settings(session_, NGHTTP2_FLAG_NONE, iv, sizeof iv / sizeof iv[0]) != 0) {
            return ENOMEM;
        }
        if (options_.connection_window_size > NGHTTP2_INITIAL_CONNECTION_WINDOW_SIZE) {
            if (nghttp2_session_set_local_window_size(session_, NGHTTP2_FLAG_NONE, 0,
                                                      options_.connection_window_size) != 0) {
                return ENOMEM;
            }
        }
        return 0;
    }

    // ---- requests (under the connection mutex) ---------------------------------

    // New streams may start here (no GOAWAY either way, ids left, alive).
    bool accepting() const noexcept { return !dead_ && fatal_ == 0 && !goaway_received_ && !goaway_sent_; }

    // Starts a request. 0, or: EAGAIN (this connection takes no new streams:
    // use another one), EBUSY (stream limit, see StreamLimit), EINVAL
    // (nghttp2 rejected the headers). On 0 the call is live: exactly one
    // completion follows (complete() or the deadline timer).
    int submit(CallT& call, const nghttp2_nv* nv, std::size_t nvlen, std::string_view body) noexcept {
        if (!accepting()) {
            return EAGAIN;
        }
        const std::uint32_t peer_limit =
            nghttp2_session_get_remote_settings(session_, NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS);
        std::size_t allowed = peer_limit;
        if (options_.stream_limit == StreamLimit::kQueue) {
            allowed += options_.max_queued_requests;
        }
        if (live_count_ >= allowed) {
            return EBUSY;
        }
        Record* rec = acquire();
        rec->call = &call;
        rec->body = body.data();
        rec->body_size = body.size();
        rec->has_body = !body.empty();
        nghttp2_data_provider2 provider{};
        provider.source.ptr = rec;
        provider.read_callback = &read_body;
        const int32_t id =
            nghttp2_submit_request2(session_, nullptr, nv, nvlen, rec->has_body ? &provider : nullptr, rec);
        if (id < 0) {
            rec->call = nullptr;
            release(rec);
            if (id == NGHTTP2_ERR_STREAM_ID_NOT_AVAILABLE) {
                // Stream ids used up (2^30 requests): no new streams here; a
                // GOAWAY lets the connection close once the live ones end.
                goaway_sent_ = true;
                static_cast<void>(nghttp2_submit_goaway(session_, NGHTTP2_FLAG_NONE, 0, NGHTTP2_NO_ERROR, nullptr, 0));
                return EAGAIN;
            }
            return id == NGHTTP2_ERR_NOMEM ? ENOMEM : EINVAL;
        }
        rec->id = id;
        call.record = rec;
        CountersT::bump(counters_.requests);
        return 0;
    }

    // The call was completed by its deadline timer (ETIMEDOUT): detach it
    // and reset the stream if it is still live.
    void cancel(CallT& call) noexcept {
        auto* rec = static_cast<Record*>(call.record);
        if (rec == nullptr) {
            return;  // already closed (complete() lost the claim)
        }
        call.record = nullptr;
        detach(rec);
        if (!dead_ && fatal_ == 0) {
            if (nghttp2_submit_rst_stream(session_, NGHTTP2_FLAG_NONE, rec->id, NGHTTP2_CANCEL) == 0) {
                CountersT::bump(counters_.streams_reset);
            }
        }
    }

    // ---- connection --------------------------------------------------------------

    // Bytes read from the transport. 0, or EPROTO (fatal: nghttp2 cannot go
    // on; the connection must be dropped).
    int recv(const void* data, std::size_t size) noexcept {
        if (dead_ || fatal_ != 0) {
            return EPROTO;
        }
        const nghttp2_ssize r = nghttp2_session_mem_recv2(session_, static_cast<const uint8_t*>(data), size);
        if (r < 0) {
            fatal_ = static_cast<int>(r);
            return EPROTO;
        }
        return 0;
    }

    // Appends up to about `limit` bytes of frames to `out`.
    void fill(std::string& out, std::size_t limit) noexcept {
        if (dead_ || fatal_ != 0) {
            return;
        }
        std::size_t added = 0;
        while (added < limit) {
            const uint8_t* data = nullptr;
            const nghttp2_ssize n = nghttp2_session_mem_send2(session_, &data);
            if (n < 0) {
                fatal_ = static_cast<int>(n);
                return;
            }
            if (n == 0) {
                return;
            }
            out.append(reinterpret_cast<const char*>(data), static_cast<std::size_t>(n));
            added += static_cast<std::size_t>(n);
        }
    }

    bool want_write() noexcept { return !dead_ && fatal_ == 0 && nghttp2_session_want_write(session_) != 0; }

    // Nothing more to do on this connection: GOAWAY sent or received and
    // every stream finished (nghttp2's want_read / want_write both 0), or a
    // fatal error. Flush, then close.
    bool finished() noexcept {
        return dead_ || fatal_ != 0 ||
               (nghttp2_session_want_read(session_) == 0 && nghttp2_session_want_write(session_) == 0);
    }

    // The error in-flight requests get when the connection goes away now.
    int loss_error(int transport_error) const noexcept {
        if (fatal_ != 0 || protocol_error_) {
            return EPROTO;
        }
        return transport_error;
    }

    // The transport is gone: every live call completes with `error`. The
    // session does nothing more afterwards.
    // (EAGAIN for those whose HEADERS never left: not processed.)
    void lost(int error) noexcept {
        dead_ = true;
        for (Record* r = live_; r != nullptr; r = r->next) {
            complete(r, r->headers_sent ? error : EAGAIN, 0);
        }
    }

    // Queues a PING (keepalive). false if the session cannot send any more.
    bool ping() noexcept {
        if (dead_ || fatal_ != 0 || goaway_sent_) {
            return false;
        }
        if (nghttp2_submit_ping(session_, NGHTTP2_FLAG_NONE, nullptr) != 0) {
            return false;
        }
        CountersT::bump(counters_.pings_sent);
        return true;
    }

    // PING ACKs received so far.
    std::uint64_t ping_acks() const noexcept { return ping_acks_; }

    // Graceful close: GOAWAY(NO_ERROR); finished() once it is sent.
    void terminate() noexcept {
        if (dead_ || fatal_ != 0 || session_ == nullptr) {
            return;
        }
        goaway_sent_ = true;
        static_cast<void>(nghttp2_session_terminate_session(session_, NGHTTP2_NO_ERROR));
    }

    bool goaway_received() const noexcept { return goaway_received_; }
    std::int32_t goaway_last_stream_id() const noexcept { return goaway_last_stream_id_; }
    // Streams live (submitted, not yet closed; also detached ones).
    std::size_t live_streams() const noexcept { return live_count_; }
    // Records ever allocated (the pool's size).
    std::size_t pooled_records() const noexcept { return records_.size(); }

private:
    struct Record {
        Record* prev = nullptr;
        Record* next = nullptr;
        CallT* call = nullptr;      // nullptr: detached (completed / timed out)
        const char* body = nullptr;
        std::size_t body_size = 0;
        std::size_t body_pos = 0;
        std::size_t header_bytes = 0;
        std::int32_t id = 0;
        int error = 0;              // a local verdict (EMSGSIZE, EPROTO)
        bool has_body = false;
        bool headers_sent = false;  // our HEADERS went out (nghttp2 handed them over)
        bool end_stream = false;    // the peer's END_STREAM arrived
        bool skip_block = false;    // inside an interim (1xx) header block
        bool live = false;
    };

    static Session& self(void* user_data) noexcept { return *static_cast<Session*>(user_data); }

    Record* acquire() noexcept {
        Record* r = free_;
        if (r != nullptr) {
            free_ = r->next;
        } else {
            records_.push_back(std::make_unique<Record>());
            r = records_.back().get();
        }
        *r = Record{};
        r->live = true;
        r->next = live_;
        if (live_ != nullptr) {
            live_->prev = r;
        }
        live_ = r;
        ++live_count_;
        return r;
    }

    void release(Record* r) noexcept {
        if (!r->live) {
            return;
        }
        if (r->prev != nullptr) {
            r->prev->next = r->next;
        } else {
            live_ = r->next;
        }
        if (r->next != nullptr) {
            r->next->prev = r->prev;
        }
        --live_count_;
        *r = Record{};
        r->next = free_;
        free_ = r;
    }

    // A live record by stream id (nghttp2's stream user data, or the live
    // list for a stream nghttp2 has not opened yet).
    Record* find(std::int32_t id) noexcept {
        if (id <= 0) {
            return nullptr;
        }
        if (auto* r = static_cast<Record*>(nghttp2_session_get_stream_user_data(session_, id)); r != nullptr) {
            return r;
        }
        for (Record* r = live_; r != nullptr; r = r->next) {
            if (r->id == id) {
                return r;
            }
        }
        return nullptr;
    }

    static void detach(Record* rec) noexcept {
        rec->call = nullptr;
        rec->body = nullptr;
    }

    // Hands `error` to the record's caller (once; nothing if detached).
    void complete(Record* rec, int error, std::uint32_t reset_code) noexcept {
        CallT* call = rec->call;
        if (call == nullptr) {
            return;
        }
        detach(rec);
        call->record = nullptr;
        if (call->deadline != nullptr && !call->deadline->claim()) {
            return;  // the deadline timer completed it; the caller cancel()s
        }
        Response& res = *call->response;
        if (error != 0) {
            res.clear();
        }
        ResponseWriter::set_stream_id(res, rec->id);
        if (error == ECONNABORTED) {
            ResponseWriter::set_reset_code(res, reset_code);
        }
        ResponseWriter::finish(res);
        call->promise.set(error);  // the last touch of the caller's frame
    }

    // A local verdict on a live stream: reset it, report `error` at close.
    void fail_stream(Record* rec, int error) noexcept {
        if (rec->error != 0) {
            return;
        }
        rec->error = error;
        if (nghttp2_submit_rst_stream(session_, NGHTTP2_FLAG_NONE, rec->id, NGHTTP2_CANCEL) == 0) {
            CountersT::bump(counters_.streams_reset);
        }
    }

    // ---- nghttp2 callbacks (inside recv() / fill(), under the mutex) ---------------

    static nghttp2_ssize read_body(nghttp2_session*, int32_t, uint8_t* buf, std::size_t length, uint32_t* flags,
                                   nghttp2_data_source* source, void*) noexcept {
        auto* rec = static_cast<Record*>(source->ptr);
        if (rec->body == nullptr) {
            return NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE;  // detached: the caller's buffer is gone
        }
        const std::size_t n = std::min(length, rec->body_size - rec->body_pos);
        if (n != 0) {
            std::memcpy(buf, rec->body + rec->body_pos, n);
        }
        rec->body_pos += n;
        if (rec->body_pos == rec->body_size) {
            *flags |= NGHTTP2_DATA_FLAG_EOF;
        }
        return static_cast<nghttp2_ssize>(n);
    }

    static int on_begin_headers(nghttp2_session* session, const nghttp2_frame* frame, void*) noexcept {
        if (frame->hd.type == NGHTTP2_HEADERS) {
            if (auto* rec = static_cast<Record*>(nghttp2_session_get_stream_user_data(session, frame->hd.stream_id));
                rec != nullptr) {
                rec->skip_block = false;
            }
        }
        return 0;
    }

    static int on_header(nghttp2_session* session, const nghttp2_frame* frame, const uint8_t* name,
                         std::size_t namelen, const uint8_t* value, std::size_t valuelen, uint8_t,
                         void* user_data) noexcept {
        if (frame->hd.type != NGHTTP2_HEADERS) {
            return 0;
        }
        auto* rec = static_cast<Record*>(nghttp2_session_get_stream_user_data(session, frame->hd.stream_id));
        if (rec == nullptr || rec->call == nullptr || rec->error != 0) {
            return 0;
        }
        Session& s = self(user_data);
        const std::string_view n(reinterpret_cast<const char*>(name), namelen);
        const std::string_view v(reinterpret_cast<const char*>(value), valuelen);
        rec->header_bytes += namelen + valuelen + 32;  // RFC 9113 6.5.2 accounting
        if (rec->header_bytes > s.options_.max_response_header_bytes) {
            s.fail_stream(rec, EMSGSIZE);
            return 0;
        }
        if (n == ":status") {
            int status = 0;
            const auto r = std::from_chars(v.data(), v.data() + v.size(), status);
            if (r.ec != std::errc{} || r.ptr != v.data() + v.size() || v.size() != 3 || status < 100) {
                s.fail_stream(rec, EPROTO);
                return 0;
            }
            if (status < 200) {
                rec->skip_block = true;  // interim response: the final one follows
                return 0;
            }
            ResponseWriter::set_status(*rec->call->response, status);
            return 0;
        }
        if (rec->skip_block || n.empty() || n[0] == ':') {
            return 0;
        }
        ResponseWriter::add_header(*rec->call->response, n, v);
        return 0;
    }

    static int on_data_chunk_recv(nghttp2_session* session, uint8_t, int32_t stream_id, const uint8_t* data,
                                  std::size_t len, void* user_data) noexcept {
        auto* rec = static_cast<Record*>(nghttp2_session_get_stream_user_data(session, stream_id));
        if (rec == nullptr || rec->call == nullptr || rec->error != 0) {
            return 0;
        }
        Session& s = self(user_data);
        if (ResponseWriter::body_size(*rec->call->response) + len > s.options_.max_response_body_bytes) {
            s.fail_stream(rec, EMSGSIZE);
            return 0;
        }
        ResponseWriter::append_body(*rec->call->response, data, len);
        return 0;
    }

    static int on_frame_recv(nghttp2_session* session, const nghttp2_frame* frame, void* user_data) noexcept {
        Session& s = self(user_data);
        switch (frame->hd.type) {
            case NGHTTP2_DATA:
            case NGHTTP2_HEADERS:
                if ((frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0) {
                    if (auto* rec =
                            static_cast<Record*>(nghttp2_session_get_stream_user_data(session, frame->hd.stream_id));
                        rec != nullptr) {
                        rec->end_stream = true;
                    }
                }
                break;
            case NGHTTP2_GOAWAY:
                s.goaway_received_ = true;
                s.goaway_last_stream_id_ = frame->goaway.last_stream_id;
                CountersT::bump(s.counters_.goaways_received);
                break;
            case NGHTTP2_PING:
                if ((frame->hd.flags & NGHTTP2_FLAG_ACK) != 0) {
                    ++s.ping_acks_;
                    CountersT::bump(s.counters_.ping_acks);
                }
                break;
            default: break;
        }
        return 0;
    }

    static int on_invalid_frame_recv(nghttp2_session* session, const nghttp2_frame* frame, int,
                                     void*) noexcept {
        // e.g. a malformed response header block: nghttp2 resets the stream
        // itself; report it as a protocol error rather than a server reset.
        if (frame->hd.stream_id > 0) {
            if (auto* rec = static_cast<Record*>(nghttp2_session_get_stream_user_data(session, frame->hd.stream_id));
                rec != nullptr && rec->error == 0) {
                rec->error = EPROTO;
            }
        }
        return 0;
    }

    static int on_frame_send(nghttp2_session* session, const nghttp2_frame* frame, void* user_data) noexcept {
        if (frame->hd.type == NGHTTP2_HEADERS && frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
            if (auto* rec = static_cast<Record*>(nghttp2_session_get_stream_user_data(session, frame->hd.stream_id));
                rec != nullptr) {
                rec->headers_sent = true;
            }
        } else if (frame->hd.type == NGHTTP2_GOAWAY && frame->goaway.error_code != NGHTTP2_NO_ERROR) {
            self(user_data).protocol_error_ = true;  // nghttp2 gave up on the peer
        }
        return 0;
    }

    static int on_stream_close(nghttp2_session*, int32_t stream_id, uint32_t error_code, void* user_data) noexcept {
        Session& s = self(user_data);
        Record* rec = s.find(stream_id);
        if (rec == nullptr) {
            return 0;
        }
        int error;
        if (rec->error != 0) {
            error = rec->error;
        } else if (rec->end_stream) {
            error = rec->call != nullptr && rec->call->response->status() == 0 ? EPROTO : 0;
        } else if (error_code == NGHTTP2_REFUSED_STREAM) {
            error = EAGAIN;  // not processed (GOAWAY above it, or refused): retryable
        } else {
            error = ECONNABORTED;
        }
        s.complete(rec, error, error_code);
        s.release(rec);
        return 0;
    }

    const ClientOptions& options_;
    CountersT& counters_;
    nghttp2_session* session_ = nullptr;
    std::vector<std::unique_ptr<Record>> records_;
    Record* free_ = nullptr;
    Record* live_ = nullptr;
    std::size_t live_count_ = 0;
    std::uint64_t ping_acks_ = 0;
    int fatal_ = 0;
    std::int32_t goaway_last_stream_id_ = 0;
    bool dead_ = false;
    bool goaway_received_ = false;
    bool goaway_sent_ = false;
    bool protocol_error_ = false;
};

}  // namespace detail

}  // namespace fibers::http2
