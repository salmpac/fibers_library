// The server's connection engine over a scripted in-memory stream: parsing
// edge cases (every split of the input across reads, pipelining, chunked
// bodies with extensions and trailers), smuggling defenses, limits,
// keep-alive rules, Expect: 100-continue, the router and the response API.
// No fibers: serve_connection() runs on the test thread (the scripted stream
// never waits).

#include <gtest/gtest.h>

#include <cstddef>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "fibers/http/connection.hpp"
#include "fibers/http/router.hpp"
#include "http_test_util.hpp"

namespace http = fibers::http;
using http_test::ParsedResponse;
using http_test::ScriptedStream;

namespace {

// Echoes what the server parsed: "<METHOD> <target>|<path>|<query a>|<header
// X-Test>|<body size>|<body>".
void echo(http::Request& req, http::Response& res) noexcept {
    std::string out;
    out += req.method_name();
    out += ' ';
    out += req.target();
    out += '|';
    out += req.path();
    out += '|';
    out += req.query("a").value_or("-");
    out += '|';
    out += req.header("x-test");
    out += '|';
    out += std::to_string(req.body().size());
    out += '|';
    out += req.body();
    res.header("Content-Type", "text/plain");
    res.send(out);
}

class Engine {
public:
    explicit Engine(http::ServerOptions options = {}, http::Handler handler = {}) {
        if (!handler) {
            router.add(http::Method::kGet, "/{p}", &echo);
            router.add(http::Method::kPost, "/{p}", &echo);
            router.add(http::Method::kPut, "/{p}", &echo);
            router.add(http::Method::kGet, "/", &echo);
            router.add(http::Method::kPost, "/", &echo);
            handler = http::Handler::ref(counting);
        }
        core = std::make_unique<http::ServerCore>(handler, options);
        state = std::make_unique<http::detail::ConnectionState>();
    }

    ScriptedStream run(std::vector<std::string> pieces, int end_error = 0) {
        ScriptedStream s;
        s.pieces = std::move(pieces);
        s.end_error = end_error;
        http::serve_connection(s, *state, *core);
        EXPECT_TRUE(s.closed);
        return s;
    }

    std::vector<ParsedResponse> responses(std::vector<std::string> pieces, bool* ok_out = nullptr,
                                          const std::vector<bool>& heads = {}) {
        last = run(std::move(pieces));
        bool ok = false;
        auto r = http_test::parse_responses(last.output, ok, heads);
        if (ok_out != nullptr) {
            *ok_out = ok;
        } else {
            EXPECT_TRUE(ok) << last.output;
        }
        return r;
    }

    struct Counting {
        Engine* self;
        void operator()(http::Request& q, http::Response& r) const noexcept {
            ++self->handled;
            self->router.handle(q, r);
        }
    };

    http::Router router;
    Counting counting{this};
    int handled = 0;
    std::unique_ptr<http::ServerCore> core;
    std::unique_ptr<http::detail::ConnectionState> state;
    ScriptedStream last;
};

// Responses without Date (it may tick between runs).
std::string normalize(const std::vector<ParsedResponse>& rs) {
    std::string out;
    for (const ParsedResponse& r : rs) {
        out += std::to_string(r.status) + "\n";
        for (const auto& [n, v] : r.headers) {
            if (!http::iequals(n, "Date")) {
                out += n + ": " + v + "\n";
            }
        }
        out += r.body + "\n--\n";
    }
    return out;
}

const std::string kGet = "GET /a?a=x%20y&b=2 HTTP/1.1\r\nHost: h\r\nX-Test: v1\r\n\r\n";
const std::string kPostLength = "POST /p HTTP/1.1\r\nHost: h\r\nContent-Length: 11\r\n\r\nhello world";
const std::string kPostChunked =
    "POST /c HTTP/1.1\r\nHost: h\r\nTransfer-Encoding: chunked\r\nX-Test: ch\r\n\r\n"
    "5;ext=1\r\nhello\r\n6;a=\"q;x\";b\r\n world\r\n0\r\nTrailer-A: 1\r\nTrailer-B: 2\r\n\r\n";
const std::string kGetClose = "GET /end HTTP/1.1\r\nHost: h\r\nConnection: close\r\n\r\n";

TEST(HttpParse, SimpleGet) {
    Engine e;
    const auto rs = e.responses({kGet});
    ASSERT_EQ(rs.size(), 1u);
    EXPECT_EQ(rs[0].status, 200);
    EXPECT_EQ(rs[0].body, "GET /a?a=x%20y&b=2|/a|x y|v1|0|");
    EXPECT_EQ(rs[0].header("Content-Length"), std::to_string(rs[0].body.size()));
    EXPECT_EQ(rs[0].header("Date").size(), 29u);
    EXPECT_FALSE(rs[0].has("Connection"));
    EXPECT_EQ(e.handled, 1);
}

TEST(HttpParse, ContentLengthAndChunkedBodies) {
    Engine e;
    const auto rs = e.responses({kPostLength + kPostChunked});
    ASSERT_EQ(rs.size(), 2u);
    EXPECT_EQ(rs[0].body, "POST /p|/p|-||11|hello world");
    EXPECT_EQ(rs[1].body, "POST /c|/c|-|ch|11|hello world");  // trailers not exposed as headers
}

TEST(HttpParse, PipelinedRequestsShareOneWrite) {
    Engine e;
    const auto rs = e.responses({kGet + kPostLength + kPostChunked + kGetClose});
    ASSERT_EQ(rs.size(), 4u);
    EXPECT_EQ(rs[3].body, "GET /end|/end|-||0|");
    EXPECT_EQ(rs[3].header("Connection"), "close");
    EXPECT_EQ(e.last.writes, 1u) << "pipelined responses are flushed together";
    EXPECT_TRUE(e.last.shut);
}

// The same bytes split at every boundary into two reads, and byte by byte:
// identical responses.
TEST(HttpParse, EverySplitAcrossReads) {
    const std::vector<std::string> inputs = {
        kGet, kPostLength, kPostChunked, kGet + kPostLength + kPostChunked + kGetClose,
        "GET /x HTTP/1.1\r\nHost: h\r\nExpect: 100-continue\r\nContent-Length: 3\r\n\r\nabc",
    };
    for (const std::string& in : inputs) {
        Engine reference;
        const std::string want = normalize(reference.responses({in}));
        for (std::size_t k = 1; k < in.size(); ++k) {
            Engine e;
            bool ok = false;
            const auto rs = e.responses({in.substr(0, k), in.substr(k)}, &ok);
            // A split head with Expect gets a 100 Continue first.
            std::vector<ParsedResponse> finals;
            for (const auto& r : rs) {
                if (r.status != 100) {
                    finals.push_back(r);
                }
            }
            ASSERT_TRUE(ok) << "split at " << k << " of " << in;
            ASSERT_EQ(normalize(finals), want) << "split at " << k << " of " << in;
        }
        std::vector<std::string> bytes;
        for (const char c : in) {
            bytes.emplace_back(1, c);
        }
        Engine e;
        bool ok = false;
        auto rs = e.responses(bytes, &ok);
        std::erase_if(rs, [](const ParsedResponse& r) { return r.status == 100; });
        ASSERT_TRUE(ok);
        EXPECT_EQ(normalize(rs), want) << "byte by byte: " << in;
    }
}

TEST(HttpParse, RandomSplits) {
    const std::string in = kGet + kPostChunked + kPostLength + kPostChunked + kGet;
    Engine reference;
    const std::string want = normalize(reference.responses({in}));
    std::mt19937 rng(42);
    for (int round = 0; round < 300; ++round) {
        std::vector<std::string> pieces;
        std::size_t pos = 0;
        while (pos < in.size()) {
            const std::size_t n = std::min<std::size_t>(in.size() - pos, 1 + rng() % 40);
            pieces.push_back(in.substr(pos, n));
            pos += n;
        }
        Engine e;
        ASSERT_EQ(normalize(e.responses(pieces)), want) << "round " << round;
    }
}

// One error response, then the connection closes; nothing after the bad
// request is served.
void expect_rejected(const std::string& request, int status) {
    Engine e;
    const auto rs = e.responses({request + kGet});
    ASSERT_EQ(rs.size(), 1u) << request;
    EXPECT_EQ(rs[0].status, status) << request;
    EXPECT_EQ(rs[0].header("Connection"), "close") << request;
    EXPECT_EQ(e.handled, 0) << request;
    EXPECT_TRUE(e.last.shut);
}

TEST(HttpParse, MalformedRequestLines) {
    expect_rejected("G@T / HTTP/1.1\r\n\r\n", 400);
    expect_rejected("GET /a b HTTP/1.1\r\n\r\n", 400);
    expect_rejected("GET / HTTX/1.1\r\n\r\n", 400);
    expect_rejected("GET / HTTP/1.1\nHost: h\n\n", 400);       // bare LF
    expect_rejected("GET / HTTP/1.1\r\n Host: h\r\n\r\n", 400);  // whitespace before the first field
    expect_rejected("GET / HTTP/1.1 extra\r\n\r\n", 400);
}

TEST(HttpParse, MalformedHeaders) {
    expect_rejected("GET / HTTP/1.1\r\nX: a\r\n b\r\n\r\n", 400);              // obs-fold
    expect_rejected("GET / HTTP/1.1\r\nContent-Length : 0\r\n\r\n", 400);      // space before colon
    expect_rejected("GET / HTTP/1.1\r\nBad Name: x\r\n\r\n", 400);
    expect_rejected(std::string("GET / HTTP/1.1\r\nX: a\0b\r\n\r\n", 27), 400);  // NUL in a value
    expect_rejected("GET / HTTP/1.1\r\nX: a\rb\r\n\r\n", 400);                 // bare CR
    expect_rejected("GET / HTTP/1.1\r\n: novalue\r\n\r\n", 400);
}

// Request smuggling: ambiguous framing is rejected, the "smuggled" request
// behind it is never served.
TEST(HttpParse, SmugglingDefenses) {
    expect_rejected("POST / HTTP/1.1\r\nContent-Length: 3\r\nContent-Length: 3\r\n\r\nabc", 400);
    expect_rejected("POST / HTTP/1.1\r\nContent-Length: 3\r\nContent-Length: 4\r\n\r\nabcd", 400);
    expect_rejected("POST / HTTP/1.1\r\nContent-Length: 3, 3\r\n\r\nabc", 400);
    expect_rejected("POST / HTTP/1.1\r\nContent-Length: +3\r\n\r\nabc", 400);
    expect_rejected("POST / HTTP/1.1\r\nContent-Length: -1\r\n\r\n", 400);
    expect_rejected("POST / HTTP/1.1\r\nContent-Length: 0x3\r\n\r\nabc", 400);
    expect_rejected("POST / HTTP/1.1\r\nContent-Length: 99999999999999999999999\r\n\r\n", 400);
    expect_rejected("POST / HTTP/1.1\r\nContent-Length: 6\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", 400);
    expect_rejected("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nContent-Length: 6\r\n\r\n0\r\n\r\n", 400);
    expect_rejected("POST / HTTP/1.1\r\nTransfer-Encoding: chunked, gzip\r\n\r\n0\r\n\r\n", 400);
    expect_rejected("POST / HTTP/1.1\r\nTransfer-Encoding: gzip\r\n\r\nabc", 400);
    expect_rejected("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n",
                    400);
    expect_rejected("POST / HTTP/1.1\r\nTransfer-Encoding: xchunked\r\n\r\n0\r\n\r\n", 400);
    expect_rejected("POST / HTTP/1.0\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", 400);
    expect_rejected("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n", 400);  // bad chunk size
    expect_rejected("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\nffffffffffffffffff\r\n", 400);
    expect_rejected("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nxy\r\n0\r\n\r\n", 400);  // no CRLF
    expect_rejected("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n1\nx\n0\n\n", 400);  // bare LF
    expect_rejected("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n1 \r\nx\r\n0\r\n\r\n", 400);
}

TEST(HttpParse, Versions) {
    expect_rejected("GET / HTTP/2.0\r\n\r\n", 505);
    expect_rejected("GET / HTTP/0.9\r\n\r\n", 505);
    expect_rejected("GET / HTTP/1.2\r\n\r\n", 505);
    expect_rejected("GET / HTTP/3.0\r\n\r\n", 505);
    expect_rejected("CONNECT h:443 HTTP/1.1\r\nHost: h:443\r\n\r\n", 501);
    expect_rejected("GET / HTTP/1.1\r\nExpect: something\r\n\r\n", 417);
}

TEST(HttpParse, HeaderLimits) {
    http::ServerOptions o;
    o.max_header_bytes = 1024;
    o.max_headers = 10;
    {
        Engine e(o);
        const auto rs = e.responses({"GET /" + std::string(2000, 'a') + " HTTP/1.1\r\n\r\n"});
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 414);
    }
    {
        Engine e(o);
        const auto rs = e.responses({"GET / HTTP/1.1\r\nX: " + std::string(2000, 'a') + "\r\n\r\n"});
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 431);
    }
    {
        // Exactly at the limit is fine; one byte more is not.
        const std::string base = "GET / HTTP/1.1\r\nX: ";
        const std::size_t pad = o.max_header_bytes - base.size() - 4;
        Engine ok_engine(o);
        auto rs = ok_engine.responses({base + std::string(pad, 'b') + "\r\n\r\n"});
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 200);
        Engine e(o);
        rs = e.responses({base + std::string(pad + 1, 'b') + "\r\n\r\n"});
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 431);
    }
    {
        std::string many = "GET / HTTP/1.1\r\n";
        for (int i = 0; i < 11; ++i) {
            many += "H" + std::to_string(i) + ": v\r\n";
        }
        Engine e(o);
        const auto rs = e.responses({many + "\r\n"});
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 431);
    }
    {
        // The limit also holds when the head trickles in.
        std::vector<std::string> pieces = {"GET / HTTP/1.1\r\n"};
        for (int i = 0; i < 100; ++i) {
            pieces.push_back("X-Pad: " + std::string(20, 'p') + "\r\n");
        }
        o.max_headers = 1000;
        Engine e(o);
        const auto rs = e.responses(pieces);
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 431);
    }
}

TEST(HttpParse, BodyLimits) {
    http::ServerOptions o;
    o.max_body_bytes = 100;
    {
        Engine e(o);
        // Too large by Content-Length: refused before the body is read, and
        // no 100 Continue is sent.
        const auto rs = e.responses(
            {"POST / HTTP/1.1\r\nExpect: 100-continue\r\nContent-Length: 101\r\n\r\n", std::string(101, 'x')});
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 413);
        EXPECT_EQ(e.handled, 0);
    }
    {
        Engine e(o);
        const auto rs = e.responses({"POST / HTTP/1.1\r\nContent-Length: 100\r\n\r\n" + std::string(100, 'x')});
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 200);
    }
    {
        Engine e(o);
        std::string chunked = "POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n";
        for (int i = 0; i < 11; ++i) {
            chunked += "a\r\n0123456789\r\n";
        }
        const auto rs = e.responses({chunked + "0\r\n\r\n"});
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 413);
    }
}

TEST(HttpParse, TrailerAndExtensionLimits) {
    http::ServerOptions o;
    o.max_header_bytes = 1024;
    {
        // Trailer fields beyond max_header_bytes, even in small pieces.
        std::vector<std::string> pieces = {"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nx\r\n0\r\n"};
        for (int i = 0; i < 100; ++i) {
            pieces.push_back("T" + std::to_string(i) + ": " + std::string(20, 't') + "\r\n");
        }
        pieces.push_back("\r\n");
        Engine e(o);
        const auto rs = e.responses(pieces);
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 431);
    }
    {
        // An endless chunk extension.
        std::vector<std::string> pieces = {"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n1;ext="};
        for (int i = 0; i < 100; ++i) {
            pieces.push_back(std::string(50, 'e'));
        }
        Engine e(o);
        const auto rs = e.responses(pieces);
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 400);
    }
}

TEST(HttpParse, LargeBodiesUsePooledBuffers) {
    http::ServerOptions o;
    o.read_buffer_bytes = 16 * 1024;
    o.max_body_bytes = 1024 * 1024;
    std::string payload(300 * 1024, '\0');
    for (std::size_t i = 0; i < payload.size(); ++i) {
        payload[i] = static_cast<char>('a' + i * 7 % 26);
    }
    const std::string echoed_prefix = "POST /big|/big|-||" + std::to_string(payload.size()) + "|";
    {
        // Content-Length, arriving in 1000-byte reads.
        std::string req = "POST /big HTTP/1.1\r\nContent-Length: " + std::to_string(payload.size()) + "\r\n\r\n" +
                          payload + kGet;
        std::vector<std::string> pieces;
        for (std::size_t i = 0; i < req.size(); i += 1000) {
            pieces.push_back(req.substr(i, 1000));
        }
        Engine e(o);
        const auto rs = e.responses(pieces);
        ASSERT_EQ(rs.size(), 2u);
        EXPECT_EQ(rs[0].body, echoed_prefix + payload);
        EXPECT_EQ(rs[1].status, 200);
    }
    {
        // Chunked, chunks of varying size, starting in place and moving to a
        // pooled buffer.
        std::string req = "POST /big HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n";
        std::size_t pos = 0;
        std::size_t size = 1;
        while (pos < payload.size()) {
            const std::size_t n = std::min(size, payload.size() - pos);
            char line[32];
            std::snprintf(line, sizeof line, "%zx\r\n", n);
            req += line + payload.substr(pos, n) + "\r\n";
            pos += n;
            size = size * 3 % 5000 + 1;
        }
        req += "0\r\n\r\n" + kGet;
        std::vector<std::string> pieces;
        for (std::size_t i = 0; i < req.size(); i += 777) {
            pieces.push_back(req.substr(i, 777));
        }
        Engine e(o);
        const auto rs = e.responses(pieces);
        ASSERT_EQ(rs.size(), 2u);
        EXPECT_EQ(rs[0].body, echoed_prefix + payload);
        EXPECT_EQ(rs[1].status, 200);
    }
}

TEST(HttpParse, KeepAliveRules) {
    {
        // HTTP/1.0: closes by default.
        Engine e;
        const auto rs = e.responses({"GET / HTTP/1.0\r\n\r\n" + kGet});
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].header("Connection"), "close");
        EXPECT_EQ(e.handled, 1);
    }
    {
        // HTTP/1.0 with keep-alive: kept, and said so.
        Engine e;
        const auto rs = e.responses({"GET / HTTP/1.0\r\nConnection: keep-alive\r\n\r\n" + kGet});
        ASSERT_EQ(rs.size(), 2u);
        EXPECT_EQ(rs[0].header("Connection"), "keep-alive");
        EXPECT_EQ(e.handled, 2);
    }
    {
        // Connection: close (any case, in a list): the response says so,
        // nothing after it is served.
        Engine e;
        const auto rs = e.responses({"GET / HTTP/1.1\r\nConnection: foo, CLOSE\r\n\r\n" + kGet});
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].header("Connection"), "close");
        EXPECT_EQ(e.handled, 1);
    }
    {
        // HTTP/1.1 default: keep-alive until the peer closes.
        Engine e;
        const auto rs = e.responses({kGet, kGet, kGet});
        ASSERT_EQ(rs.size(), 3u);
        EXPECT_FALSE(rs[2].has("Connection"));
        EXPECT_FALSE(e.last.shut) << "peer EOF: closed quietly";
    }
}

TEST(HttpParse, ExpectContinue) {
    Engine e;
    const auto rs = e.responses(
        {"PUT /x HTTP/1.1\r\nExpect: 100-Continue\r\nContent-Length: 5\r\n\r\n", "hello"});
    ASSERT_EQ(rs.size(), 2u);
    EXPECT_EQ(rs[0].status, 100);
    EXPECT_EQ(rs[1].status, 200);
    EXPECT_EQ(rs[1].body, "PUT /x|/x|-||5|hello");
    // Body already here: no 100.
    Engine e2;
    const auto rs2 =
        e2.responses({"PUT /x HTTP/1.1\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\nhello"});
    ASSERT_EQ(rs2.size(), 1u);
    EXPECT_EQ(rs2[0].status, 200);
}

TEST(HttpParse, IncompleteRequests) {
    {
        // EOF in the middle of a head or body: closed without a response.
        Engine e;
        EXPECT_EQ(e.run({"GET / HTTP/1.1\r\nHost:"}).output, "");
        EXPECT_EQ(e.run({"POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\nabc"}).output, "");
    }
    {
        // A read deadline in the middle of a head: 408.
        Engine e;
        ScriptedStream s = e.run({"GET / HTTP/1.1\r\nHost:"}, ETIMEDOUT);
        bool ok = false;
        const auto rs = http_test::parse_responses(s.output, ok);
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 408);
        // ... in the body: 408; while idle: nothing.
        s = e.run({"POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\nabc"}, ETIMEDOUT);
        const auto rs2 = http_test::parse_responses(s.output, ok);
        ASSERT_EQ(rs2.size(), 1u);
        EXPECT_EQ(rs2[0].status, 408);
        s = e.run({kGet}, ETIMEDOUT);
        const auto rs3 = http_test::parse_responses(s.output, ok);
        ASSERT_EQ(rs3.size(), 1u);
        EXPECT_EQ(rs3[0].status, 200);
    }
    {
        // Leading empty lines before a request are allowed (RFC 9112 2.2).
        Engine e;
        const auto rs = e.responses({"\r\n\r\n" + kGet});
        ASSERT_EQ(rs.size(), 1u);
        EXPECT_EQ(rs[0].status, 200);
    }
}

TEST(HttpParse, HeaderDeadlineOnlyForSplitHeads) {
    Engine e;
    // Fresh connection: one deadline (from accept). Heads in one read: no
    // more read deadlines except the idle one between requests.
    ScriptedStream s = e.run({kGet, kGet});
    const std::size_t whole = s.deadlines;
    s = e.run({kGet.substr(0, 10), kGet.substr(10), kGet.substr(0, 10), kGet.substr(10)});
    EXPECT_GT(s.deadlines, whole);
}

TEST(HttpParse, WriteErrorClosesConnection) {
    Engine e;
    ScriptedStream s;
    s.pieces = {kGet, kGet};
    s.write_error = EPIPE;
    http::serve_connection(s, *e.state, *e.core);
    EXPECT_TRUE(s.closed);
    EXPECT_EQ(e.handled, 1);
}

// ---- router ----------------------------------------------------------------

TEST(HttpRouter, ParamsQueryAndPrecedence) {
    http::Router router;
    std::string seen;
    ASSERT_TRUE(router.get("/flights/{id}", [&](http::Request& q, http::Response& r) noexcept {
        seen = "id=" + std::string(q.param("id"));
        r.send("ok");
    }));
    ASSERT_TRUE(router.get("/flights/new", [&](http::Request&, http::Response& r) noexcept {
        seen = "new";
        r.send("ok");
    }));
    ASSERT_TRUE(router.get("/{a}/x/{b}", [&](http::Request& q, http::Response& r) noexcept {
        seen = std::string(q.param("a")) + "," + std::string(q.param("b"));
        r.send("ok");
    }));
    ASSERT_TRUE(router.get("/q", [&](http::Request& q, http::Response& r) noexcept {
        seen.clear();
        for (const http::QueryParam& p : q.query_params()) {
            seen += std::string(p.name) + "=" + std::string(p.value) + ";";
        }
        seen += q.query("missing") ? "!" : "?";
        r.send("ok");
    }));
    ASSERT_TRUE(router.post("/flights/{id}", [&](http::Request&, http::Response& r) noexcept { r.send("p"); }));
    EXPECT_FALSE(router.get("/flights/{id}", [](http::Request&, http::Response&) noexcept {}));  // duplicate
    EXPECT_FALSE(router.get("no-slash", [](http::Request&, http::Response&) noexcept {}));
    EXPECT_FALSE(router.get("/a{b}", [](http::Request&, http::Response&) noexcept {}));
    EXPECT_FALSE(router.get("/{}", [](http::Request&, http::Response&) noexcept {}));

    Engine e({}, router.handler());
    auto rs = e.responses({"GET /flights/42 HTTP/1.1\r\n\r\n"});
    EXPECT_EQ(seen, "id=42");
    rs = e.responses({"GET /flights/new HTTP/1.1\r\n\r\n"});
    EXPECT_EQ(seen, "new");
    rs = e.responses({"GET /flights/a%2Fb%20c HTTP/1.1\r\n\r\n"});
    EXPECT_EQ(seen, "id=a/b c");
    rs = e.responses({"GET /p%41/x/z HTTP/1.1\r\n\r\n"});
    EXPECT_EQ(seen, "pA,z");
    rs = e.responses({"GET /q?a=1&b=x+y%26z&&c&d=%zz&%61=2 HTTP/1.1\r\n\r\n"});
    EXPECT_EQ(seen, "a=1;b=x y&z;c=;d=%zz;a=2;?");
    rs = e.responses({"GET http://example.com:80/flights/7?x=1 HTTP/1.1\r\n\r\n"});
    EXPECT_EQ(seen, "id=7");

    rs = e.responses({"GET /nope HTTP/1.1\r\n\r\n"});
    ASSERT_EQ(rs.size(), 1u);
    EXPECT_EQ(rs[0].status, 404);
    rs = e.responses({"GET /flights/ HTTP/1.1\r\n\r\n"});  // empty parameter
    EXPECT_EQ(rs[0].status, 404);
    rs = e.responses({"GET /flights/1/ HTTP/1.1\r\n\r\n"});  // trailing slash
    EXPECT_EQ(rs[0].status, 404);
    rs = e.responses({"DELETE /flights/1 HTTP/1.1\r\n\r\n"});
    ASSERT_EQ(rs.size(), 1u);
    EXPECT_EQ(rs[0].status, 405);
    EXPECT_EQ(rs[0].header("Allow"), "GET, HEAD, POST");
    // HEAD falls back to GET, without the body.
    bool ok = false;
    rs = e.responses({"HEAD /flights/9 HTTP/1.1\r\n\r\n"}, &ok, {true});
    ASSERT_TRUE(ok);
    ASSERT_EQ(rs.size(), 1u);
    EXPECT_EQ(rs[0].status, 200);
    EXPECT_EQ(rs[0].header("Content-Length"), "2");
    EXPECT_EQ(rs[0].body, "");
    EXPECT_EQ(e.last.output.substr(e.last.output.size() - 4), "\r\n\r\n");
}

TEST(HttpRouter, CaseInsensitiveHeadersAndCustomNotFound) {
    http::Router router;
    router.get("/h", [](http::Request& q, http::Response& r) noexcept {
        r.send(std::string(q.header("CONTENT-type")) + "|" + std::string(q.header("x-MiXeD")) + "|" +
               (q.has_header("x-empty") ? "y" : "n") + "|" + (q.has_header("absent") ? "y" : "n"));
    });
    router.not_found([](http::Request& q, http::Response& r) noexcept {
        r.send(410, "gone: " + std::string(q.path()));
    });
    Engine e({}, router.handler());
    auto rs = e.responses(
        {"GET /h HTTP/1.1\r\ncontent-TYPE: text/x  \r\nX-Mixed:\t v \t\r\nX-Empty:\r\n\r\n"});
    ASSERT_EQ(rs.size(), 1u);
    EXPECT_EQ(rs[0].body, "text/x|v|y|n");
    rs = e.responses({"GET /zz HTTP/1.1\r\n\r\n"});
    EXPECT_EQ(rs[0].status, 410);
    EXPECT_EQ(rs[0].body, "gone: /zz");
}

// ---- response API ------------------------------------------------------------

TEST(HttpResponse, HeadersStatusAndFraming) {
    http::Router router;
    router.get("/r", [](http::Request&, http::Response& r) noexcept {
        r.status(201);
        EXPECT_TRUE(r.header("X-A", "1"));
        EXPECT_FALSE(r.header("X-Bad", "a\r\nInjected: yes"));
        EXPECT_FALSE(r.header("Bad Name", "x"));
        EXPECT_FALSE(r.header("Content-Length", "5"));
        EXPECT_FALSE(r.header("Transfer-Encoding", "chunked"));
        EXPECT_EQ(r.send("body"), 0);
        EXPECT_EQ(r.send("again"), EALREADY);
        EXPECT_FALSE(r.header("X-Late", "1"));
    });
    router.get("/empty", [](http::Request&, http::Response& r) noexcept { r.status(204); });
    router.get("/nothing", [](http::Request&, http::Response&) noexcept {});
    router.get("/close", [](http::Request&, http::Response& r) noexcept {
        r.header("Connection", "close");
        r.send("bye");
    });
    Engine e({}, router.handler());
    auto rs = e.responses({"GET /r HTTP/1.1\r\n\r\nGET /empty HTTP/1.1\r\n\r\nGET /nothing HTTP/1.1\r\n\r\n"
                           "GET /close HTTP/1.1\r\n\r\nGET /r HTTP/1.1\r\n\r\n"});
    ASSERT_EQ(rs.size(), 4u);
    EXPECT_EQ(rs[0].status, 201);
    EXPECT_EQ(rs[0].header("X-A"), "1");
    EXPECT_FALSE(rs[0].has("Injected"));
    EXPECT_EQ(rs[0].body, "body");
    EXPECT_EQ(rs[1].status, 204);
    EXPECT_FALSE(rs[1].has("Content-Length"));
    EXPECT_EQ(rs[2].status, 200);
    EXPECT_EQ(rs[2].header("Content-Length"), "0");
    EXPECT_EQ(rs[3].header("Connection"), "close");
}

TEST(HttpResponse, ChunkedStreaming) {
    http::Router router;
    router.get("/s", [](http::Request&, http::Response& r) noexcept {
        r.header("Content-Type", "text/plain");
        EXPECT_EQ(r.write("x"), EINVAL);
        EXPECT_EQ(r.begin_chunked(), 0);
        for (int i = 0; i < 5; ++i) {
            EXPECT_EQ(r.write("part" + std::to_string(i) + ";"), 0);
        }
        EXPECT_EQ(r.write(std::string(70000, 'z')), 0);
        // end() is implicit
    });
    Engine e({}, router.handler());
    auto rs = e.responses({"GET /s HTTP/1.1\r\n\r\nGET /s HTTP/1.1\r\n\r\n"});
    ASSERT_EQ(rs.size(), 2u);
    for (const auto& r : rs) {
        EXPECT_TRUE(r.chunked);
        EXPECT_FALSE(r.has("Content-Length"));
        EXPECT_EQ(r.body, "part0;part1;part2;part3;part4;" + std::string(70000, 'z'));
    }
    // HTTP/1.0: no chunking, the body ends with the connection.
    Engine e10({}, router.handler());
    rs = e10.responses({"GET /s HTTP/1.0\r\nConnection: keep-alive\r\n\r\n"});
    ASSERT_EQ(rs.size(), 1u);
    EXPECT_FALSE(rs[0].chunked);
    EXPECT_EQ(rs[0].header("Connection"), "close");
    EXPECT_EQ(rs[0].body, "part0;part1;part2;part3;part4;" + std::string(70000, 'z'));
    // HEAD: head only.
    Engine eh({}, router.handler());
    bool ok = false;
    rs = eh.responses({"HEAD /s HTTP/1.1\r\n\r\n"}, &ok, {true});
    ASSERT_TRUE(ok);
    ASSERT_EQ(rs.size(), 1u);
    EXPECT_EQ(rs[0].body, "");
    EXPECT_EQ(eh.last.output.find("part"), std::string::npos);
}

TEST(HttpResponse, LargeBodyWrittenDirectly) {
    const std::string big(1 << 20, 'q');
    http::Router router;
    router.get("/big", [&](http::Request&, http::Response& r) noexcept { r.send(big); });
    Engine e({}, router.handler());
    const auto rs = e.responses({"GET /big HTTP/1.1\r\n\r\nGET /big HTTP/1.1\r\n\r\n"});
    ASSERT_EQ(rs.size(), 2u);
    EXPECT_EQ(rs[0].body, big);
    EXPECT_EQ(rs[1].body, big);
}

TEST(HttpMessage, Helpers) {
    char out[64];
    const std::string_view in = "a%20b+c%2x%41%";
    const std::size_t n = http::percent_decode(in, out, false);
    EXPECT_EQ(std::string_view(out, n), "a b+c%2xA%");
    const std::size_t m = http::percent_decode(in, out, true);
    EXPECT_EQ(std::string_view(out, m), "a b c%2xA%");
    EXPECT_TRUE(http::iequals("Content-Length", "content-length"));
    EXPECT_FALSE(http::iequals("Content-Length", "content-lengt"));
    EXPECT_TRUE(http::has_token("keep-alive, Upgrade", "upgrade"));
    EXPECT_FALSE(http::has_token("keep-alive, Upgraded", "upgrade"));
    char date[http::detail::kDateLength];
    http::detail::format_http_date(784111777, date);
    EXPECT_EQ(std::string_view(date, sizeof date), "Sun, 06 Nov 1994 08:49:37 GMT");
}

}  // namespace
