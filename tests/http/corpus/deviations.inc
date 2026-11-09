// Deviations of our server from the upstream expectation (see http_corpus_test.cpp):
// {"<source> <case name>", {expect (0 ok / 1 error / 2 incomplete), complete messages, error status, "reason"}},
//
// After "Connection: close" (or HTTP/1.0 without keep-alive) the server answers and closes: what follows is never
// parsed, so there is no error response for it (llhttp, fed the rest, reports "Data after Connection: close").
{"llhttp connection.md:84 No restart when keep-alive is off (1.0)", {0, 1, 0, "closes after a 1.0 request; the rest is not parsed"}},
{"llhttp connection.md:297 CRLF between requests, explicit `close`", {0, 1, 0, "closes after Connection: close; the rest is not parsed"}},
{"llhttp connection.md:347 CRLF between requests, explicit `close` followed by tab", {0, 1, 0, "closes after Connection: close; the rest is not parsed"}},
// No protocol upgrades: an Upgrade request is served as plain HTTP, and the bytes after it (the other protocol in
// llhttp's test) are parsed as the next HTTP request -- here garbage, so 400 and close.
{"llhttp connection.md:545 Multiple tokens with folding and LWS", {1, 1, 400, "Upgrade ignored; the WebSocket bytes are not HTTP"}},
{"llhttp connection.md:682 Setting a flag and pausing", {0, 1, 0, "Upgrade ignored; served as plain HTTP"}},
{"llhttp connection.md:716 Emitting part of body and pausing", {1, 1, 400, "Upgrade ignored; the following bytes are not HTTP"}},
{"llhttp connection.md:756 Upgrade GET request", {1, 1, 400, "Upgrade ignored; the following bytes are not HTTP"}},
{"llhttp connection.md:815 Upgrade POST request", {1, 1, 400, "Upgrade ignored; the following bytes are not HTTP"}},
// CONNECT (a tunnel) is not supported.
{"llhttp method.md:30 CONNECT request", {1, 0, 501, "CONNECT not implemented"}},
{"llhttp method.md:65 CONNECT request with CAPS", {1, 0, 501, "CONNECT not implemented"}},
{"llhttp method.md:99 CONNECT with body", {1, 0, 501, "CONNECT not implemented"}},
{"llhttp uri.md:191 Underscore in hostname", {1, 0, 501, "CONNECT not implemented"}},
// HTTP/0.9 (a request line without a version) is not supported.
{"llhttp sample.md:317 No HTTP version", {1, 0, 505, "HTTP/0.9 not supported"}},
// picohttpparser is more lenient than strict llhttp; each of these leniencies is a known request-smuggling or
// parser-discrepancy vector, so rejecting is the safer choice (RFC 9112 allows rejecting them).
{"picohttpparser test_request: parse multiline", {1, 0, 400, "obs-fold rejected (RFC 9112 5.2)"}},
{"picohttpparser test_request: accept MSB chars", {1, 0, 400, "raw bytes >= 0x80 in the request-target rejected"}},
{"picohttpparser chunked (consume_trailer=0): 6 ; comment\\r\\nhello \\r\\n5\\r\\nworld\\r\\n0\\r\\n", {1, 0, 400, "whitespace after the chunk size rejected"}},
{"picohttpparser chunked (consume_trailer=1): b\\r\\nhello world\\r\\n0\\r\\n\\n", {1, 0, 400, "bare LF line end rejected"}},
{"picohttpparser chunked (consume_trailer=1): 6\\r\\nhello \\r\\n5\\r\\nworld\\r\\n0\\r\\na: b\\nc: d\\n\\n", {1, 0, 400, "bare LF line end rejected"}},
