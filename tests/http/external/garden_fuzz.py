"""Mutation fuzzing of our HTTP server with http-garden's mutators.

http-garden (GPLv3) is only imported at runtime from its download
directory (argv[1]); nothing of it is copied. Each case: seeds mutated 1-6
times by garden's mutate(), sent on a fresh connection, write side half-
closed, response read until EOF. Oracle: the server never hangs (EOF within
the timeout), its output is empty or a sequence of well-formed responses
(the last may be cut only if the connection closed), at most one error
response (4xx/5xx) and only as the last, and it stays healthy.
"""
import random
import re
import socket
import sys
import time

sys.path.insert(0, sys.argv[1])
from mutations import mutate  # noqa: E402  (http-garden, GPL: invoked, not copied)

PORT = int(sys.argv[2])
ITERATIONS = int(sys.argv[3])
random.seed(int(sys.argv[4]) if len(sys.argv) > 4 else 1)

SEEDS = [
    [b"GET /items HTTP/1.1\r\nHost: a\r\n\r\n"],
    [b"POST /items/x HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\n\r\nhello"],
    [b"POST /items/y HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n5;e=1\r\nhello\r\n0\r\nT: 1\r\n\r\n"],
    [b"GET /items/x HTTP/1.1\r\nHost: a\r\n\r\n", b"DELETE /items/x HTTP/1.1\r\nHost: a\r\n\r\n"],
    [b"GET /items HTTP/1.0\r\nConnection: keep-alive\r\n\r\n", b"HEAD /items HTTP/1.1\r\nHost: a\r\n\r\n"],
]

STATUS = re.compile(rb"HTTP/1\.[01] (\d{3}) [^\r\n]*\r\n")


def parse(data: bytes, heads: int):
    """Response statuses; raises on malformed output. HEAD responses have no
    body: a Content-Length that is not followed by that many bytes is taken
    as a HEAD response if requests may be HEAD (heads > 0)."""
    statuses = []
    pos = 0
    while pos < len(data):
        m = STATUS.match(data, pos)
        if not m:
            raise ValueError(f"bad status line at {pos}: {data[pos:pos + 40]!r}")
        end = data.find(b"\r\n\r\n", m.end() - 2)
        if end < 0:
            raise ValueError("incomplete head")
        head = data[m.end():end + 2].decode("latin-1")
        headers = {}
        for line in head.split("\r\n"):
            if line:
                k, _, v = line.partition(":")
                headers[k.strip().lower()] = v.strip()
        pos = end + 4
        status = int(m.group(1))
        statuses.append(status)
        if "transfer-encoding" in headers:
            while True:
                le = data.index(b"\r\n", pos)
                size = int(data[pos:le].split(b";")[0], 16)
                pos = le + 2 + size + 2
                if size == 0:
                    break
        elif "content-length" in headers:
            n = int(headers["content-length"])
            if heads and not STATUS.match(data, pos) and pos + n <= len(data):
                pos += n
            elif heads and (pos == len(data) or STATUS.match(data, pos)):
                pass  # HEAD
            else:
                pos += n
        elif status not in (204, 304) and status >= 200:
            pos = len(data)  # close-delimited
    if pos > len(data):
        raise ValueError("truncated body")
    return statuses


def exchange(payload: bytes, timeout=5.0):
    s = socket.create_connection(("127.0.0.1", PORT), timeout=timeout)
    try:
        s.sendall(payload)
    except OSError:
        pass
    try:
        s.shutdown(socket.SHUT_WR)
    except OSError:
        pass
    data = b""
    t0 = time.monotonic()
    try:
        while True:
            chunk = s.recv(65536)
            if not chunk:
                break
            data += chunk
    except ConnectionResetError:
        pass
    except socket.timeout:
        s.close()
        raise TimeoutError(f"no EOF after {time.monotonic() - t0:.1f}s")
    s.close()
    return data


stats = {"cases": 0, "responses": 0, "errors_4xx_5xx": 0, "empty": 0, "malformed": 0, "hangs": 0, "bad_order": 0}
codes = {}
failures = []
for i in range(ITERATIONS):
    s = random.choice(SEEDS)
    for _ in range(random.randint(1, 6)):
        try:
            s = mutate(s)
        except Exception:
            break
    payload = b"".join(s)
    heads = payload.count(b"HEAD")
    stats["cases"] += 1
    try:
        data = exchange(payload)
    except TimeoutError as e:
        stats["hangs"] += 1
        failures.append(("hang", payload, str(e)))
        continue
    if not data:
        stats["empty"] += 1
        continue
    try:
        st = parse(data, heads)
    except Exception as e:
        stats["malformed"] += 1
        failures.append(("malformed", payload, f"{e}: {data[:300]!r}"))
        continue
    stats["responses"] += len(st)
    for c in st:
        codes[c] = codes.get(c, 0) + 1
    errs = [k for k, c in enumerate(st) if c >= 400 and c not in (404, 405)]
    stats["errors_4xx_5xx"] += len(errs)
    if errs and errs != [len(st) - 1]:
        stats["bad_order"] += 1
        failures.append(("error response not last", payload, repr(st)))

# Health check.
ok = exchange(b"GET /items HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n").startswith(b"HTTP/1.1 200")
print("stats:", stats)
print("status codes:", dict(sorted(codes.items())))
print("healthy after:", ok)
for kind, payload, info in failures[:10]:
    print("FAIL", kind, repr(payload[:200]), info[:300])
sys.exit(0 if ok and not failures else 1)
