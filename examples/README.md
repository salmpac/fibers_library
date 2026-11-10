# Examples: echo, HTTP and metrics over fibers

| binary | what |
|---|---|
| `echo_server` | echo server on fibers: `net::Listener`, an accept loop in a fiber, one fiber per connection (`read_some` -> `write_all`) |
| `echo_client` | load generator on fibers: N connections, closed loop with D requests in flight per connection, or open loop at a fixed rate with latency from the intended send time (wrk2 style, no coordinated omission); fixed-bucket log-linear histogram |
| `echo_epoll_baseline` | the reference: plain edge-triggered epoll, one thread per core, an `SO_REUSEPORT` listener per thread, no fibers |
| `tls_echo` | TLS echo server (`--server`) and client (`--client`) over `fibers::tls` (OpenSSL 3): handshake per connection, optional mTLS / ALPN, handshake and round-trip percentiles; built when `FIBERS_TLS` is on |
| `apns_send` | push notifications through APNs with `http2::ApnsClient` (fibers::http2): ES256 provider token from a `.p8` key, HTTP/2 over TLS (ALPN h2), `--count` / `--concurrency` sends multiplexed on one connection; built when `FIBERS_HTTP2` is on |

`tls_echo` quick start (the build generates a throwaway PKI in
`build/<preset>/examples/tls_certs/`; run from that directory):

```sh
tls_echo --server --port=7443 [--client-ca=tls_certs/ca.pem --require-client-cert] [--alpn=h2,http/1.1]
tls_echo --client --port=7443 --server-name=localhost --connections=64 --messages=2000 --size=64 \
         [--cert=tls_certs/client.pem --key=tls_certs/client.key] [--alpn=http/1.1]
```

## Build

Examples are off by default (`FIBERS_BUILD_EXAMPLES`); the `clang-release` and
`gcc-release` presets turn them on:

```sh
cmake --preset clang-release && cmake --build --preset clang-release -j 6 \
    --target echo_server echo_client echo_epoll_baseline
# binaries: build/clang-release/examples/
```

or with any configuration: `-DFIBERS_BUILD_EXAMPLES=ON`.

## Run

```sh
# server: 1 worker on CPU 0, its poller on CPU 2, the (idle) blocking thread on 1
echo_server --port=7000 --workers=1 --worker-cpus=0 --pollers=1 --poller-cpus=2 \
            --blocking=1 --blocking-cpus=1 [--max-connections=20000 --stack-kb=64]

# reference server on the same core(s)
echo_epoll_baseline --port=7000 --threads=1 --cpus=0

# closed loop: 100 connections x 16 requests in flight, 64-byte messages
echo_client --port=7000 --connections=100 --pipeline=16 --size=64 --duration=6 --warmup=2 \
            --workers=9 --worker-cpus=4-7,9,12-15 --pollers=1 --poller-cpus=8 --idle-spins=64

# open loop: 100k requests/s in total over 1000 connections
echo_client --port=7000 --connections=1000 --rate=100000 --duration=6 --warmup=2 ...
```

CPU lists use the Linux cpulist syntax (`0,2,4-6`); thread i of a pool goes to
`cpus[i % n]` (`sync::CpuList`, `ThreadPoolOptions::cpus`,
`PollerPoolOptions::cpus`, `BlockingPoolOptions::cpus`). The client retries
`ECONNREFUSED` (20 ms apart, `--connect-retries`), so it can be started right
after the server. It prints a `RESULT ...` line (rps, p50/p90/p99/p99.9/max in
microseconds, its own CPU time in cores). The servers stop on SIGINT / SIGTERM
(the fiber server closes the listener and every connection, waits for the
fibers, stops the scheduler).

`--idle-spins=N` sets `ThreadPoolOptions::idle_spins_before_park`. An idle
worker spins (searching for work) for that many pauses before it parks; the
default (8192) keeps idle workers busy for tens of microseconds, so a
lightly loaded process shows ~100% CPU on every worker. The client in the
measurements below uses 64, so its CPU usage is work, not spinning.

## Measurements

i5-14400F (6 P-cores with HT, CPUs 0-11, of which 10-11 are isolated and
unused; 4 E-cores, CPUs 12-15), Linux 7.2, clang-release, loopback TCP,
64-byte messages, 2 s warm-up + 6 s measured per point, an otherwise idle
machine (other processes used median 0.2, at most 0.8 cores during the
windows). The server runs on distinct physical P-cores within 0-9, and the
HT siblings of the server's cores stay idle. The client gets the rest of
0-9 plus the E-cores: one client worker per CPU, its poller on CPU 8,
`--idle-spins=64`. Placement was checked by sampling `/proc/<pid>/task/*`
(the last CPU each thread ran on, plus its affinity) during every run.

Loopback TCP runs the receive path on the SENDER's CPU: the client's sends
cost server-side work on client CPUs and vice versa. Absolute numbers are
therefore kernel-dominated. What matters is fiber vs epoll on the same cores.

Cells: **requests/s**, then p50 / p99 / p99.9 latency in µs (ms where noted).
Saturation marks:

- ᶜ: some client CPU was ≥ 95 % busy. The point is client-bound, or bound by
  both sides, so its rps is a lower bound for the server.
- ˢ: some server CPU was ≥ 95 % busy. On the fiber server this mark is
  mostly *not* a bottleneck. An idle `ThreadPool` worker spins for
  `idle_spins_before_park` (8192 pauses by default) before it parks, so a
  fiber worker shows ~100 % even at 1 connection. The epoll threads block in
  `epoll_wait`, so for them ˢ means really busy.

The last column is the client's CPU: busy cores summed over its CPUs, and
the busiest CPU in %, for each of the three servers.

#### Group A

fiber: worker 0, poller 2; epoll 1t: 0; epoll 2t: 0,2; client: workers 4-7,9,12-15, poller 8 (the fiber server's idle blocking thread: CPU 1).

| conns x depth | fiber 1w+1p | epoll 1t | epoll 2t | client CPU (cores busy / max CPU %) |
|---|---|---|---|---|
| 1 x 1 | **55k**ˢ 14.2 / 26.9 / 283 | **84k** 10.6 / 17.2 / 154 | **87k** 10.6 / 16.6 / 110 | 0.9 / 11% ; 1.5 / 23% ; 1.5 / 23% |
| 1 x 16 | **314k**ˢ 43.5 / 80.9 / 87.0 | **332k** 48.6 / 59.9 / 72.7 | **271k** 47.6 / 85.0 / 91.1 | 1.3 / 23% ; 1.4 / 90% ; 1.3 / 32% |
| 100 x 1 | **335k**ˢ 291 / 373 / 778 | **368k**ˢ 266 / 356 / 406 | **713k**ˢ 141 / 203 / 324 | 4.8 / 50% ; 4.9 / 51% ; 6.5 / 68% |
| 100 x 16 | **2457k**ᶜˢ 614 / 2.0ms / 4.7ms | **2716k**ᶜˢ 582 / 942 / 2.5ms | **2678k**ᶜˢ 63.0 / 8.2ms / 21ms | 9.0 / 98% ; 9.1 / 99% ; 9.3 / 100% |
| 1000 x 1 | **305k**ˢ 3.3ms / 4.1ms / 4.9ms | **339k**ˢ 2.9ms / 4.1ms / 6.0ms | **685k**ˢ 1.4ms / 2.0ms / 2.6ms | 4.7 / 49% ; 4.8 / 51% ; 6.6 / 68% |
| 1000 x 16 | **2010k**ᶜˢ 7.9ms / 10ms / 12ms | **1887k**ˢ 8.5ms / 9.8ms / 11ms | **2620k**ᶜˢ 60.9 / 141ms / 323ms | 8.7 / 95% ; 7.6 / 83% ; 9.3 / 100% |
| 10000 x 1 | **228k** 12ms / 81ms / 83ms | **250k** 40ms / 58ms / 64ms | **480k** 21ms / 25ms / 27ms | 4.2 / 45% ; 4.0 / 42% ; 5.5 / 57% |
| 10000 x 16 | **2005k**ˢ 121ms / 161ms / 224ms | **2242k** 61ms / 182ms / 203ms | **2855k** 10ms / 432ms / 499ms | 8.6 / 92% ; 8.7 / 94% ; 8.8 / 95% |

#### Group B

fiber: workers 0,4, poller 2; epoll 2t: 0,4; epoll 3t: 0,2,4; client: workers 6,7,9,12-15, poller 8 (the fiber server's idle blocking thread: CPU 1).

| conns x depth | fiber 2w+1p | epoll 2t | epoll 3t | client CPU (cores busy / max CPU %) |
|---|---|---|---|---|
| 1 x 1 | **55k**ˢ 17.2 / 25.3 / 170 | **85k** 10.9 / 15.7 / 95.2 | **81k** 11.1 / 18.7 / 154 | 0.8 / 13% ; 1.4 / 23% ; 1.3 / 21% |
| 1 x 16 | **279k**ˢ 48.6 / 80.9 / 99.3 | **297k** 49.7 / 85.0 / 93.2 | **309k** 45.6 / 82.9 / 93.2 | 1.3 / 24% ; 1.3 / 35% ; 1.3 / 34% |
| 100 x 1 | **636k**ˢ 154 / 315 / 446 | **739k**ˢ 137 / 195 / 219 | **1032k**ᶜˢ 97.3 / 145 / 223 | 5.8 / 78% ; 6.3 / 85% ; 7.4 / 97% |
| 100 x 16 | **1831k**ᶜˢ 65.0 / 7.1ms / 191ms | **1914k**ᶜˢ 60.9 / 16ms / 149ms | **1810k**ᶜˢ 63.0 / 82.9 / 281ms | 7.3 / 100% ; 7.4 / 100% ; 7.4 / 100% |
| 1000 x 1 | **604k**ˢ 1.3ms / 4.5ms / 5.6ms | **698k**ˢ 1.4ms / 2.0ms / 2.3ms | **1036k**ᶜˢ 926 / 1.5ms / 2.6ms | 5.9 / 80% ; 6.3 / 84% ; 7.6 / 99% |
| 1000 x 16 | **1831k**ᶜˢ 66.6 / 306ms / 914ms | **1927k**ᶜˢ 60.9 / 237ms / 1091ms | **1805k**ᶜˢ 63.0 / 82.9 / 2919ms | 7.3 / 100% ; 7.4 / 100% ; 7.4 / 100% |
| 10000 x 1 | **477k** 13ms / 41ms / 42ms | **494k** 20ms / 23ms / 24ms | **733k** 14ms / 16ms / 18ms | 5.1 / 70% ; 5.1 / 66% ; 6.4 / 82% |
| 10000 x 16 | **1875k** 66.6 / 1393ms / 2131ms | **1984k** 62.0 / 2315ms / 2651ms | **1789k** 64.0 / 3389ms / 5973ms | 6.9 / 94% ; 6.9 / 93% ; 6.9 / 93% |

#### Group C

fiber: workers 0,4,6, poller 2; epoll 3t: 0,4,6; epoll 4t: 0,2,4,6; client: workers 9,12-15, poller 8 (the fiber server's idle blocking thread: CPU 1).

| conns x depth | fiber 3w+1p | epoll 3t | epoll 4t | client CPU (cores busy / max CPU %) |
|---|---|---|---|---|
| 1 x 1 | **55k**ˢ 16.0 / 28.4 / 203 | **81k** 12.2 / 17.2 / 70.7 | **82k** 11.6 / 15.7 / 63.0 | 0.9 / 18% ; 1.2 / 23% ; 1.2 / 24% |
| 1 x 16 | **222k**ˢ 72.7 / 82.9 / 145 | **226k** 78.8 / 87.0 / 95.2 | **220k** 78.8 / 85.0 / 91.1 | 1.3 / 32% ; 1.3 / 36% ; 1.2 / 32% |
| 100 x 1 | **944k**ᶜˢ 103 / 215 / 299 | **1032k**ᶜˢ 91.1 / 227 / 274 | **1034k**ᶜ 89.1 / 240 / 291 | 5.5 / 100% ; 5.6 / 100% ; 5.6 / 100% |
| 100 x 16 | **1195k**ᶜˢ 70.7 / 80.9 / 348ms | **1181k**ᶜ 68.6 / 85.0 / 91.1 | **1121k**ᶜ 72.7 / 89.1 / 93.2 | 5.3 / 100% ; 5.4 / 100% ; 5.4 / 100% |
| 1000 x 1 | **935k**ᶜˢ 199 / 3.6ms / 4.4ms | **1036k**ᶜˢ 178 / 5.6ms / 5.8ms | **1009k**ᶜ 158 / 5.7ms / 6.0ms | 5.6 / 100% ; 5.6 / 100% ; 5.6 / 100% |
| 1000 x 16 | **1193k**ᶜˢ 70.7 / 78.8 / 82.9 | **1149k**ᶜ 70.7 / 89.1 / 91.1 | **1108k**ᶜ 76.8 / 91.1 / 93.2 | 5.3 / 100% ; 5.4 / 100% ; 5.4 / 100% |
| 10000 x 1 | **747k** 8.8ms / 72ms / 81ms | **759k** 13ms / 18ms / 19ms | **932k** 120 / 127ms / 127ms | 5.3 / 94% ; 5.3 / 93% ; 5.2 / 94% |
| 10000 x 16 | **1194k**ᶜˢ 70.7 / 78.8 / 82.9 | **1142k**ᶜ 72.7 / 87.0 / 89.1 | **1122k**ᶜ 74.8 / 87.0 / 91.1 | 5.1 / 96% ; 5.2 / 97% ; 5.2 / 96% |

#### Open loop (group A placement): latency from the intended send time

| conns | target | fiber 1w+1p | epoll 1t | epoll 2t | client CPU (cores busy / max %) |
|---|---|---|---|---|---|
| 100 | 50k/s | 50kˢ 23.3 / 795 / 2.8ms / 4.8ms | 50k 16.6 / 133 / 2.0ms / 2.9ms | 50k 16.6 / 166 / 2.4ms / 4.8ms | 1.0 / 16% ; 1.2 / 27% ; 1.4 / 24% |
| 100 | 100k/s | 100kˢ 20.7 / 876 / 4.3ms / 8.9ms | 100k 16.6 / 42.5 / 2.5ms / 4.1ms | 100k 16.6 / 219 / 2.3ms / 3.1ms | 2.9 / 82% ; 1.8 / 24% ; 2.5 / 34% |
| 100 | 200k/s | 200kˢ 21.2 / 647 / 2.7ms / 4.1ms | 200k 15.7 / 25.9 / 680 / 1.8ms | 200k 16.0 / 195 / 2.5ms / 4.6ms | 4.4 / 72% ; 4.0 / 43% ; 4.0 / 44% |
| 1000 | 50k/s | 50kˢ 24.3 / 909 / 2.7ms / 3.6ms | 50k 17.7 / 348 / 2.3ms / 3.7ms | 50k 17.7 / 332 / 2.1ms / 4.2ms | 0.8 / 20% ; 1.2 / 16% ; 1.1 / 15% |
| 1000 | 100k/s | 100kˢ 22.3 / 141 / 2.3ms / 3.5ms | 100k 17.2 / 26.9 / 2.5ms / 3.7ms | 100k 17.2 / 28.9 / 1.8ms / 3.0ms | 2.8 / 32% ; 2.1 / 52% ; 2.7 / 34% |
| 1000 | 200k/s | 200kˢ 22.3 / 128 / 2.7ms / 5.6ms | 200k 16.3 / 25.3 / 2.4ms / 3.7ms | 200k 16.6 / 68.6 / 2.3ms / 3.5ms | 4.2 / 45% ; 4.1 / 45% ; 4.4 / 73% |


### Reading the numbers

- **Per-request cost, unsaturated (1 connection, and N x 1 in group A).**
  The fiber server needs one worker core *plus* a poller core to match one
  epoll thread. 100 x 1 gives 335k vs 368k rps, 1000 x 1 gives 305k vs
  339k, and epoll on both cores (2t) does about twice that. Every request
  costs the fiber server two thread hand-offs: the poller's `epoll_wait`
  reports readiness, the fiber's Task goes through the `ThreadPool`
  mail_box to the worker, and the worker resumes the fiber. The epoll loop
  reads and writes in place. With one connection that hand-off shows as
  latency: p50 14 µs vs 10.6 µs, and 55k vs 84k rps.
- **Scaling with workers (groups B, C).** Two fiber workers + poller reach
  636k at 100 x 1 against 739k for epoll on the two worker cores (1032k on
  all three). With three workers (group C) most points are client-bound
  (ᶜ). There the fiber server matches epoll at 3 threads: 944k vs 1032k at
  100 x 1, and 1.19M vs 1.18M at 100 x 16.
- **Pipelined points (x 16)** are client-bound or both-bound everywhere
  (ᶜ). They show that the fiber server keeps up with what this client can
  generate (2.0-2.5M rps on 2 cores) but do not rank the servers.
- **Many connections (10000 x 1).** Throughput is close (228k vs 250k
  single core, 747k vs 759k three cores) but the fiber server's tail is
  much longer: p99 81 ms vs 58 ms, and 72 ms vs 18 ms in group C, while
  its p50 is lower. Readiness for 10k connections arrives in bursts and
  is handed to the worker in poller batches. Order then depends on the
  ThreadPool queues (mail_box shards, overflow_box), not on arrival order,
  so some connections wait several rounds. The epoll loop serves them in
  `epoll_wait` order.
- **Open loop (latency at a fixed rate, no coordinated omission).** At
  50k-200k rps the fiber server's p50 is 20-24 µs vs 16-17 µs for epoll,
  and its p99 is 0.13-0.9 ms vs 0.03-0.35 ms. The extra p99 comes from
  waking a parked worker (futex) and a sleeping poller (eventfd) when
  requests arrive spread out, which a busy-polling epoll thread does not
  pay. p99.9 and max are similar (2-5 ms; scheduler noise of the loopback
  path on both).

# HTTP over fibers (`fibers::http`, `FIBERS_HTTP=ON`)

| binary | what |
|---|---|
| `http_hello` | `GET /hello` -> `Hello, World!` (keep-alive; the benchmark target) |
| `http_rest` | a tiny REST service: `GET /items`, `GET / POST / DELETE /items/{id}` on an in-memory map behind a `fiber::Mutex` |
| `http_bench.py` | benchmark driver: starts the server, runs wrk (closed loop) and wrk2 (fixed rate), accounts client / server CPU from `/proc/stat` |

Both servers take echo_server's placement flags (`--workers --worker-cpus
--pollers --poller-cpus --blocking --blocking-cpus --idle-spins
--max-connections --stack-kb`, `sync::CpuList` syntax) plus `--port`,
`--host`, `--grace-ms`, and stop gracefully on SIGINT / SIGTERM
(`Server::stop(grace)`: idle keep-alive connections close at once,
requests in flight finish with `Connection: close`).

```sh
cmake --preset clang-release && cmake --build --preset clang-release -j 6 --target http_hello http_rest
build/clang-release/examples/http_rest --port=8080 --workers=2 --worker-cpus=0,4 --pollers=1 --poller-cpus=2
curl -X POST --data 'hello' localhost:8080/items/a; curl localhost:8080/items/a; curl -X DELETE localhost:8080/items/a

# benchmark (wrk / wrk2 are in the dev shell)
examples/http_bench.py --server build/clang-release/examples/http_hello \
    --server-args="--workers=1 --worker-cpus=0 --pollers=1 --poller-cpus=2 --blocking=1 --blocking-cpus=1" \
    --server-cpus=0,2 --client-cpus=6-9,12-15 --connections=1,100,1000 \
    --rates=100:50000,1000:150000
```

## Measurements (indicative)

Same machine as above (i5-14400F, Linux 7.2, clang-release, loopback),
keep-alive `GET /hello` (14-byte body, ~120-byte response), 2 s warm-up +
10 s measured per point. **Indicative**: the machine was shared with two
other build/test agents; their load was light while measuring (load average
0.9-2.4 before the runs, most of it our own spinning worker, poller and wrk
threads) but not zero. Server on distinct physical P-cores, HT siblings
idle: A = 1 worker on CPU 0 + poller on CPU 2; B = 2 workers on CPUs 0, 4 +
poller on CPU 2 (blocking thread on the idle sibling 1). wrk / wrk2: one
thread per CPU on 6-9 and E-cores 12-15 (never 10-11).

Client CPU (`client`) is the busy time of the client CPUs summed, then the
busiest of them, measured over the run (it includes loopback kernel work);
`wrk` is wrk's own CPU (rusage). No client CPU passed 76 %, so no point is
client-bound. Server CPU: the fiber worker spins while idle (default
`idle_spins_before_park`), so it reads ~100 % even at low load.

**Closed loop (wrk)**, latency from wrk:

| conns | A: 1 worker + poller | B: 2 workers + poller | client cores / max CPU (A ; B) |
|---|---|---|---|
| 1 | **74k** rps, p50 13 µs, p99 24 µs | **53k** rps, p50 16 µs, p99 2.5 ms | 0.6 / 19 % ; 2.9 / 40 % |
| 100 | **303k** rps, p50 310 µs, p99 429 µs | **611k** rps, p50 155 µs, p99 318 µs | 2.9 / 47 % ; 4.5 / 68 % |
| 1000 | **261k** rps, p50 3.8 ms, p99 4.6 ms | **588k** rps, p50 1.8 ms, p99 4.0 ms | 4.0 / 76 % ; 4.6 / 67 % |

**Fixed rate (wrk2)**: corrected latency (from the intended send time) and,
in brackets, uncorrected (service) latency, p50 / p99 / p99.9. wrk2 sends
on a millisecond timer, so its corrected numbers have a floor of ~0.6-1 ms
even at 1 % load (at 1 connection x 1000 rps: corrected p50 623 µs,
uncorrected 12 µs); the uncorrected column shows the server.

| server | conns | rate | achieved | corrected p50 / p99 / p99.9 | (uncorrected p50 / p99 / p99.9) | client cores / max CPU |
|---|---|---|---|---|---|---|
| A | 100 | 50k | 50k | 642 µs / 1.2 ms / 2.7 ms | (22 µs / 88 µs / 1.9 ms) | 0.5 / 10 % |
| A | 100 | 150k | 150k | 940 µs / 2.2 ms / 3.7 ms | (49 µs / 297 µs / 1.8 ms) | 1.4 / 21 % |
| A | 100 | 250k | 249k | 1.1 ms / 2.9 ms / 6.1 ms | (187 µs / 323 µs / 523 µs) | 2.0 / 31 % |
| A | 1000 | 50k | 48k | 845 µs / 1.9 ms / 3.7 ms | (63 µs / 261 µs / 2.5 ms) | 0.7 / 12 % |
| A | 1000 | 150k | 145k | 880 µs / 2.0 ms / 4.7 ms | (70 µs / 610 µs / 2.9 ms) | 1.7 / 28 % |
| A | 1000 | 250k | 242k | 1.1 ms / 2.6 ms / 4.8 ms | (176 µs / 1.5 ms / 3.7 ms) | 2.6 / 37 % |
| B | 100 | 100k | 100k | 763 µs / 1.4 ms / 3.0 ms | (32 µs / 133 µs / 2.0 ms) | 0.9 / 15 % |
| B | 100 | 300k | 299k | 1.0 ms / 2.2 ms / 3.8 ms | (49 µs / 206 µs / 327 µs) | 2.2 / 32 % |
| B | 100 | 450k | 449k | 1.6 ms / 151 ms / 171 ms | (136 µs / 375 µs / 1.4 ms) | 4.5 / 62 % |
| B | 1000 | 100k | 97k | 940 µs / 2.7 ms / 4.2 ms | (121 µs / 1.7 ms / 2.8 ms) | 1.6 / 31 % |
| B | 1000 | 300k | 291k | 1.1 ms / 2.5 ms / 4.0 ms | (160 µs / 1.1 ms / 2.7 ms) | 3.4 / 55 % |
| B | 1000 | 450k | 432k | 1.2 ms / 3.5 ms / 7.3 ms | (276 µs / 1.8 ms / 4.2 ms) | 3.8 / 54 % |

Reading them:

- One worker + poller serves ~300k keep-alive requests/s, two workers
  ~600k: the HTTP layer (llhttp parse, routing, response head into the
  write buffer, one `send`) scales with workers like the echo server does
  and costs little on top of the fiber / netpoller round trip (echo
  group A: 335k rps at 100 x 1).
- At 1 connection B is slower than A (53k vs 74k, p99 2.5 ms): the only
  fiber hops between two workers and the second one parks and wakes; with
  one connection a single worker is the better placement.
- 1000 connections cost ~15 % throughput on one worker vs 100 (fiber /
  socket working set); on two workers the difference is within noise.
- Fixed rate up to ~80 % of the closed-loop maximum keeps the service
  latency (uncorrected) at p99 0.1-0.6 ms on 100 connections. The B, 100,
  450k point had a ~150 ms stall in its corrected tail (p90 99 ms) with a
  normal uncorrected histogram, i.e. wrk2 fell behind its schedule for a
  moment (shared machine); treat that row as an outlier.

## Conformance checks

Besides the unit tests (`tests/http`, all presets) the parser path was
checked against outside test material and tools:

- **llhttp's and picohttpparser's own test suites** (MIT;
  `tests/http/corpus`, attribution in `THIRD_PARTY_NOTICES`): 185 request
  cases replayed through the server's connection loop, whole, split at every
  byte and byte by byte (`HttpCorpus.*`). 166 behave exactly as upstream
  expects, 19 are documented deviations (`corpus/deviations.inc`: no
  protocol upgrades, CONNECT 501, no HTTP/0.9, close instead of an error
  after `Connection: close`, and picohttpparser leniencies we reject:
  obs-fold, raw non-ASCII targets, whitespace after a chunk size, bare LF).
- **smuggler** (defparam/smuggler, MIT; `tests/http/external/run_smuggler.sh`
  against `http_rest`): all 1768 CL.TE / TE.CL mutations of its default,
  doubles and exhaustive configs, POST and GET: OK, no desync signal; 400
  for every ambiguous framing, 2xx only where the "TE" is not a
  Transfer-Encoding header at all (`Transfer_Encoding`, `X: X\tTransfer-Encoding`).
- **http-garden** (GPLv3; only invoked, never copied): its mutators,
  imported at runtime by `tests/http/external/garden_fuzz.py`, produced
  2 x 20000 mutated request streams against an ASan + UBSan `http_rest`:
  no crash, hang, malformed response stream or error response followed by
  more responses. (Its full differential harness needs docker images of
  every target server and was not run.)
- **REDbot** (MIT) on `http_hello` and `http_rest` responses (200, 404):
  no protocol findings (Date correct, Content-Length correct; the only
  notes are about caching, as the examples send no Cache-Control).

# Metrics (`fibers::metrics`, Prometheus)

| binary | what |
|---|---|
| `metrics_server` | `GET /hello`, `GET / POST / DELETE /items/{id}` (in-memory map) instrumented per route, plus `GET /metrics` |

Same flags as the other HTTP servers. Each route's handler is wrapped once
at startup: the wrapper holds the route's instruments (one counter per
status class, one latency histogram), so a request costs a few relaxed
atomics and no lookup. The scheduler's own collectors
(`metrics::register_scheduler`) are registered through
`run_http_server`'s setup hook, once the scheduler exists.

```sh
cmake --preset clang-release && cmake --build --preset clang-release -j 6 --target metrics_server
build/clang-release/examples/metrics_server --port=8080 --workers=2
curl -X POST --data hello localhost:8080/items/a; curl localhost:8080/items/b
curl -s localhost:8080/metrics | promtool check metrics   # promtool is in the dev shell
```

```
# (excerpt, after POST + GET + DELETE /items/a and GET /items/zzz)
app_http_requests_total{route="/items/{id}",code="2xx"} 3
app_http_requests_total{route="/items/{id}",code="4xx"} 1
app_http_request_duration_seconds_bucket{route="/items/{id}",le="0.0001"} 4
...
app_http_requests_in_flight 0
fibers_spawned_total 7
fibers_workers_parked 1
fibers_poller_sleeps_total{poller="0"} 29
fibers_stacks_mapped 7
...
```

The API in short (`include/fibers/metrics/`):

```cpp
metrics::Registry registry;                     // RegistryOptions{.shards}: rows per counter
auto sent = registry.counter_family("sms_sent_total", "SMS sent.", {"channel", "status"});
metrics::Counter sms_ok = sent.with({"sms", "ok"});    // once; keep the handle
metrics::Gauge queue = registry.gauge("sms_queue_depth", "Queued SMS.");
metrics::Histogram latency = registry.histogram(
    "sms_send_duration_seconds", "Send latency.", metrics::kLatencyBucketsUs, 1e6);  // observe() in µs

sms_ok.inc();  queue.set(n);  latency.observe(elapsed_us);  // any thread or fiber

registry.add_collector(fn, context);            // values that live elsewhere, read at scrape time
metrics::register_scheduler(registry, scheduler);
router.get("/metrics", metrics::http_handler(registry));
```

- **Hot path.** `Counter::inc` / `add`: one call of a `[[gnu::noinline]]`
  thread-local accessor (fibers migrate, so the shard is looked up per
  update, never cached across a park) and one relaxed `fetch_add` on the
  calling thread's row; rows of different shards never share a cache line.
  `Histogram::observe`: bucket search (linear up to 8 bounds, binary
  beyond) and two relaxed `fetch_add`s on the thread's row (bucket, sum).
  `Gauge`: one cell (`set` = store, `add` = shared `fetch_add`). No
  allocation, lock, hashing or label lookup on any update.
- **Scrapes** sum the rows (relaxed loads) and take no lock: they run
  concurrently with updates, other scrapes and registration (append-only
  lists published with release / acquire). Totals are exact once the
  updaters happen-before the scrape; a running scrape is a snapshot in
  which counters never go down and a histogram's `_count` equals its
  `+Inf` bucket.
- **The endpoint** renders into 4 KiB on the handler's fiber stack: a small
  exposition goes out with `Content-Length`, a larger one as HTTP chunks.
- **Format**: text exposition 0.0.4 (HELP / TYPE, escaping of help text
  and label values, `_bucket{le}` / `_sum` / `_count`, `+Inf`), checked
  by golden tests and by `promtool check metrics` (ctest
  `metrics_promtool_check`, when promtool is on PATH).
- **Built-in collectors** (opt-in): `fibers_workers`, `_workers_parked`,
  `_worker_parks_total`, `_worker_queued_tasks{worker}`, `_pollers`,
  `_poller_{sleeps,timer_wakeups,timers_fired,timers_cancelled,timer_heap_compactions}_total{poller}`,
  `_blocking_threads`, `_blocking_threads_sleeping`,
  `_blocking_sleeps_total`, `_stacks_mapped`, `_stacks_max`,
  `_stack_size_bytes`, `_spawned_total`, `_finished_total`, `_alive`
  (prefix configurable). The fiber counts are kept by the Scheduler: a
  per-worker cache line written only by its worker (a relaxed load + store,
  no RMW) plus one shared slot (`fetch_add`) for spawns from other threads.

Micro-benchmark (`benchmarks/metrics_bench.cpp`, clang-release, ns per
operation per thread, threads on distinct physical cores; indicative, the
machine was busy):

| | 1 thread | 4 threads |
|---|---|---|
| shared `std::atomic` `fetch_add` | 4.5 | 35 |
| per-thread padded atomic (the floor) | 4.3 | 4.0 |
| `Counter::inc` (16 shards) | 5.9 | 6.0 |
| `Counter::inc` (1 shard) | 4.4 | 39 |
| `Gauge::add` | 4.1 | 37 |
| `Histogram::observe` (16 buckets) | 10.4 | 12.9 |

A scrape of 100 counters and 10 histograms (16 shards) takes about 17 µs.

# Synchronization primitives (`examples/sync/`)

Small, heavily commented programs, one per primitive of `fibers/fiber/`.
Each runs fibers on a `Scheduler`, prints what happened, joins everything,
stops the scheduler and exits 0 (1 if a check failed), in a few tens of
milliseconds. `main` is a plain thread: it spawns fibers with
`WaitGroup::go(scheduler, f)` / `fiber::go(scheduler, f)` and waits for them
with `WaitGroup::wait()`, `Event::wait()`, `Oneshot::get()` /
`Future::get()` or `Semaphore::acquire()`, which park a fiber but block a
plain thread (never call them from a plain callback running on a worker:
that blocks the worker). Only `promise_future` keeps a root fiber, to show
a fiber consumer parking. They are built with the other examples and, when the
tests are configured too, registered as ctest smoke tests
(`example.sync.*`, label `examples`).

```sh
cmake --preset clang-release && cmake --build --preset clang-release -j 6 \
    --target mutex_counter wait_group_fanout promise_future semaphore_pool event_broadcast context_deadline
build/clang-release/examples/mutex_counter --workers=4 --fibers=100 --increments=1000
ctest --preset clang-release -L examples     # or: ctest --test-dir build/clang-release -R example.sync
```

- **`mutex_counter`** (`fiber::Mutex`, flags `--workers --fibers
  --increments --yield-every`). N fibers x M increments of shared state
  under `std::lock_guard`, yielding inside the critical section now and then
  while an invariant is checked on every entry. Then a fiber sleeps 20 ms
  holding the lock while 2 x workers contenders wait for it and a ticker
  fiber keeps running: contenders park, workers stay free. The header
  explains why a `std::mutex` must never be held across a park, sleep or
  I/O.
- **`wait_group_fanout`** (`fiber::WaitGroup`, `--workers --fibers
  --numbers --serve-ms`). Fan-out / fan-in: `wg.go(scheduler, ...)` N
  fibers that sum slices into plain slots, `wg.wait()` on main, combine.
  Then graceful shutdown of a toy server: the acceptor, every request and
  every request's child are in one group; shutdown stops the acceptor and
  calls `wait()` (on main) while requests are in flight and still adding
  children (legal: they hold a count).
- **`promise_future`** (`fiber::Oneshot`, `make_promise_future`,
  `--workers`). Request / response with `fiber::Result<T>`: (a) a
  `Oneshot<T>` on the consumer's stack (zero allocation; it must outlive the
  producer), (b) a pooled `Promise` / `Future` with `get()`, and with
  `try_get()` polling followed by `detach()` (the late `set()` is harmless),
  (c) producers on a plain `std::thread` and inside
  `this_fiber::blocking(...)`, (d) a broken promise (dropped unset, or
  `abandon()`) as `Error::kBroken`.
- **`semaphore_pool`** (`fiber::Semaphore`, `SemaphoreGuard`, `--workers
  --connections --clients --queries`). K "connections" shared by many
  client fibers (never more than K in use), fail-fast probes with
  `std::try_to_lock` while the pool is saturated, and the FIFO hand-off
  with mixed sizes: a 3-permit request at the head is not overtaken by the
  1-permit requests behind it. The queue forms deterministically (main
  spawns the next waiter only once `waiters_approx()` counts the previous
  one) and the order is checked: when a waiter is granted, no earlier
  arrival may still be queued (exit 1 otherwise).
- **`event_broadcast`** (`fiber::Event`, `--workers --runners`). N fibers
  park on one event; main (a plain thread) fires it with `set()`; a second
  `set()` is a no-op and a late `wait()` returns at once.
- **`context_deadline`** (`fibers::Context`, `--workers --fast --slow
  --deadline-ms`). A request handler with a deadline fans out (`wg.go`,
  which inherits the context) to backends over loopback sockets: the fast
  calls answer, the slow ones never do, and a CPU-bound fiber loops on
  `this_fiber::cancelled()`. At the deadline the slow reads return
  `ETIMEDOUT`, the loop stops, `wg.wait()` (a join, not interrupted) returns
  and the handler returns right after the deadline. Part 2: the same under
  `Context::cancellable()`, cancelled by main ("the client went away"):
  `ECANCELED`. Part 3: a per-step budget with `Context::child_of(ctx, t)` +
  `fiber::ContextScope`; the request lives on. See
  [docs/cancellation.md](../docs/cancellation.md) for what is cancellable.

## Request contexts: cancellation and deadlines

`fibers::Context` gives a tree of fibers one deadline / one cancel switch,
cooperatively (nothing is killed, nothing throws):

```cpp
fibers::Context ctx = fibers::Context::with_timeout(sched, 200ms);  // or cancellable(), child_of(parent, t)
fiber::go(sched, ctx, [&]() noexcept {                // runs under ctx; go(f) / wg.go(f) inherit it
    net::IoResult r = conn.read_some(buf, n);         // ETIMEDOUT at the deadline, ECANCELED on cancel()
    if (fiber::this_fiber::cancelled()) return;       // the cooperative check for CPU loops
});
ctx.cancel();                                         // any thread
```

| wait | under an ended context |
|---|---|
| socket read / write / accept / connect, TLS, `net::Connection`, `BufferedReader`, HTTP/1.1 and HTTP/2 client calls | `ECANCELED` / `ETIMEDOUT`; effective deadline = min(socket deadline, context deadline) |
| `net::ConnectionPool::acquire` | `ECANCELED` / `ETIMEDOUT` |
| `Mutex`, `Event`, `WaitGroup`, `Oneshot` / `Future`, `Semaphore`, `sleep_for`, `blocking` / file ops, `resolve` | not interrupted: poll `this_fiber::cancelled()`, or wait for fibers whose network calls are interrupted |

An operation that does not have to wait (data already buffered, an idle
pooled connection) still succeeds. One poller timer per context with a
deadline (none per wait). The context must outlive every fiber under it:
keep it on the handler's stack and join the children before returning
(destroying it while a fiber is parked under it fails fast). Design and
memory orders: [docs/cancellation.md](../docs/cancellation.md).

| primitive | when to use | key rule |
|---|---|---|
| `fiber::Mutex` | shared state modified by several fibers; the critical section may park (I/O, sleep, another lock) | lock only in fibers; never hold a `std::mutex` / spin lock across a park; barging, not FIFO |
| `fiber::WaitGroup` | wait for N fibers: fan-out / fan-in, draining in-flight work on shutdown | `add()` before the work can finish (`wg.go` does it); one-shot: no `add()` after the group has finished; `wait()` parks a fiber, blocks a plain thread |
| `fiber::Oneshot<T>` | one reply to one waiting fiber, zero allocation | lives in the consumer's frame: observe the completion (`get()`) before leaving it |
| `fiber::make_promise_future<T>` | one reply when the consumer may give up (poll, detach) | `T` must fit the scheduler's `TaskMemoryPool`; `get()` / `try_get()` consume once |
| `fiber::Semaphore` | at most K at a time: a resource pool, a byte budget, backpressure between threads and fibers | FIFO, no overtaking (even `try_acquire` fails while anyone waits); `acquire(n)` with n > capacity waits forever |
| `fiber::Event` | a one-shot broadcast: start gun, "ready", "shutting down" | cannot be reset; `wait()` parks a fiber, blocks a plain thread; destroy after every `wait()` returned |
