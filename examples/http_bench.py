#!/usr/bin/env python3
"""Benchmark driver for http_hello with wrk / wrk2 (examples/README.md).

    http_bench.py --server build/clang-release/examples/http_hello \\
        --server-args="--workers=1 --worker-cpus=0 --pollers=1 --poller-cpus=2 --blocking-cpus=1" \\
        --server-cpus=0,2 --client-cpus=6-9,12-15 \\
        [--connections=1,100,1000] [--rates=100:50000,1000:100000] [--duration=10] [--warmup=2]

Starts the server, then for each point: a warm-up run, the measured run
(wrk closed loop, or wrk2 at a fixed rate with corrected latency), and
CPU accounting from /proc/stat over the measured run: busy cores summed
over the client CPUs and the busiest of them, the same for the server's
CPUs; plus wrk's own CPU time (rusage of the child). Prints one line per
point. wrk2 points show the corrected latency (from the intended send
time) and the uncorrected one (service time): at low rates the corrected
numbers are dominated by wrk2's own millisecond send timer. Load generator
pinned with taskset; one wrk thread per client CPU (at most one per
connection).
"""
import argparse
import os
import re
import resource
import signal
import subprocess
import sys
import time


def cpu_list(text):
    out = []
    for part in text.split(','):
        if '-' in part:
            a, b = part.split('-')
            out.extend(range(int(a), int(b) + 1))
        elif part:
            out.append(int(part))
    return out


def proc_stat():
    stats = {}
    with open('/proc/stat') as f:
        for line in f:
            m = re.match(r'cpu(\d+) (.*)', line)
            if m:
                v = list(map(int, m.group(2).split()))
                idle = v[3] + v[4]
                stats[int(m.group(1))] = (sum(v), idle)
    return stats


def busy(before, after, cpus):
    per = []
    for c in cpus:
        total = after[c][0] - before[c][0]
        idle = after[c][1] - before[c][1]
        per.append((total - idle) / total if total else 0.0)
    return sum(per), max(per) if per else 0.0


def parse_wrk(out):
    def num(pattern, scale=1.0):
        m = re.search(pattern, out)
        return float(m.group(1)) * scale if m else float('nan')

    def lat(token):
        m = re.match(r'([\d.]+)(us|ms|s)', token)
        if not m:
            return float('nan')
        return float(m.group(1)) * {'us': 1, 'ms': 1e3, 's': 1e6}[m.group(2)]

    rps = num(r'Requests/sec:\s+([\d.]+)')
    pct = {}
    # wrk: "  99%   1.23ms"; wrk2 (corrected, HdrHistogram): " 99.900%    1.23ms"
    for p, token in re.findall(r'^\s*([\d.]+)%\s+([\d.]+(?:us|ms|s))\s*$', out, re.M):
        key = f'{float(p):g}'
        if key not in pct:
            pct[key] = lat(token)
    errors = re.search(r'Non-2xx or 3xx responses: (\d+)', out)
    socket_errors = re.search(r'Socket errors: (.*)', out)
    return rps, pct, (errors.group(1) if errors else '0'), (socket_errors.group(1) if socket_errors else '')


def fmt_us(v):
    if v != v:
        return '-'
    return f'{v / 1000:.1f}ms' if v >= 1000 else f'{v:.0f}'


def run_point(args, tool, connections, rate, client_cpus, server_cpus):
    threads = min(len(client_cpus), connections)
    base = ['taskset', '-c', ','.join(map(str, client_cpus)), tool, f'-t{threads}', f'-c{connections}',
            '--latency', args.url]
    if rate:
        base[4:4] = [f'-R{rate}', '--u_latency']
    warm = base[:]
    warm[4:4] = [f'-d{args.warmup}s']
    subprocess.run(warm, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=args.warmup + 60)
    measured = base[:]
    measured[4:4] = [f'-d{args.duration}s']
    load = open('/proc/loadavg').read().split()[0]
    r0 = resource.getrusage(resource.RUSAGE_CHILDREN)
    s0 = proc_stat()
    t0 = time.monotonic()
    p = subprocess.run(measured, capture_output=True, text=True, timeout=args.duration + 120)
    t1 = time.monotonic()
    s1 = proc_stat()
    r1 = resource.getrusage(resource.RUSAGE_CHILDREN)
    wall = t1 - t0
    wrk_cpu = (r1.ru_utime - r0.ru_utime + r1.ru_stime - r0.ru_stime) / wall
    cl_sum, cl_max = busy(s0, s1, client_cpus)
    sv_sum, sv_max = busy(s0, s1, server_cpus)
    # wrk2 --u_latency: the corrected histogram first, then the uncorrected one.
    corrected, _, uncorrected = p.stdout.partition('Uncorrected Latency')
    rps, _, non2xx, sockerr = parse_wrk(p.stdout)
    pct = parse_wrk(corrected)[1]
    upct = parse_wrk(uncorrected)[1] if uncorrected else {}
    return {
        'upct': upct,
        'tool': os.path.basename(tool), 'conns': connections, 'rate': rate, 'rps': rps, 'pct': pct,
        'non2xx': non2xx, 'sockerr': sockerr, 'client_sum': cl_sum, 'client_max': cl_max, 'wrk_cpu': wrk_cpu,
        'server_sum': sv_sum, 'server_max': sv_max, 'load': load, 'raw': p.stdout,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--server', required=True)
    ap.add_argument('--server-args', default='')
    ap.add_argument('--server-cpus', required=True)
    ap.add_argument('--client-cpus', required=True)
    ap.add_argument('--port', type=int, default=18080)
    ap.add_argument('--connections', default='1,100,1000')
    ap.add_argument('--rates', default='', help='conns:rate,... for wrk2')
    ap.add_argument('--duration', type=int, default=10)
    ap.add_argument('--warmup', type=int, default=2)
    ap.add_argument('--wrk', default='wrk')
    ap.add_argument('--wrk2', default='wrk2')
    ap.add_argument('--label', default='')
    ap.add_argument('--raw', action='store_true')
    args = ap.parse_args()
    args.url = f'http://127.0.0.1:{args.port}/hello'
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(resource.RLIMIT_NOFILE, (hard, hard))
    client_cpus = cpu_list(args.client_cpus)
    server_cpus = cpu_list(args.server_cpus)

    server = subprocess.Popen([args.server, f'--port={args.port}'] + args.server_args.split(),
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            if subprocess.run(['curl', '-s', '-o', '/dev/null', args.url]).returncode == 0:
                break
            time.sleep(0.1)
        results = []
        for c in map(int, filter(None, args.connections.split(','))):
            results.append(run_point(args, args.wrk, c, 0, client_cpus, server_cpus))
            r = results[-1]
            print(f"{args.label} wrk c={c}: {r['rps']:.0f} rps p50 {fmt_us(r['pct'].get('50'))} p99 "
                  f"{fmt_us(r['pct'].get('99'))} | client {r['client_sum']:.1f} cores (max {r['client_max']:.0%}), "
                  f"wrk {r['wrk_cpu']:.1f} | server {r['server_sum']:.1f} (max {r['server_max']:.0%}) | load {r['load']} "
                  f"| non2xx {r['non2xx']} {r['sockerr']}", flush=True)
        for spec in filter(None, args.rates.split(',')):
            c, rate = map(int, spec.split(':'))
            results.append(run_point(args, args.wrk2, c, rate, client_cpus, server_cpus))
            r = results[-1]
            print(f"{args.label} wrk2 c={c} R={rate}: {r['rps']:.0f} rps p50 {fmt_us(r['pct'].get('50'))} p90 "
                  f"{fmt_us(r['pct'].get('90'))} p99 {fmt_us(r['pct'].get('99'))} p99.9 {fmt_us(r['pct'].get('99.9'))} "
                  f"(uncorrected p50 {fmt_us(r['upct'].get('50'))} p99 {fmt_us(r['upct'].get('99'))} p99.9 "
                  f"{fmt_us(r['upct'].get('99.9'))}) | client {r['client_sum']:.1f} cores (max {r['client_max']:.0%}), "
                  f"wrk {r['wrk_cpu']:.1f} | server "
                  f"{r['server_sum']:.1f} (max {r['server_max']:.0%}) | load {r['load']} | non2xx {r['non2xx']} "
                  f"{r['sockerr']}", flush=True)
        if args.raw:
            for r in results:
                print(r['raw'])
    finally:
        server.send_signal(signal.SIGTERM)
        try:
            server.wait(timeout=15)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait()
    return 0


if __name__ == '__main__':
    sys.exit(main())
