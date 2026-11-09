#!/usr/bin/env bash
# Runs defparam/smuggler (MIT, https://github.com/defparam/smuggler) against
# the http_rest example: every config (default, doubles, exhaustive), POST and
# GET, CL.TE and TE.CL probes. Bounded; kills what it starts.
#
#   run_smuggler.sh <http_rest binary> <smuggler checkout> <output dir>
#
# smuggler flags a mutation when a probe times out (the server waits for body
# bytes the other framing would not send); "OK" = no desync signal.
set -u
BIN="$1"; SMUGGLER="$2"; OUT="$3"
PORT=${PORT:-18282}
mkdir -p "$OUT"
timeout 1800 systemd-run --user --scope -q -p MemoryMax=2G -p MemorySwapMax=0 \
    "$BIN" --port=$PORT --workers=2 --idle-spins=64 > "$OUT/server.log" 2>&1 &
PID=$!
for i in $(seq 1 50); do
    curl -s -o /dev/null "http://127.0.0.1:$PORT/items" && break
    sleep 0.1
done
cd "$SMUGGLER"
for cfg in default doubles exhaustive; do
    for method in POST GET; do
        timeout 600 python3 smuggler.py -u "http://127.0.0.1:$PORT/items/smuggle" -m $method \
            -c "$cfg.py" -t 3 --no-color > "$OUT/smuggler_${cfg}_${method}.log" 2>&1
        echo "config=$cfg method=$method exit=$?"
    done
done
# Verdicts: count OK vs anything else, and the status codes seen.
python3 -I - "$OUT" <<'PY'
import collections, glob, os, re, sys
for f in sorted(glob.glob(os.path.join(sys.argv[1], 'smuggler_*.log'))):
    t = open(f, errors='replace').read().replace('\r', '\n')
    v = re.findall(r'\[([^\]]+)\]\s*:\s*(OK|[^\n(]*Issue[^\n]*|DISABLED[^\n]*)', t)
    codes = collections.Counter(re.findall(r'(?:TECL|CLTE): [0-9.]+ - (\d+)', t))
    bad = [x for x in v if x[1] != 'OK']
    print(os.path.basename(f), 'mutations:', len(v), 'non-OK:', bad[:5], 'statuses:', dict(codes))
PY
kill -TERM $PID
wait $PID
