#!/usr/bin/env bash
# Mutation fuzzing with http-garden's mutators (GPLv3,
# https://github.com/narfindustries/http-garden): garden_fuzz.py imports
# them at runtime from the given checkout -- nothing of http-garden is
# copied here. Run it against an ASan + UBSan build of http_rest, e.g.
#   cmake -B build/asan-examples -DCMAKE_CXX_COMPILER=clang++ -DCMAKE_BUILD_TYPE=Debug \
#         -DFIBERS_SANITIZE=address,undefined -DFIBERS_HTTP=ON -DFIBERS_BUILD_EXAMPLES=ON
#
#   run_garden.sh <http_rest binary> <http-garden checkout> <output dir> [iterations] [seed]
set -u
BIN="$1"; GARDEN="$2"; OUT="$3"; ITER="${4:-20000}"; SEED="${5:-7}"
HERE="$(cd "$(dirname "$0")" && pwd)"
PORT=${PORT:-18383}
mkdir -p "$OUT"
timeout 3600 systemd-run --user --scope -q -p MemoryMax=4G -p MemorySwapMax=0 \
    -E ASAN_OPTIONS=halt_on_error=1:detect_leaks=0 -E UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
    "$BIN" --port=$PORT --workers=2 --idle-spins=64 > "$OUT/server.log" 2>&1 &
PID=$!
for i in $(seq 1 100); do
    curl -s -o /dev/null "http://127.0.0.1:$PORT/items" && break
    sleep 0.1
done
timeout 3000 python3 "$HERE/garden_fuzz.py" "$GARDEN/tools" $PORT "$ITER" "$SEED"
echo "fuzz exit=$?"
kill -TERM $PID
wait $PID
echo "server exit=$?"
grep -E "ERROR: AddressSanitizer|runtime error|SUMMARY" "$OUT/server.log" | head -5
