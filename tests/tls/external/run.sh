#!/bin/sh
# Runs EXTERNAL TLS test tools against the tls_echo example server:
# tlsfuzzer scripts (malformed / truncated / fuzzed handshakes and records,
# fragmentation, KeyUpdates, abrupt closes) and testssl.sh (protocol and
# cipher scan, vulnerability probes: hundreds of short, often aborted
# connections).
#
#   tests/tls/external/run.sh <tls_echo> <certs dir> <output dir> [quick]
#
# Needs the dev shell (flake.nix): testssl.sh, python3 with ecdsa, openssl,
# and FIBERS_TLSFUZZER / FIBERS_TLSLITE_NG pointing at the pinned sources.
# The tools are GPL / LGPL: they are only invoked from here, nothing of them
# is copied into the repository. Not part of ctest (minutes, and external
# tools); run it by hand, ideally with an ASan build of tls_echo (memory
# errors then land in fuzz-server.err / scan-server.err).
#
# tlsfuzzer: every script runs twice, against `tls_echo --http10` and
# against `openssl s_server -www` -- the same request / response / close
# behaviour, certificate (an all-RSA self-signed one made here: the scripts
# offer RSA signature algorithms only), OpenSSL, and cipher policy
# ("DEFAULT:@SECLEVEL=0": the TLS 1.2 scripts use SHA-1 signatures and
# static RSA). A script that fails the same way on both is OpenSSL's policy
# or the script's expectations; a different result on tls_echo points at our
# BIO pump. A run that reaches its timeout is a hang.
#
# testssl.sh scans a second tls_echo with the library's default policy.
#
# At the end both servers must still serve a fresh connection, and on
# SIGTERM they report how many connections are still active (> 0 with every
# client gone = a connection stuck in TLS).
#
# Exit status 0: no hang, no tls_echo-only difference, servers alive, nothing
# left active, no sanitizer report.
set -u

echo_bin=$1
certs=$2
out=$3
quick=${4:-}
mkdir -p "$out"

: "${FIBERS_TLSFUZZER:?run inside the dev shell (direnv exec .)}"
: "${FIBERS_TLSLITE_NG:?run inside the dev shell}"
export PYTHONPATH="$FIBERS_TLSFUZZER:$FIBERS_TLSLITE_NG"

free_port() {
    python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])'
}

openssl req -x509 -newkey rsa:2048 -nodes -keyout "$out/fuzz.key" -out "$out/fuzz.pem" -days 30 \
    -subj /CN=localhost -addext "subjectAltName=DNS:localhost" 2> /dev/null || exit 2

fuzz_port=$(free_port)
ref_port=$(free_port)
scan_port=$(free_port)
ciphers='DEFAULT:@SECLEVEL=0'

# Every server dies after an hour whatever happens (timeout passes TERM on).
timeout -s KILL 3600 "$echo_bin" --server --http10 --host=127.0.0.1 --port="$fuzz_port" \
    --cert="$out/fuzz.pem" --key="$out/fuzz.key" --ciphers="$ciphers" --workers=2 \
    > "$out/fuzz-server.log" 2> "$out/fuzz-server.err" &
fuzz_server=$!
timeout -s KILL 3600 openssl s_server -accept "127.0.0.1:$ref_port" -cert "$out/fuzz.pem" -key "$out/fuzz.key" \
    -cipher "$ciphers" -www -quiet > "$out/reference.log" 2>&1 < /dev/null &
reference=$!
timeout -s KILL 3600 "$echo_bin" --server --host=127.0.0.1 --port="$scan_port" \
    --cert="$certs/server.pem" --key="$certs/server.key" --workers=2 \
    > "$out/scan-server.log" 2> "$out/scan-server.err" &
scan_server=$!
cleanup() {
    kill -9 "$fuzz_server" "$reference" "$scan_server" 2> /dev/null
}
trap cleanup EXIT
sleep 1

status=0

scripts="
test-tls13-conversation.py
test-conversation.py
test-tls13-keyupdate.py
test-tls13-zero-length-data.py
test-zero-length-data.py
test-tls13-record-padding.py
test-record-layer-fragmentation.py
test-truncating-of-client-hello.py
test-truncating-of-finished.py
test-tls13-finished.py
test-tls13-finished-plaintext.py
test-fuzzed-finished.py
test-fuzzed-MAC.py
test-fuzzed-ciphertext.py
test-fuzzed-plaintext.py
test-fuzzed-padding.py
test-invalid-client-hello.py
test-invalid-client-hello-w-record-overflow.py
test-invalid-content-type.py
test-invalid-version.py
test-invalid-cipher-suites.py
test-invalid-compression-methods.py
test-invalid-session-id.py
test-invalid-server-name-extension.py
test-empty-extensions.py
test-large-hello.py
test-client-hello-max-size.py
test-ccs.py
test-tls13-count-tickets.py
"
if [ -n "$quick" ]; then
    scripts="test-tls13-conversation.py test-tls13-keyupdate.py test-truncating-of-client-hello.py"
fi

printf '%-48s %-14s %-14s\n' "tlsfuzzer script" tls_echo "s_server -www" > "$out/summary.txt"
for script in $scripts; do
    for target in ours reference; do
        if [ "$target" = ours ]; then p=$fuzz_port; else p=$ref_port; fi
        timeout 300 python3 "$FIBERS_TLSFUZZER/scripts/$script" -h 127.0.0.1 -p "$p" \
            > "$out/$script.$target.log" 2>&1
        rc=$?
        case $rc in
            0) result="pass:$(grep -m1 '^PASS: ' "$out/$script.$target.log" | cut -d' ' -f2)" ;;
            124) result=HANG ;;
            *) result="FAIL:$(grep -m1 '^FAIL: ' "$out/$script.$target.log" | cut -d' ' -f2)" ;;
        esac
        eval "result_$target=\$result"
    done
    printf '%-48s %-14s %-14s\n' "$script" "$result_ours" "$result_reference" >> "$out/summary.txt"
    if [ "$result_ours" = HANG ] || [ "$result_ours" != "$result_reference" ]; then
        status=1  # compare $script.ours.log with $script.reference.log
    fi
    if ! kill -0 "$fuzz_server" 2> /dev/null; then
        echo "tls_echo died during $script" >> "$out/summary.txt"
        status=1
        break
    fi
done

if [ -z "$quick" ]; then
    timeout 900 testssl.sh --quiet --color 0 --warnings batch --ip one "127.0.0.1:$scan_port" \
        > "$out/testssl.log" 2>&1
    rc=$?
    echo "testssl.sh exit $rc (124 = timeout; otherwise the number of findings; testssl.log)" >> "$out/summary.txt"
    [ "$rc" = 124 ] && status=1
fi

# Still serving?
if printf 'GET / HTTP/1.0\r\n\r\n' | timeout 20 openssl s_client -connect "127.0.0.1:$fuzz_port" \
        -CAfile "$out/fuzz.pem" -verify_return_error -servername localhost -quiet 2> /dev/null |
        grep -q '200 ok'; then
    echo "after the tools: fuzz server still serves" >> "$out/summary.txt"
else
    echo "after the tools: fuzz server does NOT serve" >> "$out/summary.txt"
    status=1
fi
if printf 'ping\n' | timeout 20 openssl s_client -connect "127.0.0.1:$scan_port" -CAfile "$certs/ca.pem" \
        -verify_return_error -servername localhost -quiet 2> /dev/null | head -c 5 | grep -q ping; then
    echo "after the tools: scan server still serves (echo ok)" >> "$out/summary.txt"
else
    echo "after the tools: scan server does NOT serve" >> "$out/summary.txt"
    status=1
fi

sleep 2  # let the last connections wind down
kill -TERM "$fuzz_server" "$scan_server" 2> /dev/null
for _ in $(seq 1 100); do
    kill -0 "$fuzz_server" 2> /dev/null || kill -0 "$scan_server" 2> /dev/null || break
    sleep 0.1
done
sleep 0.5
for name in fuzz-server scan-server; do
    tail -n 1 "$out/$name.log" >> "$out/summary.txt"
    grep -q " 0 still active" "$out/$name.log" || status=1
    if grep -q "AddressSanitizer\|ThreadSanitizer\|runtime error\|fatal:" "$out/$name.err"; then
        echo "$name.err has sanitizer / fatal reports" >> "$out/summary.txt"
        status=1
    fi
done
echo "exit status $status" >> "$out/summary.txt"
cat "$out/summary.txt"
exit $status
