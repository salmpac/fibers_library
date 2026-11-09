#!/bin/sh
# Generates the throwaway PKI of the TLS tests / benchmark / example into
# $1 with the openssl CLI $2. Nothing here is secret or committed: keys are
# made fresh at build time.
#
#   ca.pem / ca.key                 test CA
#   server.pem / server.key         SAN DNS:localhost, IP:127.0.0.1, IP:::1 (EC P-256)
#   server_rsa.pem / .key           same names, RSA 2048 (stack depth / benchmark)
#   client.pem / client.key         CN=fibers test client, clientAuth (mTLS)
#   wronghost.pem / .key            SAN DNS:wrong.example only
#   expired.pem / .key              SAN localhost, validity in 2020
#   other_ca.pem / .key             a CA nobody trusts
#   untrusted.pem / .key            SAN localhost, issued by other_ca
set -eu
out=$1
openssl=$2
mkdir -p "$out"
cd "$out"

ec="-newkey ec -pkeyopt ec_paramgen_curve:P-256"

ca() {  # name subject
    "$openssl" req -x509 $ec -nodes -keyout "$1.key" -out "$1.pem" -days 3650 -subj "$2" \
        -addext "basicConstraints=critical,CA:TRUE" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
}

leaf() {  # name issuer subject san eku keyspec [x509 validity args...]
    name=$1 issuer=$2 subj=$3 san=$4 eku=$5 keyspec=$6
    shift 6
    "$openssl" req $keyspec -nodes -keyout "$name.key" -out "$name.csr" -subj "$subj" \
        ${san:+-addext "subjectAltName=$san"} \
        -addext "extendedKeyUsage=$eku" \
        -addext "basicConstraints=critical,CA:FALSE" 2>/dev/null
    "$openssl" x509 -req -in "$name.csr" -CA "$issuer.pem" -CAkey "$issuer.key" \
        -set_serial "0x$("$openssl" rand -hex 8)" \
        -copy_extensions copyall -out "$name.pem" "$@" 2>/dev/null
    rm -f "$name.csr"
}

ca ca "/CN=fibers test CA"
ca other_ca "/CN=fibers untrusted CA"
leaf server ca "/CN=localhost" "DNS:localhost,IP:127.0.0.1,IP:::1" serverAuth "$ec" -days 3650
leaf server_rsa ca "/CN=localhost" "DNS:localhost,IP:127.0.0.1,IP:::1" serverAuth "-newkey rsa:2048" -days 3650
leaf client ca "/CN=fibers test client" "" clientAuth "$ec" -days 3650
leaf wronghost ca "/CN=wrong.example" "DNS:wrong.example" serverAuth "$ec" -days 3650
leaf untrusted other_ca "/CN=localhost" "DNS:localhost,IP:127.0.0.1" serverAuth "$ec" -days 3650
# Expired: -not_before / -not_after need OpenSSL >= 3.4; on older CLIs the
# certificate is missing and the test skips.
leaf expired ca "/CN=localhost" "DNS:localhost,IP:127.0.0.1" serverAuth "$ec" \
    -not_before 20200101000000Z -not_after 20200201000000Z || rm -f expired.pem expired.key
touch stamp
