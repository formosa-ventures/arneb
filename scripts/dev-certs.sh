#!/usr/bin/env bash
#
# Generate a throwaway local CA plus a server certificate for trying the
# security settings ([tls], [cluster.tls]) on one machine. Not for
# production.
#
# Output (git-ignored): .dev-certs/{ca.crt,ca.key,server.crt,server.key}
# The server certificate covers localhost, 127.0.0.1 and
# host.docker.internal, so the same pair serves pgwire, the Trino
# listener and every node's Flight server.
#
# Usage:
#   ./scripts/dev-certs.sh            # writes .dev-certs/ (keeps an existing CA)
#   OUT=/tmp/certs ./scripts/dev-certs.sh

set -euo pipefail

OUT="${OUT:-$(git rev-parse --show-toplevel)/.dev-certs}"
mkdir -p "$OUT"
cd "$OUT"

if [[ ! -f ca.key ]]; then
    openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
        -keyout ca.key -out ca.crt -subj "/CN=arneb dev CA" \
        -addext "basicConstraints=critical,CA:TRUE" \
        -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
fi

openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
    -subj "/CN=localhost" 2>/dev/null
cat > server.ext <<'EXT'
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1
EXT
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days 365 -out server.crt -extfile server.ext 2>/dev/null
rm -f server.csr server.ext ca.srl

echo "wrote $OUT: ca.crt server.crt server.key"
