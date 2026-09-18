#!/usr/bin/env bash
# Regenerate the mutual-TLS replication test fixtures under testdata/repl/.
# A good CA signs server + client; a separate rogue CA signs rogue, so the
# mutual-TLS test (validating against the good CA) accepts client and refuses
# rogue. Certs carry Subject/Authority Key Identifiers so chain builders can link
# leaf -> CA. Long validity so the fixtures do not expire.
set -euo pipefail
cd "$(dirname "$0")/../../testdata/repl"
DAYS=36500
subj() { echo "/C=IN/O=NovaDB Test/CN=$1"; }

mkca() { # name
  local n=$1
  openssl genrsa -out "$n.key" 2048 >/dev/null 2>&1
  cat > "$n.cnf" <<CNF
[req]
distinguished_name=dn
x509_extensions=v3
prompt=no
[dn]
CN=$n
O=NovaDB Test
[v3]
basicConstraints=critical,CA:TRUE
keyUsage=critical,keyCertSign,cRLSign
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid:always
CNF
  openssl req -x509 -new -key "$n.key" -days $DAYS -out "$n.crt" -config "$n.cnf" >/dev/null 2>&1
  rm -f "$n.cnf"
}

issue() { # name caname
  local n=$1 ca=$2
  openssl genrsa -out "$n.key" 2048 >/dev/null 2>&1
  openssl req -new -key "$n.key" -out "$n.csr" -subj "$(subj "$n")" >/dev/null 2>&1
  cat > "$n.ext" <<EXT
basicConstraints=CA:FALSE
keyUsage=critical,digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth
subjectKeyIdentifier=hash
authorityKeyIdentifier=keyid,issuer
subjectAltName=DNS:localhost,IP:127.0.0.1
EXT
  openssl x509 -req -in "$n.csr" -CA "$ca.crt" -CAkey "$ca.key" -CAcreateserial \
    -out "$n.crt" -days $DAYS -extfile "$n.ext" >/dev/null 2>&1
  rm -f "$n.csr" "$n.ext"
}

mkca ca
mkca rogue-ca
issue server ca
issue client ca
issue rogue  rogue-ca
rm -f ca.srl rogue-ca.srl rogue-ca.key rogue-ca.crt
echo "generated: $(ls *.crt *.key | tr '\n' ' ')"
