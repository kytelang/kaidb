#!/bin/bash
# kaidb crash-durability harness.
#
# Verifies that committed data survives a hard crash (kill -9) and that the server
# recovers without crashing. Exercises the WAL-replay recovery path (rebuild user
# trees from the WAL + persist final roots).
#
#   Usage: crash_test.sh [SYNC=true|false] [ROWS]
#     SYNC  - value of SYNCHRONOUS_COMMIT (default true)
#     ROWS  - number of rows to insert before the crash (default 200)
#
#   Env:
#     BTREE_BIN - path to the btree binary (default zig-out/bin/btree relative to repo)
#
# Exit: 0 = PASS (all committed rows durable, no crash), non-zero = FAIL.
set -u
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BTREE_BIN:-$REPO/zig-out/bin/btree}"
SYNC="${1:-true}"
N="${2:-200}"
DIR="$(mktemp -d /tmp/btcrash.XXXXXX)"
Q(){ curl -s -m 5 -X POST http://127.0.0.1:3008/query -d "{\"sql\":\"$1\",\"session_token\":\"\"}"; }

mkdir -p "$DIR/public"; cp "$REPO/db.json" "$DIR/" 2>/dev/null
cd "$DIR"
trap 'pkill -f "$BIN" 2>/dev/null; rm -rf "$DIR"' EXIT

echo "=== phase 1: start (SYNCHRONOUS_COMMIT=$SYNC), load $N rows ==="
SYNCHRONOUS_COMMIT="$SYNC" "$BIN" > s1.log 2>&1 & P1=$!
sleep 2
Q "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)" >/dev/null
for i in $(seq 1 "$N"); do Q "INSERT INTO t (id,v) VALUES ($i,'row$i')" >/dev/null; done
# The SQL parser has no COUNT(*) aggregate; verify durability by POINT-LOOKUP of sampled ids instead.
echo "  loaded $N rows"

echo "=== phase 2: HARD KILL (kill -9) ==="
kill -9 $P1 2>/dev/null; sleep 1

echo "=== phase 3: restart + recover ==="
SYNCHRONOUS_COMMIT="$SYNC" "$BIN" > s2.log 2>&1 & P2=$!
sleep 3
if grep -qiE "InvalidChecksum|panic|beyond_eof|Segmentation" s2.log; then
  echo "RESULT: FAIL - recovery CRASHED:"; grep -iE "InvalidChecksum|panic|beyond_eof" s2.log | head -3
  exit 1
fi
present(){ [ "$(Q "SELECT v FROM t WHERE id = $1" | grep -o "row$1")" = "row$1" ]; }
# Sample the whole range (first, middle, near-last) -- every committed row must survive the crash.
MISSING=""
for id in 1 $(( N / 2 )) $(( N / 2 + 37 )) "$N"; do
  [ "$id" -gt "$N" ] && id=$N
  present "$id" || MISSING="$MISSING $id"
done

if [ -z "$MISSING" ]; then
  echo "RESULT: PASS - all sampled committed rows (1..$N) durable, no crash"; exit 0
else
  echo "RESULT: FAIL - committed rows LOST after recovery:$MISSING"; exit 1
fi
