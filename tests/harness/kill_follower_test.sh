#!/bin/bash
# kaidb kill-follower + CATCH-UP harness (R6/P8): kill the follower while the leader keeps writing, then
# restart it and prove the leader RECONNECTS and BACKFILLS the writes the follower missed -- the follower
# ends fully consistent, no committed loss.
#
# Exercises: seq-always-advances (missed ships are detectable gaps), the retained-frame ring, and
# reconnectAndCatchUp (probe follower confirmed_seq -> re-ship the gap).
#
#   Usage: kill_follower_test.sh
#   Env:   BTREE_BIN (default zig-out/bin/btree)
# Exit: 0 = PASS (follower caught up on ALL rows), non-zero = FAIL.
set -u
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BTREE_BIN:-$REPO/zig-out/bin/btree}"
[ -x "$BIN" ] || { echo "build btree first"; exit 1; }

FDIR="$(mktemp -d /tmp/btkf_f.XXXXXX)"; LDIR="$(mktemp -d /tmp/btkf_l.XXXXXX)"
QL(){ curl -s -m 5 -X POST http://127.0.0.1:3008/query -d "{\"sql\":\"$1\",\"session_token\":\"\"}"; }
QF(){ curl -s -m 5 -X POST http://127.0.0.1:3018/query -d "{\"sql\":\"$1\",\"session_token\":\"\"}"; }
mkdir -p "$FDIR/public" "$LDIR/public"
printf '{"address":"127.0.0.1","port":3019,"base_dir":".","pool_size":1000}\n' > "$FDIR/db.json"
printf '{"address":"127.0.0.1","port":3009,"base_dir":".","pool_size":1000}\n' > "$LDIR/db.json"
FENV="PRIMARY=false REPLICA_ENABLED=true REPLICA_PORT=3020 HTTP_PORT=3018 SYNCHRONOUS_COMMIT=true"
start_follower(){ ( cd "$FDIR" && env $FENV "$BIN" >> s.log 2>&1 ) & echo $!; }
trap 'pkill -f "$BIN" 2>/dev/null; rm -rf "$FDIR" "$LDIR"' EXIT

echo "=== start follower + leader; write rows 1-2 (replicated) ==="
FPID=$(start_follower); sleep 3
( cd "$LDIR" && env PRIMARY=true REPLICA_ENABLED=true REPLICA_PORT=3020 HTTP_PORT=3008 SYNCHRONOUS_COMMIT=true "$BIN" > s.log 2>&1 ) & LPID=$!
sleep 3
QL "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" >/dev/null
QL "INSERT INTO kv (id, v) VALUES (1, 'r1')" >/dev/null
QL "INSERT INTO kv (id, v) VALUES (2, 'r2')" >/dev/null
sleep 1
echo "  follower has r2? $(QF "SELECT v FROM kv WHERE id = 2" | grep -o r2)"

echo "=== KILL follower; write rows 3-4 while it is DOWN (leader retains them) ==="
kill -9 "$FPID" 2>/dev/null; sleep 1
QL "INSERT INTO kv (id, v) VALUES (3, 'r3')" >/dev/null   # async ship fails -> retained, seq advances
QL "INSERT INTO kv (id, v) VALUES (4, 'r4')" >/dev/null
sleep 1

echo "=== RESTART follower; write row 5 -> leader reconnects + backfills 3,4 then ships 5 ==="
FPID=$(start_follower); sleep 3
QL "INSERT INTO kv (id, v) VALUES (5, 'r5')" >/dev/null
sleep 2

echo "=== verify the follower caught up on ALL rows ==="
miss=""
for i in 1 2 3 4 5; do
  got=$(QF "SELECT v FROM kv WHERE id = $i" | grep -o "r$i")
  [ "$got" = "r$i" ] || miss="$miss $i"
  echo "  follower id=$i -> ${got:-MISSING}"
done

if [ -z "$miss" ]; then
  echo "RESULT: PASS - follower caught up on all rows after restart (backfilled the gap)"; exit 0
else
  echo "RESULT: FAIL - follower still missing rows:$miss"
  echo "--- leader log tail ---"; grep -iE "catch-up|reconnect|ship|repl" "$LDIR/s.log" | tail -8
  exit 1
fi
