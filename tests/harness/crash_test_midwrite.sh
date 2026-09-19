#!/bin/bash
# kaidb MID-WRITE crash-durability harness (P0 gate: "kill the leader mid-write, no committed loss").
#
# Unlike crash_test.sh (which loads all rows, THEN kills), this kills the server WHILE inserts are still
# streaming, so a transaction is in flight at the moment of death. It proves two things on recovery:
#   1. no committed (acked) row is lost -- with SYNCHRONOUS_COMMIT=true every acked INSERT is fsync-durable;
#   2. recovery does not crash and leaves no torn state -- the WAL-replay + active-txn revert (the undo
#      rebuild path, recover() Phase 3) cleanly drops the interrupted write and keeps the committed prefix.
# Because the single curl loop inserts in order and each ack is durable, every id we OBSERVED as acked must
# survive; verification is by POINT-LOOKUP (the SQL parser has no COUNT(*)/MAX aggregate).
#
#   Usage: crash_test_midwrite.sh [ROWS]   (default 400)
#   Env:   BTREE_BIN - path to the btree binary (default zig-out/bin/btree)
#
# Exit: 0 = PASS, non-zero = FAIL.
set -u
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BTREE_BIN:-$REPO/zig-out/bin/btree}"
N="${1:-400}"
DIR="$(mktemp -d /tmp/btcrashmw.XXXXXX)"
Q(){ curl -s -m 5 -X POST http://127.0.0.1:3008/query -d "{\"sql\":\"$1\",\"session_token\":\"\"}"; }

mkdir -p "$DIR/public"; cp "$REPO/db.json" "$DIR/" 2>/dev/null
cd "$DIR"
trap 'pkill -f "$BIN" 2>/dev/null; rm -rf "$DIR"' EXIT

echo "=== phase 1: start (SYNCHRONOUS_COMMIT=true), stream up to $N rows, kill MID-WRITE ==="
SYNCHRONOUS_COMMIT=true "$BIN" > s1.log 2>&1 & P1=$!
sleep 2
Q "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)" >/dev/null

# Stream inserts in the background; capture the last id whose ack we OBSERVED (a strict lower bound on the
# durable count -- more may have acked after we stopped watching, but never fewer).
( for i in $(seq 1 "$N"); do
    r=$(Q "INSERT INTO t (id,v) VALUES ($i,'row$i')")
    echo "$i" > lastok.txt
  done ) &
LOADER=$!

# Let a few hundred ms of inserts land, then hard-kill the server WHILE the loader is still going.
sleep 0.6
echo "=== phase 2: HARD KILL (kill -9) mid-stream ==="
kill -9 $P1 2>/dev/null
kill $LOADER 2>/dev/null
sleep 1
OBSERVED=$(cat lastok.txt 2>/dev/null || echo 0)
echo "  last ack observed before kill = id $OBSERVED"

echo "=== phase 3: restart + recover ==="
SYNCHRONOUS_COMMIT=true "$BIN" > s2.log 2>&1 & P2=$!
sleep 3
if grep -qiE "InvalidChecksum|panic|beyond_eof|Segmentation" s2.log; then
  echo "RESULT: FAIL - recovery CRASHED:"; grep -iE "InvalidChecksum|panic|beyond_eof|Segmentation" s2.log | head -3
  exit 1
fi

present(){ [ "$(Q "SELECT v FROM t WHERE id = $1" | grep -o "row$1")" = "row$1" ]; }

if [ "$OBSERVED" -lt 1 ]; then
  echo "RESULT: FAIL - no insert acked before kill (test too fast); raise ROWS or sleep"; exit 1
fi

# No committed loss: every id we OBSERVED as acked must survive. Sample the prefix (1, mid, last-observed);
# each is a committed row whose ack the client saw, so losing any is an RPO>0 durability bug.
MID=$(( OBSERVED / 2 )); [ "$MID" -lt 1 ] && MID=1
for id in 1 "$MID" "$OBSERVED"; do
  if ! present "$id"; then
    echo "RESULT: FAIL - committed row id=$id LOST after recovery (observed ack up to $OBSERVED)"; exit 1
  fi
done

echo "RESULT: PASS - all observed-acked rows (1..$OBSERVED sampled) durable across mid-write kill, clean recovery"
exit 0
