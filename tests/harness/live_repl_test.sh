#!/bin/bash
# NovaDB LIVE replication harness (R1-R6): a running LEADER process ships committed writes to a running
# FOLLOWER process over the network, and the follower serves them.
#
# Proves the leader<->follower shipping that main.zig now wires (becomeDurableLeader when primary +
# replica.enabled): start the follower (listens for frames on its replica.port), start the leader (ships to
# it), write on the leader over HTTP, then read the row back FROM THE FOLLOWER.
#
#   Usage: live_repl_test.sh
#   Env:   BTREE_BIN (default zig-out/bin/btree)
# Exit: 0 = PASS (the write replicated), non-zero = FAIL.
set -u
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${BTREE_BIN:-$REPO/zig-out/bin/btree}"
[ -x "$BIN" ] || { echo "build btree first: (cd $REPO && zig build)"; exit 1; }

FDIR="$(mktemp -d /tmp/btlive_f.XXXXXX)"
LDIR="$(mktemp -d /tmp/btlive_l.XXXXXX)"
QL(){ curl -s -m 5 -X POST http://127.0.0.1:3008/query -d "{\"sql\":\"$1\",\"session_token\":\"\"}"; }
QF(){ curl -s -m 5 -X POST http://127.0.0.1:3018/query -d "{\"sql\":\"$1\",\"session_token\":\"\"}"; }
trap 'pkill -f "$BIN" 2>/dev/null; rm -rf "$FDIR" "$LDIR"' EXIT

# The replication topology comes from ENV VARS (env is the explicit topology layer); each
# server keeps a minimal db.json for base_dir. TCP `port` differs so the two do not collide on that listener.
mkdir -p "$FDIR/public" "$LDIR/public"
printf '{"address":"127.0.0.1","port":3019,"base_dir":".","pool_size":1000}\n' > "$FDIR/db.json"
printf '{"address":"127.0.0.1","port":3009,"base_dir":".","pool_size":1000}\n' > "$LDIR/db.json"

echo "=== phase 1: start FOLLOWER (repl listener :3020, query :3018) ==="
( cd "$FDIR" && SYNCHRONOUS_COMMIT=true PRIMARY=false REPLICA_ENABLED=true REPLICA_PORT=3020 HTTP_PORT=3018 "$BIN" > s.log 2>&1 ) & FPID=$!
sleep 3

echo "=== phase 2: start LEADER (ships to :3020, query :3008) ==="
( cd "$LDIR" && SYNCHRONOUS_COMMIT=true PRIMARY=true REPLICA_ENABLED=true REPLICA_PORT=3020 HTTP_PORT=3008 "$BIN" > s.log 2>&1 ) & LPID=$!
sleep 3

echo "=== phase 3: write on the LEADER ==="
QL "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" >/dev/null
QL "INSERT INTO kv (id, v) VALUES (1, 'shipped')" >/dev/null
QL "INSERT INTO kv (id, v) VALUES (2, 'alsoshipped')" >/dev/null
sleep 1

echo "=== phase 4: read back FROM THE FOLLOWER ==="
R1=$(QF "SELECT v FROM kv WHERE id = 1" | grep -o "shipped")
R2=$(QF "SELECT v FROM kv WHERE id = 2" | grep -o "alsoshipped")
echo "  follower id=1 -> ${R1:-MISSING} ; id=2 -> ${R2:-MISSING}"

if [ "$R1" = "shipped" ] && [ "$R2" = "alsoshipped" ]; then
  echo "RESULT: PASS - leader writes replicated to the follower over the network"; exit 0
else
  echo "RESULT: FAIL - follower did not receive the shipped rows"
  echo "--- leader log tail ---"; tail -8 "$LDIR/s.log" 2>/dev/null
  echo "--- follower log tail ---"; tail -8 "$FDIR/s.log" 2>/dev/null
  exit 1
fi
