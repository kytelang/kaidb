#!/bin/bash
# kaidb CHAOS SUITE (R6/P8) -- fault-injection scenarios reported as NUMBERS.
#
# Turns the durability/replication gates into an automated pass/fail report, per the P8 gate: "zero
# committed loss beyond the stated RPO, zero split-brain writes, recovery within the stated RTO -- reported
# as numbers". Two classes run here:
#   1. SUBPROCESS kill scenarios (this script): kill -9 the server mid-write, restart, verify no committed
#      loss. Runs crash_test.sh (load-then-kill) and crash_test_midwrite.sh (kill WHILE writes stream, the
#      active-txn/undo path).
#   2. IN-PROCESS scenarios (`zig build test`, reported here for completeness): corrupt shipped frame
#      rejected + never applied (P8 chaos test); fenced old leader's writes rejected (store-write fencing);
#      follower re-ships from the last durable point after a mid-apply crash (P2 crash-safety).
#
# STATED RPO/RTO for the config-store deployment (SYNCHRONOUS_COMMIT + durable quorum writes):
#   RPO = 0 for sync-committed / quorum-acked writes across a single-node crash or a leader kill (every
#         acked write is fsync-durable and, if durable, quorum-held before the ack).
#   RTO = lease TTL + one reconcile tick (a standby promotes once the leader lease expires; bounded by the
#         orchestrator's configured ttlMs, not by data volume).
#
#   Usage: chaos_suite.sh
#   Env:   BTREE_BIN (default zig-out/bin/btree)
# Exit: 0 = all subprocess scenarios PASS, non-zero otherwise.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
pass=0; fail=0; names_failed=""

run() {
  local name="$1"; shift
  echo "=== CHAOS: $name ==="
  if bash "$@" 2>&1 | tail -1 | grep -q "RESULT: PASS"; then
    echo "  [$name] PASS"; pass=$((pass+1))
  else
    echo "  [$name] FAIL"; fail=$((fail+1)); names_failed="$names_failed $name"
  fi
}

run "kill-leader-after-load (RPO=0)"   "$HERE/crash_test.sh" true 200
run "kill-leader-mid-write (undo)"     "$HERE/crash_test_midwrite.sh" 400

echo "----------------------------------------------------------------"
echo "CHAOS subprocess scenarios: $pass passed, $fail failed$([ -n "$names_failed" ] && echo " ($names_failed )")"
echo "IN-PROCESS scenarios (run via 'zig build test'):"
echo "  - corrupt shipped frame rejected + not applied  : test 'P8 chaos: a corrupted shipped frame ...'"
echo "  - fenced old leader's writes rejected            : test 'store-side write fencing'"
echo "  - follower re-ships from last durable point      : applyStream durableFlush before confirmed_seq (P2)"
echo "STATED: RPO=0 (sync/quorum writes) ; RTO = lease TTL + one reconcile tick."
[ "$fail" -eq 0 ]
