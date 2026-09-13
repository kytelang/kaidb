#!/usr/bin/env bash
# Host gate for NovaDB (the B+Tree storage engine + SQL database, Zig). Builds and runs the full unit
# suite on THIS host OS, exiting non-zero on any failure. See ../CI-POLICY.md. Nothing merges red.
set -uo pipefail
cd "$(cd "$(dirname "$0")" && pwd)"
OS="$(uname -s)-$(uname -m)"
fail=0
step() { echo; echo ">>> $* [$OS]"; }

# Leftover test artifacts on disk can make a --seed-shuffled run pick up stale DB files (and, with
# orphaned concurrent test binaries, starve each other into phantom failures). Start clean.
find . -maxdepth 1 \( -name 'test_*.db' -o -name 'test_*_wal' -o -name 'repl_backfill' \) -exec rm -rf {} + 2>/dev/null || true

step "zig build (btree + btree-cli)"
zig build || fail=1

if [ $fail -eq 0 ]; then
  step "zig build test (std.testing.allocator leak-checks every test)"
  zig build test || fail=1
fi

echo
if [ $fail -eq 0 ]; then echo "GATE PASS  novadb  [$OS]"; else echo "GATE FAIL  novadb  [$OS]"; fi
exit $fail
