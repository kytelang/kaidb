#!/usr/bin/env bash
# Host gate for kaidb (the B+Tree storage engine + SQL database, Zig). Builds and runs the full unit
# suite on THIS host OS, exiting non-zero on any failure. See ../CI-POLICY.md. Nothing merges red.
set -uo pipefail
cd "$(cd "$(dirname "$0")" && pwd)"
OS="$(uname -s)-$(uname -m)"
fail=0
step() { echo; echo ">>> $* [$OS]"; }

# Leftover test artifacts on disk can make a --seed-shuffled run pick up stale DB files (and, with
# orphaned concurrent test binaries, starve each other into phantom failures). Start clean.
find . -maxdepth 1 \( -name 'test_*.db' -o -name 'test_*_wal' -o -name 'repl_backfill' -o -name 'udf' \) -exec rm -rf {} + 2>/dev/null || true

step "zig build (btree + btree-cli)"
zig build || fail=1

if [ $fail -eq 0 ]; then
  step "zig build test (std.testing.allocator leak-checks every test)"
  zig build test || fail=1
fi

if [ $fail -eq 0 ]; then
  step "zig test (wasm engine: metering, determinism, marshalling, hardening, fuzz)"
  # The embedded WebAssembly engine's own suites are standalone files (they import only the
  # engine, not the whole DB). Run each explicitly so the sandbox guarantees are gated.
  # fuzz_test is the M6 fuzzer + determinism-differential + replay suite (embed-wasm.md section 13).
  for t in smoke_test udf_test registry_test fuzz_test; do
    zig test "src/wasm/$t.zig" || fail=1
  done
fi

if [ $fail -eq 0 ]; then
  step "zig test -OReleaseSafe (wasm engine under the safety-checked optimized build)"
  # Zig's ASAN-equivalent for pure-Zig code: ReleaseSafe keeps runtime safety (bounds, overflow,
  # undefined-memory poisoning that catches use-after-free) while optimizing, so the sandbox and
  # the fuzzer are exercised on the release codegen path, not just Debug. (embed-wasm.md M6.)
  for t in smoke_test udf_test registry_test fuzz_test; do
    zig test -OReleaseSafe "src/wasm/$t.zig" || fail=1
  done
fi

echo
if [ $fail -eq 0 ]; then echo "GATE PASS  kaidb  [$OS]"; else echo "GATE FAIL  kaidb  [$OS]"; fi
exit $fail
