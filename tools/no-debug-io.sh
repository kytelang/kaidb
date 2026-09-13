#!/usr/bin/env bash
# D3 lint: no raw std.debug.print stdout in the DB hot paths (recovery, storage, durability, server,
# replication, query execution). Debug/trace output must go through the scoped, leveled std.log
# (silent at the default level). CLI user-facing output (src/cli.zig) is exempt. Non-zero on violation.
set -u
HERE="$(cd "$(dirname "$0")/.." && pwd)"
cd "$HERE" || exit 2
hits="$(grep -rnE '^[[:space:]]*std\.debug\.print' src/schema src/storage src/durability src/query src/proto --include='*.zig' 2>/dev/null)"
if [ -n "$hits" ]; then
  echo "no-debug-io: raw std.debug.print in a hot path (use std.log.scoped(...).debug instead):" >&2
  echo "$hits" >&2
  exit 1
fi
echo "no-debug-io: clean (no raw debug I/O in DB hot paths)"
