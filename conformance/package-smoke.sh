#!/bin/sh
set -eu

root="${ZED_PKG_TEST_TARGET:-$(pwd)}"
cd "$root"

for required in \
  .zpkg.toml \
  gleam.toml \
  src/rx.gleam \
  src/rx/runtime.gleam \
  src/rx/protocol.gleam \
  src/rx/effect.gleam
do
  [ -f "$required" ] || {
    echo "[rx-gleam package-smoke] missing packaged file: $required" >&2
    exit 1
  }
done

grep -q 'pub opaque type Observable' src/rx.gleam
grep -q 'pub opaque type Runtime' src/rx/runtime.gleam
grep -q 'pub opaque type Effect' src/rx/effect.gleam

echo "[rx-gleam package-smoke] PASS"
