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
  src/rx/effect.gleam \
  src/rx/future.gleam \
  src/rx/flow.gleam \
  docs/COOKBOOK.md \
  docs/FORMAL_METHODS.md \
  formal/RxProtocol.tla \
  formal/RxAsyncFlow.tla
do
  [ -f "$required" ] || {
    echo "[rx-gleam package-smoke] missing packaged file: $required" >&2
    exit 1
  }
done

grep -q 'pub opaque type Observable' src/rx.gleam
grep -q 'pub opaque type Runtime' src/rx/runtime.gleam
grep -q 'pub opaque type Effect' src/rx/effect.gleam
grep -q 'pub opaque type Future' src/rx/future.gleam
grep -q 'pub fn concat_map' src/rx/flow.gleam
grep -q 'pub fn merge_map' src/rx/flow.gleam
grep -q 'pub fn map_ordered' src/rx/flow.gleam
grep -q 'pub fn filter_async_concurrent' src/rx/flow.gleam

echo "[rx-gleam package-smoke] PASS"
