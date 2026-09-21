#!/bin/sh
set -eu

root="${ZED_PKG_TEST_TARGET:-$(pwd)}"
cd "$root"

for required in \
  .zpkg.toml \
  gleam.toml \
  manifest.toml \
  src/rx.gleam \
  src/rx/runtime.gleam \
  src/rx/protocol.gleam \
  src/rx/effect.gleam \
  src/rx/future.gleam \
  src/rx/flow.gleam \
  docs/COOKBOOK.md \
  docs/USE_CASES.md \
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
grep -q '^# rx-gleam server-side use cases$' docs/USE_CASES.md

grep -q 'name = "gleam_erlang"' manifest.toml
grep -q 'name = "gleam_otp"' manifest.toml
grep -q 'name = "gleam_stdlib"' manifest.toml

use_case_count=$(grep -Ec '^## [0-9]+\. ' docs/USE_CASES.md)
[ "$use_case_count" = "20" ] || {
  echo "[rx-gleam package-smoke] expected 20 use cases, found $use_case_count" >&2
  exit 1
}

echo "[rx-gleam package-smoke] PASS"
