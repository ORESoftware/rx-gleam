#!/bin/sh
set -eu

for required in \
  LICENSE \
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
  docs/FORMAL_METHODS.md
do
  [ -f "$required" ] || {
    echo "[rx-gleam zed-pre-install] missing required file: $required" >&2
    exit 1
  }
done
