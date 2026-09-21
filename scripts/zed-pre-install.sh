#!/bin/sh
set -eu
[ -f .zpkg.toml ] || { echo "missing .zpkg.toml" >&2; exit 1; }
[ -f gleam.toml ] || { echo "missing gleam.toml" >&2; exit 1; }
[ -f src/rx.gleam ] || { echo "missing public rx module" >&2; exit 1; }
[ -f src/rx/runtime.gleam ] || { echo "missing runtime module" >&2; exit 1; }
[ -f src/rx/protocol.gleam ] || { echo "missing protocol module" >&2; exit 1; }
