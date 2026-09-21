#!/bin/sh
set -eu

mode="${1:---quick}"
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

fail() {
  echo "[rx-gleam conformance] $*" >&2
  exit 1
}

command -v gleam >/dev/null 2>&1 || fail "gleam is required"

gleam format --check src test
gleam test

for required in \
  gleam.toml \
  .zpkg.toml \
  src/rx.gleam \
  src/rx/runtime.gleam \
  src/rx/protocol.gleam \
  src/rx/effect.gleam \
  formal/RxProtocol.tla \
  formal/RxProtocol.cfg \
  docs/FORMAL_METHODS.md \
  docs/COOKBOOK.md
do
  [ -f "$required" ] || fail "missing required file: $required"
done

# Public protocol constructors must stay exhaustive and explicit.
grep -q 'Terminated, Complete' src/rx/protocol.gleam || fail "protocol terminal matrix is incomplete"
grep -q 'Terminated, Error' src/rx/protocol.gleam || fail "protocol terminal matrix is incomplete"
grep -q 'Terminated, Next' src/rx/protocol.gleam || fail "protocol terminal matrix is incomplete"

# The runtime must remain one serialized actor, not spawn per operator.
actor_count=$(grep -R 'actor\.new' src --include='*.gleam' | wc -l | tr -d ' ')
[ "$actor_count" = "1" ] || fail "expected exactly one actor.new in src, found $actor_count"

if [ "$mode" = "--full" ]; then
  command -v git >/dev/null 2>&1 || fail "git is required for full conformance"
  [ -f .githooks/pre-commit ] || fail "missing pre-commit hook"
  [ -f .githooks/pre-push ] || fail "missing pre-push hook"

  # Run TLC when available; lack of Java/TLC is not silently treated as proof.
  if [ -n "${TLA2TOOLS_JAR:-}" ]; then
    command -v java >/dev/null 2>&1 || fail "TLA2TOOLS_JAR is set but java is unavailable"
    java -cp "$TLA2TOOLS_JAR" tlc2.TLC -config formal/RxProtocol.cfg formal/RxProtocol.tla
  else
    echo "[rx-gleam conformance] TLC not run: set TLA2TOOLS_JAR to enable model checking" >&2
  fi
fi

echo "[rx-gleam conformance] PASS ($mode)"
