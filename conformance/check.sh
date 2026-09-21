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
  src/rx/future.gleam \
  src/rx/flow.gleam \
  src/rx/flow_model.gleam \
  test/cookbook_test.gleam \
  formal/RxProtocol.tla \
  formal/RxProtocol.cfg \
  formal/RxAsyncFlow.tla \
  formal/RxAsyncFlowOrdered.cfg \
  formal/RxAsyncFlowCompletion.cfg \
  docs/FORMAL_METHODS.md \
  docs/COOKBOOK.md
do
  [ -f "$required" ] || fail "missing required file: $required"
done

# Public protocol constructors must stay exhaustive and explicit.
grep -q 'Terminated, CompleteKind' src/rx/protocol.gleam || fail "protocol terminal matrix is incomplete"
grep -q 'Terminated, ErrorKind' src/rx/protocol.gleam || fail "protocol terminal matrix is incomplete"
grep -q 'Terminated, NextKind' src/rx/protocol.gleam || fail "protocol terminal matrix is incomplete"

# The runtime must remain one serialized actor, not spawn per operator.
actor_count=$(grep -R 'actor\.new' src --include='*.gleam' | wc -l | tr -d ' ')
[ "$actor_count" = "1" ] || fail "expected exactly one actor.new in src, found $actor_count"

# The cookbook is an executable 20-recipe contract.
recipe_count=$(grep -c '^pub fn cookbook_[0-9][0-9]_.*_test()' test/cookbook_test.gleam)
[ "$recipe_count" = "20" ] || fail "expected exactly 20 cookbook tests, found $recipe_count"

if [ "$mode" = "--full" ]; then
  command -v git >/dev/null 2>&1 || fail "git is required for full conformance"
  [ -f .githooks/pre-commit ] || fail "missing pre-commit hook"
  [ -f .githooks/pre-push ] || fail "missing pre-push hook"

  # Run all TLA+ models when available; lack of Java/TLC is not silently proof.
  if [ -n "${TLA2TOOLS_JAR:-}" ]; then
    command -v java >/dev/null 2>&1 || fail "TLA2TOOLS_JAR is set but java is unavailable"
    java -cp "$TLA2TOOLS_JAR" tlc2.TLC -config formal/RxProtocol.cfg formal/RxProtocol.tla
    java -cp "$TLA2TOOLS_JAR" tlc2.TLC -config formal/RxAsyncFlowOrdered.cfg formal/RxAsyncFlow.tla
    java -cp "$TLA2TOOLS_JAR" tlc2.TLC -config formal/RxAsyncFlowCompletion.cfg formal/RxAsyncFlow.tla
  else
    echo "[rx-gleam conformance] TLC not run: set TLA2TOOLS_JAR to enable model checking" >&2
  fi
fi

echo "[rx-gleam conformance] PASS ($mode)"
