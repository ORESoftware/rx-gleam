#!/bin/sh
set -eu

mode="${1:---quick}"
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

fail() {
  echo "[rx-gleam conformance] $*" >&2
  exit 1
}

case "$mode" in
  --quick|--full) ;;
  *) fail "unknown mode: $mode (expected --quick or --full)" ;;
esac

command -v gleam >/dev/null 2>&1 || fail "gleam is required"

gleam format --check src test
gleam test

for required in \
  LICENSE \
  gleam.toml \
  manifest.toml \
  .zpkg.toml \
  src/rx.gleam \
  src/rx/eager.gleam \
  src/rx/runtime.gleam \
  src/rx/protocol.gleam \
  src/rx/effect.gleam \
  src/rx/future.gleam \
  src/rx/flow.gleam \
  src/rx/flow_model.gleam \
  test/eager_test.gleam \
  test/hardening_test.gleam \
  test/cookbook_test.gleam \
  test/use_cases_test.gleam \
  test/runtime_shutdown_test.gleam \
  formal/RxProtocol.tla \
  formal/RxProtocol.cfg \
  formal/RxLifecycle.tla \
  formal/RxLifecycle.cfg \
  formal/RxAsyncFlow.tla \
  formal/RxAsyncFlowOrdered.cfg \
  formal/RxAsyncFlowCompletion.cfg \
  docs/FORMAL_METHODS.md \
  docs/COOKBOOK.md \
  docs/USE_CASES.md \
  docs/FULL_STACK_BROWSER.md
do
  [ -f "$required" ] || fail "missing required file: $required"
done

grep -q '^MIT License$' LICENSE || fail "LICENSE is not the declared MIT license"

# Public protocol constructors must stay exhaustive and explicit.
grep -q 'Terminated, CompleteKind' src/rx/protocol.gleam || fail "protocol terminal matrix is incomplete"
grep -q 'Terminated, ErrorKind' src/rx/protocol.gleam || fail "protocol terminal matrix is incomplete"
grep -q 'Terminated, NextKind' src/rx/protocol.gleam || fail "protocol terminal matrix is incomplete"

# The eager API is part of the public contract and must keep its bridge into
# the actor-backed Observable API.
grep -q 'pub opaque type Eager' src/rx/eager.gleam || fail "missing eager public type"
grep -q 'pub fn to_observable' src/rx/eager.gleam || fail "missing eager-to-observable bridge"

# The runtime must remain one serialized actor. Library source must not hide
# workers or blocking receives behind operators/Futures.
actor_count=$(grep -R 'actor\.new' src --include='*.gleam' | wc -l | tr -d ' ')
[ "$actor_count" = "1" ] || fail "expected exactly one actor.new in src, found $actor_count"
if grep -R -n 'process\.spawn\|process\.spawn_unlinked' src --include='*.gleam'; then
  fail "library source must not spawn hidden worker processes"
fi
if grep -R -n 'process\.receive' src --include='*.gleam'; then
  fail "library source must not block on process.receive"
fi

grep -q 'RuntimeStopped' src/rx/runtime.gleam || fail "runtime must expose typed stopped-runtime registration failure"
grep -q 'cancel_all_entries(state.entries)' src/rx/runtime.gleam || fail "runtime stop must tear down active subscriptions"

# The cookbook is an executable 20-recipe contract.
recipe_count=$(grep -c '^pub fn cookbook_[0-9][0-9]_.*_test()' test/cookbook_test.gleam)
[ "$recipe_count" = "20" ] || fail "expected exactly 20 cookbook tests, found $recipe_count"

# The server-side use-cases guide is intentionally a 20-case contract, with the
# five anchor cases backed by integration tests.
use_case_count=$(grep -Ec '^## [0-9]+\. ' docs/USE_CASES.md)
[ "$use_case_count" = "20" ] || fail "expected exactly 20 server-side use cases, found $use_case_count"
use_case_test_count=$(grep -c '^pub fn use_case_0[1-5]_.*_test()' test/use_cases_test.gleam)
[ "$use_case_test_count" = "5" ] || fail "expected exactly 5 anchor use-case tests, found $use_case_test_count"

grep -q '^## 1\. Async queue with an async processing step$' docs/USE_CASES.md || fail "missing async queue use case"
grep -q '^## 2\. De-duplicating requests or stream items with an in-memory set$' docs/USE_CASES.md || fail "missing de-duplication use case"
grep -q '^## 3\. Grouping requests by tenant, partition, account, or resource key$' docs/USE_CASES.md || fail "missing grouping use case"
grep -q '^## 4\. Merging multiple server-side push sources$' docs/USE_CASES.md || fail "missing stream merging use case"
grep -q '^## 5\. Rebasing heterogeneous streams onto one canonical stream$' docs/USE_CASES.md || fail "missing stream rebasing use case"

# Browser/full-stack documentation must preserve the target distinction: Gleam
# application code targets JavaScript in browsers; WASM is an interoperability
# layer rather than a claimed third application target.
grep -q 'Gleam -> JavaScript' docs/FULL_STACK_BROWSER.md || fail "full-stack browser doc must describe the JavaScript target"
grep -q 'does not currently expose WebAssembly as an application-code compilation target' docs/FULL_STACK_BROWSER.md || fail "full-stack browser doc must not imply direct Gleam-to-WASM application compilation"
grep -q 'gleam check --target javascript' docs/FULL_STACK_BROWSER.md || fail "full-stack browser doc must define a future JavaScript conformance gate"

if [ "$mode" = "--full" ]; then
  command -v git >/dev/null 2>&1 || fail "git is required for full conformance"
  [ -f .githooks/pre-commit ] || fail "missing pre-commit hook"
  [ -f .githooks/pre-push ] || fail "missing pre-push hook"
  [ -n "${TLA2TOOLS_JAR:-}" ] || fail "full conformance requires TLA2TOOLS_JAR"
  [ -f "$TLA2TOOLS_JAR" ] || fail "TLA2TOOLS_JAR does not name a file: $TLA2TOOLS_JAR"
  command -v java >/dev/null 2>&1 || fail "java is required for full conformance"

  java -XX:+UseParallelGC -cp "$TLA2TOOLS_JAR" tlc2.TLC -config formal/RxProtocol.cfg formal/RxProtocol.tla
  java -XX:+UseParallelGC -cp "$TLA2TOOLS_JAR" tlc2.TLC -config formal/RxLifecycle.cfg formal/RxLifecycle.tla
  java -XX:+UseParallelGC -cp "$TLA2TOOLS_JAR" tlc2.TLC -config formal/RxAsyncFlowOrdered.cfg formal/RxAsyncFlow.tla
  java -XX:+UseParallelGC -cp "$TLA2TOOLS_JAR" tlc2.TLC -config formal/RxAsyncFlowCompletion.cfg formal/RxAsyncFlow.tla
fi

echo "[rx-gleam conformance] PASS ($mode)"
