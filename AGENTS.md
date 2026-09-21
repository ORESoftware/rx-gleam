# Agent instructions

## Architecture invariants

- Keep the public reactive runtime single-actor. `src/` should contain exactly one `actor.new` call unless this document and the formal model are intentionally revised together.
- Do not introduce hidden worker pools, schedulers, implicit child processes, per-operator actors, or blocking `process.receive` calls in library source.
- Keep concurrency outside the Rx runtime. Adapt external work through `rx/effect.Effect`, `rx/future.Future`, or another explicitly modeled nonblocking boundary.
- Observer callbacks, diagnostics, Future start callbacks, producer setup, and projection functions must return promptly; blocking work belongs in application-owned concurrency.
- Subscription and flow registration must remain reentrant. A callback running on the runtime actor must be able to subscribe another stream on that same runtime without waiting for the actor to reply to itself.
- `runtime.stop` must tear down active subscriptions and cancel active async flows before the runtime actor exits.
- Keep `Observable(value, error)`, `Future(value, error)`, and `Effect(value, error)` fully typed. Do not erase public values or errors to `Dynamic` for implementation convenience.
- Preserve the protocol grammar `Next* (Error | Complete)?`.

## State machines and formal methods

- Prefer explicit algebraic data types and exhaustive `case` expressions over wildcard branches in protocol/state-machine code.
- Any new stateful operator must define its transition states, terminal behavior, cancellation behavior, and error behavior before implementation.
- Add bounded exhaustive trace generation and differential/reference-model tests for every new state machine.
- Update `formal/RxProtocol.tla`, `formal/RxAsyncFlow.tla`, or add a focused TLA+ module when observable protocol or concurrency semantics change.
- Never describe a test run as a formal proof. Only claim TLC/model-checking evidence when TLC actually ran successfully.
- `sh conformance/check.sh --full` must fail when `TLA2TOOLS_JAR` or Java is unavailable; it must never silently degrade to a non-formal gate.

## Error handling

- Domain errors use the `error` type parameter.
- Protocol/library defects use dedicated library error types and must not be silently converted into domain errors.
- A stopped runtime is reported as typed `RuntimeStopped`; invalid flow concurrency is `InvalidConcurrency`.
- Cancellation is not an error and must not fabricate `Error` or `Complete` unless an operator's documented contract explicitly requires it.
- Prefer total functions; avoid panic/assert in library code when an ordinary typed result can represent the condition.

## Operator design

- Operators must compose without changing the single-actor serialization guarantee.
- `merge_map`, `concat_map`, `switch_map`, and `exhaust_map` should share one internal flattening transition machine rather than drift into four independent implementations.
- Every operator needs protocol-preservation, ordering, cancellation, resource-lifetime, reentrancy, composition, late-completion, and shutdown tests.
- Any operator that buffers input must document whether buffering is bounded or unbounded and where backpressure/load shedding belongs.

## Tooling and quality gates

- `.zpkg.toml` is the zed-pkg package authority.
- `manifest.toml` is the committed Gleam dependency lock and must not drift under `gleam deps download`.
- Keep zed lifecycle hooks, `publish.smoke_test`, `.githooks/`, and `conformance/check.sh` aligned.
- Before push, run `zed validate` and `sh conformance/check.sh --full` with `TLA2TOOLS_JAR` set.
- Use `zed r2g` before publishing a release so the pruned installed artifact is tested rather than only the source tree.
- Keep the cookbook contract at exactly 20 executable recipes unless its tests/conformance contract is intentionally revised in the same change.
- Keep `docs/USE_CASES.md` at exactly 20 server-side use cases unless its conformance contract is intentionally revised in the same change; the first five anchor cases must retain executable integration tests.
- CI must keep a declared-minimum Gleam compatibility job and a current-toolchain full Zed/TLC/conformance job, with third-party Actions pinned by commit SHA.
