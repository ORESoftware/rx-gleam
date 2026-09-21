# Agent instructions

## Architecture invariants

- Keep the public reactive runtime single-actor. `src/` should contain exactly one `actor.new` call unless this document and the formal model are intentionally revised together.
- Do not introduce hidden worker pools, schedulers, implicit child processes, or per-operator actors.
- Keep concurrency outside the Rx runtime. Adapt external work through `rx/effect.Effect` or another explicitly modeled boundary.
- Keep `Observable(value, error)` and `Effect(value, error)` fully typed. Do not erase public values or errors to `Dynamic` for implementation convenience.
- Preserve the protocol grammar `Next* (Error | Complete)?`.

## State machines and formal methods

- Prefer explicit algebraic data types and exhaustive `case` expressions over wildcard branches in protocol/state-machine code.
- Any new stateful operator must define its transition states, terminal behavior, cancellation behavior, and error behavior before implementation.
- Add bounded exhaustive trace generation and differential/reference-model tests for every new state machine.
- Update `formal/RxProtocol.tla` or add a focused TLA+ module when observable protocol or concurrency semantics change.
- Never describe a test run as a formal proof. Only claim TLC/model-checking evidence when TLC actually ran successfully.

## Error handling

- Domain errors use the `error` type parameter.
- Protocol/library defects use dedicated library error types and must not be silently converted into domain errors.
- Cancellation is not an error and must not fabricate `Error` or `Complete` unless an operator's documented contract explicitly requires it.
- Prefer total functions; avoid panic/assert in library code when an ordinary typed result can represent the condition.

## Operator design

- Operators must compose without changing the single-actor serialization guarantee.
- `merge_map`, `concat_map`, `switch_map`, and `exhaust_map` should share one internal flattening transition machine rather than drift into four independent implementations.
- Every operator needs protocol-preservation, ordering, cancellation, resource-lifetime, reentrancy, and composition tests.

## Tooling and quality gates

- `.zpkg.toml` is the zed-pkg package authority.
- Keep zed lifecycle hooks, `publish.smoke_test`, `.githooks/`, and `conformance/check.sh` aligned.
- Before push, run `zed validate` and `sh conformance/check.sh --full`.
- Use `zed r2g` before publishing a release so the pruned installed artifact is tested rather than only the source tree.
- Keep the cookbook at 20 or more practical end-user problems and update it when public APIs change.
