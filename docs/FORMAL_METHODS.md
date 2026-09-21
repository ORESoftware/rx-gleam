# Formal methods and conformance

`rx-gleam` treats observable and async-flow semantics as protocols, not as informal callback conventions.

## Core notification safety properties

The public notification contract is the ReactiveX grammar:

```text
OnNext* (OnError | OnComplete)?
```

For every subscription:

- `OnNext` is accepted only while the protocol is `Open`;
- exactly one terminal event may be accepted;
- after `OnError` or `OnComplete`, the protocol is absorbing;
- `OnError` and `OnComplete` are mutually exclusive;
- observer work is dispatched through one runtime actor and therefore serialized;
- operators must not create hidden actors or schedulers;
- async/concurrent work is represented by `Future`/`Effect` and owned by application code.

`src/rx/protocol.gleam` is the executable notification state machine. Its `case` expression intentionally enumerates every `(Phase, Notification)` pair so the Gleam compiler participates in exhaustiveness checking.

## Async flow safety properties

`src/rx/flow_model.gleam` is an implementation-independent reference machine for async flattening. It models sequence assignment, FIFO pending work, bounded active work, out-of-order completion, ordered buffering, failure, cancellation, input completion, and drain.

The runtime implementation in `src/rx/runtime.gleam` must refine these properties:

- pending work starts in FIFO sequence order;
- active projected work never exceeds the configured concurrency bound;
- `concat_map` is the concurrency-1 specialization;
- `merge_map` emits successful values in completion order;
- `map_ordered` and concurrent async filtering may execute out of order but emit in input sequence order;
- projected or source errors are fail-fast;
- failure/cancellation clears queued work and cancels active work through application-supplied cancellation functions;
- late or duplicate completion for a removed flow is ignored;
- source completion becomes downstream completion only after pending, active, and ordered-completion buffers drain.

## Exhaustive finite traces

`test/protocol_test.gleam` enumerates every notification trace over `{OnNext, OnError, OnComplete}` through length 6 and compares the implementation with an independently written reference transition system.

`test/flow_model_test.gleam` performs bounded exhaustive exploration of the pure async-flow reference machine for concurrency 1, ordered concurrency 2, and completion-order concurrency 2. Every reached state must satisfy capacity, sequence, disjointness, and terminal-state invariants.

Finite exhaustive checking is not a proof for arbitrary trace length, so the repository also carries TLA+ models.

## TLA+

`formal/RxProtocol.tla` models notification terminal-state semantics. `formal/RxProtocol.cfg` supplies its TLC bounds.

`formal/RxAsyncFlow.tla` independently models the async flow scheduler with explicit `Enqueue`, `Start`, successful/failed completion, ordered flush, source failure, input completion, cancellation, and drain actions. It checks bounded concurrency, unique/disjoint work sets, known sequence bounds, ordered-prefix emission, and terminal work cleanup.

The async model is checked in both public ordering modes:

- `formal/RxAsyncFlowOrdered.cfg` — concurrency 2 with input-order emission;
- `formal/RxAsyncFlowCompletion.cfg` — concurrency 2 with completion-order emission.

Run all models through the same conformance gate:

```sh
export TLA2TOOLS_JAR=/path/to/tla2tools.jar
sh conformance/check.sh --full
```

The conformance script explicitly reports when TLC was not executed. CI or a local run without TLC must never be described as a formal proof.

## Operator proof obligations

Every new operator must document and test these obligations:

1. **Protocol preservation**: it never emits after termination and never emits two terminals.
2. **Ordering**: output ordering is defined for every input ordering it accepts.
3. **Cancellation**: cancellation is idempotent and does not manufacture terminal notifications.
4. **Error mapping**: user errors, source errors, cancellation, and library defects are not conflated.
5. **Serialization**: callbacks visible to an observer run only through the owning `Runtime` actor.
6. **Resource lifetime**: every acquired external resource has an explicit release/cancel path.
7. **Reentrancy**: synchronous Future/source completion cannot violate state transitions.
8. **Composition**: nesting the operator in another operator preserves the same protocol laws.
9. **Concurrency bound**: the number of active projected Futures never exceeds the configured limit.
10. **Late completion safety**: a cancelled/failed flow cannot be resurrected by a resolver called afterward.

Stateful flattening operators share the same actor-owned flow transition machinery rather than maintaining unrelated schedulers. A new scheduling policy should first be expressible in the pure flow model and its invariants before it is considered stable.

## Error model

Domain errors remain typed as the `error` parameter of `Observable(value, error)`, `Future(value, error)`, and `Effect(value, error)`. Library protocol/configuration violations use library error types such as `ProtocolError` and `RuntimeError`; they are never silently converted to user-domain errors.

The library should prefer total functions and exhaustive pattern matching. A new wildcard branch in protocol/state-machine code requires justification in review because it can hide newly introduced states from compiler exhaustiveness checks.

## Quality gates

`.zpkg.toml`, `.githooks/pre-commit`, `.githooks/pre-push`, and `conformance/check.sh` form one quality-control chain. Quick hooks run formatting and tests. Pre-push additionally requires zed package validation and the full conformance suite. The conformance script guards the architectural invariant that source code contains exactly one `actor.new` site and that the cookbook has exactly 20 executable recipe tests.
