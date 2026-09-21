# Formal methods and conformance

`rx-gleam` treats observable semantics as a protocol, not as informal callback convention.

## Core safety properties

The public contract is the ReactiveX grammar:

```text
Next* (Error | Complete)?
```

For every subscription:

- `Next` is accepted only while the protocol is `Open`.
- exactly one terminal event may be accepted;
- after `Error` or `Complete`, the protocol is absorbing;
- `Error` and `Complete` are mutually exclusive;
- observer work is dispatched through one runtime actor and therefore serialized;
- operators must not create hidden actors or schedulers;
- async/concurrent work is represented by `Effect` and owned by application code.

`src/rx/protocol.gleam` is the executable state machine. Its `case` expression intentionally enumerates every `(Phase, Notification)` pair so the Gleam compiler participates in exhaustiveness checking.

## Exhaustive finite traces

`test/protocol_test.gleam` enumerates every trace over `{Next, Error, Complete}` through length 6 and compares the implementation with an independently written reference transition system. Increasing the bound is cheap and encouraged when the alphabet grows.

Finite exhaustive checking is not a proof for arbitrary trace length, so the repository also carries a TLA+ model.

## TLA+

`formal/RxProtocol.tla` models the protocol independently of Gleam implementation details. `formal/RxProtocol.cfg` checks bounded traces and invariants with TLC.

Run:

```sh
export TLA2TOOLS_JAR=/path/to/tla2tools.jar
sh conformance/check.sh --full
```

The conformance script explicitly reports when TLC was not executed. CI must never describe a run without TLC as a formal proof.

## Operator proof obligations

Every new operator must document and test these obligations:

1. **Protocol preservation**: it never emits after termination and never emits two terminals.
2. **Ordering**: output ordering is defined for every input ordering it accepts.
3. **Cancellation**: cancellation is idempotent and does not manufacture terminal notifications.
4. **Error mapping**: user errors, source errors, cancellation, and library defects are not conflated.
5. **Serialization**: callbacks visible to an observer run only through the owning `Runtime` actor.
6. **Resource lifetime**: every acquired external resource has an explicit release/cancel path.
7. **Reentrancy**: synchronous source/effect completion cannot violate state transitions.
8. **Composition**: nesting the operator in another operator preserves the same protocol laws.

Stateful flattening operators (`merge_map`, `concat_map`, `switch_map`, `exhaust_map`) should share one internal transition machine rather than four unrelated implementations. The transition model should be extended before those operators are considered stable.

## Error model

Domain errors remain typed as the `error` parameter of `Observable(value, error)` and `Effect(value, error)`. Library protocol violations use `ProtocolError`; they are never silently converted to user-domain errors.

The library should prefer total functions and exhaustive pattern matching. A new wildcard branch in protocol/state-machine code requires justification in review because it can hide newly introduced states from compiler exhaustiveness checks.

## Quality gates

`.zpkg.toml`, `.githooks/pre-commit`, `.githooks/pre-push`, and `conformance/check.sh` form one quality-control chain. Quick hooks run formatting and tests. Pre-push additionally validates the zed package contract and the full conformance suite. The script also guards the architectural invariant that source code contains exactly one `actor.new` site.
