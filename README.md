# rx-gleam

A serialized, single-actor Reactive Extensions library for Gleam.

## Design

`rx-gleam` separates **reactive composition** from **application concurrency**:

- one `rx/runtime.Runtime` owns exactly one OTP actor;
- observer callbacks are dispatched through that actor and therefore execute serially;
- operators do not create hidden actors, worker pools, or schedulers;
- callers can use BEAM processes, timers, sockets, ports, FFI callbacks, or any other async mechanism through `rx/effect.Effect`;
- observable values and errors stay statically typed; the runtime does not erase them to `Dynamic`.

The guiding rule is: **ReactiveX defines composition; BEAM defines concurrency.**

The initial implementation targets current Gleam 1.15+ and Gleam OTP 1.x APIs (`gleam_otp` 1.3.x / `gleam_erlang` 1.3.x). CI is pinned to Gleam 1.18 and OTP 28.

## Core API

```gleam
import gleam/io
import rx
import rx/runtime

pub fn example() {
  let assert Ok(rt) = runtime.start()

  let assert Ok(subscription) =
    rx.from_list([1, 2, 3, 4, 5])
    |> rx.map(fn(x) { x * 2 })
    |> rx.filter(fn(x) { x > 4 })
    |> rx.subscribe(
      rt,
      rx.observer(
        fn(value) { io.debug(value) },
        fn(error) { io.debug(error) },
        fn() { Nil },
      ),
    )

  rx.unsubscribe(subscription)
}
```

`subscribe` returns `Result(Subscription, RuntimeError)` rather than hiding runtime startup failure behind a panic. Cancellation is idempotent and subscription teardown runs at most once.

Current primitives include `Observable(value, error)`, `Observer(value, error)`, `Subscription`, `Emitter(value, error)`, `Runtime`, `RuntimeError`, `Effect(value, error)`, `create`, `of`, `from_list`, `empty`, `fail`, `map`, `filter`, `tap`, cancellation, and effect-to-observable conversion.

## Protocol contract

Every subscription follows the ReactiveX grammar:

```text
Next* (Error | Complete)?
```

`src/rx/protocol.gleam` models this explicitly as an exhaustive state machine. The runtime uses that model before delivering notifications, so invalid traces such as `Complete, Next(_)` or `Error(_), Complete` are not merely documented—they are rejected before the observer callback runs.

The repository includes:

- exhaustive generated protocol traces through length 6;
- an independently implemented reference model used as a differential oracle;
- runtime tests for post-terminal rejection and exactly-once teardown;
- a TLA+ specification under `formal/`;
- explicit operator proof obligations in [`docs/FORMAL_METHODS.md`](docs/FORMAL_METHODS.md).

A finite test bound is not described as a mathematical proof. TLC is the formal-model checker and the conformance script reports whether it actually ran.

## Async without an Rx scheduler

`Effect(value, error)` is deliberately agnostic:

```gleam
Effect(
  fn(resolve: fn(Result(value, error)) -> Nil) -> fn() -> Nil,
)
```

The implementation can launch a process, issue I/O, register a timer, call an Erlang library, or bridge an FFI callback. Completion feeds back into the observable runtime and observer-visible work is serialized again by the owning actor.

There is deliberately no unsafe `effect.then` shortcut. Correct dependent-effect cancellation needs actor-owned state, so that functionality belongs in the shared flattening state machine for `concat_map`, `merge_map`, `switch_map`, and `exhaust_map`.

## 20 common problems

See [`docs/COOKBOOK.md`](docs/COOKBOOK.md) for 20 concrete patterns covering finite streams, errors, cancellation, effects, child processes, protocol validation, and formal checks.

## Quality control

The repository uses zed-pkg as its package/governance layer:

```sh
zed validate
```

`.zpkg.toml` defines canonical `pre-install` / `post-install` lifecycle hooks and an `r2g` `publish.smoke_test`. Git hooks are committed under `.githooks/`:

```sh
git config core.hooksPath .githooks
```

`pre-commit` runs the quick conformance gate. `pre-push` refuses to bypass `zed validate` and runs full conformance.

Direct source-tree checks:

```sh
sh conformance/check.sh --quick
sh conformance/check.sh --full
```

To include the TLA+ model check:

```sh
export TLA2TOOLS_JAR=/path/to/tla2tools.jar
sh conformance/check.sh --full
```

Before a release, test the actual packaged artifact rather than only the checkout:

```sh
zed r2g
```

## Roadmap

The next state-machine layer will add shared implementations for `merge_map`, `concat_map`, `switch_map`, and `exhaust_map`, followed by `scan`, subjects, timers, combination operators, retry/recovery, and virtual-time testing. Stateful operators must extend the formal transition model before they are considered stable.
