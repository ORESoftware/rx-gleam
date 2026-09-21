# rx-gleam

A serialized, single-actor Reactive Extensions library for Gleam.

## Design

`rx-gleam` separates **reactive composition** from **application concurrency**:

- one `rx/runtime.Runtime` owns exactly one OTP actor;
- observer callbacks are dispatched through that actor and therefore execute serially;
- operators do not create hidden actors, worker pools, or schedulers;
- callers can use BEAM processes, timers, sockets, ports, FFI callbacks, or any other async mechanism through `rx/effect.Effect` / `rx/future.Future`;
- observable values and errors stay statically typed; the runtime does not erase them to `Dynamic`.

The guiding rule is: **ReactiveX defines composition; BEAM defines concurrency.**

The package declares Gleam `>= 1.15.4`. CI verifies that floor on OTP 27 and separately runs the full Gleam 1.18 / OTP 28 conformance gate. `manifest.toml` is committed and checked for dependency-resolution drift.

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

`subscribe` returns `Result(Subscription, RuntimeError)`. A stopped runtime produces the typed `RuntimeStopped` error instead of waiting on a request/reply timeout. Cancellation is idempotent and subscription teardown runs at most once.

Registration is deliberately nonblocking and reentrant: an observer callback may subscribe another Observable on the same runtime without asking the runtime actor to synchronously reply to itself.

`runtime.stop(runtime)` is also nonblocking, so it is safe to request shutdown from inside an observer callback. Before the actor exits it runs stored subscription teardowns and cancels active async-flow Futures.

Current primitives include `Observable(value, error)`, `Observer(value, error)`, `Subscription`, `Emitter(value, error)`, `Runtime`, `RuntimeError`, `Effect(value, error)`, `Future(value, error)`, `create`, `of`, `from_list`, `empty`, `fail`, `map`, `filter`, `tap`, cancellation, `from_future`, `concat_map`, bounded `merge_map`, ordered concurrent mapping, and async filtering.

## Callback discipline

The runtime actor is a **serialization boundary, not a blocking-work executor**. Observer handlers, diagnostics, Observable producer setup, Future start callbacks, and async projection functions must return promptly. They should start or register application-owned async work and report completion later.

Do not call blocking `process.receive`, sleep, perform long CPU work, or run blocking I/O inside an Rx callback. If an operation can block, move it to an application-owned process/OTP component or a nonblocking callback API and expose it as a `Future`/`Effect`.

The library enforces this architectural side of the contract mechanically: production `src/` may contain exactly one `actor.new`, and conformance rejects library-owned `process.spawn*` and `process.receive` calls.

A `Future` producer must invoke its resolver at most once. When a Future participates in an Observable flow, duplicate or late completions are additionally suppressed by the actor-owned flow state machine.

## Protocol contract

Every subscription follows the ReactiveX grammar:

```text
Next* (Error | Complete)?
```

`src/rx/protocol.gleam` models this explicitly as an exhaustive state machine. The runtime uses that model before delivering notifications, so invalid traces such as `Complete, Next(_)` or `Error(_), Complete` are not merely documented—they are rejected before the observer callback runs.

The repository includes:

- exhaustive generated protocol traces through length 6;
- an independently implemented reference model used as a differential oracle;
- runtime tests for post-terminal rejection, reentrant subscription, shutdown cleanup, and exactly-once teardown;
- TLA+ specifications under `formal/` for the notification protocol and async-flow machine;
- explicit operator proof obligations in [`docs/FORMAL_METHODS.md`](docs/FORMAL_METHODS.md).

A finite test bound is not described as a mathematical proof. TLC is the formal-model checker; `--full` conformance refuses to report PASS unless TLC is configured and all models succeed.

## Async without an Rx scheduler

`Effect(value, error)` / `Future(value, error)` are deliberately execution-agnostic. The producer may launch a process, issue I/O, register a timer, call an Erlang library, or bridge an FFI callback. Completion feeds back into the observable runtime and observer-visible work is serialized again by the owning actor.

Async flattening state is owned by that same runtime actor. `concat_map` provides strict FIFO single-flight work; `merge_map` bounds concurrent Futures and emits in completion order; `map_ordered` allows concurrent work while preserving input order; async filters use the same machinery.

## Cookbook

See [`docs/COOKBOOK.md`](docs/COOKBOOK.md) for the 20 executable API recipes mirrored 1:1 by `test/cookbook_test.gleam`.

## Server-side use cases

See [`docs/USE_CASES.md`](docs/USE_CASES.md) for 20 problem statements with Gleam solutions focused on service and BEAM workloads, including:

- async FIFO queues with asynchronous processing;
- in-memory request/stream-item de-duplication;
- keyed request grouping/partition routing;
- merging multiple push sources;
- rebasing heterogeneous inputs onto one canonical event stream;
- bounded RPC fan-out, ordered enrichment, async authorization, transactional writes, retries, batching, timeouts, dependency joins, pause/resume gates, reducer actors, watchdogs, dependent service calls, progress streams, moving aggregates, and connection-scoped cancellation.

The first five use-case patterns have dedicated integration tests in `test/use_cases_test.gleam`. The examples keep application-owned state and concurrency explicit; they do not invent hidden Rx actors or claim operators that do not exist yet.

## Quality control

The repository uses zed-pkg as its package/governance layer:

```sh
zed validate
```

`.zpkg.toml` defines canonical `pre-install` / `post-install` lifecycle hooks and an `r2g` `publish.smoke_test`. Git hooks are committed under `.githooks/`:

```sh
git config core.hooksPath .githooks
```

`pre-commit` runs the quick conformance gate. `pre-push` refuses to bypass Zed validation or the formal gate and requires `TLA2TOOLS_JAR`.

Quick source-tree checks:

```sh
sh conformance/check.sh --quick
```

Full conformance requires TLA+ Tools and will fail rather than silently skip model checking:

```sh
export TLA2TOOLS_JAR=/path/to/tla2tools.jar
sh conformance/check.sh --full
```

Before a release, test the actual packaged artifact rather than only the checkout:

```sh
zed r2g
```

## Roadmap

The next stateful/combinator layer should cover observable-to-observable `merge` / `concat`, `distinct` variants, keyed grouping/partitioning, `scan`, buffering/windowing, retry/recovery, timeout, `zip` / `combine_latest`, `switch_map`, `exhaust_map`, subjects, timers, and virtual-time testing. Stateful operators must extend the executable reference model and formal/conformance coverage before they are considered stable.
