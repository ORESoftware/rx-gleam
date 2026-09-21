# rx-gleam cookbook: 20 common problems

All examples assume one `rx/runtime.Runtime`. That runtime owns one Rx actor. Operators do not spawn worker actors. External async work is represented by `Future`/`Effect` and may use any mechanism the application chooses. Every numbered recipe below has a corresponding `cookbook_XX_*_test` in `test/cookbook_test.gleam`.

1. **Emit one value** — `rx.of(value)` emits the value and then completes.
2. **Emit a finite list** — `rx.from_list(values)` preserves list order and then completes.
3. **Complete without values** — `rx.empty()` emits only completion.
4. **Fail immediately** — `rx.fail(reason)` emits only the typed error.
5. **Transform values** — `source |> rx.map(fn(x) { transform(x) })` preserves ordering and terminal behavior.
6. **Filter values** — `source |> rx.filter(fn(x) { predicate(x) })` emits only matching values.
7. **Inspect without changing values** — `source |> rx.tap(fn(x) { inspect(x) })` performs the side effect and forwards the original value.
8. **Subscribe with explicit handlers** — build `rx.observer(on_next, on_error, on_complete)` and pass it to `rx.subscribe`.
9. **Cancel a subscription safely** — retain the returned `Subscription` and call `rx.unsubscribe(subscription)`; cancellation is idempotent and teardown runs once.
10. **Represent an already-successful async result** — `future.pure(value)`.
11. **Represent an already-failed async result** — `future.fail(reason)`.
12. **Transform one async result** — `future.map(my_future, transform)` preserves the source cancellation path.
13. **Register a `use`-friendly async continuation** — `use result <- future.await(my_future)` registers the continuation without blocking the Rx runtime actor.
14. **Turn one Future into a stream** — `flow.from_future(my_future)` emits one success then completes, or emits the Future error.
15. **Adapt an asynchronously-pushing source** — `rx.create` may retain its emitter and call `rx.next`, `rx.error`, or `rx.complete` later from a callback/process/port/FFI boundary.
16. **Serialize async work in strict FIFO order** — `flow.concat_map(source, project)` allows exactly one projected Future in flight and queues later inputs.
17. **Bound concurrent async work** — `flow.merge_map(source, project, concurrency)` dispatches queued inputs FIFO while successful results emit in completion order.
18. **Run concurrently but emit in input order** — `flow.map_ordered(source, project, concurrency)` buffers early completions until all earlier sequence numbers can emit.
19. **Filter with asynchronous predicates** — `flow.filter_async` is serial; `flow.filter_async_concurrent(source, predicate, concurrency)` evaluates predicates concurrently while preserving source order.
20. **Fail fast and suppress late results** — projected errors terminate the flow, cancel other active Futures, discard queued work, and ignore late resolver calls.

## Async source example

```gleam
import rx

fn callback_source(register_callback) {
  rx.create(fn(emitter) {
    register_callback(fn(value) {
      rx.next(emitter, value)
    })

    fn() {
      // unregister/cancel the callback source when possible
      Nil
    }
  })
}
```

`rx.create` does not require the producer to emit during subscription. The emitter is a capability that can be retained by application code and used later; delivery is serialized through the owning runtime actor.

## Future example with an application-owned process

```gleam
import gleam/erlang/process
import rx/future

fn expensive(value: Int) -> future.Future(Int, String) {
  future.new(fn(resolve) {
    let pid = process.spawn_unlinked(fn() {
      resolve(Ok(value * 2))
    })

    fn() {
      process.kill(pid)
    }
  })
}
```

The library does not care that this Future uses a process. A database callback, timer, port, socket, or FFI completion can implement the same contract. `spawn_unlinked` here is an application choice, not something rx-gleam does internally.

## Choosing an async flattening policy

Use `concat_map` when side effects must begin one at a time and preserve FIFO order. Use `merge_map` when bounded parallelism matters and completion order is acceptable. Use `map_ordered` when work may run concurrently but downstream order must match source order. Use `filter_async_concurrent` when the same ordered-concurrency rule is needed for predicates.

## Verification and release gates

The 20 recipes are executable behavior tests. Protocol and state-machine verification are additional gates, not cookbook slots:

- `protocol.validate([OnNext(...), ..., OnComplete])` checks raw notification traces.
- `test/protocol_test.gleam` exhaustively explores bounded notification traces.
- `test/flow_model_test.gleam` exhaustively explores the pure async-flow reference machine.
- `formal/RxProtocol.tla` models notification terminal-state semantics.
- `formal/RxAsyncFlow.tla` models FIFO dispatch, bounded concurrency, ordered/completion-order emission, failure, cancellation, and drain.
- `zed validate`, `sh conformance/check.sh --full`, and `zed r2g` are the release-quality gates. TLC participates in the full gate when `TLA2TOOLS_JAR` is configured; a run without TLC is never described as a formal proof.

## Design rule

ReactiveX defines composition; BEAM defines concurrency. `rx-gleam` serializes observer-visible execution through one runtime actor but does not own the application's concurrency strategy.
