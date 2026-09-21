# rx-gleam cookbook: 20 common problems

All examples assume one `rx/runtime.Runtime`. That runtime owns one actor. Operators do not spawn actors. External async work is represented by `rx/effect.Effect` and may use any mechanism the application chooses.

1. **Emit one value** — `rx.of(value)`.
2. **Emit a finite list** — `rx.from_list(values)`.
3. **Complete without values** — `rx.empty()`.
4. **Fail immediately** — `rx.fail(reason)`.
5. **Transform values** — `source |> rx.map(fn(x) { transform(x) })`.
6. **Filter values** — `source |> rx.filter(fn(x) { predicate(x) })`.
7. **Inspect without changing values** — `source |> rx.tap(fn(x) { log(x) })`.
8. **Subscribe with three explicit terminal paths** — build `rx.observer(on_next, on_error, on_complete)` and pass it to `rx.subscribe`.
9. **Cancel a subscription** — retain the returned `Subscription` and call `rx.unsubscribe(subscription)`.
10. **Wrap callback-based async work** — `effect.new(fn(resolve) { start_work(resolve) })`.
11. **Turn one async result into a stream** — `effect.to_observable(my_effect)`.
12. **Represent successful async work** — `effect.pure(value)`.
13. **Represent failed async work** — `effect.fail(reason)`.
14. **Transform an async result** — `effect.map(my_effect, transform)`.
15. **Chain dependent async work** — `effect.then(first, fn(value) { second(value) })`.
16. **Use a user-owned BEAM process** — spawn it inside an `Effect`; resolve back into rx-gleam when it finishes. The Rx runtime remains one actor.
17. **Use a timer/socket/FFI callback** — adapt it to `Effect`; no Rx scheduler abstraction is required.
18. **Validate a raw notification trace** — `protocol.validate([Next(...), ..., Complete])` rejects post-terminal events.
19. **Model-check protocol changes** — run TLC with `formal/RxProtocol.tla` and `formal/RxProtocol.cfg` before changing terminal-state semantics.
20. **Run the full quality gate** — `zed run check` (or `sh conformance/check.sh --full`) checks format, tests, structural invariants, hooks, and TLC when `TLA2TOOLS_JAR` is configured.

## Async example

```gleam
import gleam/erlang/process
import rx
import rx/effect

fn expensive(value: Int) -> effect.Effect(Int, String) {
  effect.new(fn(resolve) {
    let pid = process.start(fn() {
      resolve(Ok(value * 2))
    }, False)

    fn() {
      process.kill(pid)
    }
  })
}
```

The library does not care that this effect uses a process. A database callback, timer, port, socket, or FFI completion can implement the same interface.

## Design rule

ReactiveX defines composition; BEAM defines concurrency. `rx-gleam` serializes observer execution but does not own the application's concurrency strategy.
