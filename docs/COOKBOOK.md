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
9. **Cancel a subscription** — retain the returned `Subscription` and call `rx.unsubscribe(subscription)`; repeated cancellation is a no-op and teardown runs once.
10. **Wrap callback-based async work** — `effect.new(fn(resolve) { start_work(resolve) })`.
11. **Turn one async result into a stream** — `effect.to_observable(my_effect)`.
12. **Represent successful async work** — `effect.pure(value)`.
13. **Represent failed async work** — `effect.fail(reason)`.
14. **Transform an async result** — `effect.map(my_effect, transform)`; cancellation remains the source effect's cancellation.
15. **Build reusable chains** — compose ordinary stream operators with `|>`; dependent async flattening is intentionally reserved for the formally modeled `concat_map`/`merge_map` layer rather than an unsafe effect shortcut.
16. **Use a user-owned BEAM process** — spawn it inside an `Effect`; resolve back into rx-gleam when it finishes. The Rx runtime remains one actor.
17. **Use a timer/socket/FFI callback** — adapt it to `Effect`; no Rx scheduler abstraction is required.
18. **Validate a raw notification trace** — `protocol.validate([Next(...), ..., Complete])` rejects post-terminal events.
19. **Model-check protocol changes** — run TLC with `formal/RxProtocol.tla` and `formal/RxProtocol.cfg` before changing terminal-state semantics.
20. **Run the release-quality gates** — `zed validate`, `sh conformance/check.sh --full`, then `zed r2g` before publishing; TLC participates when `TLA2TOOLS_JAR` is configured.

## Async example

```gleam
import gleam/erlang/process
import rx/effect

fn expensive(value: Int) -> effect.Effect(Int, String) {
  effect.new(fn(resolve) {
    let pid = process.spawn_unlinked(fn() {
      resolve(Ok(value * 2))
    })

    fn() {
      process.kill(pid)
    }
  })
}
```

The library does not care that this effect uses a process. A database callback, timer, port, socket, or FFI completion can implement the same interface. `spawn_unlinked` here is an application choice, not something rx-gleam does internally.

## Why there is no `effect.then` yet

A dependent effect can begin only after the previous effect resolves. Correct cancellation therefore has to remember which inner effect is currently active. Rather than hide mutable state or add another library-owned process, rx-gleam will implement dependent async composition through the same actor-owned transition machine used by `concat_map`, `merge_map`, `switch_map`, and `exhaust_map`.

## Design rule

ReactiveX defines composition; BEAM defines concurrency. `rx-gleam` serializes observer execution but does not own the application's concurrency strategy.
