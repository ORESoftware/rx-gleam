# rx-gleam

Composable reactive streams for Gleam with both **lazy/cold observables** and **eager finite sequences**.

The core is intentionally runtime-agnostic and single-process by default. `rx_gleam` does not own a scheduler, start actors, or impose an async runtime. A producer can receive BEAM messages, call FFI, start a child process, block on I/O, or stay entirely synchronous; the library only asks it for the next `Step`.

## Design

- `rx.Observable(value, error)` — cold and lazy; each subscription gets a fresh source.
- `rx/eager.Eager(value, error)` — finite and eager; transformations run immediately.
- `rx.create` + `rx.source` — low-level escape hatch for custom producers.
- immutable continuations instead of hidden mutable state.
- no scheduler abstraction in the core.
- no required OTP dependency; the implementation is pure Gleam and can target Erlang or JavaScript.

## Lazy API

```gleam
import rx

pub fn example() {
  let stream: rx.Observable(Int, String) =
    rx.from_list([1, 2, 3, 4, 5])
    |> rx.map(fn(value) { value * 2 })
    |> rx.filter(fn(value) { value > 4 })
    |> rx.scan(0, fn(total, value) { total + value })
    |> rx.take(2)

  rx.to_list(stream)
  // Ok([6, 14])
}
```

Pipeline construction is lazy. `to_list`, `fold`, `drain`, and `subscribe` are terminal operations and start consumption immediately.

## Eager API

```gleam
import rx/eager

pub fn example() {
  let values: eager.Eager(Int, String) =
    eager.from_list([1, 2, 3, 4])
    |> eager.map(fn(value) { value * 10 })
    |> eager.filter(fn(value) { value >= 20 })
    |> eager.take(2)

  eager.to_result(values)
  // Ok([20, 30])
}
```

The eager API is deliberately simple: it is a composable materialized sequence with error propagation. If a caller wants laziness around eager work, wrap it with `eager.defer`.

```gleam
let lazy =
  eager.defer(fn() {
    eager.from_list(expensive_values())
    |> eager.map(transform)
  })
```

## Bridging eager and lazy

```gleam
let eager_values: eager.Eager(Int, String) = eager.from_list([1, 2, 3])

let lazy_values =
  eager_values
  |> eager.to_observable
  |> rx.map(fn(value) { value + 1 })
```

`eager.from_observable` performs the opposite conversion and subscribes immediately.

## Custom producers and async work

The low-level API is intentionally agnostic about *how* a value arrives.

```gleam
import rx

fn make_stream() -> rx.Observable(Message, StreamError) {
  rx.create(fn() {
    rx.source(
      fn() {
        // This function can receive a BEAM message, call FFI, wait for I/O,
        // or otherwise obtain the next value however the application wants.
        next_message_step()
      },
      fn() {
        cleanup_subscription()
      },
    )
  })
}
```

A producer returns one of:

```gleam
rx.Emit(value, next)
rx.Failed(error)
rx.Complete
```

`next` is another zero-argument function returning the following `Step`. That continuation is the stream state. Stateful operators such as `scan`, `take`, `skip`, and `distinct_until_changed_by` therefore remain immutable and local to the current subscriber.

If an application wants a child actor/process, it can create one inside `rx.create` and have the source read from it. If it wants everything in one actor, it can do that too. The library stays neutral.

## Initial operators

Lazy: `map`, `filter`, `tap`, `take`, `skip`, `scan`, `start_with`, `distinct_until_changed_by`, `defer`, `defer_value`, `defer_result`, `subscribe`, `to_list`, `fold`, and `drain`.

Eager: `map`, `filter`, `tap`, `take`, `skip`, `scan`, `flat_map`, `append`, `fold`, and conversion to/from lazy observables.

## Why no `async` keyword?

Gleam does not need an `async` keyword in this API. Asynchrony is a property of the producer implementation or runtime integration, not of the observable type itself. The observable stays composable whether the producer is synchronous, waits on a mailbox, delegates to a child process, or bridges to foreign async code.

## Development

```sh
gleam deps download
gleam format --check src test
gleam test
gleam test --target javascript
```

## License

MIT
