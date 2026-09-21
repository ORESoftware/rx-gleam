import gleam/list

/// Controls whether a synchronous subscription should continue consuming values.
pub type Control {
  Continue
  Stop
}

/// A single pull from an observable source.
///
/// `Emit` carries both the value and the continuation for the next pull. This
/// keeps stream state immutable and local to a single subscription.
pub type Step(value, error) {
  Emit(value, next: fn() -> Step(value, error))
  Failed(error)
  Complete
}

/// A per-subscription source.
///
/// `close` is called exactly once by the built-in terminal runners when they
/// finish normally, fail, or are stopped by an observer.
pub opaque type Source(value, error) {
  Source(next: fn() -> Step(value, error), close: fn() -> Nil)
}

/// A cold observable. Calling `open` creates a fresh source for each
/// subscription, so pipelines are lazy by default.
pub opaque type Observable(value, error) {
  Observable(open: fn() -> Source(value, error))
}

/// Observer callbacks used by `subscribe`.
pub type Observer(value, error) {
  Observer(
    on_next: fn(value) -> Control,
    on_error: fn(error) -> Nil,
    on_complete: fn() -> Nil,
  )
}

/// Construct a source from a pull function and a cleanup function.
///
/// This is the lowest-level API. A caller may block, receive a message, launch
/// a process, await an external runtime primitive through FFI, or do nothing
/// asynchronous at all inside `next`. rx_gleam does not choose a scheduler.
pub fn source(
  next: fn() -> Step(value, error),
  close: fn() -> Nil,
) -> Source(value, error) {
  Source(next:, close:)
}

/// Construct a cold observable from a source factory.
pub fn create(open: fn() -> Source(value, error)) -> Observable(value, error) {
  Observable(open:)
}

/// Construct an observer.
pub fn observer(
  on_next: fn(value) -> Control,
  on_error: fn(error) -> Nil,
  on_complete: fn() -> Nil,
) -> Observer(value, error) {
  Observer(on_next:, on_error:, on_complete:)
}

/// Emit all values from a list.
pub fn from_list(values: List(value)) -> Observable(value, error) {
  create(fn() { source(list_step(values), noop) })
}

/// Emit exactly one value.
pub fn of(value: value) -> Observable(value, error) {
  from_list([value])
}

/// Complete without emitting a value.
pub fn empty() -> Observable(value, error) {
  create(fn() { source(fn() { Complete }, noop) })
}

/// Fail immediately.
pub fn fail(error: error) -> Observable(value, error) {
  create(fn() { source(fn() { Failed(error) }, noop) })
}

/// Delay construction of an observable until subscription time.
pub fn defer(
  factory: fn() -> Observable(value, error),
) -> Observable(value, error) {
  create(fn() { open(factory()) })
}

/// Delay evaluation of a single value until subscription time.
pub fn defer_value(factory: fn() -> value) -> Observable(value, error) {
  defer(fn() { of(factory()) })
}

/// Delay evaluation of a `Result` until subscription time.
pub fn defer_result(
  factory: fn() -> Result(value, error),
) -> Observable(value, error) {
  defer(fn() {
    case factory() {
      Ok(value) -> of(value)
      Error(error) -> fail(error)
    }
  })
}

/// Lazily transform each value.
pub fn map(
  observable: Observable(a, error),
  mapper: fn(a) -> b,
) -> Observable(b, error) {
  create(fn() {
    let Source(next, close) = open(observable)
    source(map_step(next, mapper), close)
  })
}

/// Lazily retain values matching a predicate.
pub fn filter(
  observable: Observable(value, error),
  predicate: fn(value) -> Bool,
) -> Observable(value, error) {
  create(fn() {
    let Source(next, close) = open(observable)
    source(filter_step(next, predicate), close)
  })
}

/// Run a side-effect for every value while preserving the original values.
pub fn tap(
  observable: Observable(value, error),
  effect: fn(value) -> Nil,
) -> Observable(value, error) {
  map(observable, fn(value) {
    effect(value)
    value
  })
}

/// Emit at most `count` values.
pub fn take(
  observable: Observable(value, error),
  count: Int,
) -> Observable(value, error) {
  create(fn() {
    let Source(next, close) = open(observable)
    source(take_step(next, count), close)
  })
}

/// Skip the first `count` values.
pub fn skip(
  observable: Observable(value, error),
  count: Int,
) -> Observable(value, error) {
  create(fn() {
    let Source(next, close) = open(observable)
    source(skip_step(next, count), close)
  })
}

/// Emit a running accumulator after each input value.
pub fn scan(
  observable: Observable(value, error),
  initial: accumulator,
  reducer: fn(accumulator, value) -> accumulator,
) -> Observable(accumulator, error) {
  create(fn() {
    let Source(next, close) = open(observable)
    source(scan_step(next, initial, reducer), close)
  })
}

/// Prepend eager values to a lazy observable without evaluating the source.
pub fn start_with(
  observable: Observable(value, error),
  values: List(value),
) -> Observable(value, error) {
  create(fn() {
    let Source(next, close) = open(observable)
    source(prefix_step(values, next), close)
  })
}

/// Remove adjacent duplicates using a caller-provided equality function.
pub fn distinct_until_changed_by(
  observable: Observable(value, error),
  equals: fn(value, value) -> Bool,
) -> Observable(value, error) {
  create(fn() {
    let Source(next, close) = open(observable)
    source(distinct_step(next, NoneSeen, equals), close)
  })
}

/// Subscribe synchronously in the current process/actor.
///
/// No child process is started by rx_gleam. A producer is free to start one.
pub fn subscribe(
  observable: Observable(value, error),
  observer: Observer(value, error),
) -> Nil {
  let Source(next, close) = open(observable)
  subscribe_loop(next, close, observer)
}

/// Eagerly consume an observable into a list.
///
/// This is a terminal operation: subscription begins immediately.
pub fn to_list(
  observable: Observable(value, error),
) -> Result(List(value), error) {
  let Source(next, close) = open(observable)
  collect(next, close, [])
}

/// Eagerly fold an observable to one value.
pub fn fold(
  observable: Observable(value, error),
  initial: accumulator,
  reducer: fn(accumulator, value) -> accumulator,
) -> Result(accumulator, error) {
  let Source(next, close) = open(observable)
  fold_loop(next, close, initial, reducer)
}

/// Eagerly consume all values, ignoring them.
pub fn drain(observable: Observable(value, error)) -> Result(Nil, error) {
  fold(observable, Nil, fn(_, _) { Nil })
}

fn open(observable: Observable(value, error)) -> Source(value, error) {
  let Observable(open) = observable
  open()
}

fn noop() -> Nil {
  Nil
}

fn list_step(values: List(value)) -> fn() -> Step(value, error) {
  fn() {
    case values {
      [] -> Complete
      [first, ..rest] -> Emit(first, list_step(rest))
    }
  }
}

fn map_step(
  next: fn() -> Step(a, error),
  mapper: fn(a) -> b,
) -> fn() -> Step(b, error) {
  fn() {
    case next() {
      Emit(value, continuation) ->
        Emit(mapper(value), map_step(continuation, mapper))
      Failed(error) -> Failed(error)
      Complete -> Complete
    }
  }
}

fn filter_step(
  next: fn() -> Step(value, error),
  predicate: fn(value) -> Bool,
) -> fn() -> Step(value, error) {
  fn() { filter_next(next, predicate) }
}

fn filter_next(
  next: fn() -> Step(value, error),
  predicate: fn(value) -> Bool,
) -> Step(value, error) {
  case next() {
    Emit(value, continuation) -> {
      case predicate(value) {
        True -> Emit(value, filter_step(continuation, predicate))
        False -> filter_next(continuation, predicate)
      }
    }
    Failed(error) -> Failed(error)
    Complete -> Complete
  }
}

fn take_step(
  next: fn() -> Step(value, error),
  remaining: Int,
) -> fn() -> Step(value, error) {
  fn() {
    case remaining <= 0 {
      True -> Complete
      False ->
        case next() {
          Emit(value, continuation) ->
            Emit(value, take_step(continuation, remaining - 1))
          Failed(error) -> Failed(error)
          Complete -> Complete
        }
    }
  }
}

fn skip_step(
  next: fn() -> Step(value, error),
  remaining: Int,
) -> fn() -> Step(value, error) {
  fn() { skip_next(next, remaining) }
}

fn skip_next(
  next: fn() -> Step(value, error),
  remaining: Int,
) -> Step(value, error) {
  case next() {
    Emit(value, continuation) -> {
      case remaining > 0 {
        True -> skip_next(continuation, remaining - 1)
        False -> Emit(value, continuation)
      }
    }
    Failed(error) -> Failed(error)
    Complete -> Complete
  }
}

fn scan_step(
  next: fn() -> Step(value, error),
  accumulator: accumulator,
  reducer: fn(accumulator, value) -> accumulator,
) -> fn() -> Step(accumulator, error) {
  fn() {
    case next() {
      Emit(value, continuation) -> {
        let updated = reducer(accumulator, value)
        Emit(updated, scan_step(continuation, updated, reducer))
      }
      Failed(error) -> Failed(error)
      Complete -> Complete
    }
  }
}

fn prefix_step(
  values: List(value),
  next: fn() -> Step(value, error),
) -> fn() -> Step(value, error) {
  fn() {
    case values {
      [] -> next()
      [first, ..rest] -> Emit(first, prefix_step(rest, next))
    }
  }
}

type Seen(value) {
  NoneSeen
  Seen(value)
}

fn distinct_step(
  next: fn() -> Step(value, error),
  seen: Seen(value),
  equals: fn(value, value) -> Bool,
) -> fn() -> Step(value, error) {
  fn() { distinct_next(next, seen, equals) }
}

fn distinct_next(
  next: fn() -> Step(value, error),
  seen: Seen(value),
  equals: fn(value, value) -> Bool,
) -> Step(value, error) {
  case next() {
    Emit(value, continuation) -> {
      case seen {
        NoneSeen -> Emit(value, distinct_step(continuation, Seen(value), equals))
        Seen(previous) ->
          case equals(previous, value) {
            True -> distinct_next(continuation, seen, equals)
            False -> Emit(value, distinct_step(continuation, Seen(value), equals))
          }
      }
    }
    Failed(error) -> Failed(error)
    Complete -> Complete
  }
}

fn subscribe_loop(
  next: fn() -> Step(value, error),
  close: fn() -> Nil,
  subscriber: Observer(value, error),
) -> Nil {
  case next() {
    Emit(value, continuation) -> {
      case subscriber.on_next(value) {
        Continue -> subscribe_loop(continuation, close, subscriber)
        Stop -> close()
      }
    }
    Failed(error) -> {
      subscriber.on_error(error)
      close()
    }
    Complete -> {
      subscriber.on_complete()
      close()
    }
  }
}

fn collect(
  next: fn() -> Step(value, error),
  close: fn() -> Nil,
  reversed: List(value),
) -> Result(List(value), error) {
  case next() {
    Emit(value, continuation) -> collect(continuation, close, [value, ..reversed])
    Failed(error) -> {
      close()
      Error(error)
    }
    Complete -> {
      close()
      Ok(list.reverse(reversed))
    }
  }
}

fn fold_loop(
  next: fn() -> Step(value, error),
  close: fn() -> Nil,
  accumulator: accumulator,
  reducer: fn(accumulator, value) -> accumulator,
) -> Result(accumulator, error) {
  case next() {
    Emit(value, continuation) ->
      fold_loop(continuation, close, reducer(accumulator, value), reducer)
    Failed(error) -> {
      close()
      Error(error)
    }
    Complete -> {
      close()
      Ok(accumulator)
    }
  }
}
