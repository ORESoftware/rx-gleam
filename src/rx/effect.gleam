import rx

/// A runtime-agnostic asynchronous computation.
///
/// `start` receives a resolver and returns a cancellation function.
/// The effect implementation may use a BEAM process, timer, socket, FFI callback,
/// or any other mechanism. rx-gleam does not spawn work for the effect.
pub opaque type Effect(value, error) {
  Effect(start: fn(fn(Result(value, error)) -> Nil) -> fn() -> Nil)
}

pub fn new(
  start: fn(fn(Result(value, error)) -> Nil) -> fn() -> Nil,
) -> Effect(value, error) {
  Effect(start)
}

pub fn run(
  effect: Effect(value, error),
  resolve: fn(Result(value, error)) -> Nil,
) -> fn() -> Nil {
  let Effect(start) = effect
  start(resolve)
}

pub fn pure(value: value) -> Effect(value, error) {
  Effect(fn(resolve) {
    resolve(Ok(value))
    fn() { Nil }
  })
}

pub fn fail(reason: error) -> Effect(value, error) {
  Effect(fn(resolve) {
    resolve(Error(reason))
    fn() { Nil }
  })
}

pub fn map(
  effect: Effect(a, error),
  transform: fn(a) -> b,
) -> Effect(b, error) {
  Effect(fn(resolve) {
    run(effect, fn(result) {
      case result {
        Ok(value) -> resolve(Ok(transform(value)))
        Error(reason) -> resolve(Error(reason))
      }
    })
  })
}

/// Convert one effect result into an Observable.
///
/// The effect's cancellation function becomes the Observable subscription
/// teardown. Multiple or late resolver calls are still governed by the same
/// runtime protocol as every other Observable producer.
pub fn to_observable(
  effect: Effect(value, error),
) -> rx.Observable(value, error) {
  rx.create(fn(emitter) {
    run(effect, fn(result) {
      case result {
        Ok(value) -> {
          rx.next(emitter, value)
          rx.complete(emitter)
        }
        Error(reason) -> rx.error(emitter, reason)
      }
    })
  })
}
