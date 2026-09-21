import rx

/// A runtime-agnostic asynchronous computation.
///
/// `start` receives a one-shot resolver and returns a cancellation function.
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

pub fn then(
  effect: Effect(a, error),
  next: fn(a) -> Effect(b, error),
) -> Effect(b, error) {
  Effect(fn(resolve) {
    run(effect, fn(result) {
      case result {
        Ok(value) -> {
          let _cancel_inner = run(next(value), resolve)
          Nil
        }
        Error(reason) -> resolve(Error(reason))
      }
    })
  })
}

pub fn to_observable(effect: Effect(value, error)) -> rx.Observable(value, error) {
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
